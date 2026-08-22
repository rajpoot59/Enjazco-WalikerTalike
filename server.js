// server.js — walkie-talkie push-to-talk relay for EnjazCo.
//
// This server does NOT keep its own employee/team database (see the old
// db.js and admin.html, both removed). It's a thin real-time relay:
// clients authenticate with the same Odoo per-user token the supervisor
// app already stores after a normal Odoo login, this server asks Odoo
// (via odoo_client.js -> walkie_api.py) which team/channel that employee
// is on, and groups sockets into rooms accordingly. Teams and employees are
// still managed however you already manage them in Odoo
// (x_team_analytic_account, hr.employee) — nothing new to maintain here.
//
// Also handles "calls": a ring/accept flow for private 1:1 sessions. Once a
// call is accepted, the two sides establish a LIVE WebRTC audio connection
// (see the webrtc-* events below) instead of push-to-talk -- this relay only
// carries the signaling (SDP offer/answer + ICE candidates); the actual
// audio flows directly between the two phones/browsers (or via the
// coturn-walkie TURN server when a direct path isn't possible). Team-channel
// broadcast and direct paging still use the original record-then-send
// push-to-talk clips (ptt-start/ptt-audio/ptt-end) -- only in-call audio
// changed.
//
// A call target can be reached THREE ways, tried in this order:
//   1. A live socket already connected for that employee (app open, or the
//      Odoo browser panel open) -- rings instantly over the socket.
//   2. No live socket, but a push token is registered for them (Android app
//      via register-fcm-token, or a browser via POST /api/push/register) --
//      wakes them via Firebase Cloud Messaging instead. A person can have
//      SEVERAL registered push targets at once (their phone AND their
//      desk browser); every one of them gets pushed, whichever they answer
//      from first wins and the rest get a cancel push.
//   3. Neither -- call-error, "not online right now".
//
// Push-to-talk (broadcast AND direct paging) is additionally limited to
// employees whose shift/tracking is currently running -- see
// shiftActiveByEmployee below. Calls are NEVER limited by this; they ring
// regardless of shift state.

const fs = require('fs');
const path = require('path');
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const odoo = require('./odoo_client');
const turnCredentials = require('./turn_credentials');
const push = require('./push_notifications');

const PORT = process.env.PORT || 3000;
const CALL_RING_TIMEOUT_MS = Number(process.env.CALL_RING_TIMEOUT_MS || 30_000);

// Call history: who called whom, when, for how long, and how it ended.
// Deliberately a flat JSON file rather than a real database -- call volume
// here (a few hundred employees, a handful of calls each per day) is far
// too small to need one, and this avoids adding a native-module dependency
// to a box that's already had its share of build friction. Loaded into
// memory once at startup; if this ever grows into the tens of thousands of
// entries, move it to SQLite -- not worth the complexity before then.
const CALL_HISTORY_PATH = process.env.CALL_HISTORY_PATH || path.join(__dirname, 'call_history.json');
let callHistory = [];
try {
  callHistory = JSON.parse(fs.readFileSync(CALL_HISTORY_PATH, 'utf8'));
  console.log(`[history] loaded ${callHistory.length} call record(s) from ${CALL_HISTORY_PATH}`);
} catch (err) {
  console.log(`[history] starting with empty call history (${err.code === 'ENOENT' ? 'no file yet' : err.message})`);
}
let historySaveTimer = null;
function saveCallHistorySoon() {
  clearTimeout(historySaveTimer);
  historySaveTimer = setTimeout(() => {
    fs.writeFile(CALL_HISTORY_PATH, JSON.stringify(callHistory), err => {
      if (err) console.error(`[history] failed to save: ${err.message}`);
    });
  }, 500);
}
// Records one finished call. Called from every place a call reaches a
// terminal state (endCall, the ring-timeout, and call-decline) -- never
// from the live/active path, so this only ever fires once per call.
function recordCallHistory(call, status) {
  const now = Date.now();
  const durationSec = call.answeredAt ? Math.max(0, Math.round((now - call.answeredAt) / 1000)) : 0;
  callHistory.push({
    callId: call.callId,
    callerEmployeeNumber: call.callerEmployeeNumber,
    callerName: call.callerName || '',
    calleeEmployeeNumber: call.targetEmployeeNumber,
    calleeName: call.targetName || '',
    status, // 'answered' | 'missed' | 'declined' | 'cancelled'
    invitedAt: call.invitedAt || now,
    answeredAt: call.answeredAt || null,
    endedAt: now,
    durationSec
  });
  saveCallHistorySoon();
}

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

app.get('/healthz', (req, res) => {
  res.json({ status: 'ok', odoo: odoo.ODOO_BASE_URL });
});

// Who's actually connected to the relay right now, across every team --
// used by the app's "find someone to call" directory so it only shows
// people who are actually reachable, instead of the whole company roster.
app.get('/api/online', (req, res) => {
  const online = Array.from(socketsByEmployee.keys()).map(employeeNumber => ({
    employeeNumber,
    name: employeeNames.get(employeeNumber) || ''
  }));
  res.json({ status: 'ok', online });
});

// Registers a Web Push token for the CURRENTLY LOGGED IN Odoo user (any
// browser tab -- doesn't require the walkie-talkie panel to be open, only
// that the systray widget's push-registration script has run once, see
// enjaz_walkie_systray/static/src/js/walkie_push_web.js). This is a plain
// REST call rather than a socket event on purpose: it needs to work even
// when no socket/panel is open at all, which is exactly the case this
// exists to cover.
//
// Cross-origin from the Odoo domain to this relay's own domain -- needs its
// own CORS handling since it's a real fetch(), unlike the websocket-only
// socket.io connection the panel uses (which never goes through the
// CORS-checked HTTP polling transport in this app).
const ODOO_ORIGIN = new URL(odoo.ODOO_BASE_URL).origin;
app.use('/api/push/register', (req, res, next) => {
  res.header('Access-Control-Allow-Origin', ODOO_ORIGIN);
  res.header('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});
app.post('/api/push/register', async (req, res) => {
  const { appToken, fcmToken, platform } = req.body || {};
  if (!appToken || !fcmToken) {
    return res.status(400).json({ status: 'error', message: 'appToken and fcmToken are required.' });
  }
  let resolved;
  try {
    resolved = await odoo.resolveToken(appToken);
  } catch (err) {
    return res.status(401).json({ status: 'error', message: err.message });
  }
  registerPushToken(resolved.employeeNumber, fcmToken, platform || 'web');
  console.log(`[push] registered ${platform || 'web'} token for employee ${resolved.employeeNumber} via REST`);
  res.json({ status: 'ok' });
});

// Returns ONLY the requesting employee's own call history (as caller or
// callee) -- never anyone else's, enforced here server-side rather than
// trusted to the client. `direction`/`otherEmployeeNumber`/`otherName` are
// computed relative to whoever's token this is, so both the phone app and
// the web panel can render a row and redial from it with no extra lookups.
app.use('/api/call-history', (req, res, next) => {
  res.header('Access-Control-Allow-Origin', ODOO_ORIGIN);
  res.header('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});
app.post('/api/call-history', async (req, res) => {
  const { token, limit } = req.body || {};
  if (!token) return res.status(400).json({ status: 'error', message: 'token is required.' });
  let resolved;
  try {
    resolved = await odoo.resolveToken(token);
  } catch (err) {
    return res.status(401).json({ status: 'error', message: err.message });
  }
  const me = resolved.employeeNumber;
  const max = Math.min(Number(limit) || 50, 200);

  const mine = callHistory.filter(h => h.callerEmployeeNumber === me || h.calleeEmployeeNumber === me);
  mine.sort((a, b) => b.endedAt - a.endedAt);

  const history = mine.slice(0, max).map(h => {
    const outgoing = h.callerEmployeeNumber === me;
    return {
      callId: h.callId,
      direction: outgoing ? 'outgoing' : 'incoming',
      otherEmployeeNumber: outgoing ? h.calleeEmployeeNumber : h.callerEmployeeNumber,
      otherName: outgoing ? h.calleeName : h.callerName,
      status: h.status,
      startedAt: h.invitedAt,
      answeredAt: h.answeredAt,
      endedAt: h.endedAt,
      durationSec: h.durationSec
    };
  });

  res.json({ status: 'ok', history });
});

const server = http.createServer(app);
const io = new Server(server, { maxHttpBufferSize: 5e6 }); // allow ~5MB audio blobs

// channelId -> Map(employeeNumber -> { name, socketId })
const onlineByChannel = new Map();
// employeeNumber -> Set(socketId)  (global, for direct/intercom calls)
const socketsByEmployee = new Map();
// employeeNumber -> name (only present while at least one socket is online)
// -- used purely for the /api/online directory listing above.
const employeeNames = new Map();
// employeeNumber -> Map(fcmToken -> platform), so we can still ring someone
// via push when they have NO live socket (app killed/screen off, or no
// browser tab open at all). A person can have more than one registered
// token at once (phone + desk browser) -- every one of them gets pushed;
// deliberately never cleared on disconnect, that's exactly the case this
// map exists to cover. Only removed if a send comes back as a dead-token
// error (not currently distinguished -- see the TODO in call-invite).
const pushTargetsByEmployee = new Map();
// employeeNumber -> boolean, set by the client via the 'shift-status' event
// whenever their shift/tracking starts or stops. Push-to-talk (broadcast
// AND direct paging) is limited to employees whose shift is active; calls
// are NOT affected by this at all -- call-invite below never consults this
// map. Defaults to true (nothing filtered) for anyone who has never sent a
// shift-status event -- covers desk/browser users (no shift concept at
// all) and older app builds that predate this feature, so neither loses
// push-to-talk silently just for staying quiet about it.
const shiftActiveByEmployee = new Map();
function isShiftActive(employeeNumber) {
  const v = shiftActiveByEmployee.get(String(employeeNumber));
  return v === undefined ? true : v;
}
// callId -> { callerSocketId, callerEmployeeNumber, callerName,
//             targetEmployeeNumber, targetSocketId, ringSocketIds,
//             pushedTokens, iceServers, status, timeout }
const activeCalls = new Map();
let callSeq = 1;

function registerPushToken(employeeNumber, token, platform) {
  if (!employeeNumber || !token) return;
  if (!pushTargetsByEmployee.has(employeeNumber)) pushTargetsByEmployee.set(employeeNumber, new Map());
  pushTargetsByEmployee.get(employeeNumber).set(token, platform || 'android');
}

function broadcastRoster(channelId, fullRoster) {
  const online = onlineByChannel.get(channelId) || new Map();
  const roster = fullRoster.map(r => ({
    employeeNumber: r.employeeNumber,
    name: r.name,
    online: online.has(r.employeeNumber)
  }));
  io.to(`team:${channelId}`).emit('roster', roster);
}

function emitToEmployee(employeeNumber, event, payload) {
  const key = String(employeeNumber);
  if (!isShiftActive(key)) return false;
  const set = socketsByEmployee.get(key);
  if (!set || set.size === 0) return false;
  set.forEach(socketId => io.to(socketId).emit(event, payload));
  return true;
}

// Broadcasts a push-to-talk event to everyone online in a team channel
// EXCEPT the sender, and except anyone whose shift isn't currently active.
// Deliberately goes through onlineByChannel (one socket per employee,
// already used for the roster) rather than the socket.io room directly, so
// the shift filter can be applied per-employee before emitting.
function broadcastPttToTeam(channelId, excludeSocketId, event, payload) {
  const online = onlineByChannel.get(channelId);
  if (!online) return;
  online.forEach(({ socketId }, employeeNumber) => {
    if (socketId === excludeSocketId) return;
    if (!isShiftActive(employeeNumber)) return;
    io.to(socketId).emit(event, payload);
  });
}

function cancelPushesForCall(call) {
  (call.pushedTokens || []).forEach(({ token }) => push.sendCallCancelPush(token, { callId: call.callId }));
}

function endCall(callId, { notify = true } = {}) {
  const call = activeCalls.get(callId);
  if (!call) return;
  clearTimeout(call.timeout);
  activeCalls.delete(callId);
  recordCallHistory(call, call.status === 'active' ? 'answered' : 'cancelled');
  if (!notify) return;
  const targets = call.targetSocketId ? [call.targetSocketId] : (call.ringSocketIds || []);
  targets.forEach(sid => io.to(sid).emit('call-ended', { callId }));
  io.to(call.callerSocketId).emit('call-ended', { callId });
  // If this call was (also) reachable via a background push and never got
  // accepted from that side, tell it to drop its ringing UI -- otherwise it
  // rings until its own local timeout.
  if (call.status !== 'active') cancelPushesForCall(call);
}

// The "other side" of a call, relative to whichever socket sent a signaling
// message -- used to relay WebRTC offer/answer/ICE candidates without the
// two peers ever needing to know each other's socket id directly.
function otherPartySocketId(call, socketId) {
  if (call.callerSocketId === socketId) return call.targetSocketId;
  if (call.targetSocketId === socketId) return call.callerSocketId;
  return null;
}

io.on('connection', socket => {
  let me = null; // { employeeNumber, name, channelId, roster }

  socket.on('auth', async ({ token } = {}) => {
    console.log(`[auth] attempt from socket ${socket.id}, token present: ${!!token}`);
    let resolved;
    try {
      resolved = await odoo.resolveToken(token);
    } catch (err) {
      console.error(`[auth] rejected for socket ${socket.id}: ${err.message}`);
      socket.emit('auth-error', { error: err.message });
      return;
    }

    if (!resolved.hasTeam) {
      socket.emit('auth-error', {
        error: 'You are not assigned to a team in Odoo yet — ask your admin to add you to a team (x_team_analytic_account) before using the walkie-talkie.'
      });
      return;
    }

    me = {
      employeeNumber: resolved.employeeNumber,
      name: resolved.name,
      channelId: resolved.channelId,
      roster: resolved.roster
    };

    const room = `team:${me.channelId}`;
    socket.join(room);

    if (!onlineByChannel.has(me.channelId)) onlineByChannel.set(me.channelId, new Map());
    onlineByChannel.get(me.channelId).set(me.employeeNumber, { name: me.name, socketId: socket.id });

    if (!socketsByEmployee.has(me.employeeNumber)) socketsByEmployee.set(me.employeeNumber, new Set());
    socketsByEmployee.get(me.employeeNumber).add(socket.id);
    employeeNames.set(me.employeeNumber, me.name);

    console.log(`[auth] ok: socket ${socket.id} is employee #${me.employeeNumber} (${me.name}), channel ${me.channelId}, now ${socketsByEmployee.get(me.employeeNumber).size} live socket(s) for this employee`);

    socket.emit('auth-ok', {
      channelId: me.channelId,
      channelName: resolved.channelName,
      employeeNumber: me.employeeNumber,
      name: me.name
    });
    broadcastRoster(me.channelId, me.roster);
  });

  // Sent once a client has a live FCM/Web-Push token (right after Firebase
  // init, and again whenever the token rotates) so this employee can still
  // be reached by a call-invite even while fully offline from the relay's
  // socket. `platform` is informational only -- FCM itself determines how
  // to deliver based on the token, see push_notifications.js's header
  // comment -- but it makes the logs below readable.
  socket.on('register-fcm-token', ({ token, platform } = {}) => {
    if (!me || !token) return;
    registerPushToken(me.employeeNumber, token, platform || 'android');
    console.log(`[push] registered ${platform || 'android'} token for employee ${me.employeeNumber} via socket`);
  });

  // Sent whenever this employee's shift/tracking starts or stops (and once
  // more on every reconnect, so a network blip can't leave the relay with a
  // stale value). Only affects push-to-talk routing -- see
  // shiftActiveByEmployee's comment above. Calls are unaffected.
  socket.on('shift-status', ({ active } = {}) => {
    if (!me) return;
    shiftActiveByEmployee.set(me.employeeNumber, !!active);
    console.log(`[shift] employee ${me.employeeNumber} shift is now ${active ? 'ACTIVE' : 'inactive'}`);
  });

  // `target` is an optional employee number: when present this is a direct
  // 1:1 intercom call to that person, like paging a single extension; when
  // absent it's a broadcast to the whole team channel. Unchanged by the
  // live-audio call feature below -- this is still record-then-send clips.
  // Both paths are limited to employees whose shift is currently active
  // (see emitToEmployee / broadcastPttToTeam above) -- calls are not.

  socket.on('ptt-start', ({ target } = {}) => {
    if (!me) return;
    const payload = { employeeNumber: me.employeeNumber, name: me.name, direct: !!target };
    if (target) {
      const targetKey = String(target);
      const ok = emitToEmployee(targetKey, 'peer-ptt-start', payload);
      if (!ok) {
        const error = !isShiftActive(targetKey)
          ? `Employee #${target} hasn't started their shift yet.`
          : `Employee #${target} is not online right now.`;
        socket.emit('ptt-error', { error });
      }
    } else {
      broadcastPttToTeam(me.channelId, socket.id, 'peer-ptt-start', payload);
    }
  });

  socket.on('ptt-audio', ({ mimeType, audio, target } = {}) => {
    if (!me || !audio) return;
    const payload = { employeeNumber: me.employeeNumber, name: me.name, mimeType, audio, direct: !!target };
    if (target) {
      emitToEmployee(target, 'peer-ptt-audio', payload);
    } else {
      broadcastPttToTeam(me.channelId, socket.id, 'peer-ptt-audio', payload);
    }
  });

  socket.on('ptt-end', ({ target } = {}) => {
    if (!me) return;
    const payload = { employeeNumber: me.employeeNumber, direct: !!target };
    if (target) {
      emitToEmployee(target, 'peer-ptt-end', payload);
    } else {
      broadcastPttToTeam(me.channelId, socket.id, 'peer-ptt-end', payload);
    }
  });

  // --- calls: ring -> accept/decline -> (if accepted) live WebRTC audio ---
  // Deliberately NEVER checks shift state -- calls ring regardless.

  socket.on('call-invite', ({ target } = {}) => {
    if (!me) { console.warn('[call-invite] rejected: socket not authenticated yet'); return; }
    if (!target) { console.warn(`[call-invite] rejected from employee ${me.employeeNumber}: no target given`); return; }
    const targetKey = String(target);
    const targetSet = socketsByEmployee.get(targetKey);
    const ringSocketIds = targetSet ? Array.from(targetSet) : [];
    const pushMap = pushTargetsByEmployee.get(targetKey);
    const pushedTokens = (ringSocketIds.length === 0 && pushMap)
      ? Array.from(pushMap.entries()).map(([token, platform]) => ({ token, platform }))
      : [];

    console.log(`[call-invite] employee ${me.employeeNumber} -> ${targetKey}: ${ringSocketIds.length} live socket(s), ${pushedTokens.length} push target(s) registered${pushMap ? ` (total ${pushMap.size} ever registered)` : ''}`);

    if (ringSocketIds.length === 0 && pushedTokens.length === 0) {
      socket.emit('call-error', { error: `Employee #${target} is not online right now.` });
      return;
    }

    const callId = `call_${callSeq++}_${Date.now()}`;
    const iceServers = turnCredentials.buildIceServers(callId);

    const timeout = setTimeout(() => {
      const call = activeCalls.get(callId);
      if (call && call.status === 'ringing') {
        activeCalls.delete(callId);
        recordCallHistory(call, 'missed');
        io.to(call.callerSocketId).emit('call-timeout', { callId });
        (call.ringSocketIds || []).forEach(sid => io.to(sid).emit('call-cancelled', { callId }));
        cancelPushesForCall(call);
      }
    }, CALL_RING_TIMEOUT_MS);

    const call = {
      callId,
      callerSocketId: socket.id,
      callerEmployeeNumber: me.employeeNumber,
      callerName: me.name,
      targetEmployeeNumber: targetKey,
      targetName: employeeNames.get(targetKey) || '', // best-effort; call-accept fills this in for sure
      targetSocketId: null,
      ringSocketIds,
      pushedTokens,
      iceServers,
      status: 'ringing',
      invitedAt: Date.now(),
      answeredAt: null,
      timeout
    };
    activeCalls.set(callId, call);

    // Ack back to the caller with the callId (so their "ringing" screen can
    // send an explicit call-end/cancel before anyone answers) plus the ICE
    // server list its WebRTC connection will need once accepted.
    socket.emit('call-ringing', { callId, iceServers });

    if (ringSocketIds.length > 0) {
      ringSocketIds.forEach(sid => io.to(sid).emit('incoming-call', {
        callId, fromEmployeeNumber: me.employeeNumber, fromName: me.name, iceServers
      }));
      return;
    }

    // Push-only path: wake every registered device/browser for this
    // employee at once. Whichever one answers first wins (call-accept
    // works from a fresh socket regardless of whether it was ever in
    // ringSocketIds); the rest get a cancel push once accepted.
    Promise.all(pushedTokens.map(({ token }) =>
      push.sendIncomingCallPush(token, {
        callId, fromEmployeeNumber: me.employeeNumber, fromName: me.name, iceServers
      })
    )).then(results => {
      const anySent = results.some(Boolean);
      console.log(`[call-invite] push fan-out for call ${callId}: ${results.filter(Boolean).length}/${results.length} sent successfully`);
      if (anySent) return;
      const stillRinging = activeCalls.get(callId);
      if (stillRinging && stillRinging.status === 'ringing') {
        clearTimeout(stillRinging.timeout);
        activeCalls.delete(callId);
        io.to(stillRinging.callerSocketId).emit('call-error', {
          error: `Employee #${target} could not be reached (push notification failed to send).`
        });
      }
    });
  });

  socket.on('call-accept', ({ callId } = {}) => {
    const call = activeCalls.get(callId);
    if (!call || call.status !== 'ringing') {
      console.warn(`[call-accept] socket ${socket.id} tried to accept call ${callId}, but it's ${call ? call.status : 'unknown/expired'}`);
      return;
    }
    clearTimeout(call.timeout);
    call.status = 'active';
    call.targetSocketId = socket.id;
    call.answeredAt = Date.now();

    const accepterNumber = me ? me.employeeNumber : call.targetEmployeeNumber;
    const accepterName = me ? me.name : '';
    call.targetName = accepterName || call.targetName;

    console.log(`[call-accept] call ${callId} answered by ${accepterNumber} (socket ${socket.id})`);

    io.to(call.callerSocketId).emit('call-accepted', {
      callId, employeeNumber: accepterNumber, name: accepterName, iceServers: call.iceServers
    });
    socket.emit('call-accepted', {
      callId, employeeNumber: call.callerEmployeeNumber, name: call.callerName, iceServers: call.iceServers
    });

    // tell any *other* devices/browsers that were ringing for the same
    // employee (live sockets, and any push targets) to stop, since one of
    // them just answered.
    (call.ringSocketIds || []).forEach(sid => {
      if (sid !== socket.id) io.to(sid).emit('call-cancelled', { callId });
    });
    cancelPushesForCall(call);
  });

  socket.on('call-decline', ({ callId } = {}) => {
    const call = activeCalls.get(callId);
    if (!call) return;
    clearTimeout(call.timeout);
    activeCalls.delete(callId);
    recordCallHistory(call, 'declined');
    io.to(call.callerSocketId).emit('call-declined', { callId });
    (call.ringSocketIds || []).forEach(sid => {
      if (sid !== socket.id) io.to(sid).emit('call-cancelled', { callId });
    });
    cancelPushesForCall(call);
  });

  socket.on('call-end', ({ callId } = {}) => {
    endCall(callId);
  });

  // --- WebRTC signaling relay: this server never inspects the SDP/ICE
  // payloads, it just forwards them to whichever socket is the OTHER side
  // of this specific call. Only valid once a call is 'active' (accepted).

  socket.on('webrtc-offer', ({ callId, sdp } = {}) => {
    const call = activeCalls.get(callId);
    if (!call || call.status !== 'active') return;
    const target = otherPartySocketId(call, socket.id);
    if (target) io.to(target).emit('webrtc-offer', { callId, sdp });
  });

  socket.on('webrtc-answer', ({ callId, sdp } = {}) => {
    const call = activeCalls.get(callId);
    if (!call || call.status !== 'active') return;
    const target = otherPartySocketId(call, socket.id);
    if (target) io.to(target).emit('webrtc-answer', { callId, sdp });
  });

  socket.on('webrtc-ice-candidate', ({ callId, candidate } = {}) => {
    const call = activeCalls.get(callId);
    if (!call || call.status !== 'active') return;
    const target = otherPartySocketId(call, socket.id);
    if (target) io.to(target).emit('webrtc-ice-candidate', { callId, candidate });
  });

  socket.on('disconnect', () => {
    // clean up any calls this socket was the caller or the (accepted) callee for
    for (const [callId, call] of activeCalls.entries()) {
      if (call.callerSocketId === socket.id || call.targetSocketId === socket.id) {
        endCall(callId);
      }
    }

    if (!me) return;
    const online = onlineByChannel.get(me.channelId);
    if (online) {
      online.delete(me.employeeNumber);
      broadcastRoster(me.channelId, me.roster);
    }
    const set = socketsByEmployee.get(me.employeeNumber);
    if (set) {
      set.delete(socket.id);
      if (set.size === 0) {
        socketsByEmployee.delete(me.employeeNumber);
        employeeNames.delete(me.employeeNumber);
      }
    }
  });
});

server.listen(PORT, () => {
  console.log(`Walkie-Talkie relay running at http://localhost:${PORT}`);
  console.log(`Resolving employees/teams from Odoo at ${odoo.ODOO_BASE_URL}`);
});