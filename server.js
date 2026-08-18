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
// audio flows directly between the two phones (or via the coturn-walkie TURN
// server when a direct path isn't possible). Team-channel broadcast and
// direct paging still use the original record-then-send push-to-talk clips
// (ptt-start/ptt-audio/ptt-end) -- only in-call audio changed.

const path = require('path');
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const odoo = require('./odoo_client');
const turnCredentials = require('./turn_credentials');
const push = require('./push_notifications');

const PORT = process.env.PORT || 3000;
const CALL_RING_TIMEOUT_MS = Number(process.env.CALL_RING_TIMEOUT_MS || 30_000);

const app = express();
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

const server = http.createServer(app);
const io = new Server(server, { maxHttpBufferSize: 5e6 }); // allow ~5MB audio blobs

// channelId -> Map(employeeNumber -> { name, socketId })
const onlineByChannel = new Map();
// employeeNumber -> Set(socketId)  (global, for direct/intercom calls)
const socketsByEmployee = new Map();
// employeeNumber -> name (only present while at least one socket is online)
// -- used purely for the /api/online directory listing above.
const employeeNames = new Map();
// employeeNumber -> latest FCM registration token, so we can still ring a
// phone via push when it has NO live socket (app killed / screen off).
// Deliberately never cleared on disconnect -- that's exactly the case this
// map exists to cover. Only overwritten when a fresher token registers.
const fcmTokensByEmployee = new Map();
// callId -> { callerSocketId, callerEmployeeNumber, callerName,
//             targetEmployeeNumber, targetSocketId, ringSocketIds, status, timeout }
const activeCalls = new Map();
let callSeq = 1;

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
  const set = socketsByEmployee.get(String(employeeNumber));
  if (!set || set.size === 0) return false;
  set.forEach(socketId => io.to(socketId).emit(event, payload));
  return true;
}

function endCall(callId, { notify = true } = {}) {
  const call = activeCalls.get(callId);
  if (!call) return;
  clearTimeout(call.timeout);
  activeCalls.delete(callId);
  if (!notify) return;
  const targets = call.targetSocketId ? [call.targetSocketId] : (call.ringSocketIds || []);
  targets.forEach(sid => io.to(sid).emit('call-ended', { callId }));
  io.to(call.callerSocketId).emit('call-ended', { callId });
  // If this call was only reachable via a background push (no live socket
  // when invited) and never got accepted, tell that device to drop its
  // full-screen ringing UI -- otherwise it rings until its own timeout.
  if (call.pushedToken && call.status !== 'active') {
    push.sendCallCancelPush(call.pushedToken, { callId });
  }
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

    socket.emit('auth-ok', {
      channelId: me.channelId,
      channelName: resolved.channelName,
      employeeNumber: me.employeeNumber,
      name: me.name
    });
    broadcastRoster(me.channelId, me.roster);
  });

  // Sent once the app has a live FCM token (right after Firebase init, and
  // again whenever the token rotates) so this employee can still be reached
  // by a call-invite even while fully offline from the relay's socket.
  socket.on('register-fcm-token', ({ token } = {}) => {
    if (!me || !token) return;
    fcmTokensByEmployee.set(me.employeeNumber, token);
  });

  // `target` is an optional employee number: when present this is a direct
  // 1:1 intercom call to that person, like paging a single extension; when
  // absent it's a broadcast to the whole team channel. Unchanged by the
  // live-audio call feature below -- this is still record-then-send clips.

  socket.on('ptt-start', ({ target } = {}) => {
    if (!me) return;
    const payload = { employeeNumber: me.employeeNumber, name: me.name, direct: !!target };
    if (target) {
      const ok = emitToEmployee(target, 'peer-ptt-start', payload);
      if (!ok) socket.emit('ptt-error', { error: `Employee #${target} is not online right now.` });
    } else {
      socket.to(`team:${me.channelId}`).emit('peer-ptt-start', payload);
    }
  });

  socket.on('ptt-audio', ({ mimeType, audio, target } = {}) => {
    if (!me || !audio) return;
    const payload = { employeeNumber: me.employeeNumber, name: me.name, mimeType, audio, direct: !!target };
    if (target) {
      emitToEmployee(target, 'peer-ptt-audio', payload);
    } else {
      socket.to(`team:${me.channelId}`).emit('peer-ptt-audio', payload);
    }
  });

  socket.on('ptt-end', ({ target } = {}) => {
    if (!me) return;
    const payload = { employeeNumber: me.employeeNumber, direct: !!target };
    if (target) {
      emitToEmployee(target, 'peer-ptt-end', payload);
    } else {
      socket.to(`team:${me.channelId}`).emit('peer-ptt-end', payload);
    }
  });

  // --- calls: ring -> accept/decline -> (if accepted) live WebRTC audio ---

  socket.on('call-invite', ({ target } = {}) => {
    if (!me || !target) return;
    const targetKey = String(target);
    const targetSet = socketsByEmployee.get(targetKey);
    const ringSocketIds = targetSet ? Array.from(targetSet) : [];
    // No live socket (app backgrounded/killed/screen off)? Fall back to
    // waking it via FCM instead of failing outright, as long as it has
    // registered a push token at some point.
    const fcmToken = ringSocketIds.length === 0 ? fcmTokensByEmployee.get(targetKey) : null;

    if (ringSocketIds.length === 0 && !fcmToken) {
      socket.emit('call-error', { error: `Employee #${target} is not online right now.` });
      return;
    }

    const callId = `call_${callSeq++}_${Date.now()}`;
    const iceServers = turnCredentials.buildIceServers(callId);

    const timeout = setTimeout(() => {
      const call = activeCalls.get(callId);
      if (call && call.status === 'ringing') {
        activeCalls.delete(callId);
        io.to(call.callerSocketId).emit('call-timeout', { callId });
        (call.ringSocketIds || []).forEach(sid => io.to(sid).emit('call-cancelled', { callId }));
        if (call.pushedToken) push.sendCallCancelPush(call.pushedToken, { callId });
      }
    }, CALL_RING_TIMEOUT_MS);

    activeCalls.set(callId, {
      callId,
      callerSocketId: socket.id,
      callerEmployeeNumber: me.employeeNumber,
      callerName: me.name,
      targetEmployeeNumber: targetKey,
      targetSocketId: null,
      ringSocketIds,
      pushedToken: fcmToken,
      status: 'ringing',
      timeout
    });

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

    // Push-only path: the callee's app has to reconnect+auth+call-accept on
    // its own once the notification is tapped/answered, same as it would
    // from the normal in-app ring -- this relay doesn't do anything special
    // for it beyond getting the notification there.
    push.sendIncomingCallPush(fcmToken, {
      callId, fromEmployeeNumber: me.employeeNumber, fromName: me.name, iceServers
    }).then(sent => {
      if (sent) return;
      const call = activeCalls.get(callId);
      if (call && call.status === 'ringing') {
        clearTimeout(call.timeout);
        activeCalls.delete(callId);
        io.to(call.callerSocketId).emit('call-error', {
          error: `Employee #${target} could not be reached (push notification failed).`
        });
      }
    });
  });

  socket.on('call-accept', ({ callId } = {}) => {
    const call = activeCalls.get(callId);
    if (!call || call.status !== 'ringing') return;
    clearTimeout(call.timeout);
    call.status = 'active';
    call.targetSocketId = socket.id;

    const accepterNumber = me ? me.employeeNumber : call.targetEmployeeNumber;
    const accepterName = me ? me.name : '';

    io.to(call.callerSocketId).emit('call-accepted', {
      callId, employeeNumber: accepterNumber, name: accepterName
    });
    socket.emit('call-accepted', {
      callId, employeeNumber: call.callerEmployeeNumber, name: call.callerName
    });

    // tell any *other* devices that were ringing for the same employee to
    // stop ringing, since one of them just answered
    (call.ringSocketIds || []).forEach(sid => {
      if (sid !== socket.id) io.to(sid).emit('call-cancelled', { callId });
    });
  });

  socket.on('call-decline', ({ callId } = {}) => {
    const call = activeCalls.get(callId);
    if (!call) return;
    clearTimeout(call.timeout);
    activeCalls.delete(callId);
    io.to(call.callerSocketId).emit('call-declined', { callId });
    (call.ringSocketIds || []).forEach(sid => {
      if (sid !== socket.id) io.to(sid).emit('call-cancelled', { callId });
    });
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