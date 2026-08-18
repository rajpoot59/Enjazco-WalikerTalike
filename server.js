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

const path = require('path');
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const odoo = require('./odoo_client');

const PORT = process.env.PORT || 3000;

const app = express();
app.use(express.static(path.join(__dirname, 'public')));

app.get('/healthz', (req, res) => {
  res.json({ status: 'ok', odoo: odoo.ODOO_BASE_URL });
});

const server = http.createServer(app);
const io = new Server(server, { maxHttpBufferSize: 5e6 }); // allow ~5MB audio blobs

// channelId -> Map(employeeNumber -> { name, socketId })
const onlineByChannel = new Map();
// employeeNumber -> Set(socketId)  (global, for direct/intercom calls)
const socketsByEmployee = new Map();

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

io.on('connection', socket => {
  let me = null; // { employeeNumber, name, channelId, roster }

  socket.on('auth', async ({ token } = {}) => {
    let resolved;
    try {
      resolved = await odoo.resolveToken(token);
    } catch (err) {
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

    socket.emit('auth-ok', {
      channelId: me.channelId,
      channelName: resolved.channelName,
      employeeNumber: me.employeeNumber,
      name: me.name
    });
    broadcastRoster(me.channelId, me.roster);
  });

  // `target` is an optional employee number: when present this is a direct
  // 1:1 intercom call to that person, like paging a single extension; when
  // absent it's a broadcast to the whole team channel.

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

  socket.on('disconnect', () => {
    if (!me) return;
    const online = onlineByChannel.get(me.channelId);
    if (online) {
      online.delete(me.employeeNumber);
      broadcastRoster(me.channelId, me.roster);
    }
    const set = socketsByEmployee.get(me.employeeNumber);
    if (set) {
      set.delete(socket.id);
      if (set.size === 0) socketsByEmployee.delete(me.employeeNumber);
    }
  });
});

server.listen(PORT, () => {
  console.log(`Walkie-Talkie relay running at http://localhost:${PORT}`);
  console.log(`Resolving employees/teams from Odoo at ${odoo.ODOO_BASE_URL}`);
});
