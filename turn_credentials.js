// turn_credentials.js — generates short-lived TURN credentials for the
// walkie-talkie project's OWN coturn instance (coturn-walkie.service /
// /etc/turnserver-walkie.conf on the server) -- completely separate from
// the Jitsi Meet coturn instance also running on this box. The app never
// sees TURN_SECRET itself, only these short-lived, per-call credentials
// (coturn's "REST API" time-limited credential scheme: username is
// "<expiryUnixSeconds>:<label>", password is base64(HMAC-SHA1(secret, username))).

const crypto = require('crypto');

const TURN_SECRET = process.env.TURN_SECRET || '';
const TURN_HOST = process.env.TURN_HOST || 'phone.enjaz-co.com';
const TURN_PORT = process.env.TURN_PORT || '33478';
const TURNS_PORT = process.env.TURNS_PORT || '33578';
const TURN_TTL_SECONDS = Number(process.env.TURN_TTL_SECONDS || 3600); // 1 hour -- plenty for one call

function buildIceServers(label) {
  if (!TURN_SECRET) {
    // Not configured yet -- still return a public STUN server so two
    // devices on the same network (or with easy NATs) keep working, just
    // without a TURN relay fallback for harder networks.
    console.warn('[turn_credentials] TURN_SECRET not set -- calls will only work without TURN relay fallback.');
    return [{ urls: 'stun:stun.l.google.com:19302' }];
  }

  const expiry = Math.floor(Date.now() / 1000) + TURN_TTL_SECONDS;
  const username = `${expiry}:${label}`;
  const password = crypto.createHmac('sha1', TURN_SECRET).update(username).digest('base64');

  return [
    { urls: 'stun:stun.l.google.com:19302' },
    { urls: `stun:${TURN_HOST}:${TURN_PORT}` },
    { urls: `turn:${TURN_HOST}:${TURN_PORT}?transport=udp`, username, credential: password },
    { urls: `turn:${TURN_HOST}:${TURN_PORT}?transport=tcp`, username, credential: password },
    { urls: `turns:${TURN_HOST}:${TURNS_PORT}?transport=tcp`, username, credential: password }
  ];
}

module.exports = { buildIceServers };