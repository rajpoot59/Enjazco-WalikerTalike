// odoo_client.js — talks to the Odoo controller added in walkie_api.py to
// resolve "who is this token, and what team/channel are they on".
//
// No employee/team data is stored locally anymore (see the old db.js,
// removed) — Odoo (x_team_analytic_account / hr.employee) is the single
// source of truth, exactly like the rest of the ENJAZ-CO supervisor app.

const ODOO_BASE_URL = (process.env.ODOO_BASE_URL || 'https://erp.enjaz-co.com').replace(/\/+$/, '');
const ROSTER_CACHE_MS = Number(process.env.ROSTER_CACHE_MS || 30_000);

// token -> { data, fetchedAt }
const cache = new Map();

/**
 * Resolves an Odoo x_app_token to { employee, hasTeam, channelId,
 * channelName, roster }, or throws with a human-readable message.
 *
 * This calls /api/walkie/roster the same way the existing Flutter app
 * calls /api/auth/check: flat JSON body, flat JSON response (see
 * walkie_api.py's comment block for why). If your Odoo actually wraps the
 * response as {"jsonrpc":"2.0","result":{...}}, only the `body.result ||
 * body` line below needs to change.
 */
async function resolveToken(token, { forceRefresh = false } = {}) {
  if (!token) throw new Error('Missing token.');

  const cached = cache.get(token);
  if (!forceRefresh && cached && Date.now() - cached.fetchedAt < ROSTER_CACHE_MS) {
    return cached.data;
  }

  // Short, privacy-safe fingerprint for log lines below -- enough to tell
  // "same token every time" from "a different token each attempt" without
  // printing the whole secret into pm2 logs.
  const fp = token.length > 8 ? `${token.slice(0, 4)}...${token.slice(-4)} (len ${token.length})` : `len ${token.length}`;

  let res;
  try {
    res = await fetch(`${ODOO_BASE_URL}/api/walkie/roster`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      // Odoo's type='json' controllers read the request body as JSON-RPC:
      // they pull kwargs from a top-level "params" key specifically
      // (odoo/http.py: self.params = jsonrequest.get('params', {})). A flat
      // {"token": "..."} body has no "params" key, so Odoo sees an EMPTY
      // params dict, the controller's kw.get('token') comes back None, and
      // walkie_api.py correctly (if confusingly) reports "invalid token" --
      // the token was never actually wrong, it just never arrived.
      body: JSON.stringify({ jsonrpc: '2.0', method: 'call', params: { token } })
    });
  } catch (err) {
    console.error(`[odoo_client] fetch to ${ODOO_BASE_URL}/api/walkie/roster failed for token ${fp}:`, err.message);
    throw new Error(`Could not reach Odoo at ${ODOO_BASE_URL}: ${err.message}`);
  }

  let rawText;
  let body;
  try {
    rawText = await res.text();
    body = JSON.parse(rawText);
  } catch (err) {
    console.error(`[odoo_client] non-JSON response for token ${fp}: HTTP ${res.status}, body: ${(rawText || '').slice(0, 300)}`);
    throw new Error('Odoo returned a non-JSON response.');
  }

  // Unwrap a JSON-RPC envelope if present; otherwise use the body as-is.
  const data = body && body.result ? body.result : body;

  if (!data || data.status !== 'ok') {
    console.error(`[odoo_client] roster rejected token ${fp}: HTTP ${res.status}, message="${(data && data.message) || 'none'}", raw=${JSON.stringify(body).slice(0, 300)}`);
    throw new Error((data && data.message) || 'Odoo rejected this token.');
  }

  console.log(`[odoo_client] resolved token ${fp} -> employee ${data.me?.employee_no || data.me?.employee_id}, hasTeam=${!!data.has_team}`);

  const result = {
    employeeId: data.me?.employee_id,
    employeeNumber: String(data.me?.employee_no || data.me?.employee_id || ''),
    name: data.me?.name || 'Unknown',
    hasTeam: !!data.has_team,
    channelId: data.channel_id ?? null,
    channelName: data.channel_name || null,
    isSupervisor: !!data.is_supervisor,
    isManager: !!data.is_manager,
    roster: (data.roster || []).map(r => ({
      employeeId: r.employee_id,
      employeeNumber: String(r.employee_no || r.employee_id || ''),
      name: r.name
    }))
  };

  cache.set(token, { data: result, fetchedAt: Date.now() });
  return result;
}

function invalidate(token) {
  cache.delete(token);
}

module.exports = { resolveToken, invalidate, ODOO_BASE_URL };