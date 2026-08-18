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

  let res;
  try {
    res = await fetch(`${ODOO_BASE_URL}/api/walkie/roster`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ token })
    });
  } catch (err) {
    throw new Error(`Could not reach Odoo at ${ODOO_BASE_URL}: ${err.message}`);
  }

  let body;
  try {
    body = await res.json();
  } catch (err) {
    throw new Error('Odoo returned a non-JSON response.');
  }

  // Unwrap a JSON-RPC envelope if present; otherwise use the body as-is.
  const data = body && body.result ? body.result : body;

  if (!data || data.status !== 'ok') {
    throw new Error((data && data.message) || 'Odoo rejected this token.');
  }

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
