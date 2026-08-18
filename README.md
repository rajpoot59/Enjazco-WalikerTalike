# EnjazCo Walkie-Talkie relay

A real-time push-to-talk relay used by the ENJAZ-CO supervisor app's Walkie-Talkie tab.

This server keeps **no employee or team data of its own**. Teams and employees already live in
your Odoo instance (`erp.enjaz-co.com`) — an employee's team is whichever `x_team_analytic_account`
record they're linked to via `x_supervisor_id` / `x_managers_ids` / `x_employee_ids`. This relay
just asks Odoo "who is this login token, and what team are they on" (via the new
`/api/walkie/roster` endpoint — see `../odoo_walkie_module/`) and groups connected phones into
rooms accordingly.

## How it works

1. An employee is already logged into the ENJAZ-CO app with their normal Odoo username/password
   (existing `auth_api.py` flow) — that gives them a persistent `x_app_token`.
2. The Walkie-Talkie tab connects to this relay over Socket.io and sends that same token.
3. This relay calls Odoo's `/api/walkie/roster` with the token, gets back the employee's identity
   and channel (team), and joins their socket to that channel's room.
4. Hold-to-talk records a short audio message and broadcasts it to everyone else in the room —
   or, if a specific employee number is entered, sends it only to that person (direct/intercom
   call), regardless of which team they're on.

No admin panel is needed here — team membership is managed in Odoo exactly as it already is
today (`x_team_analytic_account`, `hr.employee`).

## Project structure

```
server.js         Socket.io relay: auth via Odoo token, room-based broadcast, direct calls
odoo_client.js     Calls Odoo's /api/walkie/roster, with a short cache to avoid hammering Odoo
public/index.html  A one-line status page (this server has no employee-facing UI)
```

The actual Odoo-side endpoint (`walkie_api.py`) and install steps are in the separate
`odoo_walkie_module` folder I sent alongside this.

## Running it

Requires Node.js 18+ (uses the built-in `fetch`).

```bash
npm install
ODOO_BASE_URL=https://erp.enjaz-co.com npm start
```

`ODOO_BASE_URL` defaults to `https://erp.enjaz-co.com` if not set — override it if you ever point
this at a staging Odoo instead.

Check `http://localhost:3000/healthz` — it should report `{"status":"ok","odoo":"..."}`.

## Deploying

See `DEPLOY.md` — it's written for your own server (e.g. `mdm.enjaz-co.com`), with a managed
Railway/Render alternative if you'd rather not manage a Linux box yourself. The only thing that
changed from the version in that doc: there's no `data.json`/persistent volume to worry about
anymore, since there's no local database — just set `ODOO_BASE_URL` as an environment variable
wherever you deploy this.

## Security notes

- This relay trusts whatever Odoo says about a token — if Odoo says a token is valid and names an
  employee/team, this relay acts on that. Keep `/api/walkie/roster` behind the same protections
  (HTTPS, network access controls) as your other Odoo API endpoints.
- Audio messages are relayed in-memory only; nothing is recorded or stored server-side.
- As before, run this behind HTTPS in production — mobile OSes require it for microphone access
  outside of local development.

## Possible next steps

- **Supervisors/managers monitoring multiple teams at once** — `walkie_api.py` already reports
  `is_supervisor`/`is_manager`, so a supervisor over several teams could be given a channel
  switcher instead of a single fixed channel.
- **True live-streaming audio** instead of record-then-send, via WebRTC.
- **Push notifications** for direct calls when the app isn't in the foreground.
- **Call history/logging** for accountability, if needed.
