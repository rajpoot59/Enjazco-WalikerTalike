# Deploying so the whole company can use it

This relay needs to run somewhere that stays on 24/7 and is reachable by every employee's phone,
over HTTPS (mobile OSes block microphone access without it). It has no database of its own to
persist — the only required configuration is `ODOO_BASE_URL`, pointing at your Odoo instance
(defaults to `https://erp.enjaz-co.com`). If you have your own server available, that's usually
the simplest choice — see "Deploy to your own server" below. If not, a managed platform like
Railway or Render is the easiest alternative (further down this file).

## Deploy to your own server (e.g. mdm.enjaz-co.com)

Assumes a Linux server you can SSH into, with a domain (or subdomain) already pointed at its IP
address — e.g. `mdm.enjaz-co.com`.

1. **Copy the code to the server.** Either `git clone` your repo there, or `scp -r` the unzipped
   `walkietalkie` folder over.
2. **Install Node.js** on the server if it isn't already there (v18+, needed for the built-in
   `fetch` this relay uses to call Odoo). On Ubuntu/Debian:
   ```bash
   curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
   sudo apt-get install -y nodejs
   ```
3. **Install dependencies and keep it running** with a process manager so it survives reboots
   and restarts itself if it crashes:
   ```bash
   cd walkietalkie
   npm install --omit=dev
   sudo npm install -g pm2
   ODOO_BASE_URL=https://erp.enjaz-co.com pm2 start server.js --name walkietalkie
   pm2 save
   pm2 startup   # follow the one printed command to enable on-boot startup
   ```
4. **Put a reverse proxy in front of it for free automatic HTTPS.** The simplest option is
   [Caddy](https://caddyserver.com/docs/install):
   ```bash
   sudo apt-get install -y caddy   # or see caddyserver.com for your distro
   ```
   Then edit `/etc/caddy/Caddyfile` to just:
   ```
   mdm.enjaz-co.com {
       reverse_proxy localhost:3000
   }
   ```
   and reload it: `sudo systemctl reload caddy`. Caddy automatically gets and renews a Let's
   Encrypt certificate for that domain — no manual certificate setup needed.
5. Check `https://mdm.enjaz-co.com/healthz` — it should return
   `{"status":"ok","odoo":"https://erp.enjaz-co.com"}`.
6. Point the Walkie-Talkie tab's relay URL (baked into the Flutter app, see the Flutter
   integration notes) at `https://mdm.enjaz-co.com`.

If port 3000 needs to stay closed to the outside world (recommended — only Caddy on 443/80
should be publicly reachable), make sure your firewall only opens 80/443 and leaves 3000 for
localhost-only traffic, which the reverse-proxy setup above already assumes.

Make sure `mdm.enjaz-co.com` (or wherever this relay runs) can reach `erp.enjaz-co.com` over
HTTPS — that's the one network path this relay actually depends on at runtime.

## Managed alternative: Railway or Render

If you'd rather not manage a Linux server yourself, platforms like **Railway** or **Render**
build the app, give you an HTTPS URL automatically, and keep it running for you. Double-check
current pricing/plan details directly on railway.app or render.com before signing up — those
change over time and I can't verify today's numbers for you.

1. Push this folder to a GitHub repo (`git init && git add . && git commit -m "..." && git push`).
2. On Railway: "New Project" → "Deploy from GitHub repo" → pick it. It runs `npm install` then
   `npm start` automatically.
3. Add one environment variable: `ODOO_BASE_URL=https://erp.enjaz-co.com`.
4. Railway gives you a public HTTPS URL automatically (e.g.
   `https://enjazco-walkie-talkie-production.up.railway.app`) — check `/healthz` on it.
5. Render: same idea — "New Web Service" from the repo, build command `npm install`, start
   command `npm start`, same `ODOO_BASE_URL` environment variable.

## Share it with employees

Nothing to configure per-employee here — as soon as someone logs into the ENJAZ-CO app with
their normal Odoo account and opens the Walkie-Talkie tab, this relay resolves their team from
Odoo automatically. There's no separate account or password for this relay itself.
