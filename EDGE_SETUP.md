# Edge Ingest (no browser console)

Bypasses Cloudflare WAF: browser on bc.game forwards Crash events to your Railway worker.

## A. Railway (worker service)

1. Variables:
   ```
   EDGE_INGEST_TOKEN=<long-random-secret>
   ```
2. Enable **public networking** on the **worker** service.
3. Redeploy. Log line:
   ```
   browser-edge ingest listening on http://0.0.0.0:...
   ```
4. Copy the public URL, e.g. `https://something.up.railway.app`

Generate token:
```bash
node -e "console.log(require('crypto').randomBytes(32).toString('hex'))"
```

## B. Tampermonkey only (no console)

1. Install [Tampermonkey](https://www.tampermonkey.net/).
2. Dashboard → **Create a new script**.
3. Paste contents of `agents/browser-edge-observer.user.js`.
4. At the top of the script, change **only these two lines**:
   ```js
   const WORKER_URL = 'https://testingengine-production.up.railway.app';
   const AUTH_TOKEN = 'PASTE_EDGE_INGEST_TOKEN_HERE';
   ```
   to your real worker URL and the same token as `EDGE_INGEST_TOKEN`.
5. **File → Save** (Ctrl+S).
6. Open **https://bc.game/game/crash** (logged in).
7. Look at the **top-right pill**:
   - **TE Edge ✓** (green) = forwarding works
   - **TE Edge ✗** (red) = URL/token/network problem
   - Orange tip = you still have placeholder URL/token in the script

## C. Confirm on Railway

Logs should show:
```
edge crash ingested (...)
```
or frame decode activity after each crash.

Socket may still show `waf_blocked` — that is expected; edge is the live path.
