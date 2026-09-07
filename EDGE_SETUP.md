# Edge Ingest setup (bypass Cloudflare WAF)

Railway Node → `socketv4.bc.game` is WAF-blocked. A browser on bc.game (residential IP)
forwards Crash events to the worker over HTTPS.

## 1. Railway (worker service)

```bash
EDGE_INGEST_TOKEN=<random-32-byte-hex>
# Optional; if unset but TOKEN is set, worker listens on $PORT (Railway public port)
EDGE_INGEST_PORT=8091
```

Enable **public networking** on the **worker** service (or set TOKEN and use the service’s public URL on `$PORT`).

Generate token:
```bash
node -e "console.log(require('crypto').randomBytes(32).toString('hex'))"
```

Redeploy. Logs should show:
```text
browser-edge ingest listening on http://0.0.0.0:...
```

## 2. Userscript

1. Install Tampermonkey.
2. Add `agents/browser-edge-observer.user.js` (or `scripts/bcgame-crash-edge-forwarder.user.js`).
3. In the BC.Game tab console:
```js
window.__TE_EDGE__ = {
  url: 'https://YOUR-WORKER.up.railway.app',  // public worker URL, no :8091 if using $PORT
  token: 'SAME_AS_EDGE_INGEST_TOKEN',
};
location.reload();
```
4. Open https://bc.game/game/crash — green **TE Edge ✓** pill = OK.

## 3. Verify

Railway logs:
```text
edge crash ingested (...)
```

Worker health may still show socket `waf_blocked` — that is expected; edge is the live path.

## Paths accepted

- `POST /edge/crash`, `POST /edge/bg`, `GET /edge/health`
- Aliases: `/api/crash/edge`, `/api/edge/crash`, `/api/crash/edge/bg`
