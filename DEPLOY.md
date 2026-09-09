# Deploy notes

## Railway worker

Must deploy from **latest `main`**.

If deploy logs show:

```text
PLACEHOLDER_WILL_REPLACE
ReferenceError: PLACEHOLDER_WILL_REPLACE is not defined
```

the service is still on an intermediate bad commit. Redeploy from tip of `main`
(commit containing full `src/lib/prediction/worker.ts` with `export function startWorker`).

Expected healthy logs after boot:

```text
[worker] starting telegram=...
[worker] background prediction worker running (DATABASE_URL)
[worker] cycle ok=true fetched=... inserted=...
```

## Vercel dashboard

`npm run build` runs `vite build` then `db:migrate`. Without `DATABASE_URL` the
migrator exits 0 (PGLite path is not used on Vercel production — set
`DATABASE_URL` to the same Postgres as Railway).


## Socket path health (Path A vs Path B)

Monitor worker_state keys:
- socket_status — connected | waf_blocked | degraded | ...
- socket_waf_blocked — 1 when Path B will dominate
- effective_skip_below_ms — must stay <= 200

Alert when socket_status != connected. Feed BCGAME_SOCKET_P / BCGAME_SOCKET_T via browser edge agent when WAF blocks Node egress.


## Neon / max_client_conn (Worker Offline)

If the dashboard shows no more connections allowed (max_client_conn):

1. Use Neon pooled connection string (-pooler host), not the direct one.
2. Size the single worker pool for concurrent validation/prediction/outbox work:
   PG_POOL_MAX=8
   PG_POOL_MIN=1
   Use 3 only when the database plan has a hard connection cap; it will saturate under normal live traffic.
   PG_POOL_IDLE_MS=15000
   AUTH_PG_POOL_MAX=1
3. Run one worker replica only (each replica opens its own pool).
4. Restart the worker service after changing env so old connections release.
