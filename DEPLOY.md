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
