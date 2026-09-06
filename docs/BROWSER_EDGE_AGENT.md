# Browser-edge agent (optional low-latency path)

When Railway cannot open BC.Game Socket.IO (Cloudflare WAF), a **browser on a normal residential IP** can observe crash events and forward **already decoded** `{ gameId, multiplier, time }` to the worker.

This does **not** replace poll recovery. Poll remains the safety net when the tab is closed or the feed goes stale.

## Architecture

```
Browser (userscript)  --POST /edge/crash-->  Worker HTTP (EDGE_INGEST_PORT)
                                              └─ onGameEnd → N+1 predict → Telegram
Poll worker  — if last_edge_event_at is fresh, defer poll N+1
```

## Worker setup (Railway)

1. Set environment variables:

| Variable | Example | Purpose |
|----------|---------|---------|
| `EDGE_INGEST_PORT` | `8091` | Enable HTTP ingest on the worker |
| `EDGE_INGEST_TOKEN` | long random secret | Bearer auth (required) |
| `EDGE_STALE_MS` | `8000` | Poll defers N+1 while edge younger than this |
| `PG_POOL_MAX` | `6` | Recommended under edge + poll |

2. Expose port `8091` (or your chosen port) on the worker service.

3. Redeploy. Logs should show:
   `browser-edge ingest listening on http://0.0.0.0:8091`

4. Health check:
   `GET https://<worker-host>:8091/edge/health`

## Agent setup

1. Install Tampermonkey (or similar).
2. Add script from `agents/browser-edge-observer.user.js`.
3. On the BC.Game tab, before/with the script, set:

```js
window.__TE_EDGE__ = {
  url: 'https://YOUR_PUBLIC_WORKER_HOST:8091',
  token: 'SAME_AS_EDGE_INGEST_TOKEN',
};
```

4. Open crash game; console should show `[TE-EDGE] forwarder armed`.
5. On each decoded crash you should see `[TE-EDGE] crash <id> <mult> 200`.

Manual test from the browser console:

```js
window.__teEdgeCrash({
  gameId: '123456789',
  multiplier: 1.87,
  crashedAt: new Date().toISOString(),
});
```

## API

### `POST /edge/crash`

Headers: `Authorization: Bearer <EDGE_INGEST_TOKEN>`, `Content-Type: application/json`

```json
{
  "gameId": "5459538",
  "multiplier": 3.47,
  "crashedAt": "2026-09-06T15:00:00.000Z",
  "observedAt": 1725630000000,
  "source": "userscript"
}
```

### `POST /edge/bg`

Optional begin-round backfill for `target_round_started_at`.

```json
{
  "gameId": "5459539",
  "beganAt": "2026-09-06T15:00:05.000Z"
}
```

### `GET /edge/health`

Returns `{ edgeFresh, edgeAgeMs, lastEdgeGameId }`.

## Behaviour notes

- **Idempotent** by `game_id` (same as socket / poll).
- Writes `worker_state.last_edge_event_at` so poll does not double-fire N+1 while edge is live.
- If edge is quiet longer than `EDGE_STALE_MS`, poll recovery runs again.
- Userscript only parses **text/JSON** frames; binary FlatBuffers are ignored (no heuristic guessing).
- You must comply with BC.Game ToS and local law; this path is optional ops tooling.

## Failure modes

| Symptom | Check |
|---------|--------|
| 401 | Token mismatch |
| 503 | `EDGE_INGEST_TOKEN` unset |
| No `[TE-EDGE] crash` | Site uses binary frames only — use manual `__teEdgeCrash` or poll |
| Only WIN/LOSS | Edge not reaching worker; confirm port public + CORS/fetch |
