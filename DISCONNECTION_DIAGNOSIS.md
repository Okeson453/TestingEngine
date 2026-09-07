# Prediction engine disconnection — applied fixes (2026-09-07)

## Applied

1. **Crash-specific lastEd** — do not stamp `lastEdAtByGame("crash")` on every `ed`; only after successful normalize. Prevents poll healthy-defer when non-Crash or empty events refresh the timestamp.
2. **Always `join("crash")`** — in addition to bare `join` on `/g/cm`.
3. **Lock TTL 30s → 10s** — faster recovery after worker crash.
4. **Schema validation** — one `information_schema` query instead of 9 sequential RTTs.
5. **PG_POOL_MAX default 3** in worker.mjs (align with Neon `max_client_conn`).

## Not applied

- Raising pool to 12 — conflicts with Neon `max_client_conn`.
- Changing Socket.IO URL/namespace construction — `io(url + nsp)` is valid socket.io-client usage; production issue was WAF + false defer, not path parsing alone.

## Ops

- `PG_POOL_MAX=3`, pooled Neon URL, single worker replica
- Edge agent / `BCGAME_SOCKET_P`+`T` when WAF blocks Node egress
