# Telegram Notifications — Setup Guide

The BcTracker prediction worker can push a Telegram message the moment a new
prediction is generated (giving you the bet window of the next Crash round)
and a WIN/LOSS confirmation the moment a round resolves.

This is **push-only**. There are no bot commands (`/start`, `/stop`, `/status`
are all deliberately out of scope). Telegram is a notification surface — a
Telegram outage **must not** stop the prediction worker.

## 1. Create a Bot

1. Open Telegram, search for `@BotFather`, send `/newbot`.
2. Give it a name (e.g. `BcTracker alerts`) and a unique username
   (e.g. `bc_tracker_alerts_bot`).
3. BotFather replies with an HTTP API token, e.g. `123456789:AA...`. This is
   your `TELEGRAM_BOT_TOKEN`.

## 2. Discover your Chat ID

1. Start a chat with your new bot (send it `/start`).
2. Open `https://api.telegram.org/bot<TELEGRAM_BOT_TOKEN>/getUpdates` in a browser.
3. Look at the JSON — the chat id of the `message` you just sent is under
   `result[0].message.chat.id`. That's a positive number for a 1:1 chat.
4. For a group: add the bot to the group, send `/start@your_bot`, then call
   `getUpdates` again. Group ids are negative, typically `-100…`.

## 3. Set the Environment Variables

These are **server-only**. Set them on the Railway worker (NOT on the Vercel
dashboard — the dashboard is the read-only view and never imports Telegram).

| Variable | Required? | Purpose |
|---|---|---|
| `TELEGRAM_BOT_TOKEN` | For notifications | The token from BotFather |
| `TELEGRAM_CHAT_ID` | For notifications | The numeric chat id from step 2 |
| `PREDICTION_POLL_MS` | No (default `3000`) | Override the 3-second default poll interval; do **not** go below `2000` |
| `PREDICTION_PENDING_POLL_MS` | No (default `10000`) | Coarser poll used while a prediction is pending (see § Adaptive Polling) |

Railway injects these into `process.env` at runtime. The worker reads them at
the moment of send, so changes take effect on the next message — no redeploy
required.

The variables are **never** read from `import.meta.env`, never prefixed with
`VITE_`, and never reach the browser bundle. The existing `with-app-env.mjs`
script only forwards `VITE_*` keys to the client, which is the correct
boundary.

## 4. Verify

After deploy, the worker logs:

```
worker starting telegram=enabled pollIntervalMs=3000 pendingPollIntervalMs=10000
```

If `telegram=disabled (no env)` appears, the env vars are not reaching the
worker process — check Railway's variable editor.

The dashboard's `WorkerStatus` also exposes:

- `telegramEnabled` — whether both env vars are present.
- `telegramLastSentAt` — ISO timestamp of the most recent send attempt.
- `telegramLastError` — empty on success, or Telegram's `description` /
  our internal error code (`not_configured`, `timeout_5000ms`,
  `malformed_response`, `network_error`, etc.) on failure.

## 5. Message Formats

### Prediction (fires the moment a new prediction is generated)

```
🎯 Next round prediction
Target: 1.30x
Last round: 1.24x or after 1.24 play the next round
Probability: 62%
Confidence: 0.74
Regime: momentum-cool
```

On a cold start (no rounds in the DB yet), the "Last round" line falls back
to `Last round: (no recent round in DB yet)`.

### Validation — WIN

```
✅ WIN @ 1.42x
Target: 1.30x
Predicted prob: 62%
```

### Validation — LOSS

```
❌ LOSS @ 1.18x
Target: 1.30x
Predicted prob: 62%
```

## 6. Adaptive Polling

The worker normally polls BC.Game every `PREDICTION_POLL_MS` (default
`3000ms`). While a prediction is pending — i.e. we already fired a
prediction message and are waiting for the round to land — the worker drops
to `PREDICTION_PENDING_POLL_MS` (default `10000ms`). This saves upstream
calls during the 2–5 second resolution window without affecting end-to-end
latency.

Both intervals are clamped to a minimum of `2000ms`; anything tighter hits
cached/empty responses on BC.Game's upstream API.

## 7. Failure Isolation

Every Telegram failure mode is handled inside the worker without ever
propagating to the prediction cycle:

| Failure | What happens |
|---|---|
| Missing `TELEGRAM_BOT_TOKEN` or `TELEGRAM_CHAT_ID` | Send returns `not_configured`; the worker continues normally. No logs. |
| Telegram API down | Send returns `{ ok: false, error }`; worker continues. |
| Telegram API hangs (>5s) | AbortController fires; send returns `timeout_5000ms`; worker continues. |
| Bot token revoked (401/403) | `description` captured; worker continues; dashboard shows `telegram_last_error`. |
| Wrong chat id (400 `chat not found`) | Same as above. |
| Network failure | Send returns `network_error`; worker continues. |
| Worker restart mid-send | Lost message. The prediction is still in `pending_predictions` and will resolve in the dashboard. |

The prediction engine, DB writes, and lock release are **never** conditioned on
Telegram delivery succeeding.

## 8. What Telegram is NOT

- **Not a betting surface.** Telegram never places bets, authenticates into
  BC.Game, or alters the prediction decision.
- **Not a record of truth.** Every prediction is persisted in
  `pending_predictions`, every WIN/LOSS in `prediction_validations`. Telegram
  is push-only.
- **Not a retry queue.** A failed send is logged and dropped. Telegram's
  median RTT is ~300ms; a missed message is harmless.