# BC.Game Crash Transport — Reverse-Engineering Report

**Date:** 2026-09-07
**Method:** Live CDP (Chrome DevTools Protocol) network capture + JavaScript bundle analysis
**Capture artifacts:** `/workspace/bc-trace/` (5694 CDP events, 4078 WebSocket frames, 85 script bodies)
**Reproduction client:** `src/lib/crash/transport/bcgame-crash-transport.ts`

---

## 1. Transport Bundle Location & Loading Chain

### 1.1 Bundle Discovery (CONFIRMED)

The Crash game's transport stack lives entirely in the **Vite entry chunk** (`index-Br_P0QS-.js`, 641KB), not in the Crash game module itself. The loading chain is:

```
Page load → index-Br_P0QS-.js (entry chunk)
  ↓ imports Enter-bVWsTGyR.js (route map)
  ↓ route /game/crash → GameLayout (shared)
  ↓ Module Federation: remoteEntry.js → manifest-kUovHkMN.js
  ↓ Crash module chunks:
      080-config-ByhseCil.js  (protobuf schemas, 742KB)
      117-index-BoqBDWT7.js   (Crash game logic, 264KB)
      035-services-19vQO_vy.js (service proxies, 1.5KB)
```

The Crash module (`117-index-BoqBDWT7.js`) consumes the socket from the entry chunk via the `Le()` accessor (re-exported from `035-services-19vQO_vy.js` as `c` → `getSocket`). It does **not** implement transport itself.

### 1.2 Socket Manager Singleton (CONFIRMED from source)

```
wn(t)  →  singleton D8({uris:[{socket:""}]})
         ↓ if t provided: .socket(t)  (returns Socket.IO Socket for namespace t)
         ↓ if t omitted: returns the D8 manager itself
```

`D8` is a custom class extending `socket.io-client`'s `Manager`:

```js
// Static options (CONFIRMED from source):
static socketOptions = {
  timeout: 20000,
  reconnectionDelayMax: 10000,
  transports: ["websocket"],
  autoConnect: false,
  parser: T8,          // ← custom binary parser
  query: {}
};
```

### 1.3 Host Resolution (CONFIRMED from source)

```js
function M8() {
  const { protocol, hostname } = location;
  if (protocol === "http:" && ["dev","localhost","192"].find(n => hostname.startsWith(n)))
    return [{ socket: `${origin}`, test: `${origin}/socketapi` }];
  else {
    const apex = hostname.match(/[^.]+\.\w+$/);  // e.g. "bc.game"
    return [{ socket: `${protocol}//socketv4.${apex}` }];
  }
}
```

**Production endpoint:** `wss://socketv4.bc.game`

---

## 2. Connection Protocol

### 2.1 Engine.IO Version (CONFIRMED from live frames)

**Engine.IO v3** (`EIO=3`). The live WebSocket URL is:

```
wss://socketv4.bc.game/socket.io/?Accept-Language=en&p=<sign>&t=<source>&EIO=3&transport=websocket
```

This **invalidates** the prior investigation document (`bcgame-crash-streaming-pipeline-investigation.md`, Kilo 2026-09-02) which assumed Socket.IO v4 / EIO v4.

### 2.2 Transport Selection (CONFIRMED)

WebSocket-only. No polling fallback. The `transports: ["websocket"]` option is hardcoded. No upgrade negotiation occurs.

### 2.3 Sign/Authentication Flow (CONFIRMED from source + live capture)

The `D8.open()` method overrides the parent Manager's `open()`:

```
D8.open()
  → openWithSign()
    → await waitSocketConnect  (Promise resolved externally)
    → getSignData(uri)
      → R8()  = dynamic import("./wr_utils-C-YrHJp6.js")
        → WASM module (wasm-bindgen/Rust), exports {t1, t2}
      → s = navigator.userAgent.trim()
      → firstSign = t1(s)                    // WASM: sign from User-Agent
      → testSocketRoute(uri, firstSign)
        → fetch(`${socketHost}/test/?p=${firstSign}`, {credentials:"include"})
        → response text = c
      → finalSign = t2(c, s)                 // WASM: sign from /test/ response + UA
      → return { sign: finalSign, source: c, uri }
    → set query: { p: finalSign, t: c }
    → super.open()                           // standard Engine.IO connect
```

**Observed values (from live capture):**
- `p=015ab8550ae3c9bbe51a07907e00e1a07907ddf5` (40 hex chars = t2 output)
- `t=19b5467926c1d81b1a07907e00e` (26 hex chars = raw /test/ response)

**WASM module:** `wr_utils-C-YrHJp6.js` instantiates a 30,335-byte WebAssembly binary. Exports: `t1`, `t2`, `memory`, `__wbindgen_add_to_stack_pointer`, `__wbindgen_malloc`, `__wbindgen_realloc`, `__wbindgen_free`. Imports: `Date.now()`, `__wbindgen_throw`. String marshaling uses standard wasm-bindgen pattern (UTF-8 encode → malloc → call → read i32 pair from stack → UTF-8 decode).

### 2.4 EIO Handshake (CONFIRMED from live frames)

Server sends text frame on connect:
```json
0{"sid":"7f486d8b-18fe-4017-ad08-ccddc98b70af","upgrades":["websocket"],"pingInterval":5000,"pingTimeout":25000}
```

- `0` = EIO OPEN packet type
- `pingInterval`: 5000ms
- `pingTimeout`: 25000ms

### 2.5 Namespace Connection (CONFIRMED from live frames)

Client sends binary namespace CONNECT packets (Socket.IO type 0, EIO binary marker 0x04):

| Order | Namespace | Purpose |
|-------|-----------|---------|
| 1 | `/game-support` | Game support channel |
| 2 | `/user` | User notifications |
| 3 | `/g/cm` | **Crash game** |
| 4 | `/multi/g/cm` | Multi-crash capabilities |
| 5 | `/home` | Home page feed |

### 2.6 Join Request (CONFIRMED from live frames)

After connecting to `/g/cm`, the client sends a `join` request:

```
Binary frame: 04 82 00 00 00 00 05 2f 67 2f 63 6d 04 6a 6f 69 6e
              ↑  ↑──ackId=0──↑  ns="/g/cm"  event="join"  (no payload)
              EIO    type=2+0x80 (EVENT with ackId)
```

- `socket.request("join")` — ack-based (client expects ACK response)
- Empty data (no "crash" argument — the prior `socket.emit("join", "crash")` in the repo is WRONG)

Server responds with ACK (type 3, ackId 0) containing a 48KB CrashInfo protobuf blob.

---

## 3. Custom Binary Parser (T8)

### 3.1 Envelope Format (CONFIRMED from source + verified against 4078 live frames)

All Socket.IO packets use a custom binary envelope (NOT the standard Socket.IO parser):

```
[EIO marker: 1 byte, 0x04 for MESSAGE]
[Type: 1 byte]
  - If bit 7 (0x80) set: [AckId: 4 bytes, big-endian uint32]
[Namespace length: 1 byte]
[Namespace: N bytes, UTF-8]
[Event length: 1 byte]
[Event name: N bytes, UTF-8]
[Payload: remaining bytes, protobuf binary]
```

### 3.2 Type Values (CONFIRMED from source)

| Type | Name | Notes |
|------|------|-------|
| 0 | CONNECT | Namespace connect |
| 1 | DISCONNECT | Ignored by decoder |
| 2 | EVENT | Standard event |
| 3 | ACK | Acknowledgement |
| 4 | ERROR | |
| 5 | BINARY_EVENT | Remapped to 2 by decoder |
| 6 | BINARY_ACK | Remapped to 3 by decoder |

### 3.3 Heartbeat (CONFIRMED from live frames)

- Client sends text `"2"` (EIO v3 PING) every ~5 seconds (matches `pingInterval: 5000`)
- Server responds with text `"3"` (EIO v3 PONG)
- 21 ping frames sent in ~90s capture (consistent with 5s interval)

---

## 4. Event Stream

### 4.1 Namespace & Events (CONFIRMED from 4078 decoded live frames)

All Crash events arrive on namespace `/g/cm`:

| Event | Count (90s) | Frequency | Type |
|-------|-------------|-----------|------|
| `tb` | 1972 | ~22/s | Twice bet stream |
| `xb` | 993 | ~11/s | XBet (trenball) stream |
| `e` | 573 | ~6/s | Escape (player cashout) |
| `b` | 195 | ~2/s | Normal bet |
| `pg` | 193 | ~2/s | **Progress (multiplier update)** |
| `ed` | 4 | ~1/22s | **End (crash result)** |
| `st` | 4 | ~1/22s | **Settle** |
| `pr` | 4 | ~1/22s | **Prepare (new round)** |
| `bg` | 4 | ~1/22s | **Begin (round start)** |

Also on `/home`: `home-game-throw` (86), `home-high-roller` (25).

### 4.2 Round Lifecycle (CONFIRMED)

One round cycle ≈ 22 seconds:
```
pr (Prepare) → bg (Begin) → pg×~48 (Progress) → e×~143 (Escape) → ed (End) → st (Settle)
```

### 4.3 Protobuf Schemas (CONFIRMED from config-ByhseCil.js decode switch cases)

**Prepare (pr):**
| Field | Number | Type | Description |
|-------|--------|------|-------------|
| roundId | 1 | int64 | Round identifier |
| prepareTime | 3 | int64 | Prepare phase start (ms epoch) |
| startTime | 4 | int64 | Round start time (ms epoch) |

**Begin (bg):**
| Field | Number | Type | Description |
|-------|--------|------|-------------|
| roundId | 1 | int64 | Round identifier |
| startTime | 4 | int64 | Actual start time |
| normalBetUserIds | 5 | packed int64 | Users with active bets |

**Progress (pg):**
| Field | Number | Type | Description |
|-------|--------|------|-------------|
| elapsed | 1 | int64 | Milliseconds since round start |
| roundId | 2 | int64 | Round identifier (OPTIONAL — absent in most pg events) |

Multiplier formula (CONFIRMED from source): `multiplier = Math.pow(Math.E, 6e-5 * elapsed)`

**Escape (e):**
| Field | Number | Type | Description |
|-------|--------|------|-------------|
| userId | 1 | int64 | User who escaped |
| betId | 2 | int64 | Bet identifier |
| odds | 3 | int32 | Cash-out odds (×100) |
| force | 4 | bool | Forced escape |
| betIndex | 5 | int32 | Bet index |

**End (ed):**
| Field | Number | Type | Description |
|-------|--------|------|-------------|
| roundId | 1 | int64 | Round identifier |
| maxRate | 6 | int32 | Crash multiplier (÷100 for decimal) |
| hash | 7 | string | Provably-fair hash (ABSENT in ed — appears in st) |

**Settle (st):**
| Field | Number | Type | Description |
|-------|--------|------|-------------|
| roundId | 1 | int64 | Round identifier |
| escapes | 2 | repeated Escape | Aggregate escapes (ABSENT in observed captures) |
| maxRate | 6 | int32 | Crash multiplier (÷100) |
| hash | 7 | string | Provably-fair hash (64 hex chars) |

**CrashInfo (join response):**
| Field | Number | Type | Description |
|-------|--------|------|-------------|
| roundId | 1 | int64 | Current round |
| status | 2 | int32 | Round phase (0=idle, 1=preparing, 2=running, 3=ended) |
| prepareTime | 3 | int64 | |
| startTime | 4 | int64 | |
| hash | 6 | string | |
| maxRate | 7 | int32 | |
| houseage | 8 | int32 | |
| betLimits | 13 | repeated BetLimit | |
| normalBets | 14 | repeated NormalBet | |
| xBets | 15 | repeated XBet | |
| normalBetSize | 16 | int32 | |
| xBetSize | 17 | int32 | |
| normalBetAmount | 18 | string | |
| xBetAmount | 19 | string | |
| escapedSize | 20 | int32 | |
| twiceBets | 21 | repeated TwiceBet | |
| twiceBetSize | 22 | int32 | |
| twiceBetAmount | 23 | string | |

### 4.4 Verified Decoded Examples (from live capture)

```
ED: roundId=9586584, maxRate=370, multiplier=3.70x
PR: roundId=9586585, prepareTime=2030577720, startTime=2030584720
PG: elapsed=9905ms, multiplier=1.81x (no roundId field present)
ST: roundId=9586584, maxRate=370, multiplier=3.70x, hash=7052cf209ddc03fd81b2b16425f4941ecf14fca2d70bce2a9aa3abbcf1964d84
E:  userId=67477594, betId=1470701727, odds=180, betIndex=1
```

---

## 5. Reconnection, Heartbeat & Stale Handling

### 5.1 Reconnection (CONFIRMED from source)

The `D8` Manager inherits standard `socket.io-client` Manager reconnection:
- `reconnectionDelay: 1000ms` (default)
- `reconnectionDelayMax: 10000ms` (overridden in socketOptions)
- `randomizationFactor: 0.5` (default)
- Exponential backoff with jitter

### 5.2 Staleness Watchdog (CONFIRMED from source)

The Crash module (`ga` class) runs a 500ms interval watchdog:

```js
// Every 500ms:
if (!hasConnectedOnce) f = false;
else if (Le().readyState !== "open") f = true;           // socket not open
else if (status === 2)                                    // RUNNING
  f = (now - lastPacketAt > 8000);                       // 8s no packets
else if (status === 1)                                    // PREPARING
  f = (startTime > 0 && now > startTime + 8000)          // preparing > 8s
    || (now - lastPacketAt > 10000);                     // or 10s no packets
else                                                      // IDLE/ENDED
  f = (now - lastPacketAt > 10000);                      // 10s no packets

if (dataStale$.value !== f) dataStale$.next(f);
```

Constants: `oa = 8000` (running stale), `la = 8000` (preparing stale), `zr = 10000` (idle stale).

### 5.3 Reconnect Handler (CONFIRMED from source)

```js
onReconnect = () => {
  this.status$.next(0);         // reset to idle
  this.dataStale$.next(true);   // mark stale
};
// Then _onConnect fires → initGame() → request("join") → CrashInfo → dataStale$.next(false)
```

### 5.4 Latency Tracking (CONFIRMED from source)

```js
// In D8 constructor:
this.once("pong", i => { let s = i; this.on("pong", o => { s = 0.2*o + 0.8*s; this.latency = s; }); });
```

EWMA latency: `latency = 0.2 * latestPong + 0.8 * previousLatency`

---

## 6. Current Repo socket-client.ts — Confirmed Defects

The existing `src/lib/crash/socket-client.ts` is fundamentally broken:

| Defect | Current (wrong) | Correct (confirmed) |
|--------|-----------------|---------------------|
| Engine.IO version | Socket.IO v4 (npm package) | EIO v3 |
| Parser | Default socket.io parser | Custom T8 binary parser |
| Namespace | Default `/` | `/g/cm` |
| Join | `socket.emit("join", "crash")` | `socket.request("join")` (ack, no arg) |
| Payload encoding | JSON (`data.gameId`, `data.multiplier`) | Protocol Buffers |
| Sign flow | None | t1/t2 WASM → /test/ → p/t query params |
| Event decode | `normalizePayload` expects JSON object | Must decode protobuf wire format |
| Transport options | Allows polling fallback | WebSocket-only |

---

## 7. Reproduction Client

### 7.1 Files

- `src/lib/crash/transport/bcgame-crash-transport.ts` — standalone transport client
- `src/lib/crash/transport/wr_utils.wasm` — extracted WASM sign module (30,335 bytes)
- `src/lib/crash/transport/demo.ts` — runnable demo

### 7.2 What It Reproduces

1. ✅ WASM sign module loading (t1/t2 from wr_utils.wasm)
2. ✅ Sign flow (t1 → /test/ fetch → t2 → p/t query params)
3. ✅ EIO v3 WebSocket connection with signed URL
4. ✅ EIO handshake parsing (sid, pingInterval, pingTimeout)
5. ✅ Custom T8 binary parser (encode + decode)
6. ✅ Namespace connect (`/g/cm`)
7. ✅ Join request (EVENT with ackId=0)
8. ✅ Protobuf event decoding (pr, bg, pg, e, ed, st)
9. ✅ Heartbeat (text "2" ping every pingInterval)
10. ✅ Reconnection with exponential backoff
11. ✅ Staleness watchdog (500ms interval)
12. ✅ Isolated from UI — no DOM, no BC.Game bundle dependencies

### 7.3 Usage

```typescript
import { BcGameCrashTransport } from "./transport/bcgame-crash-transport";

const transport = new BcGameCrashTransport({
  log: (msg, data) => console.log(`[transport] ${msg}`, data ?? ""),
});

transport.on("ed", (event) => {
  console.log(`CRASH: round ${event.roundId} @ ${event.multiplier}x hash=${event.hash}`);
});

transport.on("pr", (event) => {
  console.log(`NEW ROUND: ${event.roundId} starting at ${new Date(event.startTime).toISOString()}`);
});

await transport.connect();
```

---

## 8. Confirmed vs Inferred

### CONFIRMED (from live runtime capture and source analysis)

- WebSocket endpoint URL and query parameters
- Engine.IO v3 protocol
- Custom binary parser envelope format (verified against 4078 frames)
- Sign flow sequence (t1 → /test/ → t2 → p/t)
- WASM sign module exports and string marshaling
- Namespace `/g/cm` and event names (pr/bg/pg/e/ed/st/b/xb/tb)
- Protobuf field numbers and wire types
- Multiplier formula (e^(6e-5 × elapsed))
- Heartbeat pattern (text "2" every 5s)
- Staleness watchdog thresholds (8s running, 10s idle)
- Reconnection parameters (delay 1s, max 10s, jitter 0.5)
- Join request format (ack-based, empty data, no "crash" arg)
- Host resolver (socketv4.<apex-domain>)

### INFERRED (not directly observed but derived from source logic)

- The `09` binary frames sent by the client (type 9 in custom parser) — likely internal protocol ACK/measurement frames; the standard Socket.IO layer silently ignores them
- The exact behavior of t1/t2 WASM functions (they produce hex strings; the internal algorithm is opaque without WASM decompilation)
- Whether Cloudflare will block the /test/ fetch from a datacenter IP (the CDP capture succeeded from this datacenter, but a standalone Node.js fetch may face different fingerprinting)
- Whether the settle event's `escapes` field is populated in other rounds (absent in all 4 captured settle events)
