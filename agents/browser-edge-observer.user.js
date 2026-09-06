// ==UserScript==
// @name         TestingEngine BC.Game Edge Forwarder
// @namespace    https://github.com/Okeson453/TestingEngine
// @version      1.0.0
// @description  Forward decoded crash end events to TestingEngine edge ingest (not a protocol cracker)
// @match        https://bc.game/*
// @match        https://*.bc.game/*
// @grant        none
// @run-at       document-idle
// ==/UserScript==

/**
 * Configure before use:
 *   window.__TE_EDGE__ = {
 *     url: 'https://YOUR_WORKER_HOST:8091',
 *     token: 'same-as-EDGE_INGEST_TOKEN',
 *   };
 *
 * Only forwards ALREADY DECODED gameId + multiplier + time from page/network
 * JSON if present. Does not MITM TLS or reverse-engineer binary schemas.
 * Respect BC.Game Terms of Service; use at your own risk.
 */
(function () {
  'use strict';

  const cfg = Object.assign(
    {
      url: '',
      token: '',
      // How often to allow duplicate gameId posts (ms)
      dedupeMs: 60_000,
    },
    window.__TE_EDGE__ || {},
  );

  if (!cfg.url || !cfg.token) {
    console.warn(
      '[TE-EDGE] Set window.__TE_EDGE__ = { url, token } then reload. Disabled.',
    );
    return;
  }

  const base = String(cfg.url).replace(/\/$/, '');
  const sent = new Map();

  function shouldSend(gameId) {
    const now = Date.now();
    const prev = sent.get(gameId);
    if (prev && now - prev < cfg.dedupeMs) return false;
    sent.set(gameId, now);
    if (sent.size > 200) {
      const first = sent.keys().next().value;
      sent.delete(first);
    }
    return true;
  }

  async function postCrash(payload) {
    const gameId = String(payload.gameId ?? '');
    if (!/^\d+$/.test(gameId)) return;
    if (!shouldSend(gameId)) return;
    const body = {
      gameId,
      multiplier: Number(payload.multiplier),
      crashedAt: payload.crashedAt || new Date().toISOString(),
      observedAt: Date.now(),
      source: 'userscript',
    };
    if (!Number.isFinite(body.multiplier) || body.multiplier <= 0) return;
    try {
      const res = await fetch(base + '/edge/crash', {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          authorization: 'Bearer ' + cfg.token,
        },
        body: JSON.stringify(body),
        mode: 'cors',
        keepalive: true,
      });
      const j = await res.json().catch(() => ({}));
      console.log('[TE-EDGE] crash', gameId, body.multiplier, res.status, j);
    } catch (e) {
      console.warn('[TE-EDGE] forward failed', e);
    }
  }

  async function postBg(payload) {
    const gameId = String(payload.gameId ?? '');
    if (!/^\d+$/.test(gameId)) return;
    try {
      await fetch(base + '/edge/bg', {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          authorization: 'Bearer ' + cfg.token,
        },
        body: JSON.stringify({
          gameId,
          beganAt: payload.beganAt || new Date().toISOString(),
          source: 'userscript',
        }),
        mode: 'cors',
        keepalive: true,
      });
    } catch {
      /* soft */
    }
  }

  /** Best-effort: parse Socket.IO-ish or JSON text frames if the site uses them. */
  function tryParseEvent(text) {
    if (typeof text !== 'string' || text.length < 2) return;
    // Socket.IO style: 42["ed",{...}] or 42["bg",{...}]
    let jsonText = text;
    const m = text.match(/^\d+(.+)$/s);
    if (m) jsonText = m[1];
    try {
      const data = JSON.parse(jsonText);
      const arr = Array.isArray(data) ? data : null;
      const eventName = arr ? String(arr[0] ?? '') : '';
      const payload = arr ? arr[1] : data;
      if (!payload || typeof payload !== 'object') return;

      const gameId =
        payload.gameId ?? payload.id ?? payload.game_id ?? payload.gid;
      const mult =
        payload.multiplier ??
        payload.rate ??
        payload.crash ??
        payload.crashPoint;
      const end =
        payload.crashedAt ?? payload.endTime ?? payload.time ?? payload.ts;
      const begin = payload.beganAt ?? payload.beginTime;

      const isEnd =
        /ed|crash|end|settle/i.test(eventName) ||
        (mult != null && (end != null || eventName === ''));
      const isBg = /bg|begin|start/i.test(eventName);

      if (isBg && gameId && begin) {
        void postBg({ gameId, beganAt: begin });
        return;
      }
      if (isEnd && gameId && mult != null) {
        void postCrash({
          gameId,
          multiplier: mult,
          crashedAt:
            typeof end === 'number'
              ? new Date(end < 1e12 ? end * 1000 : end).toISOString()
              : end,
        });
      }
    } catch {
      /* not JSON */
    }
  }

  // Patch WebSocket only to observe text frames (decoded JSON). Binary is ignored.
  const NativeWS = window.WebSocket;
  window.WebSocket = function (url, protocols) {
    const ws = protocols
      ? new NativeWS(url, protocols)
      : new NativeWS(url);
    try {
      if (/bc\.game|socket/i.test(String(url))) {
        ws.addEventListener(
          'message',
          function (ev) {
            if (typeof ev.data === 'string') tryParseEvent(ev.data);
          },
          true,
        );
      }
    } catch {
      /* */
    }
    return ws;
  };
  window.WebSocket.prototype = NativeWS.prototype;
  window.WebSocket.CONNECTING = 0;
  window.WebSocket.OPEN = 1;
  window.WebSocket.CLOSING = 2;
  window.WebSocket.CLOSED = 3;

  // Manual hook for console / other scripts:
  // window.__teEdgeCrash({ gameId: '123', multiplier: 2.5 })
  window.__teEdgeCrash = postCrash;
  window.__teEdgeBg = postBg;

  console.log('[TE-EDGE] forwarder armed →', base);
})();
