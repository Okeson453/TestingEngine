// ==UserScript==
// @name         TestingEngine BC.Game Edge Forwarder
// @namespace    https://github.com/Okeson453/TestingEngine
// @version      1.1.0
// @description  Forward Crash end/start events to TestingEngine edge ingest (bypasses Cloudflare WAF on Railway)
// @match        https://bc.game/*
// @match        https://*.bc.game/*
// @grant        none
// @run-at       document-idle
// ==/UserScript==

/**
 * Setup:
 *   1. Railway worker: EDGE_INGEST_TOKEN=<secret>, public domain on worker service
 *   2. In browser console once (or edit CONFIG below):
 *        window.__TE_EDGE__ = {
 *          url: 'https://YOUR-WORKER.up.railway.app',  // no :8091 if Railway $PORT
 *          token: 'same-as-EDGE_INGEST_TOKEN',
 *        };
 *   3. Reload bc.game/game/crash — green pill top-right means active.
 */
(function () {
  'use strict';

  const CONFIG = Object.assign(
    {
      url: '',
      token: '',
      dedupeMs: 45_000,
      pollMs: 1200,
      debug: true,
    },
    typeof window !== 'undefined' ? window.__TE_EDGE__ || {} : {},
  );

  if (!CONFIG.url || !CONFIG.token) {
    console.warn(
      '[TE-EDGE] Disabled. Set window.__TE_EDGE__ = { url: "https://YOUR-WORKER.up.railway.app", token: "..." } and reload.',
    );
    return;
  }

  const base = String(CONFIG.url).replace(/\/$/, '');
  const sentCrash = new Map();
  const sentBg = new Map();
  let lastIndicator = null;

  function log(...args) {
    if (CONFIG.debug) console.log('[TE-EDGE]', ...args);
  }

  function shouldSend(map, gameId) {
    const now = Date.now();
    const prev = map.get(gameId);
    if (prev && now - prev < CONFIG.dedupeMs) return false;
    map.set(gameId, now);
    if (map.size > 300) map.delete(map.keys().next().value);
    return true;
  }

  async function post(path, body) {
    try {
      const res = await fetch(base + path, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          authorization: 'Bearer ' + CONFIG.token,
        },
        body: JSON.stringify(body),
        mode: 'cors',
        keepalive: true,
      });
      const text = await res.text();
      log(path, res.status, text.slice(0, 120));
      setIndicator(res.ok);
      return res.ok;
    } catch (e) {
      log('post failed', e);
      setIndicator(false);
      return false;
    }
  }

  function postCrash(payload) {
    const gameId = String(payload.gameId ?? '');
    if (!/^\d+$/.test(gameId)) return;
    if (!shouldSend(sentCrash, gameId)) return;
    const multiplier = Number(payload.multiplier);
    if (!Number.isFinite(multiplier) || multiplier <= 0) return;
    void post('/edge/crash', {
      gameId,
      multiplier,
      crashedAt: payload.crashedAt || new Date().toISOString(),
      observedAt: Date.now(),
      source: 'userscript',
    });
  }

  function postBg(payload) {
    const gameId = String(payload.gameId ?? '');
    if (!/^\d+$/.test(gameId)) return;
    if (!shouldSend(sentBg, gameId)) return;
    void post('/edge/bg', {
      gameId,
      beganAt: payload.beganAt || new Date().toISOString(),
      observedAt: Date.now(),
      source: 'userscript',
    });
  }

  function setIndicator(ok) {
    if (typeof document === 'undefined') return;
    if (!lastIndicator) {
      lastIndicator = document.createElement('div');
      lastIndicator.id = 'te-edge-indicator';
      lastIndicator.style.cssText =
        'position:fixed;top:12px;right:12px;z-index:999999;padding:6px 10px;border-radius:999px;font:12px/1.2 system-ui;color:#fff;box-shadow:0 2px 8px rgba(0,0,0,.25)';
      document.documentElement.appendChild(lastIndicator);
    }
    lastIndicator.style.background = ok ? '#16a34a' : '#dc2626';
    lastIndicator.textContent = ok ? 'TE Edge ✓' : 'TE Edge ✗';
  }

  function tryParseFrame(text) {
    if (typeof text !== 'string' || text.length < 2) return;
    let jsonText = text;
    const m = text.match(/^\d+(.+)$/s);
    if (m) jsonText = m[1];
    try {
      const data = JSON.parse(jsonText);
      const arr = Array.isArray(data) ? data : null;
      const eventName = arr ? String(arr[0] ?? '') : '';
      const payload = arr ? arr[1] : data;
      if (!payload || typeof payload !== 'object') return;
      const gameId = payload.gameId ?? payload.id ?? payload.game_id ?? payload.gid;
      const mult = payload.multiplier ?? payload.rate ?? payload.crash ?? payload.crashPoint;
      const end = payload.crashedAt ?? payload.endTime ?? payload.time ?? payload.ts;
      const begin = payload.beganAt ?? payload.beginTime;
      if (/bg|begin|start|pr/i.test(eventName) && gameId && begin) {
        postBg({ gameId, beganAt: begin });
        return;
      }
      if ((/ed|crash|end|settle/i.test(eventName) || mult != null) && gameId && mult != null) {
        postCrash({
          gameId,
          multiplier: mult,
          crashedAt:
            typeof end === 'number'
              ? new Date(end < 1e12 ? end * 1000 : end).toISOString()
              : end,
        });
      }
    } catch {
      /* ignore */
    }
  }

  // Observe WebSocket text frames
  try {
    const OrigWS = window.WebSocket;
    window.WebSocket = function (...args) {
      const ws = new OrigWS(...args);
      ws.addEventListener('message', (ev) => {
        if (typeof ev.data === 'string') tryParseFrame(ev.data);
      });
      return ws;
    };
    window.WebSocket.prototype = OrigWS.prototype;
    Object.assign(window.WebSocket, OrigWS);
  } catch (e) {
    log('WS patch failed', e);
  }

  // Poll in-page crash history (fallback when WS frames are binary/protobuf)
  function pollHistory() {
    try {
      const roots = [window.crash, window.game && window.game.crash, window.__CRASH__];
      for (const crash of roots) {
        if (!crash) continue;
        const hist = crash.history || crash.list || crash.rounds;
        if (!Array.isArray(hist) || !hist.length) continue;
        const row = hist[0];
        const gameId = row.gameId ?? row.id ?? row.game_id;
        const mult = row.multiplier ?? row.crash ?? row.odds;
        const crashedAt = row.crashedAt ?? row.endTime ?? row.time;
        if (gameId && mult != null) {
          postCrash({ gameId, multiplier: mult, crashedAt });
        }
        break;
      }
    } catch {
      /* soft */
    }
    setTimeout(pollHistory, CONFIG.pollMs);
  }
  setTimeout(pollHistory, 2000);

  // Heartbeat health
  setInterval(() => {
    fetch(base + '/edge/health', { mode: 'cors' })
      .then((r) => setIndicator(r.ok))
      .catch(() => setIndicator(false));
  }, 15000);

  setIndicator(true);
  log('active →', base);
})();
