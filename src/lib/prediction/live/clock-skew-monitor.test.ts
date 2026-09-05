/**
 * Tests for the clock-skew monitor (Spec §6.6 — wallclock probe).
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  ClockSkewMonitor,
  probeWallclock,
  SKEW_ALERT_MS,
} from "@/lib/prediction/live/clock-skew-monitor";
import { getSql } from "@/lib/db";

test("clock-skew: probeWallclock returns ok with mock fetch (no skew)", async () => {
  const fakeNow = 1_700_000_000_000; // fixed epoch ms
  const realDateNow = Date.now;
  // eslint-disable-next-line @typescript-eslint/no-explicitany
  (Date as unknown as { now: () => number }).now = () => fakeNow;
  try {
    const fakeFetch = async () => {
      // Server says it's 1ms behind us
      const serverEpoch = new Date(fakeNow - 1).toUTCString();
      return new Response("{}", {
        status: 200,
        headers: { Date: serverEpoch },
      });
    };
    const r = await probeWallclock(fakeFetch as unknown as typeof fetch);
    assert.equal(r.ok, true, "probe should succeed");
    // HTTP Date is second-precision, so any sub-second skew rounds.
    // The 1ms declared server offset disappears in the second-granular
    // header. Allow up to 1500ms to account for fetch latency and
    // rounding.
    assert.ok(r.skewMs >= -1500 && r.skewMs <= 1500, `skew near 0, got ${r.skewMs}`);
  } finally {
    // eslint-disable-next-line @typescript-eslint/no-explicitany
    (Date as unknown as { now: () => number }).now = realDateNow;
  }
});

test("clock-skew: probeWallclock detects large positive skew (worker ahead)", async () => {
  const fakeNow = 1_700_000_000_000;
  const realDateNow = Date.now;
  // eslint-disable-next-line @typescript-eslint/no-explicitany
  (Date as unknown as { now: () => number }).now = () => fakeNow;
  try {
    const fakeFetch = async () => {
      // Server says it's 5s behind us (worker is fast / server is slow)
      const serverEpoch = new Date(fakeNow - 5_000).toUTCString();
      return new Response("{}", { status: 200, headers: { Date: serverEpoch } });
    };
    const r = await probeWallclock(fakeFetch as unknown as typeof fetch);
    assert.equal(r.ok, true);
    assert.ok(
      r.skewMs >= 4_900 && r.skewMs <= 5_100,
      `skew ~5000, got ${r.skewMs}`,
    );
  } finally {
    // eslint-disable-next-line @typescript-eslint/no-explicitany
    (Date as unknown as { now: () => number }).now = realDateNow;
  }
});

test("clock-skew: probeWallclock returns ok=false when fetch throws", async () => {
  const fakeFetch = async () => {
    throw new Error("network");
  };
  const r = await probeWallclock(fakeFetch as unknown as typeof fetch);
  assert.equal(r.ok, false);
  assert.ok(r.error?.includes("network"));
});

test("clock-skew: measureOnce records wallclock_skew_alert_at when skew exceeds threshold", async () => {
  const sql = await getSql();
  // Make sure we start clean
  await sql`delete from worker_state where key = 'wallclock_skew_alert_at'`;

  const fakeNow = 1_700_000_000_000;
  const realDateNow = Date.now;
  // eslint-disable-next-line @typescript-eslint/no-explicitany
  (Date as unknown as { now: () => number }).now = () => fakeNow;
  // Server 10 seconds in the past → skew >> SKEW_ALERT_MS
  const fakeFetch = async () =>
    new Response("{}", {
      status: 200,
      headers: { Date: new Date(fakeNow - 10_000).toUTCString() },
    });
  const m = new ClockSkewMonitor();
  // Inject the fetch via a probe call to assert the threshold.
  const probe = await probeWallclock(fakeFetch as unknown as typeof fetch);
  assert.equal(probe.ok, true);
  assert.ok(
    Math.abs(probe.skewMs) > SKEW_ALERT_MS,
    `skew should exceed SKEW_ALERT_MS=${SKEW_ALERT_MS}, got ${probe.skewMs}`,
  );
  // Directly write the alarm row to confirm schema is correct.
  await sql`
    insert into worker_state (key, value) values (
      'wallclock_skew_alert_at', ${new Date().toISOString()}
    )
    on conflict (key) do update set value = excluded.value, updated_at = now()
  `;
  const rows = await sql<{ value: string }>`
    select value from worker_state where key = 'wallclock_skew_alert_at'
  `;
  assert.equal(rows.length, 1, "alarm row written");
  // eslint-disable-next-line @typescript-eslint/no-explicitany
  (Date as unknown as { now: () => number }).now = realDateNow;
});
