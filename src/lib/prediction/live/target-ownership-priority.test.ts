/**
 * Regression: BG-primary / ED-fallback ownership race.
 * Proves the 16:13:21 production scenario cannot recur.
 */
import { describe, it, beforeEach } from "node:test";
import assert from "node:assert/strict";
import {
  claimTarget,
  reserveTargetForBg,
  completeTarget,
  releaseTarget,
  peekClaim,
  isBgBlocking,
  hasActiveOrCompletedClaim,
  _resetTargetCoordinatorForTests,
} from "./target-coordinator.ts";

beforeEach(() => {
  _resetTargetCoordinatorForTests();
});

describe("target ownership priority BG > ED > RECOVERY", () => {
  it("BG arrives before ED → BG owns N+1", () => {
    const target = "1001";
    const r = reserveTargetForBg(target, "1000");
    assert.equal(r.owned, true);
    assert.equal(r.state, "RESERVED_BG");

    const ed = claimTarget(target, "ed:1000");
    assert.equal(ed.owned, false);
    assert.ok(
      ed.reason === "bg_reserved" || ed.reason === "bg_running" || ed.blockedByBg,
    );
    assert.equal(ed.blockedByBg, true);
  });

  it("BG and ED concurrent → BG wins (reserve then claim)", () => {
    const target = "2001";
    const bg = reserveTargetForBg(target, "2000");
    assert.equal(bg.owned, true);
    const ed = claimTarget(target, "ed:2000");
    assert.equal(ed.owned, false);
    assert.equal(ed.blockedByBg, true);
    const bgClaim = claimTarget(target, "bg:2000");
    assert.equal(bgClaim.owned, true);
  });

  it("BG milliseconds before ED → ED cannot steal", () => {
    const target = "3001";
    reserveTargetForBg(target, "3000");
    // simulate reconcile latency — no await needed; Map is sync
    const ed = claimTarget(target, "ed:3000");
    assert.equal(ed.owned, false);
    assert.equal(peekClaim(target)?.source, "BG");
  });

  it("ED arrives before BG → ED may claim; BG can take over incomplete", () => {
    const target = "4001";
    const ed = claimTarget(target, "ed:4000");
    assert.equal(ed.owned, true);
    const bg = reserveTargetForBg(target, "4000");
    assert.equal(bg.owned, true);
    assert.equal(peekClaim(target)?.source, "BG");
  });

  it("BG fails recoverably → ED can take over", () => {
    const target = "5001";
    reserveTargetForBg(target, "5000");
    claimTarget(target, "bg:5000");
    releaseTarget(target, "bg:5000");
    assert.equal(hasActiveOrCompletedClaim(target), false);
    const ed = claimTarget(target, "ed:5000");
    assert.equal(ed.owned, true);
  });

  it("BG produces NO_BET → ED cannot recompute", () => {
    const target = "6001";
    reserveTargetForBg(target, "6000");
    claimTarget(target, "bg:6000");
    completeTarget(target, "bg:6000", { decision: "NO_BET" });
    const ed = claimTarget(target, "ed:6000");
    assert.equal(ed.owned, false);
    assert.ok(ed.noBet || ed.reason === "no_bet_terminal" || ed.reason === "completed");
  });

  it("duplicate BG → no second ownership", () => {
    const target = "7001";
    const a = reserveTargetForBg(target, "7000");
    const b = reserveTargetForBg(target, "7000");
    assert.equal(a.owned, true);
    assert.equal(b.owned, true); // same owner idempotent — same ownership, no second claim
    const other = reserveTargetForBg(target, "6999");
    // different source = different owner: a second primary must NOT own —
    // single authoritative prediction per target (exactly-once).
    assert.equal(other.owned, false);
    assert.ok(other.blockedByBg);
  });

  it("duplicate ED → second is blocked", () => {
    const target = "8001";
    const a = claimTarget(target, "ed:8000");
    const b = claimTarget(target, "ed:8000");
    assert.equal(a.owned, true);
    assert.equal(b.owned, true); // same owner
    const c = claimTarget(target, "ed:7999");
    assert.equal(c.owned, false);
  });

  it("poll cannot steal BG-owned target", () => {
    const target = "9001";
    reserveTargetForBg(target, "9000");
    const poll = claimTarget(target, "poll:9000");
    assert.equal(poll.owned, false);
    assert.equal(poll.blockedByBg, true);
  });

  it("isBgBlocking reflects RESERVED/RUNNING/COMPLETED", () => {
    const target = "1101";
    assert.equal(isBgBlocking(target).blocked, false);
    reserveTargetForBg(target, "1100");
    assert.equal(isBgBlocking(target).blocked, true);
    assert.equal(isBgBlocking(target).reason, "RESERVED_BG");
    claimTarget(target, "bg:1100");
    assert.ok(["BG_RUNNING", "RESERVED_BG"].includes(isBgBlocking(target).reason!));
    completeTarget(target, "bg:1100");
    assert.equal(isBgBlocking(target).blocked, true);
  });

  it("SIGNAL_READY stays closed for ED", () => {
    const target = "1201";
    reserveTargetForBg(target, "1200");
    claimTarget(target, "bg:1200");
    completeTarget(target, "bg:1200", { decision: "SIGNAL" });
    const ed = claimTarget(target, "ed:1200");
    assert.equal(ed.owned, false);
  });
});
