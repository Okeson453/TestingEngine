import { describe, it, beforeEach } from "node:test";
import assert from "node:assert/strict";
import {
  reserveTargetForBg,
  reserveTargetForPr,
  isBgOwnedOrTerminal,
  isPrimaryOwnedOrTerminal,
  claimTarget,
  completeTarget,
  _resetTargetCoordinatorForTests,
} from "./target-coordinator.ts";

describe("PR/BG primary ownership", () => {
  beforeEach(() => {
    _resetTargetCoordinatorForTests();
  });

  it("isPrimaryOwnedOrTerminal true after reserveTargetForPr", () => {
    const tid = "t-pr-a";
    const r = reserveTargetForPr(tid, "100");
    assert.equal(r.owned, true);
    assert.equal(isPrimaryOwnedOrTerminal(tid), true);
    assert.equal(isBgOwnedOrTerminal(tid), true);
  });

  it("isBgOwnedOrTerminal true after reserveTargetForBg", () => {
    const tid = "t-bg-a";
    const r = reserveTargetForBg(tid, "100");
    assert.equal(r.owned, true);
    assert.equal(isBgOwnedOrTerminal(tid), true);
  });

  it("ED claim cannot steal active PR reservation", () => {
    const tid = "t-pr-b";
    reserveTargetForPr(tid, "200");
    const ed = claimTarget(tid, "ed:200");
    assert.equal(ed.owned, false);
    assert.equal(isPrimaryOwnedOrTerminal(tid), true);
  });

  it("BG reserve is no-op when PR already owns (confirmation path)", () => {
    const tid = "t-pr-c";
    const pr = reserveTargetForPr(tid, "300");
    assert.equal(pr.owned, true);
    const bg = reserveTargetForBg(tid, "300");
    assert.equal(bg.owned, false);
    assert.equal(bg.blockedByBg, true);
  });

  it("ED claim cannot steal active BG reservation", () => {
    const tid = "t-bg-b";
    reserveTargetForBg(tid, "200");
    const ed = claimTarget(tid, "ed:200");
    assert.equal(ed.owned, false);
    assert.equal(isBgOwnedOrTerminal(tid), true);
  });

  it("terminal NO_BET from PR blocks ED", () => {
    const tid = "t-pr-d";
    const pr = claimTarget(tid, "pr:300");
    assert.equal(pr.owned, true);
    completeTarget(tid, "pr:300", { decision: "NO_BET" });
    assert.equal(isPrimaryOwnedOrTerminal(tid), true);
    const ed = claimTarget(tid, "ed:300");
    assert.equal(ed.owned, false);
  });

  it("terminal NO_BET from BG blocks ED", () => {
    const tid = "t-bg-c";
    const bg = claimTarget(tid, "bg:300");
    assert.equal(bg.owned, true);
    completeTarget(tid, "bg:300", { decision: "NO_BET" });
    assert.equal(isBgOwnedOrTerminal(tid), true);
    const ed = claimTarget(tid, "ed:300");
    assert.equal(ed.owned, false);
  });

  it("BG can claim when PR missed", () => {
    const tid = "t-bg-fallback";
    const bg = reserveTargetForBg(tid, "400");
    assert.equal(bg.owned, true);
    const claim = claimTarget(tid, "bg:400");
    assert.equal(claim.owned, true);
  });
});
