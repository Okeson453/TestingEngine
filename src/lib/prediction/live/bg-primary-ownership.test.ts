import { describe, it, beforeEach } from "node:test";
import assert from "node:assert/strict";
import {
  reserveTargetForBg,
  isBgOwnedOrTerminal,
  claimTarget,
  completeTarget,
  _resetTargetCoordinatorForTests,
} from "./target-coordinator.ts";

describe("BG-primary ownership", () => {
  beforeEach(() => {
    _resetTargetCoordinatorForTests();
  });

  it("isBgOwnedOrTerminal true after reserveTargetForBg", () => {
    const tid = "t-bg-a";
    const r = reserveTargetForBg(tid, "100");
    assert.equal(r.owned, true);
    assert.equal(isBgOwnedOrTerminal(tid), true);
  });

  it("ED claim cannot steal active BG reservation", () => {
    const tid = "t-bg-b";
    reserveTargetForBg(tid, "200");
    const ed = claimTarget(tid, "ed:200");
    assert.equal(ed.owned, false);
    assert.equal(isBgOwnedOrTerminal(tid), true);
  });

  it("terminal NO_BET blocks ED", () => {
    const tid = "t-bg-c";
    const bg = claimTarget(tid, "bg:300");
    assert.equal(bg.owned, true);
    completeTarget(tid, "bg:300", { decision: "NO_BET" });
    assert.equal(isBgOwnedOrTerminal(tid), true);
    const ed = claimTarget(tid, "ed:300");
    assert.equal(ed.owned, false);
  });
});
