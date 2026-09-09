import { test } from "node:test";
import assert from "node:assert/strict";
import {
  claimTarget,
  completeTarget,
  releaseTarget,
  hasCompletedTarget,
  _resetTargetCoordinatorForTests,
} from "@/lib/prediction/live/target-coordinator";

test("target-coordinator: first claimant owns; other owner is duplicate", () => {
  _resetTargetCoordinatorForTests();
  const a = claimTarget("100", "ed:99");
  assert.equal(a.owned, true);
  const b = claimTarget("100", "poll:99");
  assert.equal(b.owned, false);
  if (!b.owned) assert.equal(b.reason, "duplicate");
});

test("target-coordinator: same owner may re-enter an open claim", () => {
  _resetTargetCoordinatorForTests();
  assert.equal(claimTarget("200", "ed:199").owned, true);
  assert.equal(claimTarget("200", "ed:199").owned, true);
});

test("target-coordinator: release allows a different owner to claim", () => {
  _resetTargetCoordinatorForTests();
  assert.equal(claimTarget("300", "ed:299").owned, true);
  releaseTarget("300", "ed:299");
  const poll = claimTarget("300", "poll:299");
  assert.equal(poll.owned, true);
});

test("target-coordinator: complete blocks later owners", () => {
  _resetTargetCoordinatorForTests();
  assert.equal(claimTarget("400", "ed:399").owned, true);
  completeTarget("400", "ed:399");
  assert.equal(hasCompletedTarget("400"), true);
  const poll = claimTarget("400", "poll:399");
  assert.equal(poll.owned, false);
  if (!poll.owned) assert.equal(poll.reason, "completed");
});
