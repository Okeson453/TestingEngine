/**
 * P1 integration tests — ACIE per-crash independence & stale-state enforcement.
 *
 * Acceptance:
 *   Crash N → observe → state V+1 → features H → evaluate N+1 → prediction P
 *   Crash N+1 → observe → state V+2 → features H2 → evaluate N+2 → prediction P2
 * with distinct prediction IDs, advancing observation counts, and matching feature hashes
 * when input state materially changes.
 *
 * Also covers:
 *   - STALE_REJECTED when prediction attempted without ACIE observation
 *   - Idempotent duplicate ED(N)
 */

import { describe, it, expect, beforeEach } from "vitest";
import { ACIEEngine } from "@/lib/prediction/acie/engine";
import {
  getSharedACIEEngine,
  setSharedACIEEngineForTests,
  getSharedACIEInstanceId,
} from "@/lib/prediction/acie/shared-engine";
import {
  buildAcieFeatureFingerprint,
  buildProvenance,
} from "@/lib/prediction/acie/provenance";
import {
  recordAcieObservation,
  assertFreshAcieState,
  resetStaleGuardForTests,
} from "@/lib/prediction/acie/stale-guard";

describe("ACIE per-crash independence", () => {
  beforeEach(() => {
    setSharedACIEEngineForTests(null);
    resetStaleGuardForTests();
    // Fresh shared engine
    getSharedACIEEngine();
  });

  it("advances observation count and feature hash across lower-multiplier crashes", () => {
    const acie = getSharedACIEEngine();
    const crashes = [
      { id: "1001", m: 1.1 },
      { id: "1002", m: 8.5 },
      { id: "1003", m: 1.05 },
      { id: "1004", m: 20.0 },
    ];

    const lineage: Array<{
      sourceId: string;
      obs: number;
      hash: string;
      probability: number;
      action: string;
    }> = [];

    for (const c of crashes) {
      const result = acie.observeRound({
        roundId: c.id,
        crashPoint: c.m,
        timestamp: new Date().toISOString(),
      });
      const online = result.online;
      const evaln = result.evaluation;
      const snap = acie.exportSnapshot();
      const probability = evaln.psi.estimatedProbability;
      const hash = buildAcieFeatureFingerprint({
        crashPointsTail: snap.crashPoints,
        observationCount: online.observationCount ?? 0,
        regime: String(evaln.regime ?? "unknown"),
        ewmaHitRate: online.ewmaHitRate ?? 0,
        psiProbability: probability,
      });
      recordAcieObservation(c.id, online.observationCount ?? 0);
      lineage.push({
        sourceId: c.id,
        obs: online.observationCount ?? 0,
        hash,
        probability,
        action: evaln.strategy.action,
      });
    }

    // Distinct source IDs
    const ids = lineage.map((l) => l.sourceId);
    expect(new Set(ids).size).toBe(4);

    // Observation count strictly advances
    for (let i = 1; i < lineage.length; i++) {
      expect(lineage[i]!.obs).toBeGreaterThan(lineage[i - 1]!.obs);
    }

    // Feature hashes change when history changes (material state change)
    // At least after the first observation the hash should differ from initial empty-ish state
    const hashes = lineage.map((l) => l.hash);
    expect(new Set(hashes).size).toBeGreaterThanOrEqual(2);

    // Fresh state check passes for last source
    const check = assertFreshAcieState("1004");
    expect(check.ok).toBe(true);
  });

  it("rejects prediction when ACIE was not updated for the source crash", () => {
    const acie = getSharedACIEEngine();
    acie.observeRound({
      roundId: "2001",
      crashPoint: 1.2,
      timestamp: new Date().toISOString(),
    });
    recordAcieObservation("2001", acie.getOnlineState().observationCount ?? 1);

    // Attempt emission for a different source without observation
    const check = assertFreshAcieState("2002");
    expect(check.ok).toBe(false);
    if (!check.ok) {
      expect(check.reason).toBe("source_mismatch");
    }
  });

  it("rejects when no observation has been recorded at all", () => {
    resetStaleGuardForTests();
    const check = assertFreshAcieState("3001");
    expect(check.ok).toBe(false);
    if (!check.ok) {
      expect(check.reason).toBe("no_acie_observation");
    }
  });

  it("is idempotent for duplicate observeRound of the same gameId", () => {
    const acie = getSharedACIEEngine();
    const r1 = acie.observeRound({
      roundId: "4001",
      crashPoint: 1.15,
      timestamp: new Date().toISOString(),
    });
    const obs1 = r1.online.observationCount ?? 0;
    const r2 = acie.observeRound({
      roundId: "4001",
      crashPoint: 1.15,
      timestamp: new Date().toISOString(),
    });
    const obs2 = r2.online.observationCount ?? 0;
    // Duplicate should not double-count observations in online state
    // (engine is idempotent via processedRoundIds)
    expect(obs2).toBe(obs1);
  });

  it("builds provenance with NORMAL_ACIE mode and matching instance id", () => {
    const acie = getSharedACIEEngine();
    const result = acie.observeRound({
      roundId: "5001",
      crashPoint: 2.5,
      timestamp: new Date().toISOString(),
    });
    const online = result.online;
    const evaluation = result.evaluation;
    const snap = acie.exportSnapshot();
    const p = evaluation.psi.estimatedProbability;
    const hash = buildAcieFeatureFingerprint({
      crashPointsTail: snap.crashPoints,
      observationCount: online.observationCount ?? 0,
      regime: String(evaluation.regime ?? "unknown"),
      ewmaHitRate: online.ewmaHitRate ?? 0,
      psiProbability: p,
    });
    const prov = buildProvenance({
      sourceGameId: "5001",
      targetGameId: "5002",
      online,
      evaluation,
      mode: "NORMAL_ACIE",
      executionPath: "shared-acie.evaluateNext",
      probability: p,
      confidence: 0.7,
      featureHash: hash,
    });
    expect(prov.prediction_mode).toBe("NORMAL_ACIE");
    expect(prov.acie_instance_id).toBe(getSharedACIEInstanceId());
    expect(prov.source_game_id).toBe("5001");
    expect(prov.target_game_id).toBe("5002");
    expect(prov.feature_hash).toBe(hash);
    expect(prov.acie_observation_count).toBe(online.observationCount ?? 0);
  });
});
