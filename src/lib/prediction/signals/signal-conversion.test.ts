/**
 * P0 regression tests for the frozen-signal / strict-validation pipeline.
 *
 * Production failure being reproduced: PredictionEngine.predict() called
 * toSignal() (which returns an Object.freeze'd signal) and then MUTATED the
 * frozen object (featurePath / featureVersion). In strict mode that throws
 * "Cannot add property featurePath, object is not extensible" — reported
 * misleadingly as a toSignal conversion failure.
 *
 * Test A proves the mutation path is gone and the signal is frozen+complete.
 * Test B proves malformed model output fails at prediction_output_validation
 * (NOT signal_conversion) with a typed, field-level error.
 * Test C proves the canonical signal schema (featurePath, featureVersion,
 * targetRoundId, ...) is present and valid.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { PredictionEngine } from "@/lib/prediction/prediction-engine";
import { ModelRegistry } from "@/lib/prediction/models/model-registry";
import type { PredictiveModel } from "@/lib/prediction/models/baseline-model";
import type {
  HistoricalRound,
  PredictionOutput,
  Regime,
  ThresholdTarget,
} from "@/lib/prediction/types";
import { toSignal } from "@/lib/prediction/signals/signal";
import {
  PredictionOutputValidationError,
  PredictionSignalValidationError,
  summarizePredictionOutput,
  validatePredictionOutput,
  validatePredictionSignal,
} from "@/lib/prediction/signals/validate";

const STUB_IDENTITY = {
  name: "stub",
  version: "1.0.0",
  featureVersion: "stub-v1",
  targetVersion: "tv-1",
} as const;

function validOutput(target: ThresholdTarget = 1.3): PredictionOutput {
  const now = new Date().toISOString();
  return {
    predictionId: `stub-${randomUUID()}`,
    model: { ...STUB_IDENTITY },
    target,
    score: 0.55,
    probability: 0.82,
    confidence: 0.71,
    regime: null,
    dataQuality: 0.9,
    featureSummary: { f1: 1, f2: 2 },
    reasoning: ["stub reasoning"],
    timestamp: now,
    expiresAt: new Date(Date.now() + 30_000).toISOString(),
  };
}

function makeModel(impl: (target: ThresholdTarget) => PredictionOutput): PredictiveModel {
  return {
    identity: { ...STUB_IDENTITY },
    predict: (_features, target: ThresholdTarget, _regime: Regime | null) => impl(target),
  };
}

function historyRounds(n: number): HistoricalRound[] {
  const nowMs = Date.now();
  const rounds: HistoricalRound[] = [];
  for (let i = n; i >= 1; i -= 1) {
    const crashedAt = new Date(nowMs - i * 4_000).toISOString();
    const idx = n - i;
    rounds.push({
      id: String(1000 + idx),
      externalRoundId: String(1000 + idx),
      sessionId: null,
      startedAt: new Date(nowMs - i * 4_000 - 3_000).toISOString(),
      crashedAt,
      crashPoint: 1 + (idx % 13) * 0.13,
      observationSource: "test",
      dataQuality: "high",
      createdAt: crashedAt,
    });
  }
  return rounds;
}

function makeEngine(model: PredictiveModel): PredictionEngine {
  const registry = new ModelRegistry();
  registry.register(model);
  return new PredictionEngine(undefined, undefined, registry);
}

// ── Test A: frozen, complete signal; no post-toSignal mutation ─────────────

test("A: engine.predict returns a frozen, complete signal (old mutation failure is gone)", () => {
  const engine = makeEngine(makeModel((t) => validOutput(t)));
  const targetRoundId = "9001";

  // OLD BEHAVIOR: this call threw TypeError ("object is not extensible")
  // when the engine tried to write featurePath onto the frozen signal.
  const signal = engine.predict({
    priorRounds: historyRounds(30),
    targetRoundId,
    timestamp: new Date().toISOString(),
    target: 1.3,
    modelName: "stub",
  });

  assert.ok(Object.isFrozen(signal), "signal must be frozen");
  assert.ok(Object.isFrozen(signal.reasoning), "reasoning array must be frozen");
  assert.ok(Object.isFrozen(signal.featureSummary), "featureSummary must be frozen");

  // Immutability invariant: any downstream mutation attempt throws.
  assert.throws(
    () => {
      (signal as unknown as Record<string, unknown>).featurePath = "V2_INCREMENTAL";
    },
    /not extensible|not writable|cannot assign|readonly/i,
    "mutating the frozen signal must throw",
  );

  // The signal is COMPLETE at construction — featurePath is already set.
  assert.equal(signal.featurePath, "V1_FALLBACK");
  assert.equal(signal.targetRoundId, targetRoundId);
  assert.ok(signal.predictionId.startsWith("stub-"), "predictionId must come from the model output");
});

test("A2: toSignal constructs the complete object and freezes only after construction", () => {
  const output = validOutput(2.0);
  const signal = toSignal(output, { featurePath: "V2_INCREMENTAL", targetRoundId: "777" });

  assert.ok(Object.isFrozen(signal));
  assert.equal(signal.featurePath, "V2_INCREMENTAL");
  assert.equal(signal.targetRoundId, "777");
  assert.equal(signal.featureVersion, "stub-v1");
  assert.equal(signal.modelVersion, "stub@1.0.0");
  // validatePredictionSignal requires frozen — must pass on a healthy signal.
  validatePredictionSignal(signal);
});

// ── Test B: invalid prediction output fails at prediction_output_validation ─

test("B: malformed model output (probability undefined) fails at prediction_output_validation", () => {
  const engine = makeEngine(
    makeModel((t) => ({ ...validOutput(t), probability: undefined as unknown as number })),
  );

  let caught: unknown;
  try {
    engine.predict({
      priorRounds: historyRounds(30),
      targetRoundId: "9002",
      timestamp: new Date().toISOString(),
      target: 1.3,
      modelName: "stub",
    });
  } catch (e) {
    caught = e;
  }

  assert.ok(caught, "expected predict() to throw");
  const err = caught as PredictionOutputValidationError;
  assert.ok(
    err instanceof PredictionOutputValidationError,
    `expected PredictionOutputValidationError, got ${err?.name}: ${err?.message}`,
  );
  assert.equal(err.stage, "prediction_output_validation");
  assert.equal(err.invalidField, "probability");
  assert.equal(err.expectedType, "finite number in [0,1]");
  // The whole point: it must NOT be misreported as a conversion failure.
  assert.notEqual(err.stage, "signal_conversion");
  assert.notEqual(err.name, "TypeError");
});

test("B2: non-finite confidence fails at prediction_output_validation with field diagnostics", () => {
  const engine = makeEngine(
    makeModel((t) => ({ ...validOutput(t), confidence: Number.NaN })),
  );
  assert.throws(
    () =>
      engine.predict({
        priorRounds: historyRounds(30),
        targetRoundId: "9003",
        timestamp: new Date().toISOString(),
        target: 1.3,
        modelName: "stub",
      }),
    (err: unknown) => {
      const e = err as PredictionOutputValidationError;
      return e instanceof PredictionOutputValidationError
        && e.stage === "prediction_output_validation"
        && e.invalidField === "confidence";
    },
  );
});

test("B3: validatePredictionOutput rejects null/missing/malformed outputs without coercion", () => {
  // null
  assert.throws(
    () => validatePredictionOutput(null),
    (e: unknown) => (e as PredictionOutputValidationError).invalidField === "prediction",
  );
  // missing model identity
  assert.throws(
    () => validatePredictionOutput({ ...validOutput(), model: undefined as unknown as PredictionOutput["model"] }),
    (e: unknown) => (e as PredictionOutputValidationError).invalidField === "model",
  );
  // non-finite target
  assert.throws(
    () => validatePredictionOutput({ ...validOutput(), target: Number.POSITIVE_INFINITY as unknown as ThresholdTarget }),
    (e: unknown) => (e as PredictionOutputValidationError).invalidField === "target",
  );
  // bad expiresAt
  assert.throws(
    () => validatePredictionOutput({ ...validOutput(), expiresAt: "not-a-date" }),
    (e: unknown) => (e as PredictionOutputValidationError).invalidField === "expiresAt",
  );
  // healthy output passes
  validatePredictionOutput(validOutput());
});

// ── Test C: canonical signal schema ─────────────────────────────────────────

test("C: constructed signal carries the full canonical schema", () => {
  const engine = makeEngine(makeModel((t) => validOutput(t)));
  const signal = engine.predict({
    priorRounds: historyRounds(30),
    targetRoundId: "9004",
    timestamp: new Date().toISOString(),
    target: 1.3,
    modelName: "stub",
  });

  for (const field of [
    "predictionId",
    "timestamp",
    "modelVersion",
    "featureVersion",
    "featurePath",
    "targetRoundId",
    "target",
    "score",
    "probability",
    "confidence",
    "regimeId",
    "dataQuality",
    "reasoning",
    "expiresAt",
    "featureSummary",
  ] as const) {
    assert.ok(field in signal, `signal.${field} must exist (canonical schema)`);
    assert.notEqual((signal as unknown as Record<string, unknown>)[field], undefined, `signal.${field} must not be undefined`);
  }
  validatePredictionSignal(signal);
});

test("C2: signal missing featurePath fails signal_validation (schema is enforced)", () => {
  const output = validOutput(1.3);
  const malformed = {
    predictionId: output.predictionId,
    timestamp: output.timestamp,
    modelVersion: "stub@1.0.0",
    featureVersion: "stub-v1",
    // featurePath intentionally omitted
    targetRoundId: "9005",
    target: output.target,
    score: output.score,
    probability: output.probability,
    confidence: output.confidence,
    regimeId: null,
    dataQuality: output.dataQuality,
    reasoning: output.reasoning,
    expiresAt: output.expiresAt,
    featureSummary: output.featureSummary,
  };
  assert.throws(
    () => validatePredictionSignal(malformed),
    (e: unknown) => {
      const err = e as PredictionSignalValidationError;
      return err instanceof PredictionSignalValidationError
        && err.stage === "signal_validation"
        && err.invalidField === "featurePath";
    },
  );
});

test("C3: summarizePredictionOutput is bounded — never leaks full model objects", () => {
  const output = validOutput();
  const summary = summarizePredictionOutput(output);
  assert.equal(summary.predictionId, output.predictionId);
  assert.ok(typeof summary.model === "object" && summary.model !== null);
  assert.equal((summary.model as Record<string, unknown>).name, "stub");
  assert.ok(!("featureSummary" in summary), "summary must not include featureSummary");
  assert.ok(!("reasoning" in summary), "summary must not include reasoning");
  // non-object input summarises safely
  assert.deepEqual(summarizePredictionOutput(undefined), { present: false, type: "undefined" });
});
