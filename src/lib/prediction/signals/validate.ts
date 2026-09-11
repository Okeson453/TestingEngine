/**
 * Strict runtime validation for the prediction signal pipeline.
 *
 * Flow (see prediction-engine.ts):
 *
 *   PredictionOutput
 *     → validatePredictionOutput()      [stage: prediction_output_validation]
 *     → toSignal(output, context)       [stage: signal_conversion]
 *     → Object.freeze() (inside toSignal, after complete construction)
 *     → validatePredictionSignal()      [stage: signal_validation]
 *     → persistence
 *     → outbox
 *     → Telegram
 *
 * Rules:
 *   - No silent coercion, no fake defaults. Invalid output fails loudly
 *     with a typed error carrying field/expectedType/actualType.
 *   - Never dump raw model objects into logs — use summarizePredictionOutput()
 *     for a bounded, safe diagnostic summary.
 */
import type { FeaturePath, PredictionOutput, PredictionSignal } from '../types.ts';

const FEATURE_PATHS: readonly FeaturePath[] = ['V2_INCREMENTAL', 'V1_FALLBACK', 'ACIE_STATE'];

/** Runtime type name of an arbitrary value (for diagnostics). */
export function typeName(v: unknown): string {
  if (v === null) return 'null';
  if (Array.isArray(v)) return 'array';
  if (typeof v === 'number') return Number.isFinite(v) ? 'finite number' : 'non-finite number';
  return typeof v;
}

/** Diagnostic fields attached to a pipeline validation failure. */
export interface ValidationFailureOptions {
  invalidField?: string;
  expectedType?: string;
  actualType?: string;
  context?: Record<string, unknown>;
}

/** Base for typed pipeline validation errors. */
export class PipelineValidationError extends Error {
  readonly stage: string;
  readonly invalidField: string | null;
  readonly expectedType: string | null;
  readonly actualType: string | null;
  readonly failureReason: string;
  readonly context: Record<string, unknown>;

  constructor(
    stage: string,
    failureReason: string,
    opts: ValidationFailureOptions = {},
  ) {
    const parts: string[] = [`[${stage}] ${failureReason}`];
    if (opts.invalidField) {
      const bits = [`field=${opts.invalidField}`];
      if (opts.expectedType) bits.push(`expected=${opts.expectedType}`);
      if (opts.actualType !== undefined) bits.push(`actual=${opts.actualType}`);
      parts.push(`(${bits.join(', ')})`);
    }
    super(parts.join(' '));
    this.name = new.target.name;
    this.stage = stage;
    this.failureReason = failureReason;
    this.invalidField = opts.invalidField ?? null;
    this.expectedType = opts.expectedType ?? null;
    this.actualType = opts.actualType ?? typeName(opts.context?.value);
    this.context = opts.context ?? {};
  }
}

/** Thrown when a model's PredictionOutput is malformed (stage: prediction_output_validation). */
export class PredictionOutputValidationError extends PipelineValidationError {
  constructor(failureReason: string, opts: ValidationFailureOptions = {}) {
    super('prediction_output_validation', failureReason, opts);
  }
}

/** Thrown when a constructed PredictionSignal violates the canonical schema (stage: signal_validation). */
export class PredictionSignalValidationError extends PipelineValidationError {
  constructor(failureReason: string, opts: ValidationFailureOptions = {}) {
    super('signal_validation', failureReason, opts);
  }
}

function isPlainObject(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v);
}

function nonEmptyString(v: unknown): boolean {
  return typeof v === 'string' && v.trim().length > 0;
}

function finiteNumber(v: unknown): v is number {
  return typeof v === 'number' && Number.isFinite(v);
}

function probabilityRange(v: unknown): boolean {
  return finiteNumber(v) && (v as number) >= 0 && (v as number) <= 1;
}

function parseableDate(v: unknown): boolean {
  if (!nonEmptyString(v)) return false;
  return Number.isFinite(new Date(v as string).getTime());
}

/**
 * Bounded, safe summary of a PredictionOutput (or anything resembling one)
 * for structured diagnostics. Never includes the full featureSummary or
 * reasoning arrays — only scalar identity fields.
 */
export function summarizePredictionOutput(output: unknown): Record<string, unknown> {
  if (!isPlainObject(output)) return { present: false, type: typeName(output) };
  const model = isPlainObject(output.model) ? output.model : null;
  return {
    present: true,
    predictionId: typeof output.predictionId === 'string' ? output.predictionId : null,
    target: finiteNumber(output.target) ? output.target : null,
    probability: finiteNumber(output.probability) ? output.probability : null,
    confidence: finiteNumber(output.confidence) ? output.confidence : null,
    score: finiteNumber(output.score) ? output.score : null,
    dataQuality: finiteNumber(output.dataQuality) ? output.dataQuality : null,
    model: model
      ? {
          name: typeof model.name === 'string' ? model.name : null,
          version: typeof model.version === 'string' ? model.version : null,
          featureVersion: typeof model.featureVersion === 'string' ? model.featureVersion : null,
        }
      : null,
    timestamp: typeof output.timestamp === 'string' ? output.timestamp : null,
  };
}

/**
 * Strict validation of a model's PredictionOutput BEFORE toSignal() sees it.
 * Throws PredictionOutputValidationError on the first violation. Never
 * coerces, never defaults.
 */
export function validatePredictionOutput(output: unknown): asserts output is PredictionOutput {
  // Narrow once via a never-returning fail() so every check below is typed.
  const obj: Record<string, unknown> = isPlainObject(output)
    ? output
    : failOutput(output, 'prediction output object is missing or not an object', 'prediction', 'object', output);

  const fail = (reason: string, field: string, expected: string, actual: unknown): never =>
    failOutput(output, reason, field, expected, actual);

  if (!nonEmptyString(obj.predictionId)) {
    fail('required predictionId missing', 'predictionId', 'non-empty string', obj.predictionId);
  }

  // Model identity block
  const model = isPlainObject(obj.model)
    ? obj.model
    : (fail('model metadata missing or not an object', 'model', 'ModelIdentity object', obj.model) as never);
  for (const field of ['name', 'version', 'featureVersion', 'targetVersion'] as const) {
    if (!nonEmptyString(model[field])) {
      fail(`model.${field} missing`, `model.${field}`, 'non-empty string', model[field]);
    }
  }

  // Target / threshold
  if (!finiteNumber(obj.target) || (obj.target as number) <= 1) {
    fail('target threshold missing or not a finite multiplier > 1', 'target', 'finite number > 1', obj.target);
  }

  // Probability / confidence / score / dataQuality
  if (!probabilityRange(obj.probability)) {
    fail('probability missing or outside [0,1]', 'probability', 'finite number in [0,1]', obj.probability);
  }
  if (!probabilityRange(obj.confidence)) {
    fail('confidence missing or outside [0,1]', 'confidence', 'finite number in [0,1]', obj.confidence);
  }
  if (!finiteNumber(obj.score)) {
    fail('score missing or not finite', 'score', 'finite number', obj.score);
  }
  if (!finiteNumber(obj.dataQuality)) {
    fail('dataQuality missing or not finite', 'dataQuality', 'finite number', obj.dataQuality);
  }

  // Regime: null or object with a string id
  if (obj.regime !== null && obj.regime !== undefined) {
    const regime = isPlainObject(obj.regime) ? obj.regime : null;
    if (!regime || !nonEmptyString(regime.id)) {
      fail('regime present but malformed (needs string id)', 'regime.id', 'non-empty string', obj.regime);
    }
  }

  // reasoning / featureSummary
  if (!Array.isArray(obj.reasoning)) {
    fail('reasoning missing or not an array', 'reasoning', 'string[]', obj.reasoning);
  }
  if (!isPlainObject(obj.featureSummary)) {
    fail('featureSummary missing or not an object', 'featureSummary', 'Record<string, number>', obj.featureSummary);
  }

  // Timestamps
  if (!parseableDate(obj.timestamp)) {
    fail('timestamp missing or not a parseable date string', 'timestamp', 'ISO-8601 string', obj.timestamp);
  }
  if (!parseableDate(obj.expiresAt)) {
    fail('expiresAt missing or not a parseable date string', 'expiresAt', 'ISO-8601 string', obj.expiresAt);
  }
}

function failOutput(
  output: unknown,
  reason: string,
  field: string,
  expected: string,
  actual: unknown,
): never {
  throw new PredictionOutputValidationError(reason, {
    invalidField: field,
    expectedType: expected,
    actualType: typeName(actual),
    context: { prediction: summarizePredictionOutput(output), value: actual },
  });
}

/**
 * Validate a constructed PredictionSignal against the canonical schema
 * (called AFTER Object.freeze in the engine flow). Throws
 * PredictionSignalValidationError on the first violation.
 */
export function validatePredictionSignal(signal: unknown): asserts signal is PredictionSignal {
  const obj: Record<string, unknown> = isPlainObject(signal)
    ? signal
    : failSignal(signal, 'signal missing or not an object', 'signal', 'object', signal);

  const fail = (reason: string, field: string, expected: string, actual: unknown): never =>
    failSignal(signal, reason, field, expected, actual);

  if (!nonEmptyString(obj.predictionId)) {
    fail('predictionId missing', 'predictionId', 'non-empty string', obj.predictionId);
  }
  if (!nonEmptyString(obj.modelVersion)) {
    fail('modelVersion missing', 'modelVersion', 'non-empty string', obj.modelVersion);
  }
  if (!nonEmptyString(obj.featureVersion)) {
    fail('featureVersion missing', 'featureVersion', 'non-empty string', obj.featureVersion);
  }
  if (!FEATURE_PATHS.includes(obj.featurePath as FeaturePath)) {
    fail(
      'featurePath missing or not a known feature path',
      'featurePath',
      "'V2_INCREMENTAL' | 'V1_FALLBACK' | 'ACIE_STATE'",
      obj.featurePath,
    );
  }
  if (!nonEmptyString(obj.targetRoundId)) {
    fail('targetRoundId missing', 'targetRoundId', 'non-empty string', obj.targetRoundId);
  }
  if (!finiteNumber(obj.target) || (obj.target as number) <= 1) {
    fail('target missing or not a finite multiplier > 1', 'target', 'finite number > 1', obj.target);
  }
  if (!probabilityRange(obj.probability)) {
    fail('probability outside [0,1]', 'probability', 'finite number in [0,1]', obj.probability);
  }
  if (!probabilityRange(obj.confidence)) {
    fail('confidence outside [0,1]', 'confidence', 'finite number in [0,1]', obj.confidence);
  }
  if (!finiteNumber(obj.score)) {
    fail('score not finite', 'score', 'finite number', obj.score);
  }
  if (!finiteNumber(obj.dataQuality)) {
    fail('dataQuality not finite', 'dataQuality', 'finite number', obj.dataQuality);
  }
  if (obj.regimeId !== null && !nonEmptyString(obj.regimeId)) {
    fail('regimeId present but not a non-empty string or null', 'regimeId', 'string | null', obj.regimeId);
  }
  if (!Array.isArray(obj.reasoning)) {
    fail('reasoning not an array', 'reasoning', 'readonly string[]', obj.reasoning);
  }
  if (!isPlainObject(obj.featureSummary)) {
    fail('featureSummary not an object', 'featureSummary', 'Readonly<Record<string, number>>', obj.featureSummary);
  }
  if (!parseableDate(obj.timestamp)) {
    fail('timestamp not a parseable date', 'timestamp', 'ISO-8601 string', obj.timestamp);
  }
  if (!parseableDate(obj.expiresAt)) {
    fail('expiresAt not a parseable date', 'expiresAt', 'ISO-8601 string', obj.expiresAt);
  }
  if (!Object.isFrozen(signal)) {
    fail('signal is not frozen — immutability invariant violated', 'signal', 'frozen object', 'not frozen');
  }
}

function failSignal(
  signal: unknown,
  reason: string,
  field: string,
  expected: string,
  actual: unknown,
): never {
  throw new PredictionSignalValidationError(reason, {
    invalidField: field,
    expectedType: expected,
    actualType: typeName(actual),
    context: {
      predictionId: isPlainObject(signal) ? signal.predictionId : null,
      value: actual,
    },
  });
}
