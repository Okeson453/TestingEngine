/**
 * P0 regression: prediction outbox must be durably enqueued before
 * onGameEndPredict returns kind="predicted", and must not use fire-and-forget
 * persist that races BG(N+1) / temporal gates.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { readFileSync } from "node:fs";
import { join } from "node:path";

describe("durable prediction handoff ordering (P0)", () => {
  const predictorSrc = readFileSync(join(__dirname, "predictor.ts"), "utf8");
  const attemptSrc = readFileSync(join(__dirname, "prediction-attempt.ts"), "utf8");
  const notifSrc = readFileSync(join(__dirname, "notification-worker.ts"), "utf8");
  const handlersSrc = readFileSync(
    join(__dirname, "..", "events", "game-event-handlers.ts"),
    "utf8",
  );

  it("onGameEndPredict awaits runInTransaction for pending+outbox before predicted return", () => {
    const start = predictorSrc.indexOf("export async function onGameEndPredict(");
    expect(start).toBeGreaterThan(-1);
    const body = predictorSrc.slice(start);
    // No fire-and-forget persist
    expect(body).not.toMatch(/void\s+persistPromise/);
    // Durable handoff language present
    expect(body).toContain("awaiting durable outbox handoff");
    expect(body).toContain("notification_outbox");
    // Failure path does not claim predicted
    expect(body).toContain('kind: "persist_failed"');
    // Priority elevated so dispatcher prefers prediction over validation
    expect(body).toMatch(/'pending',\s*10/);
  });

  it("onGameEndPredict durable handoff defaults to the CRITICAL pool, not general", () => {
    // P0 regression (forensic timing investigation): this hot path used to
    // default to getSql() (general/dashboard pool) despite the dual-pool
    // split existing specifically to reserve capacity for it. Guards
    // against silently reintroducing that regression.
    const start = predictorSrc.indexOf("export async function onGameEndPredict(");
    expect(start).toBeGreaterThan(-1);
    const handoffStart = predictorSrc.indexOf("P0 DURABLE HANDOFF", start);
    expect(handoffStart).toBeGreaterThan(start);
    const handoffBody = predictorSrc.slice(handoffStart, handoffStart + 1500);
    expect(handoffBody).toContain("deps.getSqlFn ?? getPredictionPersistSql");
    expect(handoffBody).not.toMatch(/deps\.getSqlFn\s*\?\?\s*getSql[^A-Za-z]/);
  });

  it("attemptNPlusOnePrediction only marks attempted when kind is predicted", () => {
    expect(attemptSrc).toContain('result?.kind === "predicted"');
    expect(attemptSrc).toContain("durable outbox enqueued");
  });

  it("dispatcher claims prediction type before other kinds, then priority DESC", () => {
    // LANE SEPARATION (remediation plan §2/§11/§12): the prediction lane is
    // its own claim (`type = 'prediction'`), the normal lane claims only
    // non-predictions (`type <> 'prediction'`), and the drain loop runs the
    // prediction lane INLINE while the normal lane runs detached. A new
    // prediction therefore never depends on the age, size or Telegram
    // latency of a running background batch — a strictly stronger guarantee
    // than the old prediction-first ORDER BY inside one shared claim.
    expect(notifSrc).toContain("type = 'prediction'");
    expect(notifSrc).toContain("type <> 'prediction'");
    expect(notifSrc).toMatch(/await this\.processLane\("prediction"\)/);
    expect(notifSrc).toMatch(/runBackgroundDetached\(\)/);
    expect(notifSrc).toMatch(/priority DESC|priority desc/i);
  });

  it("bgHandler still expires undelivered prediction outbox for target on start", () => {
    expect(handlersSrc).toContain("expired_late_signal: target round started (BG received)");
    expect(handlersSrc).toContain("type = 'prediction'");
    expect(handlersSrc).toContain("target_game_id");
  });

  it("validator validation outbox uses lower priority than prediction", () => {
    const v = readFileSync(join(__dirname, "validator.ts"), "utf8");
    // validation priority 2 (window covers the metadata block between the
    // 'validation' type and the status/priority columns)
    expect(v).toMatch(/'validation'[\s\S]{0,900}'pending',\s*2/);
  });

  it("ED receipt instant threads to outbox metadata; message names source/target rounds", () => {
    // ED RECEIPT ANCHOR: ed_received_at starts the measured critical path.
    // The ED handler must capture receipt at entry and thread it through the
    // attempt chain into outbox metadata — a dropped link here makes the
    // ED→Telegram latency unmeasurable.
    expect(handlersSrc).toContain("const edReceivedAt = new Date().toISOString();");
    expect(handlersSrc).toMatch(/source: "ED"[\s\S]{0,200}edReceivedAt/);
    // Attempt layer forwards it into predictor deps (recovery leaves it undefined).
    expect(attemptSrc).toContain("edReceivedAt: input.edReceivedAt");
    // Predictor persists it in outbox metadata for end-to-end timeline queries.
    expect(predictorSrc).toContain("edReceivedAt: deps.edReceivedAt ?? null");
    // Message contract: explicit target with bet-now wording + explicit
    // completed source round (N never masquerades as the prediction target).
    expect(predictorSrc).toContain("(bet NOW — round starting)");
    expect(predictorSrc).toContain(
      "`Source round: ${gameId} completed — predicting round ${targetGameId}`",
    );
  });
});
