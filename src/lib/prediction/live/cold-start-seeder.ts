/**
 * Cold-start seeder for the prediction pipeline.
 *
 * Spec: UNIFIED_PREDICTION_PIPELINE_SOLUTION.md §7.6
 *
 * On boot, if `crash_rounds` has fewer than `MIN_HISTORY` rows, fetch up to
 * `MAX_PAGES` of history from BC.Game's REST endpoint and insert the missing
 * rounds. Never generates predictions. The seed is what makes the strict
 * causal window in `predictor.onGameStart` (`crashed_at < $beganAt`) have
 * enough rows to operate on when the database is freshly created.
 *
 * Idempotent: every INSERT uses `ON CONFLICT (game_id) DO NOTHING` via
 * `insertNewRounds`, so repeated boots converge to the same set of rows.
 */
import { getSql } from "@/lib/db";
import { fetchCrashHistory } from "@/lib/crash/fetch-bc";
import { insertNewRounds } from "@/lib/crash/ingest";
import { getLogger } from "@/lib/observability/logger";

const logger = getLogger("cold-start-seeder");

/** Minimum history required before the predictor will run. */
export const MIN_HISTORY = 100;
/** Maximum pages to fetch in a single boot pass. Each page is ~20 rows. */
export const MAX_PAGES = 5;
/** Hard timeout for the entire seeder pass; abort and proceed if exceeded. */
export const SEED_TIMEOUT_MS = 10_000;

export interface SeedResult {
  alreadySeeded: boolean;
  initialCount: number;
  finalCount: number;
  insertedTotal: number;
  pagesFetched: number;
  elapsedMs: number;
  timedOut: boolean;
}

/**
 * Run the cold-start seeder once. Returns a `SeedResult` describing the
 * pass. The function never throws — every failure is captured in the result
 * and logged, so the worker can continue to start even if BC.Game is
 * unreachable on boot.
 */
export async function runColdStartSeeder(opts?: {
  minHistory?: number;
  maxPages?: number;
  timeoutMs?: number;
  fetchHistory?: (pages: number) => ReturnType<typeof fetchCrashHistory>;
  now?: () => number;
}): Promise<SeedResult> {
  const minHistory = opts?.minHistory ?? MIN_HISTORY;
  const maxPages = opts?.maxPages ?? MAX_PAGES;
  const timeoutMs = opts?.timeoutMs ?? SEED_TIMEOUT_MS;
  const fetchHistory = opts?.fetchHistory ?? fetchCrashHistory;
  const now = opts?.now ?? Date.now;
  const t0 = now();

  const sql = await getSql();
  const initialRows = await sql<{ count: number }>`
    select count(*)::int as count from crash_rounds
  `;
  const initialCount = initialRows[0]?.count ?? 0;
  if (initialCount >= minHistory) {
    return {
      alreadySeeded: true,
      initialCount,
      finalCount: initialCount,
      insertedTotal: 0,
      pagesFetched: 0,
      elapsedMs: now() - t0,
      timedOut: false,
    };
  }

  let insertedTotal = 0;
  let pagesFetched = 0;
  let finalCount = initialCount;
  let timedOut = false;

  for (let page = 0; page < maxPages; page += 1) {
    if (now() - t0 > timeoutMs) {
      timedOut = true;
      logger.warn(
        { component: "ColdStartSeeder", elapsedMs: now() - t0, target: minHistory },
        "seed timeout reached; proceeding with partial history",
      );
      break;
    }
    let fetched;
    try {
      fetched = await fetchHistory(1);
    } catch (e) {
      logger.warn(
        { component: "ColdStartSeeder", page, error: String(e) },
        "history fetch failed; stopping seed",
      );
      break;
    }
    if (fetched.length === 0) {
      break;
    }
    pagesFetched += 1;
    try {
      const ins = await insertNewRounds(fetched);
      insertedTotal += ins.inserted;
    } catch (e) {
      logger.warn(
        { component: "ColdStartSeeder", page, error: String(e) },
        "insertNewRounds failed; stopping seed",
      );
      break;
    }
    const after = await sql<{ count: number }>`
      select count(*)::int as count from crash_rounds
    `;
    finalCount = after[0]?.count ?? finalCount;
    if (finalCount >= minHistory) {
      break;
    }
  }

  logger.info(
    {
      component: "ColdStartSeeder",
      initialCount,
      finalCount,
      insertedTotal,
      pagesFetched,
      elapsedMs: now() - t0,
      timedOut,
    },
    "cold-start seed complete",
  );
  return {
    alreadySeeded: false,
    initialCount,
    finalCount,
    insertedTotal,
    pagesFetched,
    elapsedMs: now() - t0,
    timedOut,
  };
}
