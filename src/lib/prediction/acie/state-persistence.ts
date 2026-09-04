/**
 * Postgres persistence for ACIE OnlineAdaptiveState.
 *
 * Spec: ACIE_Combined_Upgrade_Recommendations.md §5.1
 *
 * - Load latest snapshot on boot (best-effort; failure → fresh state)
 * - Save after successful onCrash (async, non-blocking, must never fail the hot path)
 * - Snapshot stays small (JSONB of online state + crashPoints + consecutiveLosses)
 */
import { getSql, type Sql } from "@/lib/db";
import { getLogger } from "@/lib/observability/logger";
import type { OnlineAdaptiveState } from "./online-state";

const logger = getLogger("acie-state-persistence");

export interface AciePersistedSnapshot {
  version: 1;
  savedAt: string;
  online: OnlineAdaptiveState;
  crashPoints: number[];
  consecutiveLosses: number;
}

export type AcieSnapshotSource = {
  exportSnapshot: () => {
    online: OnlineAdaptiveState;
    crashPoints: number[];
    consecutiveLosses: number;
  };
  importSnapshot: (snap: {
    online?: OnlineAdaptiveState;
    crashPoints?: number[];
    consecutiveLosses?: number;
  }) => void;
};

/** Load the most recent snapshot into the engine. Returns true if restored. */
export async function loadAcieStateFromDb(
  acie: AcieSnapshotSource,
  getSqlFn: () => Promise<Sql> = getSql,
): Promise<boolean> {
  try {
    const sql = await getSqlFn();
    const rows = await sql<{
      payload: AciePersistedSnapshot | string;
      observation_count: number;
    }>`
      SELECT payload, observation_count
      FROM acie_online_state
      ORDER BY created_at DESC
      LIMIT 1
    `;
    if (rows.length === 0) {
      logger.info({ component: "acie-state-persistence" }, "no prior ACIE snapshot");
      return false;
    }
    const raw = rows[0]!.payload;
    const snap: AciePersistedSnapshot =
      typeof raw === "string" ? (JSON.parse(raw) as AciePersistedSnapshot) : raw;
    if (!snap?.online) return false;
    acie.importSnapshot({
      online: snap.online,
      crashPoints: snap.crashPoints ?? [],
      consecutiveLosses: snap.consecutiveLosses ?? 0,
    });
    logger.info(
      {
        component: "acie-state-persistence",
        observationCount: rows[0]!.observation_count,
        crashPoints: snap.crashPoints?.length ?? 0,
      },
      "ACIE online state restored from Postgres",
    );
    return true;
  } catch (e) {
    logger.warn(
      { component: "acie-state-persistence", error: String(e) },
      "ACIE state load failed; continuing with fresh state",
    );
    return false;
  }
}

/** Best-effort save. Never throws to caller. */
export async function saveAcieStateToDb(
  acie: AcieSnapshotSource,
  getSqlFn: () => Promise<Sql> = getSql,
): Promise<boolean> {
  try {
    const snap = acie.exportSnapshot();
    const payload: AciePersistedSnapshot = {
      version: 1,
      savedAt: new Date().toISOString(),
      online: snap.online,
      crashPoints: snap.crashPoints.slice(-2000),
      consecutiveLosses: snap.consecutiveLosses,
    };
    const sql = await getSqlFn();
    await sql`
      INSERT INTO acie_online_state (
        snapshot_version, observation_count, ewma_hit_rate, ewma_brier,
        last_drift_detected, consecutive_losses, payload
      ) VALUES (
        1,
        ${snap.online.observationCount ?? 0},
        ${snap.online.ewmaHitRate ?? null},
        ${snap.online.ewmaBrier ?? null},
        ${Boolean(snap.online.lastDrift?.detected)},
        ${snap.consecutiveLosses},
        ${JSON.stringify(payload)}::jsonb
      )
    `;
    // Prune old rows (keep last 20)
    await sql`
      DELETE FROM acie_online_state
      WHERE id NOT IN (
        SELECT id FROM acie_online_state ORDER BY created_at DESC LIMIT 20
      )
    `;
    return true;
  } catch (e) {
    logger.warn(
      { component: "acie-state-persistence", error: String(e) },
      "ACIE state save failed (non-blocking)",
    );
    return false;
  }
}

/** Fire-and-forget save suitable for the hot path. */
export function scheduleAcieStateSave(acie: AcieSnapshotSource): void {
  const run = () => {
    void saveAcieStateToDb(acie).catch(() => undefined);
  };
  if (typeof setImmediate === "function") setImmediate(run);
  else setTimeout(run, 0);
}
