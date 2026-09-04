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

export type AcieRestoreReason =
  | "restored"
  | "state_missing"
  | "state_corrupt"
  | "schema_mismatch"
  | "db_error"
  | "deserialization_error";

export interface AcieRestoreResult {
  restored: boolean;
  reason: AcieRestoreReason;
  observationCount?: number;
  crashPoints?: number;
  error?: string;
}

/** Load the most recent snapshot. Always returns a classified reason. */
export async function loadAcieStateFromDb(
  acie: AcieSnapshotSource,
  getSqlFn: () => Promise<Sql> = getSql,
): Promise<AcieRestoreResult> {
  try {
    const sql = await getSqlFn();
    let rows: Array<{ payload: AciePersistedSnapshot | string; observation_count: number }>;
    try {
      rows = await sql<{
        payload: AciePersistedSnapshot | string;
        observation_count: number;
      }>`
        SELECT payload, observation_count
        FROM acie_online_state
        ORDER BY created_at DESC
        LIMIT 1
      `;
    } catch (e) {
      const msg = String(e);
      const reason: AcieRestoreReason =
        msg.includes("does not exist") || msg.includes("relation") || msg.includes("42P01")
          ? "schema_mismatch"
          : "db_error";
      logger.warn(
        { component: "acie-state-persistence", reason, error: msg },
        `ACIE state load failed (${reason})`,
      );
      return { restored: false, reason, error: msg };
    }
    if (rows.length === 0) {
      logger.info(
        { component: "acie-state-persistence", reason: "state_missing" },
        "no prior ACIE snapshot (state_missing)",
      );
      return { restored: false, reason: "state_missing" };
    }
    const raw = rows[0]!.payload;
    let snap: AciePersistedSnapshot;
    try {
      snap =
        typeof raw === "string" ? (JSON.parse(raw) as AciePersistedSnapshot) : raw;
    } catch (e) {
      logger.warn(
        {
          component: "acie-state-persistence",
          reason: "deserialization_error",
          error: String(e),
        },
        "ACIE snapshot JSON parse failed",
      );
      return { restored: false, reason: "deserialization_error", error: String(e) };
    }
    if (!snap?.online || typeof snap.online !== "object") {
      logger.warn(
        { component: "acie-state-persistence", reason: "state_corrupt" },
        "ACIE snapshot missing online payload",
      );
      return { restored: false, reason: "state_corrupt" };
    }
    try {
      acie.importSnapshot({
        online: snap.online,
        crashPoints: snap.crashPoints ?? [],
        consecutiveLosses: snap.consecutiveLosses ?? 0,
      });
    } catch (e) {
      logger.warn(
        {
          component: "acie-state-persistence",
          reason: "state_corrupt",
          error: String(e),
        },
        "ACIE importSnapshot rejected payload",
      );
      return { restored: false, reason: "state_corrupt", error: String(e) };
    }
    const observationCount = rows[0]!.observation_count;
    const crashPoints = snap.crashPoints?.length ?? 0;
    logger.info(
      {
        component: "acie-state-persistence",
        reason: "restored",
        observationCount,
        crashPoints,
      },
      "ACIE online state restored from Postgres",
    );
    return { restored: true, reason: "restored", observationCount, crashPoints };
  } catch (e) {
    logger.warn(
      {
        component: "acie-state-persistence",
        reason: "db_error",
        error: String(e),
      },
      "ACIE state load failed (db_error)",
    );
    return { restored: false, reason: "db_error", error: String(e) };
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
      crashPoints: snap.crashPoints.slice(-2000), // ACIE_MAX_HISTORY default
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
