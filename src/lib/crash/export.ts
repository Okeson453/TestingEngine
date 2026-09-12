/**
 * Full-history crash_rounds export (CSV + JSON).
 *
 * Design constraints (directive, sep 12):
 *  - Export from the AUTHORITATIVE crash_rounds table, never the 60-round
 *    chart window or the STATS_LIMIT dashboard slice.
 *  - Batched KEYSET reads (id > last ORDER BY id LIMIT n): every batch is a
 *    short index-scan query that holds a general-pool client for
 *    milliseconds. No full-table read, no OFFSET decay, no connection
 *    exhaustion, no competition with the live prediction pipeline (the
 *    hot path runs on the critical pool / pinned lanes).
 *  - The response is a raw ReadableStream Response (TanStack Start passes
 *    `instanceof Response` straight through), so the dataset is assembled
 *    in bounded-size chunks server-side instead of one giant string.
 *  - Deterministic ordering by id (insertion order — game_id correlates);
 *    every stored column is preserved. CSV keeps the raw DB text for
 *    numeric(12,4) multipliers (e.g. "1.3000"); JSON carries both the raw
 *    text (`multiplierRaw`) and its numeric value (`multiplier`).
 */

export type ExportFormat = "csv" | "json";

export type ExportBatchRow = {
  id: number | string;
  game_id: string;
  /** Raw DB text from numeric(12,4) — e.g. "1.3000". */
  multiplier: string | number;
  hash: string | null;
  salt: string | null;
  began_at: string | Date | null;
  crashed_at: string | Date;
  ingested_at: string | Date | null;
};

/** Fetch all rows with id > afterId, ordered by id ASC, up to limit. */
export type ExportBatchFetcher = (
  afterId: number,
  limit: number,
) => Promise<ExportBatchRow[]>;

export const EXPORT_BATCH_SIZE = 2_000;

export const CSV_COLUMNS = [
  "id",
  "game_id",
  "multiplier",
  "hash",
  "salt",
  "began_at",
  "crashed_at",
  "ingested_at",
] as const;

function csvCell(value: string | number | null | undefined): string {
  if (value == null) return "";
  const text = String(value);
  if (/[",\n\r]/.test(text)) {
    return `"${text.replace(/"/g, '""')}"`;
  }
  return text;
}

function toIsoOrNull(value: string | Date | null | undefined): string | null {
  if (value == null || value === "") return null;
  if (value instanceof Date) return value.toISOString();
  const asDate = new Date(value);
  return Number.isNaN(asDate.getTime()) ? String(value) : asDate.toISOString();
}

/** CSV text for one batch of rows (each row newline-terminated). */
export function renderCsvRows(rows: ExportBatchRow[]): string {
  let out = "";
  for (const r of rows) {
    out +=
      [
        csvCell(r.id),
        csvCell(r.game_id),
        csvCell(r.multiplier),
        csvCell(r.hash),
        csvCell(r.salt),
        csvCell(toIsoOrNull(r.began_at)),
        csvCell(toIsoOrNull(r.crashed_at)),
        csvCell(toIsoOrNull(r.ingested_at)),
      ].join(",") + "\n";
  }
  return out;
}

/** One JSON record per row, comma-separated WITHIN the batch, no trailing
 * comma — the assembler inserts separators between batches. */
export function renderJsonRows(rows: ExportBatchRow[]): string {
  const parts: string[] = [];
  for (const r of rows) {
    parts.push(
      JSON.stringify({
        id: Number(r.id),
        gameId: r.game_id,
        multiplier: Number(r.multiplier),
        multiplierRaw: String(r.multiplier),
        hash: r.hash,
        salt: r.salt,
        beganAt: toIsoOrNull(r.began_at),
        crashedAt: toIsoOrNull(r.crashed_at),
        ingestedAt: toIsoOrNull(r.ingested_at),
      }),
    );
  }
  return parts.join(",");
}

export function csvHeader(): string {
  return CSV_COLUMNS.join(",") + "\n";
}

export function jsonHeader(meta: {
  count: number;
  generatedAt: string;
}): string {
  return (
    `{"source":"crash_rounds","ordering":"id asc","exportedAt":` +
    `${JSON.stringify(meta.generatedAt)},"count":${meta.count},"rounds":[`
  );
}

export function jsonFooter(): string {
  return "]}";
}

/**
 * Generate the export in bounded-size text chunks, batch by batch. The
 * server handler pumps this into a ReadableStream; tests can collect it.
 */
export async function* generateExportChunks(opts: {
  format: ExportFormat;
  count: number;
  generatedAt: string;
  fetchBatch: ExportBatchFetcher;
  batchSize?: number;
}): AsyncGenerator<string> {
  const { format, count, generatedAt, fetchBatch } = opts;
  const batchSize = opts.batchSize ?? EXPORT_BATCH_SIZE;
  yield format === "csv" ? csvHeader() : jsonHeader({ count, generatedAt });
  let afterId = 0;
  let needComma = false;
  let total = 0;
  for (;;) {
    const rows = await fetchBatch(afterId, batchSize);
    if (rows.length === 0) break;
    afterId = Number(rows[rows.length - 1]!.id);
    total += rows.length;
    if (format === "csv") {
      yield renderCsvRows(rows);
    } else {
      yield (needComma ? "," : "") + renderJsonRows(rows);
      needComma = true;
    }
    if (rows.length < batchSize) break;
  }
  if (format === "json") yield jsonFooter();
  if (total !== count && count > 0) {
    throw new Error(
      `export row count mismatch: counted ${count}, streamed ${total}`,
    );
  }
}

/** Assemble the whole export in memory (tests and small datasets only). */
export async function assembleExport(opts: {
  format: ExportFormat;
  count: number;
  generatedAt: string;
  fetchBatch: ExportBatchFetcher;
  batchSize?: number;
}): Promise<string> {
  let out = "";
  for await (const chunk of generateExportChunks(opts)) out += chunk;
  return out;
}
