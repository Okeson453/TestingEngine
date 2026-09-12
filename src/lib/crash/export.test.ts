/**
 * Full-history export tests (src/lib/crash/export.ts).
 *
 * Covers the directive's export requirements: every stored row present in
 * order (keyset pagination across batch boundaries, no duplicates, none
 * missing), valid UTF-8 CSV with headers, valid structured JSON, exact
 * field fidelity (raw numeric text preserved), and the count-mismatch
 * guard. The fetcher is stubbed — the SQL shape is a plain keyset scan.
 */
import { describe, expect, it } from "vitest";
import {
  assembleExport,
  csvHeader,
  type ExportBatchRow,
} from "./export";

function makeRows(from: number, to: number): ExportBatchRow[] {
  const rows: ExportBatchRow[] = [];
  for (let id = from; id <= to; id++) {
    rows.push({
      id,
      game_id: `98${String(id).padStart(7, "0")}`,
      multiplier: "1.3000",
      hash: id % 3 === 0 ? `hash-with-"quotes"-${id}` : `hash-${id}`,
      salt: id % 4 === 0 ? `salt,\n with newline ${id}` : `salt-${id}`,
      began_at: new Date(Date.UTC(2026, 8, 12, 10, 0, id % 60)).toISOString(),
      crashed_at: new Date(Date.UTC(2026, 8, 12, 10, 0, (id % 60) + 1)).toISOString(),
      ingested_at: new Date(Date.UTC(2026, 8, 12, 10, 0, 30)).toISOString(),
    });
  }
  return rows;
}

/** Stub keyset fetcher over a fixed dataset, honoring afterId + limit. */
function stubFetcher(data: ExportBatchRow[]) {
  let calls = 0;
  const afterIds: number[] = [];
  return {
    afterIds,
    fetch: async (afterId: number, limit: number) => {
      calls++;
      afterIds.push(afterId);
      return data.filter((r) => Number(r.id) > afterId).slice(0, limit);
    },
    calls: () => calls,
  };
}

/** RFC-4180 record splitter: keeps raw bytes; quoted newlines stay in-record. */
function csvRecords(text: string): string[] {
  const recs: string[] = [];
  let cur = "";
  let inQ = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i]!;
    if (c === '"') {
      cur += c;
      if (inQ && text[i + 1] === '"') {
        cur += '"';
        i++;
      } else inQ = !inQ;
    } else if (c === "\n" && !inQ) {
      recs.push(cur);
      cur = "";
    } else cur += c;
  }
  if (cur.trim()) recs.push(cur);
  return recs;
}

/** RFC-4180 field splitter + decoder for one raw record. */
function csvFields(record: string): string[] {
  const fields: string[] = [];
  let cur = "";
  let inQ = false;
  for (let i = 0; i < record.length; i++) {
    const c = record[i]!;
    if (inQ) {
      if (c === '"') {
        if (record[i + 1] === '"') {
          cur += '"';
          i++;
        } else inQ = false;
      } else cur += c;
    } else if (c === '"') inQ = true;
    else if (c === ",") {
      fields.push(cur);
      cur = "";
    } else cur += c;
  }
  fields.push(cur);
  return fields;
}

describe("full-history export", () => {
  const data = makeRows(1, 25);
  const generatedAt = "2026-09-12T10:00:00.000Z";

  it("CSV: every row present exactly once, ordered by id, header first", async () => {
    const f = stubFetcher(data);
    const text = await assembleExport({
      format: "csv",
      count: data.length,
      generatedAt,
      fetchBatch: f.fetch,
      batchSize: 10,
    });

    const recs = csvRecords(text);
    expect(recs[0]).toBe(csvHeader().trimEnd());
    expect(recs.length).toBe(26); // header + 25 rows
    const ids = recs.slice(1).map((l) => Number(csvFields(l)[0]));
    expect(ids).toEqual(data.map((r) => Number(r.id)));
    // Keyset advanced monotonically: each batch starts after the last id.
    expect(f.afterIds[0]).toBe(0);
    expect(f.afterIds.slice(1)).toEqual([10, 20]);
  });

  it("CSV: escaping survives commas, quotes and newlines (RFC 4180)", async () => {
    const f = stubFetcher(data);
    const text = await assembleExport({
      format: "csv",
      count: data.length,
      generatedAt,
      fetchBatch: f.fetch,
      batchSize: 100,
    });
    const recs = csvRecords(text);
    // Row id=3: hash contains double quotes — decoded back by the parser.
    const rec3 = recs.find((r) => csvFields(r)[0] === "3");
    expect(rec3).toBeDefined();
    const fields3 = csvFields(rec3!);
    expect(fields3[3]).toBe('hash-with-"quotes"-3');
    // Row id=4: salt contains a comma AND a newline — quoted, preserved.
    const rec4 = recs.find((r) => r.includes("4") && csvFields(r)[0] === "4");
    expect(rec4).toBeDefined();
    const fields4 = csvFields(rec4!);
    expect(fields4[4]).toContain("\n");
    expect(fields4[4]).toContain(",");
    // The record count must NOT have been inflated by embedded newlines.
    expect(recs.length).toBe(26);
  });

  it("JSON: parses, holds every row in order, preserves raw multiplier text", async () => {
    const f = stubFetcher(data);
    const text = await assembleExport({
      format: "json",
      count: data.length,
      generatedAt,
      fetchBatch: f.fetch,
      batchSize: 7,
    });
    const parsed = JSON.parse(text) as {
      source: string;
      count: number;
      exportedAt: string;
      rounds: Array<{
        id: number;
        gameId: string;
        multiplier: number;
        multiplierRaw: string;
        crashedAt: string;
      }>;
    };
    expect(parsed.source).toBe("crash_rounds");
    expect(parsed.count).toBe(25);
    expect(parsed.exportedAt).toBe(generatedAt);
    expect(parsed.rounds.length).toBe(25);
    expect(parsed.rounds.map((r) => r.id)).toEqual(data.map((r) => Number(r.id)));
    expect(parsed.rounds[0]?.multiplier).toBe(1.3);
    expect(parsed.rounds[0]?.multiplierRaw).toBe("1.3000");
    expect(parsed.rounds[0]?.crashedAt).toBe(data[0]?.crashed_at);
  });

  it("JSON: commas between batches are correct (no double/trailing commas)", async () => {
    const f = stubFetcher(data);
    const text = await assembleExport({
      format: "json",
      count: data.length,
      generatedAt,
      fetchBatch: f.fetch,
      batchSize: 2,
    });
    expect(() => JSON.parse(text)).not.toThrow();
  });

  it("count mismatch between COUNT(*) and streamed rows aborts the export", async () => {
    const f = stubFetcher(data.slice(0, 10));
    await expect(
      assembleExport({
        format: "json",
        count: 25,
        generatedAt,
        fetchBatch: f.fetch,
        batchSize: 4,
      }),
    ).rejects.toThrow(/count mismatch/);
  });

  it("empty dataset yields valid empty CSV and JSON", async () => {
    const f = stubFetcher([]);
    const csv = await assembleExport({
      format: "csv",
      count: 0,
      generatedAt,
      fetchBatch: f.fetch,
      batchSize: 10,
    });
    expect(csv).toBe(csvHeader());
    const json = await assembleExport({
      format: "json",
      count: 0,
      generatedAt,
      fetchBatch: f.fetch,
      batchSize: 10,
    });
    const parsed = JSON.parse(json) as { count: number; rounds: unknown[] };
    expect(parsed.count).toBe(0);
    expect(parsed.rounds).toEqual([]);
  });

  it("scales: 12,148 rows across batches, none missing or duplicated", async () => {
    const big = makeRows(1, 12_148);
    const f = stubFetcher(big);
    const csv = await assembleExport({
      format: "csv",
      count: big.length,
      generatedAt,
      fetchBatch: f.fetch,
      batchSize: 2_000,
    });
    const recs = csvRecords(csv);
    expect(recs.length).toBe(12_149); // header + 12,148 rows
    const seen = new Set<number>();
    let last = 0;
    for (const rec of recs.slice(1)) {
      const id = Number(csvFields(rec)[0]);
      expect(id).toBeGreaterThan(last);
      last = id;
      seen.add(id);
    }
    expect(seen.size).toBe(12_148);
    expect(f.calls()).toBe(7); // ceil(12148/2000)
  });
});
