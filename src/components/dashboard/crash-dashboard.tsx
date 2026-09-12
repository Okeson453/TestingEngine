import { useQuery } from "@tanstack/react-query";
import { useEffect, useRef, useState } from "react";
import {
  Activity,
  ArrowUpRight,
  BarChart3,
  Clock3,
  Download,
  Minus,
  Radio,
  TrendingDown,
  TrendingUp,
} from "lucide-react";
import { Link } from "@tanstack/react-router";
import {
  refreshDashboard,
  exportCrashCsv,
  exportAllRounds,
} from "@/lib/crash/api";
import { bandForMultiplier } from "@/lib/crash/stats";
import {
  HIGH_THRESHOLD,
  SOURCE_URL,
  type CrashRound,
  type DashboardPayload,
} from "@/lib/crash/types";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { CrashChart } from "./crash-chart";
import {
  bandClass,
  formatClock,
  formatMultiplier,
  formatPlayers,
  formatRelative,
  formatStat,
} from "./format";
import { cn } from "@/lib/utils";

function LogoMark() {
  return (
    <svg
      viewBox="0 0 32 32"
      className="size-8 text-fg"
      aria-hidden="true"
      fill="none"
    >
      <rect width="32" height="32" rx="8" className="fill-surface-2" />
      <path
        d="M6 20c3.2-6 5.2-9 8-9 3.2 0 4.2 6 7 6 2.2 0 3.4-2.4 5-6"
        stroke="currentColor"
        strokeWidth="1.8"
        strokeLinecap="round"
      />
      <path
        d="M21 9l5 1.2-1.6 4.6"
        stroke="var(--color-low)"
        strokeWidth="1.8"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

function LiveDot({ ok }: { ok: boolean }) {
  return (
    <span className="relative inline-flex size-2" aria-hidden="true">
      <span
        className={cn(
          "absolute inset-0 rounded-full",
          ok ? "live-dot bg-high" : "bg-low",
        )}
      />
    </span>
  );
}

function Multiplier({
  value,
  className,
}: {
  value: number;
  className?: string;
}) {
  return (
    <span className={cn("font-mono font-medium tabular-nums", bandClass(value), className)}>
      {formatMultiplier(value)}×
    </span>
  );
}

function StatTile({
  label,
  value,
  hint,
}: {
  label: string;
  value: string;
  hint?: string;
}) {
  return (
    <div className="rounded-lg bg-surface-2 px-3 py-3 md:px-4">
      <p className="text-xs font-medium uppercase tracking-wide text-subtle">{label}</p>
      <p className="mt-1 font-mono text-lg font-medium tabular-nums text-fg md:text-xl">
        {value}
      </p>
      {hint ? <p className="mt-0.5 text-xs text-muted">{hint}</p> : null}
    </div>
  );
}

function HistoryChip({ round }: { round: CrashRound }) {
  const band = bandForMultiplier(round.multiplier);
  return (
    <li>
      <div
        className="flex min-h-11 items-center gap-2 rounded-md bg-surface-2 px-2.5 py-1.5"
        title={`#${round.gameId} · ${formatClock(round.crashedAt)}`}
      >
        <span
          className={cn(
            "size-1.5 shrink-0 rounded-full",
            band === "low" && "bg-low",
            band === "high" && "bg-high",
            band === "moon" && "bg-moon",
          )}
        />
        <Multiplier value={round.multiplier} className="text-sm" />
      </div>
    </li>
  );
}

function DashboardSkeleton() {
  return (
    <div className="mx-auto flex w-full max-w-6xl flex-col gap-4 px-4 py-6 md:gap-5 md:px-6 md:py-8">
      <h1 className="text-lg font-semibold tracking-tight">CrashWave</h1>
      <p className="text-sm text-muted">Loading BC.Game Crash results…</p>
      <Skeleton className="h-10 w-48" />
      <Skeleton className="h-40 w-full rounded-xl" />
      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        <Skeleton className="h-20 rounded-lg" />
        <Skeleton className="h-20 rounded-lg" />
        <Skeleton className="h-20 rounded-lg" />
        <Skeleton className="h-20 rounded-lg" />
      </div>
      <Skeleton className="h-56 w-full rounded-xl" />
    </div>
  );
}

function FeedBanner({ data }: { data: DashboardPayload }) {
  const { feed } = data;
  if (feed.ok) {
    const players = formatPlayers(feed.onlinePlayers);
    return (
      <p className="text-sm text-muted">
        Synced {formatRelative(feed.lastSyncAt)}
        {feed.inserted > 0 ? ` · ${feed.inserted} new` : ""}
        {players ? ` · ${players} playing` : ""}
      </p>
    );
  }
  return (
    <p className="text-sm text-low">
      {(() => {
                const err = String(feed.error ?? "");
                const isDb =
                  /timeout|connect|POOL|database|ECONN|exhaustion/i.test(err);
                return isDb
                  ? `Live status unavailable — database connection issue${err ? ` (${err})` : ""}. Showing last known rounds.`
                  : `Live feed paused${err ? ` — ${err}` : ""}. Showing stored rounds.`;
              })()}
    </p>
  );
}

type FullExportFormat = "csv" | "json";
type ExportStatus =
  | { state: "idle" }
  | { state: "loading"; format: FullExportFormat }
  | { state: "success"; format: FullExportFormat; count: number; filename: string }
  | { state: "error"; format: FullExportFormat; message: string };

/**
 * Stream the full-history export Response to a browser download. Reads the
 * body in chunks (the server streams batch-by-batch) and saves the assembled
 * blob — the raw count comes from the X-Export-Count header.
 */
async function downloadFullExport(
  format: FullExportFormat,
): Promise<{ count: number; filename: string }> {
  const response = await exportAllRounds({ data: { format } });
  if (!(response instanceof Response)) {
    throw new Error("Export endpoint returned an unexpected payload");
  }
  if (!response.ok) {
    throw new Error(`Export failed with status ${response.status}`);
  }
  const count = Number(response.headers.get("x-export-count") ?? 0);
  const disposition = response.headers.get("content-disposition") ?? "";
  const match = /filename="([^"]+)"/.exec(disposition);
  const filename = match?.[1] ?? `crash-rounds-full.${format}`;

  const blob = response.body
    ? await new Response(response.body).blob()
    : new Blob([await response.text()]);
  const url = URL.createObjectURL(blob);
  try {
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
  } finally {
    URL.revokeObjectURL(url);
  }
  return { count, filename };
}

export function CrashDashboard({ initial }: { initial: DashboardPayload }) {
  const [exportStatus, setExportStatus] = useState<ExportStatus>({ state: "idle" });
  const exportStatusTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  useEffect(
    () => () => {
      if (exportStatusTimer.current) clearTimeout(exportStatusTimer.current);
    },
    [],
  );

  const runFullExport = (format: FullExportFormat) => {
    if (exportStatus.state === "loading") return;
    setExportStatus({ state: "loading", format });
    downloadFullExport(format)
      .then(({ count, filename }) => {
        setExportStatus({ state: "success", format, count, filename });
      })
      .catch((err: unknown) => {
        setExportStatus({
          state: "error",
          format,
          message: err instanceof Error ? err.message : "Export failed",
        });
      })
      .finally(() => {
        exportStatusTimer.current = setTimeout(
          () => setExportStatus({ state: "idle" }),
          6_000,
        );
      });
  };

  const query = useQuery({
    queryKey: ["crash-dashboard"],
    queryFn: () => refreshDashboard(),
    initialData: initial,
    initialDataUpdatedAt: Date.now(),
    staleTime: 3_000,
    refetchInterval: 4_000,
  });

  if (query.isPending && !query.data) {
    return <DashboardSkeleton />;
  }

  if (query.isError && !query.data) {
    return (
      <main className="mx-auto flex min-h-dvh w-full max-w-lg flex-col items-center justify-center gap-3 px-6 text-center">
        <Activity className="size-8 text-low" aria-hidden="true" />
        <h1 className="text-xl font-medium">Could not load tracker</h1>
        <p className="text-sm text-muted">
          {query.error instanceof Error ? query.error.message : "Try again in a moment."}
        </p>
        <Button type="button" onClick={() => void query.refetch()}>
          Retry
        </Button>
      </main>
    );
  }

  const data = query.data!;
  const latest = data.latest;
  const streakKind = data.streaks.currentKind;
  const latestBand = latest ? bandForMultiplier(latest.multiplier) : "low";

  return (
    <main className="min-h-dvh bg-bg text-fg">
      <div className="mx-auto flex w-full max-w-6xl flex-col gap-4 px-4 py-5 md:gap-5 md:px-6 md:py-8">
        <header className="flex flex-wrap items-center justify-between gap-3">
          <div className="flex items-center gap-3">
            <LogoMark />
            <div>
              <h1 className="text-lg font-semibold tracking-tight md:text-xl">CrashWave</h1>
              <p className="text-xs text-muted">BC.Game Crash tracker</p>
            </div>
          </div>
          <div className="flex items-center gap-2">
            <Badge variant={data.feed.ok ? "live" : "warn"} className="h-8 gap-1.5 px-3">
              <LiveDot ok={data.feed.ok} />
              {data.feed.ok ? "Live" : "Offline"}
            </Badge>
            <Button
              variant="secondary"
              size="sm"
              onClick={async () => {
                const { csv, filename } = await exportCrashCsv({ data: { mode: "daily", days: 365 } });
                const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
                const url = URL.createObjectURL(blob);
                const a = document.createElement("a");
                a.href = url;
                a.download = filename;
                document.body.appendChild(a);
                a.click();
                document.body.removeChild(a);
                URL.revokeObjectURL(url);
              }}
            >
              <Download className="size-3.5" />
              CSV
            </Button>
            <Button variant="secondary" asChild>
              <a href={SOURCE_URL} target="_blank" rel="noreferrer">
                Open Crash
                <ArrowUpRight className="size-3.5" />
              </a>
            </Button>
            <Button variant="secondary" size="sm" asChild>
              <Link to="/predictions">
                <BarChart3 className="size-3.5" />
                Predictions
              </Link>
            </Button>
          </div>
        </header>

        {/* Full-history export: authoritative crash_rounds table, streamed in
            batches — not the 60-round chart window. Loading / success /
            failure states inline below the toolbar. */}
        <div className="flex flex-wrap items-center gap-2">
          <Button
            variant="secondary"
            size="sm"
            type="button"
            disabled={exportStatus.state === "loading"}
            onClick={() => runFullExport("csv")}
          >
            <Download className="size-3.5" />
            {exportStatus.state === "loading" && exportStatus.format === "csv"
              ? "Preparing CSV…"
              : "Download All Rounds (CSV)"}
          </Button>
          <Button
            variant="secondary"
            size="sm"
            type="button"
            disabled={exportStatus.state === "loading"}
            onClick={() => runFullExport("json")}
          >
            <Download className="size-3.5" />
            {exportStatus.state === "loading" && exportStatus.format === "json"
              ? "Preparing JSON…"
              : "Download All Rounds (JSON)"}
          </Button>
          {exportStatus.state === "success" && (
            <span className="text-xs text-emerald-500" role="status">
              Exported {exportStatus.count.toLocaleString("en-US")} rounds →{" "}
              {exportStatus.filename}
            </span>
          )}
          {exportStatus.state === "error" && (
            <span className="text-xs text-red-500" role="alert">
              {exportStatus.format.toUpperCase()} export failed:{" "}
              {exportStatus.message}
            </span>
          )}
        </div>

        <Card className="overflow-hidden rounded-xl p-5 md:p-6">
          <div className="flex flex-col gap-6 md:flex-row md:items-end md:justify-between">
            <div>
              <p className="text-xs font-medium uppercase tracking-wide text-subtle">
                Latest crash
              </p>
              {latest ? (
                <p
                  key={latest.gameId}
                  className={cn(
                    "rise-in mt-1 font-mono text-hero font-semibold leading-none tracking-tight tabular-nums",
                    bandClass(latest.multiplier),
                  )}
                >
                  {formatMultiplier(latest.multiplier)}×
                </p>
              ) : (
                <p className="mt-2 text-2xl text-muted">No rounds yet</p>
              )}
              <div className="mt-3 flex flex-wrap items-center gap-x-3 gap-y-1 text-sm text-muted">
                {latest ? (
                  <>
                    <span className="inline-flex items-center gap-1.5">
                      <Clock3 className="size-3.5" aria-hidden="true" />
                      {formatClock(latest.crashedAt)} · {formatRelative(latest.crashedAt)}
                    </span>
                    <span className="text-subtle">#{latest.gameId}</span>
                  </>
                ) : (
                  <span>Waiting for BC.Game Crash results.</span>
                )}
              </div>
            </div>
            <div className="flex flex-wrap gap-2">
              <Badge
                variant={
                  latestBand === "low" ? "low" : latestBand === "high" ? "high" : "moon"
                }
                className="h-8 px-3"
              >
                {latestBand === "low"
                  ? `Below ${HIGH_THRESHOLD}.00×`
                  : latestBand === "moon"
                    ? "10.00×+"
                    : `${HIGH_THRESHOLD}.00×+`}
              </Badge>
              {streakKind !== "none" ? (
                <Badge variant={streakKind === "low" ? "low" : "high"} className="h-8 px-3">
                  {data.streaks.currentCount} {streakKind} in a row
                </Badge>
              ) : null}
            </div>
          </div>
          <div className="mt-4">
            <FeedBanner data={data} />
          </div>
        </Card>

        <section className="grid grid-cols-2 gap-3 md:grid-cols-4">
          <StatTile label="Average" value={formatStat(data.stats.average)} hint={`${data.stats.count} rounds`} />
          <StatTile label="Median" value={formatStat(data.stats.median)} />
          <StatTile label="Highest" value={formatStat(data.stats.highest)} />
          <StatTile label="Lowest" value={formatStat(data.stats.lowest)} />
        </section>

        <div className="grid gap-4 lg:grid-cols-5">
          <Card className="lg:col-span-3">
            <CardHeader>
              <div>
                <CardTitle>Crash history</CardTitle>
                <CardDescription>
                  Last {data.chart.length} rounds · bars capped at 10×
                </CardDescription>
              </div>
            </CardHeader>
            <CrashChart rounds={data.chart} />
          </Card>

          <Card className="lg:col-span-2">
            <CardHeader>
              <div>
                <CardTitle>Streaks</CardTitle>
                <CardDescription>
                  Consecutive rounds below / at {HIGH_THRESHOLD}.00×
                </CardDescription>
              </div>
            </CardHeader>
            <div className="flex flex-col gap-3">
              <div className="flex items-center justify-between rounded-lg bg-surface-2 px-3 py-3">
                <div className="flex items-center gap-2 text-sm">
                  {streakKind === "low" ? (
                    <TrendingDown className="size-4 text-low" aria-hidden="true" />
                  ) : streakKind === "high" ? (
                    <TrendingUp className="size-4 text-high" aria-hidden="true" />
                  ) : (
                    <Minus className="size-4 text-muted" aria-hidden="true" />
                  )}
                  <span className="text-muted">Current</span>
                </div>
                <span className="font-mono text-sm tabular-nums text-fg">
                  {data.streaks.currentCount} {streakKind === "none" ? "—" : streakKind}
                </span>
              </div>
              <div className="flex items-center justify-between rounded-lg bg-surface-2 px-3 py-3">
                <span className="text-sm text-muted">Longest low</span>
                <span className="font-mono text-sm tabular-nums text-low">
                  {data.streaks.maxLow}
                </span>
              </div>
              <div className="flex items-center justify-between rounded-lg bg-surface-2 px-3 py-3">
                <span className="text-sm text-muted">Longest high</span>
                <span className="font-mono text-sm tabular-nums text-high">
                  {data.streaks.maxHigh}
                </span>
              </div>
            </div>
          </Card>
        </div>

        <Card>
          <CardHeader>
            <div>
              <CardTitle>Multiplier ranges</CardTitle>
              <CardDescription>Frequency across all stored rounds</CardDescription>
            </div>
          </CardHeader>
          <ul className="flex flex-col gap-2.5">
            {data.ranges.map((bucket) => (
              <li key={bucket.key} className="flex items-center gap-3">
                <span className="w-28 shrink-0 font-mono text-xs tabular-nums text-muted">
                  {bucket.label}
                </span>
                <div className="h-2 min-w-0 flex-1 overflow-hidden rounded-full bg-surface-2">
                  <div
                    className="h-full rounded-full bg-accent/80"
                    style={{ width: `${Math.max(bucket.pct, bucket.count > 0 ? 2 : 0)}%` }}
                  />
                </div>
                <span className="w-20 shrink-0 text-right font-mono text-xs tabular-nums text-fg">
                  {bucket.count} · {bucket.pct.toFixed(0)}%
                </span>
              </li>
            ))}
          </ul>
        </Card>

        <Card>
          <CardHeader>
            <div>
              <CardTitle>Recent results</CardTitle>
              <CardDescription>Newest first · {data.rounds.length} on this page</CardDescription>
            </div>
            <span className="inline-flex items-center gap-1.5 text-xs text-muted">
              <Radio className="size-3.5" aria-hidden="true" />
              Auto-refresh 4s
            </span>
          </CardHeader>
          {data.rounds.length === 0 ? (
            <p className="py-8 text-center text-sm text-muted">
              No crash results recorded yet. The tracker pulls from BC.Game Crash automatically.
            </p>
          ) : (
            <ul className="grid grid-cols-3 gap-2 sm:grid-cols-4 md:grid-cols-6 lg:grid-cols-8">
              {data.rounds.map((round) => (
                <HistoryChip key={round.gameId} round={round} />
              ))}
            </ul>
          )}
        </Card>

        <footer className="pb-6 pt-2 text-center text-xs text-subtle">
          Records crash multipliers only. No predictions, signals, or betting.
        </footer>
      </div>
    </main>
  );
}
