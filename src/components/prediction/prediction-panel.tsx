import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  Target,
  Trophy,
  XCircle,
  TrendingUp,
  TrendingDown,
  Minus,
  Activity,
  Search,
  Filter,
  ChevronLeft,
  ChevronRight,
  BarChart3,
  Zap,
  Cpu,
  History,
  ArrowLeft,
} from "lucide-react";
import { Link } from "@tanstack/react-router";
import {
  predictionGetDailyTarget,
  predictionSetDailyTarget,
  predictionGetTodayStats,
  predictionGetLifetimeStats,
  predictionGetStreaks,
  predictionGetRecent,
  predictionGetHistory,
  predictionGetPending,
  predictionGetWorkerStatus,
} from "@/lib/prediction/api";
import type { WorkerStatus } from "@/lib/prediction/api";
import type {
  DailyTarget,
  TodayStats,
  LifetimeStats,
  StreakSnapshot,
  ValidationRecord,
  ValidationHistoryResult,
  PendingStatus,
} from "@/lib/prediction/service";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { cn } from "@/lib/utils";
import { formatClock, formatRelative } from "../dashboard/format";

function formatPct(n: number): string {
  return `${(n * 100).toFixed(1)}%`;
}

function StatTile({
  label,
  value,
  hint,
  className,
}: {
  label: string;
  value: string;
  hint?: string;
  className?: string;
}) {
  return (
    <div className={cn("rounded-lg bg-surface-2 px-3 py-3 md:px-4", className)}>
      <p className="text-xs font-medium uppercase tracking-wide text-subtle">{label}</p>
      <p className="mt-1 font-mono text-lg font-medium tabular-nums text-fg md:text-xl">{value}</p>
      {hint ? <p className="mt-0.5 text-xs text-muted">{hint}</p> : null}
    </div>
  );
}

function ResultBadge({ result }: { result: "WIN" | "LOSS" }) {
  return (
    <Badge
      variant={result === "WIN" ? "high" : "low"}
      className="h-6 px-2 text-xs"
    >
      {result}
    </Badge>
  );
}

interface PredictionPanelProps {
  initial: {
    dailyTarget: DailyTarget;
    today: TodayStats;
    lifetime: LifetimeStats;
    streaks: StreakSnapshot;
    recent: ValidationRecord[];
    pending: PendingStatus;
    worker: WorkerStatus;
  };
}

export function PredictionPanel({ initial }: PredictionPanelProps) {
  const queryClient = useQueryClient();
  const [targetInput, setTargetInput] = useState(String(initial.dailyTarget.dailyTarget));
  const [historyPage, setHistoryPage] = useState(1);
  const [historyFilter, setHistoryFilter] = useState<"WIN" | "LOSS" | "all">("all");
  const [searchId, setSearchId] = useState("");

  const dailyTargetQ = useQuery({
    queryKey: ["prediction-daily-target"],
    queryFn: () => predictionGetDailyTarget(),
    initialData: initial.dailyTarget,
  });

  const todayQ = useQuery({
    queryKey: ["prediction-today"],
    queryFn: () => predictionGetTodayStats(),
    initialData: initial.today,
    refetchInterval: 4_000,
  });

  const lifetimeQ = useQuery({
    queryKey: ["prediction-lifetime"],
    queryFn: () => predictionGetLifetimeStats(),
    initialData: initial.lifetime,
    refetchInterval: 4_000,
  });

  const streaksQ = useQuery({
    queryKey: ["prediction-streaks"],
    queryFn: () => predictionGetStreaks(),
    initialData: initial.streaks,
    refetchInterval: 4_000,
  });

  const recentQ = useQuery({
    queryKey: ["prediction-recent"],
    queryFn: () => predictionGetRecent(),
    initialData: initial.recent,
    refetchInterval: 4_000,
  });

  const pendingQ = useQuery({
    queryKey: ["prediction-pending"],
    queryFn: () => predictionGetPending(),
    initialData: initial.pending,
    refetchInterval: 4_000,
  });

  const historyQ = useQuery({
    queryKey: ["prediction-history", historyPage, historyFilter],
    queryFn: () =>
      predictionGetHistory({
        data: {
          page: historyPage,
          pageSize: 20,
          result: historyFilter === "all" ? null : historyFilter,
        },
      }),
  });

  const setTargetMut = useMutation({
    mutationFn: (target: number) => predictionSetDailyTarget({ data: { target } }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["prediction-daily-target"] });
      queryClient.invalidateQueries({ queryKey: ["prediction-today"] });
    },
  });

  const dailyTarget = dailyTargetQ.data ?? initial.dailyTarget;
  const today = todayQ.data ?? initial.today;
  const lifetime = lifetimeQ.data ?? initial.lifetime;
  const streaks = streaksQ.data ?? initial.streaks;
   const recent = recentQ.data ?? initial.recent;
  const pending = pendingQ.data ?? initial.pending;
  const workerQ = useQuery({
    queryKey: ["prediction-worker"],
    queryFn: () => predictionGetWorkerStatus(),
    initialData: initial.worker,
    refetchInterval: 4_000,
  });
  const worker = workerQ.data ?? initial.worker;
  const history = historyQ.data;

  const progressPct =
    dailyTarget.dailyTarget > 0
      ? Math.min(100, (today.total / dailyTarget.dailyTarget) * 100)
      : 0;

  return (
    <main className="min-h-dvh bg-bg text-fg">
      <div className="mx-auto flex w-full max-w-6xl flex-col gap-4 px-4 py-5 md:gap-5 md:px-6 md:py-8">
        <header className="flex flex-wrap items-center justify-between gap-3">
          <div className="flex items-center gap-3">
            <div className="flex size-8 items-center justify-center rounded-lg bg-surface-2">
              <BarChart3 className="size-5 text-accent" />
            </div>
            <div>
              <h1 className="text-lg font-semibold tracking-tight md:text-xl">Prediction Validation</h1>
              <p className="text-xs text-muted">Training & accuracy tracking</p>
            </div>
          </div>
          <div className="flex items-center gap-2">
            <Button variant="secondary" size="sm" asChild>
              <Link to="/">
                <ArrowLeft className="size-3.5" />
                Dashboard
              </Link>
            </Button>
            {pending.hasPending && (
              <Badge variant="warn" className="h-8 gap-1.5 px-3">
                <Zap className="size-3.5" />
                Pending
              </Badge>
            )}
            <Badge variant="live" className="h-8 gap-1.5 px-3">
              <Activity className="size-3.5" />
              Auto-refresh 4s
            </Badge>
          </div>
        </header>

        {/* Daily Target */}
        <Card className="overflow-hidden rounded-xl p-5 md:p-6">
          <div className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
            <div className="flex items-center gap-3">
              <Target className="size-5 text-accent" />
              <div>
                <p className="text-sm font-medium">Daily Entry Target</p>
                <p className="text-xs text-muted">
                  {today.total} completed · {today.remaining} remaining
                </p>
              </div>
            </div>
            <div className="flex items-center gap-2">
              <input
                type="number"
                min={20}
                max={500}
                value={targetInput}
                onChange={(e) => setTargetInput(e.target.value)}
                className="h-9 w-20 rounded-md border border-border bg-bg px-2 text-sm font-mono tabular-nums text-fg"
              />
              <Button
                size="sm"
                onClick={() => {
                  const n = parseInt(targetInput, 10);
                  if (!Number.isNaN(n)) setTargetMut.mutate(n);
                }}
                disabled={setTargetMut.isPending}
              >
                Set
              </Button>
            </div>
          </div>
          <div className="mt-4 h-2 overflow-hidden rounded-full bg-surface-2">
            <div
              className="h-full rounded-full bg-accent transition-all"
              style={{ width: `${progressPct}%` }}
            />
          </div>
        </Card>

        {/* Performance */}
        <section className="grid grid-cols-2 gap-3 md:grid-cols-4">
          <StatTile label="Total" value={String(lifetime.total)} />
          <StatTile
            label="Wins"
            value={String(lifetime.wins)}
            className="border border-high/20"
          />
          <StatTile
            label="Losses"
            value={String(lifetime.losses)}
            className="border border-low/20"
          />
          <StatTile label="Win Rate" value={formatPct(lifetime.winRate)} />
        </section>

        {/* Worker health */}
        <Card>
          <CardHeader>
            <div className="flex items-center gap-2">
              <Cpu className="size-4 text-accent" />
              <div>
                <CardTitle>Worker Health</CardTitle>
                <CardDescription>
                  Server-side background engine (independent of this dashboard)
                </CardDescription>
              </div>
            </div>
          </CardHeader>
          <div className="flex flex-col gap-3 p-6 pt-0">
            <div className="flex flex-wrap items-center justify-between gap-3">
              <div className="flex items-center gap-2">
                <span
                  className={cn(
                    "inline-block size-2 rounded-full",
                    worker.running ? "bg-high" : "bg-low",
                  )}
                />
                <span className="font-medium">
                  {worker.running ? "Running" : "Offline"}
                </span>
              </div>
              <Badge variant={worker.running ? "live" : "warn"}>
                {worker.lastSyncOk ? "Last sync OK" : "Last sync failed"}
              </Badge>
            </div>
            <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
              <StatTile
                label="Cycles"
                value={String(worker.cyclesTotal)}
                hint={worker.running ? "poll cycles run" : "since start"}
              />
              <StatTile
                label="Last Sync"
                value={worker.lastSyncAt ? formatClock(worker.lastSyncAt) : "—"}
              hint={worker.lastSyncAt ? formatRelative(worker.lastSyncAt) : undefined}
              />
              <StatTile
                label="Today Resolved"
                value={`${worker.resolvedToday}/${worker.dailyTarget}`}
              />
              <StatTile
                label="Pending"
                value={String(worker.pendingCount)}
                hint={worker.remainingToday === 0 ? "target reached today" : `${worker.remainingToday} remaining`}
              />
            </div>
            {worker.lastError ? (
              <p className="text-xs text-low">Error: {worker.lastError}</p>
            ) : null}
          </div>
        </Card>

        <section className="grid grid-cols-2 gap-3 md:grid-cols-4">
          <StatTile label="Today" value={`${today.wins}/${today.losses}`} />
          <StatTile label="Today Win Rate" value={formatPct(today.winRate)} />
          <StatTile
            label="Current Streak"
            value={streaks.currentKind === "none" ? "—" : `${streaks.currentCount} ${streaks.currentKind}`}
            hint={streaks.currentKind === "WIN" ? "Best: " + streaks.maxWin : streaks.currentKind === "LOSS" ? "Worst: " + streaks.maxLoss : undefined}
          />
          <StatTile
            label="Best / Worst"
            value={`${streaks.maxWin} / ${streaks.maxLoss}`}
          />
        </section>

        <div className="grid gap-4 lg:grid-cols-5">
          {/* Live Validation */}
          <Card className="lg:col-span-3">
            <CardHeader>
              <div className="flex items-center gap-2">
                <Zap className="size-4 text-accent" />
                <div>
                  <CardTitle>Live Validation</CardTitle>
                  <CardDescription>Most recent predictions graded</CardDescription>
                </div>
              </div>
            </CardHeader>
            <div className="flex flex-col gap-2">
              {recent.length === 0 ? (
                <p className="py-8 text-center text-sm text-muted">
                  No validations yet. Predictions are generated and validated automatically on each poll cycle.
                </p>
              ) : (
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b border-border text-left text-xs text-subtle">
                        <th className="px-3 py-2 font-medium">Result</th>
                        <th className="px-3 py-2 font-medium">Game ID</th>
                        <th className="px-3 py-2 font-medium">Actual</th>
                        <th className="px-3 py-2 font-medium">Probability</th>
                        <th className="px-3 py-2 font-medium">Confidence</th>
                        <th className="px-3 py-2 font-medium">Regime</th>
                      </tr>
                    </thead>
                    <tbody>
                      {recent.map((r) => (
                        <tr key={r.predictionId} className="border-b border-border/50">
                          <td className="px-3 py-2">
                            <ResultBadge result={r.result} />
                          </td>
                          <td className="px-3 py-2 font-mono text-xs text-muted">{r.gameId}</td>
                          <td className="px-3 py-2 font-mono tabular-nums">
                            {r.actualMultiplier.toFixed(2)}×
                          </td>
                          <td className="px-3 py-2 font-mono tabular-nums">
                            {(r.predictedProbability * 100).toFixed(1)}%
                          </td>
                          <td className="px-3 py-2 font-mono tabular-nums">
                            {(r.predictedConfidence * 100).toFixed(1)}%
                          </td>
                          <td className="px-3 py-2 text-xs text-muted">{r.regimeName ?? "—"}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          </Card>

          {/* Streak Card */}
          <Card className="lg:col-span-2">
            <CardHeader>
              <div className="flex items-center gap-2">
                <TrendingUp className="size-4 text-accent" />
                <div>
                  <CardTitle>Streaks</CardTitle>
                  <CardDescription>Consecutive WIN / LOSS runs</CardDescription>
                </div>
              </div>
            </CardHeader>
            <div className="flex flex-col gap-3">
              <div className="flex items-center justify-between rounded-lg bg-surface-2 px-3 py-3">
                <div className="flex items-center gap-2 text-sm">
                  {streaks.currentKind === "WIN" ? (
                    <TrendingUp className="size-4 text-high" />
                  ) : streaks.currentKind === "LOSS" ? (
                    <TrendingDown className="size-4 text-low" />
                  ) : (
                    <Minus className="size-4 text-muted" />
                  )}
                  <span className="text-muted">Current</span>
                </div>
                <span className="font-mono text-sm tabular-nums text-fg">
                  {streaks.currentKind === "none"
                    ? "—"
                    : `${streaks.currentCount} ${streaks.currentKind}`}
                </span>
              </div>
              <div className="flex items-center justify-between rounded-lg bg-surface-2 px-3 py-3">
                <span className="text-sm text-muted">Best WIN streak</span>
                <span className="font-mono text-sm tabular-nums text-high">{streaks.maxWin}</span>
              </div>
              <div className="flex items-center justify-between rounded-lg bg-surface-2 px-3 py-3">
                <span className="text-sm text-muted">Worst LOSS streak</span>
                <span className="font-mono text-sm tabular-nums text-low">{streaks.maxLoss}</span>
              </div>
            </div>
          </Card>
        </div>

        {/* History */}
        <Card>
          <CardHeader>
            <div className="flex flex-wrap items-center justify-between gap-3">
              <div className="flex items-center gap-2">
                <History className="size-4 text-accent" />
                <div>
                  <CardTitle>Complete History</CardTitle>
                  <CardDescription>
                    {history?.total ?? 0} records · searchable · filterable · paginated
                  </CardDescription>
                </div>
              </div>
              <div className="flex items-center gap-2">
                <select
                  value={historyFilter}
                  onChange={(e) => {
                    setHistoryFilter(e.target.value as "WIN" | "LOSS" | "all");
                    setHistoryPage(1);
                  }}
                  className="h-9 rounded-md border border-border bg-bg px-2 text-sm text-fg"
                >
                  <option value="all">All</option>
                  <option value="WIN">WIN only</option>
                  <option value="LOSS">LOSS only</option>
                </select>
              </div>
            </div>
          </CardHeader>
          <div className="flex flex-col gap-3">
            {historyQ.isPending && !history ? (
              <div className="flex flex-col gap-2 p-4">
                <Skeleton className="h-8 w-full" />
                <Skeleton className="h-8 w-full" />
                <Skeleton className="h-8 w-full" />
              </div>
            ) : history && history.records.length > 0 ? (
              <>
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b border-border text-left text-xs text-subtle">
                        <th className="px-3 py-2 font-medium">Result</th>
                        <th className="px-3 py-2 font-medium">Game ID</th>
                        <th className="px-3 py-2 font-medium">Actual</th>
                        <th className="px-3 py-2 font-medium">Probability</th>
                        <th className="px-3 py-2 font-medium">Confidence</th>
                        <th className="px-3 py-2 font-medium">Regime</th>
                        <th className="px-3 py-2 font-medium">Resolved</th>
                      </tr>
                    </thead>
                    <tbody>
                      {history.records.map((r) => (
                        <tr key={r.predictionId} className="border-b border-border/50">
                          <td className="px-3 py-2">
                            <ResultBadge result={r.result} />
                          </td>
                          <td className="px-3 py-2 font-mono text-xs text-muted">{r.gameId}</td>
                          <td className="px-3 py-2 font-mono tabular-nums">
                            {r.actualMultiplier.toFixed(2)}×
                          </td>
                          <td className="px-3 py-2 font-mono tabular-nums">
                            {(r.predictedProbability * 100).toFixed(1)}%
                          </td>
                          <td className="px-3 py-2 font-mono tabular-nums">
                            {(r.predictedConfidence * 100).toFixed(1)}%
                          </td>
                          <td className="px-3 py-2 text-xs text-muted">{r.regimeName ?? "—"}</td>
                          <td className="px-3 py-2 text-xs text-muted">
                            {new Date(r.resolvedAt).toLocaleString()}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                <div className="flex items-center justify-between px-3 pb-3">
                  <p className="text-xs text-muted">
                    Page {history.page} of {Math.max(1, Math.ceil(history.total / history.pageSize))}
                  </p>
                  <div className="flex items-center gap-2">
                    <Button
                      variant="secondary"
                      size="sm"
                      disabled={history.page <= 1}
                      onClick={() => setHistoryPage((p) => Math.max(1, p - 1))}
                    >
                      <ChevronLeft className="size-4" />
                    </Button>
                    <Button
                      variant="secondary"
                      size="sm"
                      disabled={history.page * history.pageSize >= history.total}
                      onClick={() => setHistoryPage((p) => p + 1)}
                    >
                      <ChevronRight className="size-4" />
                    </Button>
                  </div>
                </div>
              </>
            ) : (
              <p className="py-8 text-center text-sm text-muted">
                No history records yet. Predictions accumulate automatically as rounds are discovered.
              </p>
            )}
          </div>
        </Card>

        <footer className="pb-6 pt-2 text-center text-xs text-subtle">
          Prediction engine: baseline-statistical heuristic. Not a claim of predictive ability on a provably random game.
        </footer>
      </div>
    </main>
  );
}
