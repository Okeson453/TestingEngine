import {
  Bar,
  BarChart,
  Cell,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import type { CrashRound } from "@/lib/crash/types";
import { bandFill, formatClock, formatMultiplier } from "./format";

const BAR_CAP = 10;

type ChartPoint = {
  gameId: string;
  multiplier: number;
  bar: number;
  crashedAt: string;
  fill: string;
};

function ChartTooltip({
  active,
  payload,
}: {
  active?: boolean;
  payload?: Array<{ payload: ChartPoint }>;
}) {
  if (!active || !payload?.[0]) return null;
  const point = payload[0].payload;
  return (
    <div className="rounded-md bg-surface-2 px-3 py-2 text-xs shadow-[var(--shadow-border)]">
      <p className="font-mono text-sm tabular-nums text-fg">
        {formatMultiplier(point.multiplier)}×
      </p>
      <p className="mt-0.5 text-muted">
        #{point.gameId} · {formatClock(point.crashedAt)}
      </p>
    </div>
  );
}

export function CrashChart({ rounds }: { rounds: CrashRound[] }) {
  const data: ChartPoint[] = rounds.map((round) => ({
    gameId: round.gameId,
    multiplier: round.multiplier,
    bar: Math.min(round.multiplier, BAR_CAP),
    crashedAt: round.crashedAt,
    fill: bandFill(round.multiplier),
  }));

  if (data.length === 0) {
    return (
      <div className="flex h-48 items-center justify-center text-sm text-muted">
        Waiting for the first recorded round.
      </div>
    );
  }

  return (
    <div className="h-48 w-full md:h-56">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data} margin={{ top: 8, right: 8, left: 0, bottom: 0 }} barCategoryGap="18%">
          <XAxis dataKey="gameId" hide />
          <YAxis
            domain={[0, BAR_CAP]}
            ticks={[0, 2, 5, 10]}
            tick={{ fill: "var(--color-subtle)", fontSize: 11 }}
            axisLine={false}
            tickLine={false}
            width={36}
            tickFormatter={(v: number) => `${v}×`}
          />
          <ReferenceLine y={2} stroke="var(--color-border)" strokeDasharray="3 3" />
          <Tooltip
            cursor={{ fill: "color-mix(in oklab, var(--color-fg) 6%, transparent)" }}
            content={<ChartTooltip />}
          />
          <Bar dataKey="bar" radius={[3, 3, 0, 0]} maxBarSize={18} isAnimationActive={false}>
            {data.map((entry) => (
              <Cell key={entry.gameId} fill={entry.fill} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
