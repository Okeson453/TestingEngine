import { createFileRoute } from "@tanstack/react-router";
import { PredictionPanel } from "@/components/prediction/prediction-panel";
import {
  predictionGetDailyTarget,
  predictionGetTodayStats,
  predictionGetLifetimeStats,
  predictionGetStreaks,
  predictionGetRecent,
  predictionGetPending,
  predictionGetWorkerStatus,
} from "@/lib/prediction/api";

export const Route = createFileRoute("/predictions")({
  component: PredictionsPage,
  loader: async () => {
    const [dailyTarget, today, lifetime, streaks, recent, pending, worker] = await Promise.all([
      predictionGetDailyTarget(),
      predictionGetTodayStats(),
      predictionGetLifetimeStats(),
      predictionGetStreaks(),
      predictionGetRecent(),
      predictionGetPending(),
      predictionGetWorkerStatus(),
    ]);
    return { dailyTarget, today, lifetime, streaks, recent, pending, worker };
  },
  head: () => ({
    meta: [{ title: "Prediction Validation — CrashWave" }],
  }),
});

function PredictionsPage() {
  const data = Route.useLoaderData();
  return <PredictionPanel initial={data} />;
}
