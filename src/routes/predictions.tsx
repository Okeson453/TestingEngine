import { createFileRoute } from "@tanstack/react-router";
import { PredictionPanel } from "@/components/prediction/prediction-panel";
import { predictionGetDashboardSnapshot } from "@/lib/p";

export const Route = createFileRoute("/predictions")({
  component: PredictionsPage,
  loader: async () => {
    // P0: one pinned snapshot instead of 7 concurrent server fns (pool exhaustion).
    const snap = await predictionGetDashboardSnapshot();
    return {
      dailyTarget: snap.dailyTarget,
      today: snap.today,
      lifetime: snap.lifetime,
      streaks: snap.streaks,
      recent: snap.recent,
      pending: snap.pending,
      worker: snap.worker,
    };
  },
  head: () => ({
    meta: [{ title: "Prediction Validation — CrashWave" }],
  }),
});

function PredictionsPage() {
  const data = Route.useLoaderData();
  return <PredictionPanel initial={data} />;
}
