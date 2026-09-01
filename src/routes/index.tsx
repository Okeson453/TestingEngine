import { createFileRoute } from "@tanstack/react-router";
import { CrashDashboard } from "@/components/dashboard/crash-dashboard";
import { refreshDashboard } from "@/lib/crash/api";

export const Route = createFileRoute("/")({
  loader: () => refreshDashboard(),
  component: Home,
});

function Home() {
  const initial = Route.useLoaderData();
  return <CrashDashboard initial={initial} />;
}
