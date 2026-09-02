import { runWorkerCycle } from "@/lib/prediction/worker";

export default defineEventHandler(async () => {
  const result = await runWorkerCycle();
  return {
    ran: result.ran,
    fetched: result.fetched,
    inserted: result.inserted,
    resolved: result.resolved,
    generated: result.generated,
    error: result.error,
  };
});
