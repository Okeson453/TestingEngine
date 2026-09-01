import {
  featureRowsFromCrashPoints,
  runRegimeFitJob,
} from '../regimes/regime-fit-job.ts';
import type { LearnedClusteringModel } from '../regimes/learned-clustering.ts';

export function fitRegimesOffline(crashPoints: number[], k = 8): LearnedClusteringModel {
  const { rows, outcomes } = featureRowsFromCrashPoints(crashPoints);
  return runRegimeFitJob({ featureRows: rows, outcomes, k });
}
