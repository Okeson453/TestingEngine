/**
 * Health check endpoint for monitoring system liveness.
 *
 * P3.10: Add Health Check Endpoint
 * Provides a simple endpoint to check if the worker is running and healthy.
 */

import { getSql } from "./db";
import { getLogger } from "./observability/logger";

const logger = getLogger("health");

export interface HealthStatus {
  status: "healthy" | "degraded" | "unhealthy";
  timestamp: string;
  checks: {
    database: "ok" | "error";
    databaseError?: string;
  };
  version: string;
  uptime: number;
}

const startTime = Date.now();

/**
 * Perform health checks and return system status.
 */
export async function getHealthStatus(): Promise<HealthStatus> {
  const checks: HealthStatus["checks"] = {
    database: "ok",
  };

  // Check database connectivity
  try {
    const sql = await getSql();
    await sql`SELECT 1`;
  } catch (error) {
    checks.database = "error";
    checks.databaseError = String(error);
    logger.error(
      { component: "health", error: String(error) },
      "Health check: database connection failed",
    );
  }

  // Determine overall status
  let status: HealthStatus["status"] = "healthy";
  if (checks.database === "error") {
    status = "unhealthy";
  }

  return {
    status,
    timestamp: new Date().toISOString(),
    checks,
    version: process.env.npm_package_version ?? "unknown",
    uptime: Math.round((Date.now() - startTime) / 1000), // seconds
  };
}

/**
 * Simple health check handler for HTTP endpoints.
 * Returns 200 OK for healthy, 503 for unhealthy.
 */
export async function healthCheckHandler(): Promise<{
  status: number;
  body: HealthStatus;
}> {
  const health = await getHealthStatus();
  const statusCode = health.status === "healthy" ? 200 : 503;
  return {
    status: statusCode,
    body: health,
  };
}

/**
 * Liveness probe - simpler check that just verifies the process is running.
 */
export function livenessCheck(): { status: "live"; uptime: number } {
  return {
    status: "live",
    uptime: Math.round((Date.now() - startTime) / 1000),
  };
}
