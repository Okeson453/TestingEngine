import { format, formatDistanceToNow } from "date-fns";
import { bandForMultiplier, formatMultiplier } from "@/lib/crash/stats";

export { formatMultiplier };

export function formatClock(iso: string): string {
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return "—";
  return format(date, "HH:mm:ss");
}

export function formatRelative(iso: string): string {
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return "";
  return formatDistanceToNow(date, { addSuffix: true });
}

export function formatStat(value: number | null): string {
  if (value === null || !Number.isFinite(value)) return "—";
  return `${formatMultiplier(value)}×`;
}

export function bandClass(value: number): string {
  const band = bandForMultiplier(value);
  if (band === "low") return "text-low";
  if (band === "high") return "text-high";
  return "text-moon";
}

export function bandFill(value: number): string {
  const band = bandForMultiplier(value);
  if (band === "low") return "var(--color-low)";
  if (band === "high") return "var(--color-high)";
  return "var(--color-moon)";
}

export function formatPlayers(n: number | null): string | null {
  if (n === null || !Number.isFinite(n)) return null;
  return new Intl.NumberFormat("en", { maximumFractionDigits: 0 }).format(n);
}
