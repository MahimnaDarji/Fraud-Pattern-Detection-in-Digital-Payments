export type KpiMetric = {
  label: string;
  value: number | string;
  delta: number;
  direction: "up" | "down" | "flat";
  accent: "blue" | "red" | "orange" | "green";
};

export type DashboardMetricsResponse = {
  kpis: KpiMetric[];
};