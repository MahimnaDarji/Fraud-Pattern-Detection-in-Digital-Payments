export type TrendPoint = {
  day: string;
  all: number;
  critical: number;
  high: number;
  medium: number;
  low: number;
};

export type SeverityPoint = {
  name: "Critical" | "High" | "Medium" | "Low";
  value: number;
  color: string;
};

export type AlertReasonPoint = {
  reason: string;
  count: number;
};

export type DashboardChartsResponse = {
  trend: TrendPoint[];
  severity: SeverityPoint[];
  reasons: AlertReasonPoint[];
};