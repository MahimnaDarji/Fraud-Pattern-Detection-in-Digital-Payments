export type SeverityFilter = "All" | "Critical" | "High" | "Medium" | "Low";

export type DashboardFilters = {
  severity: SeverityFilter;
  userId: string;
  merchantId: string;
  transactionId: string;
  startDate: string;
  endDate: string;
};