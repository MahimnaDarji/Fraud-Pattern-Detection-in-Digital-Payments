import type { DashboardFilters } from "../types/filters";

export function buildQueryParams(filters: DashboardFilters) {
  const params = new URLSearchParams();

  if (filters.severity !== "All") params.set("severity", filters.severity);
  if (filters.userId.trim()) params.set("user_id", filters.userId.trim());
  if (filters.merchantId.trim()) params.set("merchant_id", filters.merchantId.trim());
  if (filters.transactionId.trim()) params.set("transaction_id", filters.transactionId.trim());
  if (filters.startDate) params.set("start_date", filters.startDate);
  if (filters.endDate) params.set("end_date", filters.endDate);

  return params.toString();
}