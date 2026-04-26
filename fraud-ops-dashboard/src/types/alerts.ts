export type AlertSeverity = "Critical" | "High" | "Medium" | "Low";

export type FraudAlert = {
  time: string;
  transaction_id: string;
  user_id: string;
  merchant_id: string;
  amount: string;
  fraud_probability: number;
  anomaly_score: number;
  propagated_risk: number;
  severity: AlertSeverity;
  reason: string;
};