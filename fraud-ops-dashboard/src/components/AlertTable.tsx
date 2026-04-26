const alerts = [
  ["10:15:42", "TXN-8f3a7c1d", "U-100982", "M-77321", "$12,850", "0.92", "0.88", "0.94", "Critical"],
  ["10:14:11", "TXN-e91b2d4f", "U-104332", "M-11987", "$8,430", "0.87", "0.79", "0.91", "Critical"],
  ["10:12:03", "TXN-2c7d9a5e", "U-100221", "M-55612", "$5,210", "0.81", "0.85", "0.89", "High"],
  ["10:11:27", "TXN-7a4e6b2c", "U-108877", "M-33991", "$3,200", "0.74", "0.72", "0.80", "High"],
  ["10:09:54", "TXN-1d6f8c9b", "U-102334", "M-66780", "$7,900", "0.71", "0.78", "0.77", "High"],
  ["10:07:18", "TXN-5b2e1a44", "U-109442", "M-22410", "$1,240", "0.58", "0.61", "0.64", "Medium"],
  ["10:05:33", "TXN-0c9a3f12", "U-101876", "M-88120", "$980", "0.51", "0.54", "0.59", "Medium"],
  ["10:03:40", "TXN-4e7b2d91", "U-106773", "M-55190", "$620", "0.43", "0.49", "0.46", "Low"],
  ["10:01:22", "TXN-9a1c8d77", "U-103219", "M-44012", "$310", "0.31", "0.36", "0.33", "Low"],
];

function severityClass(severity: string) {
  if (severity === "Critical") return "text-[var(--color-risk-red)]";
  if (severity === "High") return "text-[var(--color-warning-orange)]";
  if (severity === "Medium") return "text-[#d6a23f]";
  return "text-[var(--color-safe-green)]";
}

function AlertTable() {
  return (
    <div className="max-h-[360px] overflow-auto rounded-lg border border-[var(--color-border)]">
      <table className="w-full text-left text-xs">
        <thead className="sticky top-0 bg-[var(--color-panel-soft)] text-[var(--color-text-muted)]">
          <tr>
            {["Time", "Transaction", "User", "Merchant", "Amount", "Fraud P.", "Anomaly", "Risk", "Severity"].map((h) => (
              <th key={h} className="px-3 py-2 font-medium">{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {alerts.map((row) => (
            <tr key={row[1]} className="border-t border-[var(--color-border)] hover:bg-[var(--color-panel-soft)]/50">
              {row.map((cell, index) => (
                <td
                  key={`${row[1]}-${index}`}
                  className={`px-3 py-2 ${
                    index === 8 ? `font-semibold ${severityClass(cell)}` : ""
                  }`}
                >
                  {cell}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default AlertTable;