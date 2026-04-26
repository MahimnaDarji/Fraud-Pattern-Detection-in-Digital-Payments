import AlertTable from "./AlertTable";
import Panel from "./Panel";

const alertPoints = [
  { id: "TXN-8f3a7c1d", x: 82, y: 88, severity: "Critical", amount: "$12,850" },
  { id: "TXN-e91b2d4f", x: 76, y: 82, severity: "Critical", amount: "$8,430" },
  { id: "TXN-2c7d9a5e", x: 68, y: 78, severity: "High", amount: "$5,210" },
  { id: "TXN-7a4e6b2c", x: 58, y: 70, severity: "High", amount: "$3,200" },
  { id: "TXN-5b2e1a44", x: 42, y: 54, severity: "Medium", amount: "$1,240" },
  { id: "TXN-4e7b2d91", x: 24, y: 31, severity: "Low", amount: "$620" },
];

const radarSignals = [
  { label: "Identity", x: 50, y: 13, score: 82 },
  { label: "Device", x: 84, y: 32, score: 74 },
  { label: "Merchant", x: 78, y: 72, score: 68 },
  { label: "Network", x: 50, y: 88, score: 91 },
  { label: "Velocity", x: 20, y: 72, score: 63 },
  { label: "Amount", x: 16, y: 32, score: 79 },
];

function severityDot(severity: string) {
  if (severity === "Critical") return "bg-[var(--color-risk-red)]";
  if (severity === "High") return "bg-[var(--color-warning-orange)]";
  if (severity === "Medium") return "bg-[#d6a23f]";
  return "bg-[var(--color-safe-green)]";
}

function AlertsView() {
  const polygonPoints = radarSignals.map((p) => `${p.x},${p.y}`).join(" ");

  return (
    <div className="space-y-4">
      <section className="grid grid-cols-1 gap-4 xl:grid-cols-[1.25fr_1fr]">
        <Panel title="Alert Risk Matrix">
          <div className="relative h-[330px] rounded-lg border border-[var(--color-border)] bg-[var(--color-panel-soft)] p-4">
            <div className="absolute left-4 top-4 text-xs text-[var(--color-text-muted)]">
              Higher Fraud Probability
            </div>
            <div className="absolute bottom-4 right-4 text-xs text-[var(--color-text-muted)]">
              Higher Transaction Amount
            </div>

            <div className="absolute inset-12 rounded-lg border border-[var(--color-border)]">
              <div className="absolute left-1/2 top-0 h-full w-px bg-[var(--color-border)]" />
              <div className="absolute left-0 top-1/2 h-px w-full bg-[var(--color-border)]" />

              <div className="absolute left-3 top-3 text-[10px] text-[var(--color-risk-red)]">
                Critical Zone
              </div>
              <div className="absolute bottom-3 right-3 text-[10px] text-[var(--color-safe-green)]">
                Lower Risk Zone
              </div>

              {alertPoints.map((point) => (
                <div
                  key={point.id}
                  className={`absolute h-3.5 w-3.5 rounded-full ${severityDot(
                    point.severity
                  )} shadow-lg`}
                  style={{
                    left: `${point.x}%`,
                    bottom: `${point.y}%`,
                  }}
                  title={`${point.id} | ${point.severity} | ${point.amount}`}
                />
              ))}
            </div>
          </div>
        </Panel>

        <Panel title="Investigation Signal Radar">
          <div className="relative h-[330px] rounded-lg border border-[var(--color-border)] bg-[var(--color-panel-soft)]">
            <svg viewBox="0 0 100 100" className="h-full w-full">
              <polygon
                points="50,12 84,31 84,69 50,88 16,69 16,31"
                fill="none"
                stroke="rgba(127,143,166,0.24)"
                strokeWidth="0.5"
              />
              <polygon
                points="50,24 74,38 74,62 50,76 26,62 26,38"
                fill="none"
                stroke="rgba(127,143,166,0.18)"
                strokeWidth="0.5"
              />
              <polygon
                points="50,36 64,44 64,56 50,64 36,56 36,44"
                fill="none"
                stroke="rgba(127,143,166,0.14)"
                strokeWidth="0.5"
              />

              <line x1="50" y1="50" x2="50" y2="12" stroke="rgba(127,143,166,0.18)" strokeWidth="0.4" />
              <line x1="50" y1="50" x2="84" y2="31" stroke="rgba(127,143,166,0.18)" strokeWidth="0.4" />
              <line x1="50" y1="50" x2="84" y2="69" stroke="rgba(127,143,166,0.18)" strokeWidth="0.4" />
              <line x1="50" y1="50" x2="50" y2="88" stroke="rgba(127,143,166,0.18)" strokeWidth="0.4" />
              <line x1="50" y1="50" x2="16" y2="69" stroke="rgba(127,143,166,0.18)" strokeWidth="0.4" />
              <line x1="50" y1="50" x2="16" y2="31" stroke="rgba(127,143,166,0.18)" strokeWidth="0.4" />

              <polygon
                points={polygonPoints}
                fill="rgba(79,140,255,0.20)"
                stroke="var(--color-accent-blue)"
                strokeWidth="1"
              />

              {radarSignals.map((signal) => (
                <g key={signal.label}>
                  <circle
                    cx={signal.x}
                    cy={signal.y}
                    r="2"
                    fill="var(--color-accent-blue)"
                  />
                  <text
                    x={signal.x}
                    y={signal.y < 20 ? signal.y - 4 : signal.y + 6}
                    textAnchor="middle"
                    fontSize="3.2"
                    fill="var(--color-text-secondary)"
                  >
                    {signal.label}
                  </text>
                  <text
                    x={signal.x}
                    y={signal.y < 20 ? signal.y + 6 : signal.y + 11}
                    textAnchor="middle"
                    fontSize="2.8"
                    fill="var(--color-text-muted)"
                  >
                    {signal.score}
                  </text>
                </g>
              ))}

              <circle cx="50" cy="50" r="3" fill="var(--color-risk-red)" />
              <text
                x="50"
                y="47"
                textAnchor="middle"
                fontSize="3"
                fill="var(--color-text-primary)"
              >
                Risk Core
              </text>
            </svg>
          </div>
        </Panel>
      </section>

      <section>
        <Panel title="Alert Investigation Queue">
          <AlertTable />
        </Panel>
      </section>
    </div>
  );
}

export default AlertsView;