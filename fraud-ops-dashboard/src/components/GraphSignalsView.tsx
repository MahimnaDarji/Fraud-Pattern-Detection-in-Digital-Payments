import {
  Bar,
  BarChart,
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
  ZAxis,
} from "recharts";
import Panel from "./Panel";

const graphSignals = [
  ["U-100982", "User", "0.94", "42", "18", "Critical"],
  ["M-77321", "Merchant", "0.91", "37", "15", "Critical"],
  ["U-104332", "User", "0.86", "31", "12", "High"],
  ["M-11987", "Merchant", "0.82", "28", "9", "High"],
  ["U-100221", "User", "0.74", "22", "6", "Medium"],
  ["M-55612", "Merchant", "0.61", "15", "3", "Medium"],
];

const nodeRiskData = [
  { entity: "U-100982", risk: 0.94 },
  { entity: "M-77321", risk: 0.91 },
  { entity: "U-104332", risk: 0.86 },
  { entity: "M-11987", risk: 0.82 },
  { entity: "U-100221", risk: 0.74 },
  { entity: "M-55612", risk: 0.61 },
];

const clusterData = [
  { cluster: "C1", size: 42, risk: 0.94 },
  { cluster: "C2", size: 37, risk: 0.91 },
  { cluster: "C3", size: 31, risk: 0.86 },
  { cluster: "C4", size: 22, risk: 0.74 },
  { cluster: "C5", size: 15, risk: 0.61 },
];

const propagationTrend = [
  { time: "10:00", risk: 0.58 },
  { time: "10:05", risk: 0.64 },
  { time: "10:10", risk: 0.72 },
  { time: "10:15", risk: 0.81 },
  { time: "10:20", risk: 0.94 },
];

const tooltipStyle = {
  background: "var(--color-panel-soft)",
  border: "1px solid var(--color-border)",
  borderRadius: "8px",
  color: "var(--color-text-primary)",
};

function severityClass(severity: string) {
  if (severity === "Critical") return "text-[var(--color-risk-red)]";
  if (severity === "High") return "text-[var(--color-warning-orange)]";
  if (severity === "Medium") return "text-[#d6a23f]";
  return "text-[var(--color-safe-green)]";
}

function GraphSignalsView() {
  return (
    <div className="space-y-4">
      <section className="grid grid-cols-1 gap-4 xl:grid-cols-3">
        <Panel title="Top Risk Nodes">
          <ResponsiveContainer width="100%" height={230}>
            <BarChart data={nodeRiskData} layout="vertical" margin={{ left: 24 }}>
              <XAxis type="number" domain={[0, 1]} hide />
              <YAxis
                type="category"
                dataKey="entity"
                width={80}
                stroke="var(--color-text-muted)"
                fontSize={11}
              />
              <Tooltip contentStyle={tooltipStyle} />
              <Bar
                dataKey="risk"
                fill="var(--color-risk-red)"
                radius={[0, 6, 6, 0]}
              />
            </BarChart>
          </ResponsiveContainer>
        </Panel>

        <Panel title="Cluster Size vs Risk">
          <ResponsiveContainer width="100%" height={230}>
            <ScatterChart margin={{ top: 12, right: 16, bottom: 8, left: -8 }}>
              <CartesianGrid stroke="rgba(127,143,166,0.15)" />
              <XAxis
                dataKey="size"
                name="Cluster Size"
                stroke="var(--color-text-muted)"
                fontSize={11}
              />
              <YAxis
                dataKey="risk"
                name="Risk"
                domain={[0, 1]}
                stroke="var(--color-text-muted)"
                fontSize={11}
              />
              <ZAxis range={[80, 260]} />
              <Tooltip contentStyle={tooltipStyle} />
              <Scatter
                name="Clusters"
                data={clusterData}
                fill="var(--color-accent-blue)"
              />
            </ScatterChart>
          </ResponsiveContainer>
        </Panel>

        <Panel title="Risk Propagation Trend">
          <ResponsiveContainer width="100%" height={230}>
            <LineChart data={propagationTrend} margin={{ top: 12, right: 16, left: -12 }}>
              <CartesianGrid stroke="rgba(127,143,166,0.15)" />
              <XAxis dataKey="time" stroke="var(--color-text-muted)" fontSize={11} />
              <YAxis domain={[0, 1]} stroke="var(--color-text-muted)" fontSize={11} />
              <Tooltip contentStyle={tooltipStyle} />
              <Line
                dataKey="risk"
                name="Propagated Risk"
                stroke="var(--color-warning-orange)"
                strokeWidth={2}
                dot={{ r: 3 }}
              />
            </LineChart>
          </ResponsiveContainer>
        </Panel>
      </section>

      <section className="grid grid-cols-1 gap-4 xl:grid-cols-[1.5fr_1fr]">
        <Panel title="Graph Risk Signals">
          <div className="overflow-hidden rounded-lg border border-[var(--color-border)]">
            <table className="w-full text-left text-xs">
              <thead className="bg-[var(--color-panel-soft)] text-[var(--color-text-muted)]">
                <tr>
                  {[
                    "Entity",
                    "Type",
                    "Propagated Risk",
                    "Component Size",
                    "Triangle Count",
                    "Severity",
                  ].map((h) => (
                    <th key={h} className="px-3 py-2 font-medium">
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>

              <tbody>
                {graphSignals.map((row) => (
                  <tr
                    key={row[0]}
                    className="border-t border-[var(--color-border)] hover:bg-[var(--color-panel-soft)]/50"
                  >
                    {row.map((cell, index) => (
                      <td
                        key={`${row[0]}-${index}`}
                        className={`px-3 py-2 ${
                          index === 5
                            ? `font-semibold ${severityClass(cell)}`
                            : ""
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
        </Panel>

        <Panel title="Network Summary">
          <div className="grid grid-cols-2 gap-3 text-sm">
            <div className="rounded-lg bg-[var(--color-panel-soft)] p-3">
              <p className="text-[var(--color-text-muted)]">Total Nodes</p>
              <p className="mt-1 text-2xl font-semibold">1,842</p>
            </div>
            <div className="rounded-lg bg-[var(--color-panel-soft)] p-3">
              <p className="text-[var(--color-text-muted)]">Total Edges</p>
              <p className="mt-1 text-2xl font-semibold">4,913</p>
            </div>
            <div className="rounded-lg bg-[var(--color-panel-soft)] p-3">
              <p className="text-[var(--color-text-muted)]">Communities</p>
              <p className="mt-1 text-2xl font-semibold text-[var(--color-accent-blue)]">
                36
              </p>
            </div>
            <div className="rounded-lg bg-[var(--color-panel-soft)] p-3">
              <p className="text-[var(--color-text-muted)]">Risk Clusters</p>
              <p className="mt-1 text-2xl font-semibold text-[var(--color-risk-red)]">
                11
              </p>
            </div>
          </div>

          <div className="mt-4 rounded-lg bg-[var(--color-panel-soft)] p-3">
            <p className="text-sm text-[var(--color-text-muted)]">
              Highest Risk Pattern
            </p>
            <p className="mt-1 text-lg font-semibold">
              Circular transfer cluster with repeated merchant exposure
            </p>
          </div>
        </Panel>
      </section>
    </div>
  );
}

export default GraphSignalsView;