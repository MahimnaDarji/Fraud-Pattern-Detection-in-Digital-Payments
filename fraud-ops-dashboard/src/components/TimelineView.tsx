import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import Panel from "./Panel";

const timelineEvents = [
  {
    time: "10:15:42",
    event: "Critical fraud alert triggered",
    txn: "TXN-8f3a7c1d",
    severity: "Critical",
    detail: "High anomaly score with propagated network risk above 0.90.",
  },
  {
    time: "10:14:11",
    event: "Network propagation spike",
    txn: "TXN-e91b2d4f",
    severity: "Critical",
    detail: "Connected entities increased risk score across 37-node component.",
  },
  {
    time: "10:12:03",
    event: "Multiple risky counterparties detected",
    txn: "TXN-2c7d9a5e",
    severity: "High",
    detail: "Transaction linked to merchants in a known high-risk cluster.",
  },
  {
    time: "10:07:18",
    event: "Device mismatch detected",
    txn: "TXN-5b2e1a44",
    severity: "Medium",
    detail: "Known user account appeared from a device outside normal pattern.",
  },
  {
    time: "10:03:40",
    event: "Minor velocity change",
    txn: "TXN-4e7b2d91",
    severity: "Low",
    detail: "Small frequency increase observed within rolling window.",
  },
];

const latencyData = [
  { time: "10:00", ingest: 12, scoring: 28, alerting: 41 },
  { time: "10:05", ingest: 14, scoring: 31, alerting: 45 },
  { time: "10:10", ingest: 11, scoring: 29, alerting: 39 },
  { time: "10:15", ingest: 18, scoring: 36, alerting: 52 },
  { time: "10:20", ingest: 15, scoring: 33, alerting: 47 },
];

const tooltipStyle = {
  background: "var(--color-panel-soft)",
  border: "1px solid var(--color-border)",
  borderRadius: "8px",
  color: "var(--color-text-primary)",
};

function severityClass(severity: string) {
  if (severity === "Critical") return "bg-[var(--color-risk-red)]";
  if (severity === "High") return "bg-[var(--color-warning-orange)]";
  if (severity === "Medium") return "bg-[#d6a23f]";
  return "bg-[var(--color-safe-green)]";
}

function TimelineView() {
  return (
    <div className="grid grid-cols-1 gap-4 xl:grid-cols-[1.35fr_1fr]">
      <Panel title="Streaming Event Timeline">
        <div className="space-y-4">
          {timelineEvents.map((item) => (
            <div key={item.txn} className="flex gap-4 rounded-lg bg-[var(--color-panel-soft)] p-3">
              <div className="flex w-20 shrink-0 flex-col items-end">
                <span className="text-xs font-semibold text-[var(--color-text-primary)]">
                  {item.time}
                </span>
                <span className="mt-1 text-[10px] text-[var(--color-text-muted)]">
                  UTC
                </span>
              </div>

              <div className="flex flex-col items-center">
                <span className={`h-3 w-3 rounded-full ${severityClass(item.severity)}`} />
                <span className="mt-1 h-full w-px bg-[var(--color-border)]" />
              </div>

              <div className="min-w-0 flex-1">
                <div className="flex items-center justify-between gap-3">
                  <h3 className="text-sm font-semibold text-[var(--color-text-primary)]">
                    {item.event}
                  </h3>
                  <span className="rounded-md border border-[var(--color-border)] px-2 py-1 text-[10px] text-[var(--color-text-muted)]">
                    {item.severity}
                  </span>
                </div>

                <p className="mt-1 text-xs text-[var(--color-text-muted)]">
                  {item.txn}
                </p>
                <p className="mt-2 text-xs leading-5 text-[var(--color-text-secondary)]">
                  {item.detail}
                </p>
              </div>
            </div>
          ))}
        </div>
      </Panel>

      <div className="space-y-4">
        <Panel title="Pipeline Latency">
          <ResponsiveContainer width="100%" height={270}>
            <LineChart data={latencyData} margin={{ top: 38, right: 16, left: -8, bottom: 4 }}>
              <CartesianGrid stroke="rgba(127,143,166,0.15)" />
              <XAxis dataKey="time" stroke="var(--color-text-muted)" fontSize={11} />
              <YAxis stroke="var(--color-text-muted)" fontSize={11} />
              <Tooltip contentStyle={tooltipStyle} />
              <Legend
                align="left"
                verticalAlign="top"
                height={32}
                iconType="circle"
                wrapperStyle={{
                  fontSize: 11,
                  color: "var(--color-text-muted)",
                  paddingBottom: 12,
                }}
              />
              <Line name="Ingest ms" dataKey="ingest" stroke="var(--color-safe-green)" strokeWidth={2} dot={false} />
              <Line name="Scoring ms" dataKey="scoring" stroke="var(--color-accent-blue)" strokeWidth={2} dot={false} />
              <Line name="Alerting ms" dataKey="alerting" stroke="var(--color-warning-orange)" strokeWidth={2} dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </Panel>

        <Panel title="Processing Efficiency">
          <div className="space-y-4 text-sm">
            <div className="rounded-lg bg-[var(--color-panel-soft)] p-3">
              <p className="text-[var(--color-text-muted)]">Avg End-to-End Latency</p>
              <p className="mt-1 text-2xl font-semibold">38 ms</p>
              <p className="mt-1 text-xs text-[var(--color-text-muted)]">
                Ingest → Scoring → Alert generation
              </p>
            </div>

            <div className="rounded-lg bg-[var(--color-panel-soft)] p-3">
              <p className="text-[var(--color-text-muted)]">Alerts Processed / min</p>
              <p className="mt-1 text-2xl font-semibold text-[var(--color-accent-blue)]">
                1,284
              </p>
              <p className="mt-1 text-xs text-[var(--color-text-muted)]">
                Stable throughput over last 10 mins
              </p>
            </div>

            <div className="rounded-lg bg-[var(--color-panel-soft)] p-3">
              <p className="text-[var(--color-text-muted)]">Peak Load Window</p>
              <p className="mt-1 text-lg font-semibold">10:12 - 10:18 UTC</p>
              <p className="mt-1 text-xs text-[var(--color-text-muted)]">
                Highest spike in anomaly propagation
              </p>
            </div>
          </div>
        </Panel>
      </div>
    </div>
  );
}

export default TimelineView;