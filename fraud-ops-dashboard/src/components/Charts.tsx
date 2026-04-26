import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  Line,
  LineChart,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

const trendData = [
  { day: "Apr 15", all: 180, critical: 42, high: 68, medium: 46, low: 24 },
  { day: "Apr 16", all: 240, critical: 58, high: 82, medium: 64, low: 36 },
  { day: "Apr 17", all: 290, critical: 61, high: 96, medium: 86, low: 47 },
  { day: "Apr 18", all: 340, critical: 79, high: 112, medium: 94, low: 55 },
  { day: "Apr 19", all: 318, critical: 69, high: 106, medium: 89, low: 54 },
  { day: "Apr 20", all: 285, critical: 54, high: 98, medium: 82, low: 51 },
  { day: "Apr 21", all: 252, critical: 49, high: 88, medium: 73, low: 42 },
];

const severityData = [
  { name: "Critical", value: 87, color: "var(--color-risk-red)" },
  { name: "High", value: 243, color: "var(--color-warning-orange)" },
  { name: "Medium", value: 468, color: "#d6a23f" },
  { name: "Low", value: 450, color: "var(--color-safe-green)" },
];

const reasonData = [
  { reason: "High anomaly score", count: 456 },
  { reason: "Network risk propagation", count: 238 },
  { reason: "Multiple risky counterparties", count: 186 },
  { reason: "Transaction velocity", count: 158 },
  { reason: "Unusual amount", count: 132 },
];

const tooltipStyle = {
  background: "var(--color-panel-soft)",
  border: "1px solid var(--color-border)",
  borderRadius: "8px",
  color: "var(--color-text-primary)",
};

const legendStyle = {
  fontSize: 11,
  color: "var(--color-text-muted)",
  paddingBottom: 12,
};

export function AlertsTrendChart() {
  return (
    <ResponsiveContainer width="100%" height={270}>
      <LineChart data={trendData} margin={{ top: 38, right: 16, left: -8, bottom: 4 }}>
        <CartesianGrid stroke="rgba(127,143,166,0.15)" />
        <XAxis dataKey="day" stroke="var(--color-text-muted)" fontSize={11} />
        <YAxis stroke="var(--color-text-muted)" fontSize={11} />
        <Tooltip contentStyle={tooltipStyle} />
        <Legend
          align="left"
          verticalAlign="top"
          height={32}
          iconType="circle"
          wrapperStyle={legendStyle}
        />
        <Line name="All Alerts" dataKey="all" stroke="var(--color-accent-blue)" strokeWidth={2} dot={false} />
        <Line name="Critical" dataKey="critical" stroke="var(--color-risk-red)" strokeWidth={2} dot={false} />
        <Line name="High" dataKey="high" stroke="var(--color-warning-orange)" strokeWidth={2} dot={false} />
        <Line name="Medium" dataKey="medium" stroke="#d6a23f" strokeWidth={2} dot={false} />
        <Line name="Low" dataKey="low" stroke="var(--color-safe-green)" strokeWidth={2} dot={false} />
      </LineChart>
    </ResponsiveContainer>
  );
}

export function SeverityDonutChart() {
  return (
    <ResponsiveContainer width="100%" height={270}>
      <PieChart margin={{ top: 20, right: 20, bottom: 20, left: 20 }}>
        <Pie data={severityData} dataKey="value" innerRadius={62} outerRadius={92}>
          {severityData.map((entry) => (
            <Cell key={entry.name} fill={entry.color} />
          ))}
        </Pie>
        <Tooltip contentStyle={tooltipStyle} />
        <Legend
          layout="vertical"
          align="right"
          verticalAlign="middle"
          wrapperStyle={{ fontSize: 12, color: "var(--color-text-muted)" }}
        />
      </PieChart>
    </ResponsiveContainer>
  );
}

export function AlertReasonsChart() {
  return (
    <ResponsiveContainer width="100%" height={270}>
      <BarChart data={reasonData} layout="vertical" margin={{ top: 34, right: 20, left: 34, bottom: 4 }}>
        <XAxis type="number" hide />
        <YAxis
          type="category"
          dataKey="reason"
          width={170}
          stroke="var(--color-text-muted)"
          fontSize={11}
        />
        <Tooltip contentStyle={tooltipStyle} />
        <Legend
          align="left"
          verticalAlign="top"
          height={28}
          wrapperStyle={legendStyle}
        />
        <Bar dataKey="count" name="Alert Count" fill="var(--color-accent-blue)" radius={[0, 6, 6, 0]} />
      </BarChart>
    </ResponsiveContainer>
  );
}