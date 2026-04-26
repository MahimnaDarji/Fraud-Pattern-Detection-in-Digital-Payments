type TabKey = "overview" | "alerts" | "graph" | "timeline";

type TabsProps = {
  activeTab: TabKey;
  onChange: (tab: TabKey) => void;
};

const tabs: { key: TabKey; label: string }[] = [
  { key: "overview", label: "Overview" },
  { key: "alerts", label: "Alerts" },
  { key: "graph", label: "Graph Signals" },
  { key: "timeline", label: "Timeline" },
];

function Tabs({ activeTab, onChange }: TabsProps) {
  return (
    <nav className="flex gap-7 border-b border-[var(--color-border)] text-sm">
      {tabs.map((tab) => (
        <button
          key={tab.key}
          onClick={() => onChange(tab.key)}
          className={`pb-3 transition ${
            activeTab === tab.key
              ? "border-b-2 border-[var(--color-accent-blue)] text-[var(--color-text-primary)]"
              : "text-[var(--color-text-muted)] hover:text-[var(--color-text-secondary)]"
          }`}
        >
          {tab.label}
        </button>
      ))}
    </nav>
  );
}

export default Tabs;
export type { TabKey };