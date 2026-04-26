import { useState } from "react";
import AlertTable from "./components/AlertTable";
import AlertsView from "./components/AlertsView";
import {
  AlertReasonsChart,
  AlertsTrendChart,
  SeverityDonutChart,
} from "./components/Charts";
import GraphSignalsView from "./components/GraphSignalsView";
import KpiCard from "./components/KpiCard";
import Panel from "./components/Panel";
import Tabs from "./components/Tabs";
import type { TabKey } from "./components/Tabs";
import TimelineView from "./components/TimelineView";
import { kpis } from "./data/mockDashboardData";
import MainLayout from "./layouts/MainLayout";

function App() {
  const [activeTab, setActiveTab] = useState<TabKey>("overview");

  return (
    <MainLayout>
      <div className="space-y-4">
        <section>
          <h2 className="text-2xl font-semibold tracking-tight">
            Fraud Detection in Digital Payments
          </h2>
        </section>

        <section className="grid grid-cols-1 gap-3 md:grid-cols-3 xl:grid-cols-6">
          {kpis.map((kpi) => (
            <KpiCard
              key={kpi.label}
              label={kpi.label}
              value={String(kpi.value)}
              delta={`${kpi.delta}% vs prev 7 days`}
              direction={kpi.direction}
              accent={kpi.accent}
            />
          ))}
        </section>

        <Tabs activeTab={activeTab} onChange={setActiveTab} />

        {activeTab === "overview" && (
          <>
            <section className="grid grid-cols-1 gap-4 xl:grid-cols-3">
              <Panel title="Alerts Over Time">
                <AlertsTrendChart />
              </Panel>

              <Panel title="Alerts by Severity">
                <SeverityDonutChart />
              </Panel>

              <Panel title="Top Alert Reasons">
                <AlertReasonsChart />
              </Panel>
            </section>

            <section className="grid grid-cols-1 gap-4 xl:grid-cols-[2fr_1fr]">
              <Panel title="Recent High Risk Alerts">
                <AlertTable />
              </Panel>

              <Panel title="Network Risk Summary">
                <div className="space-y-4 text-sm">
                  <div className="rounded-lg bg-[var(--color-panel-soft)] p-3">
                    <p className="text-[var(--color-text-muted)]">
                      Largest Risk Cluster
                    </p>
                    <p className="mt-1 text-xl font-semibold">
                      42 linked entities
                    </p>
                  </div>

                  <div className="rounded-lg bg-[var(--color-panel-soft)] p-3">
                    <p className="text-[var(--color-text-muted)]">
                      Highest Propagated Risk
                    </p>
                    <p className="mt-1 text-xl font-semibold text-[var(--color-risk-red)]">
                      0.94
                    </p>
                  </div>

                  <div className="rounded-lg bg-[var(--color-panel-soft)] p-3">
                    <p className="text-[var(--color-text-muted)]">
                      Suspicious Triangles
                    </p>
                    <p className="mt-1 text-xl font-semibold text-[var(--color-warning-orange)]">
                      18
                    </p>
                  </div>
                </div>
              </Panel>
            </section>
          </>
        )}

        {activeTab === "alerts" && <AlertsView />}

        {activeTab === "graph" && <GraphSignalsView />}

        {activeTab === "timeline" && <TimelineView />}
      </div>
    </MainLayout>
  );
}

export default App;