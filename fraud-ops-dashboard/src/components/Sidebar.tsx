function Sidebar() {
  return (
    <aside className="w-[280px] shrink-0 border-r border-[var(--color-border)] bg-[var(--color-bg-sidebar)] px-5 py-5">
      <div className="mb-7 border-b border-[var(--color-border)] pb-5">
        <h1 className="text-base font-semibold tracking-tight text-[var(--color-text-primary)]">
          FraudGuardian
        </h1>
        <p className="mt-1 text-xs text-[var(--color-text-muted)]">
          Investigation Console
        </p>
      </div>

      <section className="mb-6">
        <h2 className="text-sm font-semibold text-[var(--color-text-primary)]">
          Investigation Controls
        </h2>
        <p className="mt-2 text-xs leading-5 text-[var(--color-text-muted)]">
          Scope alerts, transactions, and graph signals before reviewing risk.
        </p>
      </section>

      <button className="mb-6 w-full rounded-lg border border-[var(--color-border)] bg-[var(--color-panel)] px-3 py-2 text-sm text-[var(--color-text-secondary)] transition hover:border-[var(--color-accent-blue)] hover:text-[var(--color-text-primary)]">
        Clear all filters
      </button>

      <section className="space-y-4">
        <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-[var(--color-text-muted)]">
          Scope Filters
        </p>

        <div>
          <label className="mb-1.5 block text-xs text-[var(--color-text-secondary)]">
            Severity
          </label>
          <select className="w-full rounded-lg border border-[var(--color-border)] bg-[var(--color-panel)] px-3 py-2 text-sm text-[var(--color-text-primary)] outline-none">
            <option>All</option>
            <option>Critical</option>
            <option>High</option>
            <option>Medium</option>
            <option>Low</option>
          </select>
        </div>

        <div>
          <label className="mb-1.5 block text-xs text-[var(--color-text-secondary)]">
            User / Account ID
          </label>
          <input
            className="w-full rounded-lg border border-[var(--color-border)] bg-[var(--color-panel)] px-3 py-2 text-sm text-[var(--color-text-primary)] outline-none placeholder:text-[var(--color-text-muted)]"
            placeholder="Enter user or account"
          />
        </div>

        <div>
          <label className="mb-1.5 block text-xs text-[var(--color-text-secondary)]">
            Merchant ID
          </label>
          <input
            className="w-full rounded-lg border border-[var(--color-border)] bg-[var(--color-panel)] px-3 py-2 text-sm text-[var(--color-text-primary)] outline-none placeholder:text-[var(--color-text-muted)]"
            placeholder="Enter merchant"
          />
        </div>

        <div>
          <label className="mb-1.5 block text-xs text-[var(--color-text-secondary)]">
            Transaction ID
          </label>
          <input
            className="w-full rounded-lg border border-[var(--color-border)] bg-[var(--color-panel)] px-3 py-2 text-sm text-[var(--color-text-primary)] outline-none placeholder:text-[var(--color-text-muted)]"
            placeholder="Enter transaction ID"
          />
        </div>
      </section>

    </aside>
  );
}

export default Sidebar;