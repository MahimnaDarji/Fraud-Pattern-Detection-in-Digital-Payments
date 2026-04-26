function TopBar() {
  return (
    <div className="flex h-11 items-center justify-between rounded-lg border border-[var(--color-border)] bg-[var(--color-panel)] px-4">
      <div className="flex items-center gap-3">
        <div className="flex items-center gap-2 rounded-md bg-[var(--color-panel-soft)] px-2.5 py-1">
          <span className="h-2 w-2 rounded-full bg-[var(--color-safe-green)]" />
          <span className="text-xs font-semibold text-[var(--color-text-primary)]">
            System Healthy
          </span>
        </div>
        <span className="text-xs text-[var(--color-text-muted)]">
          All systems operational
        </span>
      </div>

      <div className="flex items-center gap-3">
        <span className="text-xs font-medium text-[var(--color-text-muted)]">
          Auto refresh
        </span>

        <button className="relative h-5 w-9 rounded-full bg-[var(--color-accent-blue)]">
          <span className="absolute right-0.5 top-0.5 h-4 w-4 rounded-full bg-white" />
        </button>

        <select className="h-8 rounded-md border border-[var(--color-border)] bg-[var(--color-panel-soft)] px-2 text-xs text-[var(--color-text-primary)] outline-none">
          <option>10s</option>
          <option>30s</option>
          <option>60s</option>
        </select>

        <button className="h-8 rounded-md border border-[var(--color-border)] bg-[var(--color-panel-soft)] px-3 text-xs text-[var(--color-text-primary)] transition hover:border-[var(--color-accent-blue)]">
          Refresh
        </button>
      </div>
    </div>
  );
}

export default TopBar;