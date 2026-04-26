type KpiCardProps = {
  label: string;
  value: string;
  delta: string;
  direction: "up" | "down" | "flat";
  accent: "blue" | "red" | "orange" | "green";
};

const accentClasses = {
  blue: "bg-blue-500/10 text-blue-300",
  red: "bg-red-500/10 text-red-300",
  orange: "bg-orange-500/10 text-orange-300",
  green: "bg-green-500/10 text-green-300",
};

function KpiCard({ label, value, delta, direction, accent }: KpiCardProps) {
  const arrow = direction === "down" ? "↓" : direction === "flat" ? "→" : "↑";

  return (
    <article className="rounded-lg border border-[var(--color-border)] bg-[var(--color-panel)] p-4 transition hover:border-[var(--color-accent-blue)]/60">
      <div className="flex items-center gap-3">
        <span
          className={`flex h-7 w-7 shrink-0 items-center justify-center rounded-md text-xs font-semibold ${accentClasses[accent]}`}
        >
          {label.charAt(0)}
        </span>
        <p className="truncate text-xs font-medium text-[var(--color-text-muted)]">
          {label}
        </p>
      </div>

      <p className="mt-3 text-2xl font-semibold tracking-tight text-[var(--color-text-primary)]">
        {value}
      </p>

      <p className="mt-2 text-xs font-medium text-[var(--color-risk-red)]">
        {arrow} {delta}
      </p>
    </article>
  );
}

export default KpiCard;