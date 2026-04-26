type MainLayoutProps = {
  children: React.ReactNode;
};

function MainLayout({ children }: MainLayoutProps) {
  return (
    <div className="min-h-screen bg-[var(--color-bg-main)] text-[var(--color-text-primary)]">
      <main className="mx-auto min-h-screen max-w-[1600px] px-6 py-5">
        {children}
      </main>
    </div>
  );
}

export default MainLayout;