/**
 * Central gate for all mock plugins in the app.
 *
 * Mocks leaking into a real screen are a silent data-integrity bug — the
 * UI renders plausible numbers with no signal that they're synthetic. This
 * module is the single source of truth: mocks are OFF unless you are
 * running a dev build AND you opt in with `VITE_ENABLE_MOCKS=true`.
 *
 * Production builds: `import.meta.env.DEV === false`, so Vite's dead-code
 * elimination drops this branch entirely and any dynamic `import('./mocks')`
 * behind it never ships.
 *
 * Dev builds without the flag: function returns false; mock registration
 * is skipped; requests go to the vite-proxied real backend.
 *
 * Dev builds with `VITE_ENABLE_MOCKS=true`: returns true; mock plugins are
 * loaded; `<MockBanner>` renders a persistent warning strip so you cannot
 * forget that the data on screen is fake.
 */
export function mocksEnabled(): boolean {
  return (
    import.meta.env.DEV &&
    import.meta.env.VITE_ENABLE_MOCKS === 'true'
  );
}
