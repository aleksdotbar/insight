import { useSyncExternalStore } from "react";

import { authStore } from "./auth-store";

const DEV_VIEWER_EMAIL =
  (import.meta.env.VITE_DEV_USER_EMAIL as string | undefined) ?? "";

export type ViewerSource = "oidc" | "dev" | "none";

export type Viewer = {
  email: string | null;
  source: ViewerSource;
};

function resolve(): Viewer {
  const snap = authStore.getSnapshot();
  if (snap.user?.email) return { email: snap.user.email, source: "oidc" };
  if (DEV_VIEWER_EMAIL) return { email: DEV_VIEWER_EMAIL, source: "dev" };
  return { email: null, source: "none" };
}

export function useViewer(): Viewer {
  // Subscribe to authStore so the hook re-renders when the OIDC user changes
  // (signIn, refresh, signOut). NOTE: `resolve()` returns a new object every
  // call — no referential stability across renders. Three call sites today
  // (routes/index, routes/ic.$person.team, components/app-sidebar) all
  // destructure `email`, so the new ref doesn't propagate. If a caller ever
  // passes the whole `Viewer` to a `React.memo`'d child, wrap in `useMemo`
  // or memoize inside `resolve()`.
  useSyncExternalStore(
    authStore.subscribe,
    authStore.getSnapshot,
    authStore.getSnapshot,
  );
  return resolve();
}

export function getViewerEmail(): string | null {
  return resolve().email;
}

export function isDevImpersonating(): boolean {
  return import.meta.env.DEV && Boolean(DEV_VIEWER_EMAIL);
}
