import { useSyncExternalStore } from "react";

import { authStore } from "./auth-store";
import { readDevUserEmail } from "./dev-config";
import { getOverrideEmail, overrideStore } from "./impersonation";

const BUILD_DEV_VIEWER_EMAIL =
  (import.meta.env.VITE_DEV_USER_EMAIL as string | undefined) ?? "";

const MOCK_VIEWER_EMAIL = "bob.park@example.com";
const MOCKS_ENABLED = import.meta.env.VITE_ENABLE_MOCKS === "true";

export type ViewerSource = "override" | "oidc" | "dev" | "none";

export type Viewer = {
  email: string | null;
  source: ViewerSource;
};

// Production builds only honor a dev email explicitly injected at runtime
// (window.__DEV_CONFIG__.devUserEmail). The build-time VITE_DEV_USER_EMAIL
// fallback is consulted only in Vite dev — otherwise a production build
// that happened to have a VITE_ value baked in would silently impersonate.
function resolveDevEmail(): string {
  const runtime = readDevUserEmail();
  if (runtime) return runtime;
  return import.meta.env.DEV ? BUILD_DEV_VIEWER_EMAIL : "";
}

function resolve(): Viewer {
  const override = getOverrideEmail();
  if (override) return { email: override, source: "override" };
  const snap = authStore.getSnapshot();
  if (snap.user?.email) return { email: snap.user.email, source: "oidc" };
  if (MOCKS_ENABLED) return { email: MOCK_VIEWER_EMAIL, source: "dev" };
  const devEmail = resolveDevEmail();
  if (devEmail) return { email: devEmail, source: "dev" };
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
  useSyncExternalStore(
    overrideStore.subscribe,
    overrideStore.getSnapshot,
    overrideStore.getSnapshot,
  );
  return resolve();
}

export function getViewerEmail(): string | null {
  return resolve().email;
}

export function isDevImpersonating(): boolean {
  // True whenever a dev-mode viewer is the source of identity. Used by the
  // banner / hint components to surface "you're impersonating" UI. Safe in
  // any build: `resolveDevEmail()` returns "" in a production bundle that
  // hasn't been opted in via window.__DEV_CONFIG__.
  return MOCKS_ENABLED || Boolean(resolveDevEmail());
}
