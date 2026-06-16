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

// Email to use for the unsigned dev-mode bearer token in fetch-with-auth.
// Returns null unless the active viewer source is dev-style (`dev` for
// MOCKS/runtime/build-time dev email, `override` for sessionStorage
// impersonation). This prevents a mid-bootstrap OIDC session — where
// authStore.token is null but a build-time dev email is also configured —
// from accidentally minting an unsigned JWT bearing the OIDC user's
// identity. The function is the source of truth for "is this request
// authenticated via dev impersonation?" and intentionally does NOT
// resolve to an OIDC viewer's email.
export function getDevBearerEmail(): string | null {
  const v = resolve();
  return v.source === "dev" || v.source === "override" ? v.email : null;
}

export function isDevImpersonating(): boolean {
  // True only when the ACTIVE viewer source is dev-style — not when a
  // dev email is merely configured. Without this check an OIDC session
  // running alongside a build-time dev email would falsely show the
  // impersonation banner / hint.
  const { source } = resolve();
  return source === "dev" || source === "override";
}
