import { useSyncExternalStore } from "react";

import { authStore } from "./auth-store";

/**
 * The current viewer, derived from the session summary (`/auth/me`). The SPA is
 * email-keyed (org tree, IC routes), so `email` is the primary handle;
 * `personId` (the gateway JWT `sub`) is exposed for callers that need the UUID.
 */
export type Viewer = {
  email: string | null;
  personId: string | null;
};

function resolve(): Viewer {
  const { session } = authStore.getSnapshot();
  return {
    email: session?.email ?? null,
    personId: session?.personId ?? null,
  };
}

export function useViewer(): Viewer {
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
