import { useSyncExternalStore } from "react";

import { authStore } from "./auth-store";
import { OidcManager } from "./oidc-manager";
import type { AuthSnapshot } from "./types";

export type UseAuthResult = AuthSnapshot & {
  signIn: () => Promise<void>;
  signOut: () => Promise<void>;
};

export function useAuth(): UseAuthResult {
  const snap = useSyncExternalStore(
    authStore.subscribe,
    authStore.getSnapshot,
    authStore.getSnapshot,
  );
  return {
    ...snap,
    signIn: OidcManager.signIn,
    signOut: OidcManager.signOut,
  };
}
