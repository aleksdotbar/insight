import type { AuthSnapshot, AuthStatus, AuthUser } from "./types";

let snapshot: AuthSnapshot = {
  status: "idle",
  token: null,
  user: null,
  tenantId: null,
  error: null,
};

const listeners = new Set<() => void>();

function emit(): void {
  for (const fn of listeners) fn();
}

export const authStore = {
  subscribe(fn: () => void): () => void {
    listeners.add(fn);
    return () => {
      listeners.delete(fn);
    };
  },

  getSnapshot(): AuthSnapshot {
    return snapshot;
  },

  setStatus(status: AuthStatus, error: string | null = null): void {
    if (snapshot.status === status && snapshot.error === error) return;
    snapshot = { ...snapshot, status, error };
    emit();
  },

  setToken(token: string | null): void {
    if (snapshot.token === token) return;
    snapshot = { ...snapshot, token };
    emit();
  },

  setUser(user: AuthUser | null): void {
    snapshot = { ...snapshot, user };
    emit();
  },

  setTenantId(tenantId: string | null): void {
    if (snapshot.tenantId === tenantId) return;
    snapshot = { ...snapshot, tenantId };
    emit();
  },

  reset(): void {
    snapshot = {
      status: "idle",
      token: null,
      user: null,
      tenantId: null,
      error: null,
    };
    emit();
  },
};
