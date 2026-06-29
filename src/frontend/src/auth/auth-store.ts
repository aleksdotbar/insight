import type { AuthReason, AuthSnapshot, AuthStatus, AuthUser } from "./types";

let snapshot: AuthSnapshot = {
  status: "initializing",
  token: null,
  user: null,
  tenantId: null,
  reason: null,
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

  setStatus(status: AuthStatus, reason: AuthReason | null = null): void {
    if (snapshot.status === status && snapshot.reason === reason) return;
    snapshot = { ...snapshot, status, reason };
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
      status: "initializing",
      token: null,
      user: null,
      tenantId: null,
      reason: null,
    };
    emit();
  },
};
