const KEY = "viewer:override";
const PARAM = "__override";

const listeners = new Set<() => void>();

function emit(): void {
  for (const fn of listeners) fn();
}

function safeRead(): string | null {
  try {
    return sessionStorage.getItem(KEY);
  } catch {
    return null;
  }
}

function safeWrite(value: string | null): void {
  try {
    if (value === null) sessionStorage.removeItem(KEY);
    else sessionStorage.setItem(KEY, value);
  } catch {
    return;
  }
}

export const overrideStore = {
  subscribe(fn: () => void): () => void {
    listeners.add(fn);
    return () => {
      listeners.delete(fn);
    };
  },
  getSnapshot: safeRead,
};

export function getOverrideEmail(): string | null {
  return safeRead();
}

export function captureOverrideFromUrl(): void {
  if (typeof window === "undefined") return;
  const url = new URL(window.location.href);
  const raw = url.searchParams.get(PARAM);
  if (raw === null) return;
  safeWrite(raw.trim() || null);
  url.searchParams.delete(PARAM);
  window.history.replaceState(window.history.state, "", url);
  emit();
}

export function clearOverride(): void {
  safeWrite(null);
  emit();
}
