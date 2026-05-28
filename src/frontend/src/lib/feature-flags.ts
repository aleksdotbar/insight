import { useSyncExternalStore } from "react";

const STORAGE_KEY = "insight.metrics-v2";

function readStorage(): boolean {
  if (typeof window === "undefined") return false;
  return window.localStorage.getItem(STORAGE_KEY) === "true";
}

let state = readStorage();
const listeners = new Set<() => void>();

function subscribe(fn: () => void): () => void {
  listeners.add(fn);
  return () => {
    listeners.delete(fn);
  };
}

function getSnapshot(): boolean {
  return state;
}

export function isMetricsV2Enabled(): boolean {
  return state;
}

export function setMetricsV2Enabled(enabled: boolean): void {
  state = enabled;
  if (typeof window !== "undefined") {
    try {
      window.localStorage.setItem(STORAGE_KEY, enabled ? "true" : "false");
    } catch {
      // localStorage unavailable — in-memory still updated.
    }
  }
  for (const fn of listeners) fn();
}

export function useMetricsV2Enabled(): boolean {
  return useSyncExternalStore(subscribe, getSnapshot, getSnapshot);
}
