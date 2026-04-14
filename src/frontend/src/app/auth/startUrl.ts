/**
 * Captures the initial page URL before any router modifies it.
 * Must be called once at app startup, before React render.
 */

let startUrl: string | null = null;

export function storeStartUrl(): void {
  startUrl = window.location.href;
}

export function getStartUrl(): string | null {
  return startUrl;
}
