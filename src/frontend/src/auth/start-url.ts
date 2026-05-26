let startUrl: string | null = null;

export function storeStartUrl(): void {
  startUrl = window.location.href;
}

export function getStartUrl(): string | null {
  return startUrl;
}
