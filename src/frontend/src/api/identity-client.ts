/**
 * `subordinates` is empty until the backend `expand_subordinates` flag is on
 * (see `PersonResponse.cs`). When mocks are off and the endpoint fails the
 * caller surfaces the failure to the UI — never silently falls back to seeded
 * data.
 */

import { fetchWithAuth } from "@/api/fetch-with-auth";
import type { IdentityPerson } from "@/types/insight";

const BASE =
  (import.meta.env.VITE_IDENTITY_BASE as string | undefined) ??
  "/api/identity/v1";

export class IdentityApiError extends Error {
  status: number;
  body: unknown;

  constructor(status: number, body: unknown) {
    super(`Identity API ${status}`);
    this.name = "IdentityApiError";
    this.status = status;
    this.body = body;
  }
}

export async function getPerson(email: string): Promise<IdentityPerson> {
  const url = `${BASE}/persons/${encodeURIComponent(email)}`;
  const res = await fetchWithAuth(url);
  if (!res.ok) {
    const body = await res.json().catch(() => null);
    throw new IdentityApiError(res.status, body);
  }
  try {
    return (await res.json()) as IdentityPerson;
  } catch {
    throw new IdentityApiError(res.status, { error: "invalid_json" });
  }
}
