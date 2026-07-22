/**
 * `subordinates` is empty until the backend `expand_subordinates` flag is on
 * (see `PersonResponse.cs`). When mocks are off and the endpoint fails the
 * caller surfaces the failure to the UI — never silently falls back to seeded
 * data.
 *
 * The legacy `GET /persons/{email}` lookup (RFC 8594 deprecated) is replaced by
 * `POST /profiles` with a `{ value_type, value }` body. The wire shape
 * (`ProfileResponse`) mirrors the C# `PersonResponse`, but nearly every field is
 * optional; we normalize it back into the required-string `IdentityPerson`
 * projection the UI already consumes so callers and the org-tree sidebar are
 * unchanged.
 */

import { fetchWithAuth } from "@/api/fetch-with-auth";
import type { IdentityPerson } from "@/types/insight";

const BASE =
  (import.meta.env.VITE_IDENTITY_BASE as string | undefined) ??
  "/api/identity/v1";

/** Wire shape of `POST /profiles` (snake_case; optional fields omitted). */
interface ProfileResponse {
  person_id: string;
  insight_tenant_id: string;
  email?: string;
  display_name?: string;
  first_name?: string;
  last_name?: string;
  department?: string;
  division?: string;
  job_title?: string;
  status?: string;
  username?: string;
  employee_id?: string;
  supervisor_email?: string;
  supervisor_name?: string;
  parent_email?: string;
  parent_person_id?: string;
  subordinates?: ProfileResponse[];
  ids?: unknown[];
}

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

/**
 * Normalize a `ProfileResponse` into the FE `IdentityPerson`. `email` is the UI
 * identity (org-tree sidebar links + React keys), so the top-level profile is
 * guaranteed to carry one by `getPerson`; subordinates the wire returns without
 * an email are dropped rather than projected to `""` — an empty email would make
 * broken sidebar links and collide as duplicate React keys across siblings. Other
 * optional strings default to `""`; omitted parent/supervisor fields stay `null`.
 */
function toIdentityPerson(p: ProfileResponse): IdentityPerson {
  return {
    person_id: p.person_id,
    email: p.email ?? "",
    display_name: p.display_name ?? "",
    first_name: p.first_name ?? "",
    last_name: p.last_name ?? "",
    department: p.department ?? "",
    division: p.division ?? "",
    job_title: p.job_title ?? "",
    status: p.status ?? "",
    parent_email: p.parent_email ?? null,
    // `parent_id` has no ProfileResponse source; preserve the prior default.
    parent_id: null,
    parent_person_id: p.parent_person_id ?? null,
    supervisor_email: p.supervisor_email ?? null,
    supervisor_name: p.supervisor_name ?? null,
    subordinates: (p.subordinates ?? [])
      .filter((s) => Boolean(s.email?.trim()))
      .map(toIdentityPerson),
  };
}

export async function getPerson(email: string): Promise<IdentityPerson> {
  const url = `${BASE}/profiles`;
  const res = await fetchWithAuth(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ value_type: "email", value: email }),
  });
  if (!res.ok) {
    const body = await res.json().catch(() => null);
    throw new IdentityApiError(res.status, body);
  }
  let profile: ProfileResponse;
  try {
    profile = (await res.json()) as ProfileResponse;
  } catch {
    throw new IdentityApiError(res.status, { error: "invalid_json" });
  }
  // `email` is the queried identity + the UI's key; a profile without it is
  // unusable, so surface it rather than projecting an empty-string person.
  if (!profile.email?.trim()) {
    throw new IdentityApiError(res.status, { error: "missing_email" });
  }
  return toIdentityPerson(profile);
}
