# Spec: identity-api

**Service:** Identity Service
**Base path:** `/api/v1/identity`
**Auth:** `Authorization: Bearer <token>` required

---

## Overview

The Identity API provides current-user resolution. Used on app bootstrap to determine
who is logged in, their role, and which team they belong to.

---

## Types

```ts
/** Defined in src/app/types/identity.ts */
type PersonData = {
  person_id: string;   // Stable unique identifier (e.g. 'p1')
  name: string;        // Display name
  role: string;        // 'executive' | 'team_lead' | 'ic' (or custom strings)
  seniority: string;   // 'Junior' | 'Middle' | 'Senior' | 'Lead'
};
```

---

## Endpoints

### `GET /api/v1/identity/persons/me`

Returns the currently authenticated person.

**Response:** `PersonData`

**Errors:**
- `401` — token missing or expired → `ProblemDetail`
- `404` — person record not found → `ProblemDetail`

---

## Frontend Implementation

- `src/app/types/identity.ts` — `PersonData`
- `src/app/api/IdentityApiService.ts` — `getMe(): Promise<PersonData>`
