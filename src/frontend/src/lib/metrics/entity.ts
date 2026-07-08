/**
 * Person entity ids are emails, matched case-insensitively by the backend's
 * `normalize_entity_id` (trim + lowercase). Normalizing on the client keeps
 * query keys and response lookups stable regardless of the casing a route
 * param or identity record carries.
 */
export function normalizePersonId(email: string): string {
  return email.trim().toLowerCase();
}
