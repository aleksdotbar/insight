/**
 * Display-variant branching helpers for the IC dashboard.
 *
 * The dashboard route is shared across disciplines; the page component picks a
 * dedicated display component (engineering vs sales) based on the viewer's
 * `_identity.department`. Discipline is a separate axis from `UserRole`
 * (executive | team_lead | ic) — those describe seniority / scope of access,
 * not the kind of work the person does.
 */

/**
 * `true` when the department string indicates a sales role.
 *
 * BambooHR free-form values seen in Constructor: "Sales", "Inside Sales",
 * "Sales Operations". The word-boundary regex matches all of those without
 * matching unrelated strings like "Wholesale".
 */
export function isSalesDepartment(dept?: string | null): boolean {
  return /\bsales\b/i.test(dept ?? '');
}
