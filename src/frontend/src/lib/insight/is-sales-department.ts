/**
 * Whether a person's department should render the sales / CRM dashboard
 * (`<SalesDashboard>`) instead of the engineering one.
 *
 * The frontend build is shared across tenants, and the population that owns
 * CRM metrics differs per org:
 *   - the prod org tags these reps with a "Sales" department
 *     (also "Inside Sales", etc.);
 *   - the dev org (`insight-dev-vhc`) keeps them under a "Global Services"
 *     department — surfaced from the HR source via `person.department`
 *     (there is no separate "Functional Team" field in the ingest/identity
 *     pipeline; department is the only attribute carrying this value).
 *
 * Source data is messy — "Global Services", "Global Services Team",
 * "Global  Services" (double space), "Global-Services" all occur — so the
 * match is case-insensitive and tolerant of whitespace / hyphen separators.
 */
export function isSalesDepartment(dept?: string | null): boolean {
  const value = dept ?? "";
  return /\bsales\b/i.test(value) || /\bglobal[\s-]+services\b/i.test(value);
}
