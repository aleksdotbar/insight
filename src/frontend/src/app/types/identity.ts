/**
 * Identity types — person data from Identity Resolution service
 */

export type PersonData = {
  person_id: string;
  name: string;
  role: string;
  seniority: string;
};

/** Raw response from Identity Resolution service — recursive subordinate tree */
export type IdentityPersonRaw = {
  email: string;
  display_name: string;
  first_name: string;
  last_name: string;
  department: string;
  division: string;
  job_title: string;
  status: string;
  supervisor_email: string | null;
  supervisor_name: string | null;
  subordinates: IdentityPersonRaw[];
};

/** Enriched person — raw response + compatibility aliases for UI components */
export type IdentityPerson = Omit<IdentityPersonRaw, 'subordinates'> & {
  /** Alias for display_name — used by UI components */
  name: string;
  /** Alias for email — used as person identifier in MVP */
  person_id: string;
  /** Derived from job_title */
  role: string;
  /** Placeholder — not available from BambooHR */
  seniority: string;
  /** Recursive subordinate tree */
  subordinates: IdentityPerson[];
};

/** Convert raw Identity Resolution response to enriched person (recursive) */
export function toIdentityPerson(raw: IdentityPersonRaw): IdentityPerson {
  return {
    ...raw,
    name: raw.display_name,
    person_id: raw.email,
    role: raw.job_title,
    seniority: '',
    subordinates: raw.subordinates.map(toIdentityPerson),
  };
}
