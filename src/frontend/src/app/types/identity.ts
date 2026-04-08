/**
 * Identity types — current user shape returned by GET /api/v1/identity/persons/me
 */

export type PersonData = {
  person_id: string;
  name: string;
  role: string;
  seniority: string;
};
