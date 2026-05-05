/**
 * Mock Data Registry — Single Source of Truth
 *
 * All mock data derives from these definitions. Every handler, factory,
 * and identity service must reference this registry instead of having
 * its own hardcoded lists.
 */

// ---------------------------------------------------------------------------
// Teams
// ---------------------------------------------------------------------------

export type MockTeam = {
  id: string;
  name: string;
};

export const TEAMS: MockTeam[] = [
  { id: 'backend',  name: 'Backend' },
  { id: 'frontend', name: 'Frontend' },
  { id: 'platform', name: 'Platform' },
  { id: 'data',     name: 'Data' },
  { id: 'qa',       name: 'QA' },
  { id: 'mobile',   name: 'Mobile' },
];

// ---------------------------------------------------------------------------
// People
// ---------------------------------------------------------------------------

export type MockPerson = {
  person_id: string;
  name: string;
  team_id: string;
  role: string;
  seniority: string;
  is_lead: boolean;
  ai_tools: string[];
  /**
   * Email of this person's supervisor in the demo-tenant identity tree, or
   * `null` for top-level managers. Orthogonal to `team_id`: people can be
   * on a team but report to a manager outside that team — the demo tenant
   * has Bob Park as a cross-functional lead whose direct reports span
   * multiple `team_id` values. Consumed by `buildIdentityTree` to assemble
   * the recursive `IdentityPersonRaw` tree the Identity Resolution mock
   * returns.
   */
  supervisor_email: string | null;
};

// person_id is the canonical email used as the identity key throughout
// the FE — this is what the IR-tree subordinates carry, what the OData
// $filter `person_id eq '<email>'` looks for in TEAM_MEMBER queries, and
// what `mockIcScenario(personId)` hashes for IC bullet seeding.
//
// Demo-tenant hierarchy used by `buildIdentityTree`:
//   carol.chen (top)
//     └── bob.park (default impersonation target — `VITE_DEV_USER_EMAIL`)
//           ├── alice.kim
//           ├── david.liu
//           ├── eve.novak
//           ├── grace.wu
//           ├── iris.tan
//           └── leo.dunn
//   frank.moss, hank.reed, jake.fox, kira.sato — independent leads (top)
//
// `supervisor_email` is orthogonal to `team_id`: David Liu is on the
// frontend team but reports to Bob Park in the demo tenant, because the
// impersonated user (`bob.park@example.com`) needs a 6-person direct-reports
// surface for screenshots and team-view exercise.
export const PEOPLE: MockPerson[] = [
  // Top-level
  { person_id: 'carol.chen@example.com',    name: 'Carol Chen',  team_id: 'frontend', role: 'Tech Lead',                seniority: 'Staff',     is_lead: true,  ai_tools: ['Codex'],                 supervisor_email: null },
  // Bob Park's branch — six cross-functional reports
  { person_id: 'bob.park@example.com',      name: 'Bob Park',    team_id: 'backend',  role: 'Tech Lead',                seniority: 'Staff',     is_lead: true,  ai_tools: ['Cursor'],                supervisor_email: 'carol.chen@example.com' },
  { person_id: 'alice.kim@example.com',     name: 'Alice Kim',   team_id: 'backend',  role: 'Senior Software Engineer', seniority: 'Senior',    is_lead: false, ai_tools: ['Cursor', 'Claude Code'], supervisor_email: 'bob.park@example.com' },
  { person_id: 'david.liu@example.com',     name: 'David Liu',   team_id: 'frontend', role: 'Junior Software Engineer', seniority: 'Junior',    is_lead: false, ai_tools: ['Cursor'],                supervisor_email: 'bob.park@example.com' },
  { person_id: 'eve.novak@example.com',     name: 'Eve Novak',   team_id: 'platform', role: 'Staff Software Engineer',  seniority: 'Staff',     is_lead: false, ai_tools: ['Cursor', 'Claude Code'], supervisor_email: 'bob.park@example.com' },
  { person_id: 'grace.wu@example.com',      name: 'Grace Wu',    team_id: 'data',     role: 'Software Engineer',        seniority: 'Mid',       is_lead: false, ai_tools: ['Cursor'],                supervisor_email: 'bob.park@example.com' },
  { person_id: 'iris.tan@example.com',      name: 'Iris Tan',    team_id: 'qa',       role: 'Junior Software Engineer', seniority: 'Junior',    is_lead: false, ai_tools: [],                        supervisor_email: 'bob.park@example.com' },
  { person_id: 'leo.dunn@example.com',      name: 'Leo Dunn',    team_id: 'mobile',   role: 'Junior Software Engineer', seniority: 'Junior',    is_lead: false, ai_tools: ['Cursor'],                supervisor_email: 'bob.park@example.com' },
  // Independent leads — no demo subordinates
  { person_id: 'frank.moss@example.com',    name: 'Frank Moss',  team_id: 'platform', role: 'Tech Lead',                seniority: 'Principal', is_lead: true,  ai_tools: ['Claude Code'],           supervisor_email: null },
  { person_id: 'hank.reed@example.com',     name: 'Hank Reed',   team_id: 'data',     role: 'Tech Lead',                seniority: 'Senior',    is_lead: true,  ai_tools: ['Cursor', 'Codex'],       supervisor_email: null },
  { person_id: 'jake.fox@example.com',      name: 'Jake Fox',    team_id: 'qa',       role: 'Tech Lead',                seniority: 'Mid',       is_lead: true,  ai_tools: ['Cursor'],                supervisor_email: null },
  { person_id: 'kira.sato@example.com',     name: 'Kira Sato',   team_id: 'mobile',   role: 'Tech Lead',                seniority: 'Senior',    is_lead: true,  ai_tools: ['Claude Code', 'Codex'],  supervisor_email: null },
  // Extra members to vary team headcounts on the executive view.
  // Final shape: backend=4, frontend=4, platform=4, data=3, qa=2, mobile=2.
  // Noah/Olivia report to Alice (Backend sub-lead) — keeps Bob's direct
  // reports at 6 so sidebar count == team-view "(6/6)" for Bob.
  { person_id: 'noah.bell@example.com',     name: 'Noah Bell',     team_id: 'backend',  role: 'Senior Software Engineer', seniority: 'Senior',    is_lead: false, ai_tools: ['Cursor', 'Claude Code'], supervisor_email: 'alice.kim@example.com' },
  { person_id: 'olivia.park@example.com',   name: 'Olivia Park',   team_id: 'backend',  role: 'Software Engineer',        seniority: 'Mid',       is_lead: false, ai_tools: ['Claude Code'],           supervisor_email: 'alice.kim@example.com' },
  { person_id: 'priya.shah@example.com',    name: 'Priya Shah',    team_id: 'frontend', role: 'Software Engineer',        seniority: 'Mid',       is_lead: false, ai_tools: ['Cursor'],                supervisor_email: 'carol.chen@example.com' },
  { person_id: 'quinn.lee@example.com',     name: 'Quinn Lee',     team_id: 'frontend', role: 'Junior Software Engineer', seniority: 'Junior',    is_lead: false, ai_tools: [],                        supervisor_email: 'carol.chen@example.com' },
  { person_id: 'ravi.iyer@example.com',     name: 'Ravi Iyer',     team_id: 'platform', role: 'Senior Software Engineer', seniority: 'Senior',    is_lead: false, ai_tools: ['Codex'],                 supervisor_email: 'frank.moss@example.com' },
  { person_id: 'sara.bishop@example.com',   name: 'Sara Bishop',   team_id: 'platform', role: 'Software Engineer',        seniority: 'Mid',       is_lead: false, ai_tools: ['Cursor', 'Codex'],       supervisor_email: 'frank.moss@example.com' },
  { person_id: 'tom.alvarez@example.com',   name: 'Tom Alvarez',   team_id: 'data',     role: 'Software Engineer',        seniority: 'Mid',       is_lead: false, ai_tools: ['Cursor'],                supervisor_email: 'hank.reed@example.com' },
];

// ---------------------------------------------------------------------------
// Lookup helpers
// ---------------------------------------------------------------------------

/** Map person_id → MockPerson for O(1) lookup */
export const PEOPLE_BY_ID: Record<string, MockPerson> = Object.fromEntries(
  PEOPLE.map((p) => [p.person_id, p]),
);

/** Get all people in a team */
export function teamMembers(teamId: string): MockPerson[] {
  return PEOPLE.filter((p) => p.team_id === teamId);
}

/** Get team headcount */
export function teamHeadcount(teamId: string): number {
  return PEOPLE.filter((p) => p.team_id === teamId).length;
}

// ---------------------------------------------------------------------------
// Identity tree builder
// ---------------------------------------------------------------------------

/**
 * Raw shape returned by Identity Resolution's
 * `GET /api/identity-resolution/v1/persons/{email}` endpoint, mirrored here
 * to keep the screenset registry self-contained (no upward import into
 * `src/app/types`). Field names match `IdentityPersonRaw`.
 */
export type MockIdentityRaw = {
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
  subordinates: MockIdentityRaw[];
};

/**
 * Build the recursive identity-tree response Identity Resolution would
 * return for the supplied email, derived from `PEOPLE`. Returns `null` when
 * no person matches, mirroring the real backend's 404 path.
 *
 * Fields not encoded in `MockPerson` (department, division, status) get
 * sensible synthetic defaults derived from `team_id`. `first_name` /
 * `last_name` are split from `name` on the first whitespace.
 */
export function buildIdentityTree(email: string): MockIdentityRaw | null {
  const root = PEOPLE_BY_ID[email];
  if (!root) return null;

  const teamLabel = TEAMS.find((t) => t.id === root.team_id)?.name ?? root.team_id;
  const supervisor = root.supervisor_email ? PEOPLE_BY_ID[root.supervisor_email] : null;

  const splitName = (name: string): { first: string; last: string } => {
    const idx = name.indexOf(' ');
    return idx < 0
      ? { first: name, last: '' }
      : { first: name.slice(0, idx), last: name.slice(idx + 1) };
  };

  const toRaw = (p: MockPerson): MockIdentityRaw => {
    const { first, last } = splitName(p.name);
    const sup = p.supervisor_email ? PEOPLE_BY_ID[p.supervisor_email] : null;
    return {
      email: p.person_id,
      display_name: p.name,
      first_name: first,
      last_name: last,
      department: TEAMS.find((t) => t.id === p.team_id)?.name ?? p.team_id,
      division: 'Engineering',
      job_title: p.role,
      status: 'Active',
      supervisor_email: p.supervisor_email,
      supervisor_name: sup?.name ?? null,
      // Recursive: each direct report becomes a subtree.
      subordinates: PEOPLE
        .filter((other) => other.supervisor_email === p.person_id)
        .map(toRaw),
    };
  };

  const tree = toRaw(root);
  // Override only the root's supervisor name (already populated correctly
  // by toRaw, but kept explicit here for documentation).
  return { ...tree, supervisor_name: supervisor?.name ?? null, department: teamLabel };
}
