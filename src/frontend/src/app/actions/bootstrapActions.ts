/**
 * Bootstrap Actions
 *
 * Actions for app-level bootstrap operations.
 * Following flux architecture: Actions emit events, Effects listen and dispatch.
 */

import { eventBus, apiRegistry } from '@hai3/react';
import type { ApiUser } from '@/app/api';
import { IdentityApiService } from '@/app/api/IdentityApiService';
import { CurrentUserEvents } from '@/screensets/insight/events/currentUserEvents';
import type { IdentityPerson } from '@/app/types/identity';

/** Decode JWT payload without verification (token already validated by backend). */
function decodeJwtPayload(token: string): Record<string, unknown> | null {
  try {
    const parts = token.split('.');
    if (parts.length !== 3) return null;
    const payload = atob(parts[1].replace(/-/g, '+').replace(/_/g, '/'));
    return JSON.parse(payload) as Record<string, unknown>;
  } catch {
    return null;
  }
}

/**
 * Threshold at which a manager is treated as "executive" (gets the
 * org-level view). Counted as total reports across the whole subtree,
 * not just direct reports.
 */
const EXECUTIVE_THRESHOLD_REPORTS = 10;

/** Total descendants in a subordinate subtree (recursive). */
function countAllSubordinates(p: IdentityPerson): number {
  return p.subordinates.reduce((n, s) => n + 1 + countAllSubordinates(s), 0);
}

/** Derive a role from identity data, driven by actual reporting tree size. */
function deriveRole(person: IdentityPerson): 'executive' | 'team_lead' | 'ic' {
  const total = countAllSubordinates(person);
  if (total >= EXECUTIVE_THRESHOLD_REPORTS) return 'executive';
  if (person.subordinates.length > 0) return 'team_lead';
  return 'ic';
}

/**
 * Fetch current user — resolves identity from JWT sub claim.
 * Called by Layout on mount. Emits events for header + menu updates.
 */
export function fetchCurrentUser(): void {
  // Fire-and-forget: read token from OIDC session, resolve identity
  const storageKey = Object.keys(sessionStorage).find((k) => k.startsWith('oidc.user:'));
  if (!storageKey) return;
  const stored = sessionStorage.getItem(storageKey);
  if (!stored) return;

  let token: string;
  try {
    token = (JSON.parse(stored) as { access_token?: string }).access_token ?? '';
  } catch { return; }
  if (!token) return;

  const claims = decodeJwtPayload(token);
  const email = typeof claims?.sub === 'string' ? claims.sub : '';
  if (!email) return;

  const identity = apiRegistry.getService(IdentityApiService);
  void identity.getPersonByEmail(email).then((person) => {
    eventBus.emit('app/user/loaded', {
      user: { firstName: person.first_name, lastName: person.last_name, email: person.email } as ApiUser,
    });

    eventBus.emit(CurrentUserEvents.UserChanged, {
      personId: person.email,
      name: person.display_name,
      role: deriveRole(person),
      teamId: person.department,
      _identity: person,
    });
  }).catch(() => {
    // Identity resolution failed — emit a typed event so UI can surface the
    // error state explicitly (done in Phase 3 — see plan). For now, swallow.
  });
}

/**
 * Notify that user data has been loaded
 * Called by screens after successfully fetching user data.
 * Emits 'app/user/loaded' event so header state updates.
 */
export function notifyUserLoaded(user: ApiUser): void {
  eventBus.emit('app/user/loaded', { user });
}
