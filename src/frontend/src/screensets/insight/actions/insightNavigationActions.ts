/**
 * Insight Navigation Actions
 *
 * Translates a generic {screenId, param} click from the Menu layer into a
 * typed insight-domain SelectionRequested event. The domain knowledge
 * (IC ↔ person, team-view ↔ TeamRef, my-dashboard ↔ no selection) lives here
 * so the layout layer can stay screenset-agnostic.
 */

import { eventBus } from '@hai3/react';
import {
  InsightNavigationEvents,
  type SelectionRequest,
} from '../events/insightNavigationEvents';
import {
  IC_DASHBOARD_SCREEN_ID,
  TEAM_VIEW_SCREEN_ID,
  MY_DASHBOARD_SCREEN_ID,
  EXECUTIVE_VIEW_SCREEN_ID,
} from '../ids';

/**
 * Emit a typed selection-request derived from a raw (screenId, param) pair.
 * A `param` containing '@' is treated as an email (person_subtree for teams,
 * person_id for IC); otherwise teams receive an org_unit_name.
 */
export const requestSelection = (screenId: string, param?: string): void => {
  let req: SelectionRequest | null = null;

  if (screenId === IC_DASHBOARD_SCREEN_ID && param) {
    req = { screen: IC_DASHBOARD_SCREEN_ID, personId: param };
  } else if (screenId === TEAM_VIEW_SCREEN_ID && param) {
    req = {
      screen: TEAM_VIEW_SCREEN_ID,
      teamRef: param.includes('@')
        ? { kind: 'person_subtree', email: param }
        : { kind: 'org_unit_name', value: param },
    };
  } else if (screenId === MY_DASHBOARD_SCREEN_ID) {
    req = { screen: MY_DASHBOARD_SCREEN_ID };
  } else if (screenId === EXECUTIVE_VIEW_SCREEN_ID) {
    req = { screen: EXECUTIVE_VIEW_SCREEN_ID };
  }

  if (req) eventBus.emit(InsightNavigationEvents.SelectionRequested, req);
};
