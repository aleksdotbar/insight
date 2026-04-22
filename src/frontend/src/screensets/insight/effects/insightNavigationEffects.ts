/**
 * Insight Navigation Effects
 *
 * Listens for SelectionRequested events and updates the userContext slice.
 */

import { type AppDispatch, eventBus } from '@hai3/react';
import { InsightNavigationEvents } from '../events/insightNavigationEvents';
import {
  setSelectedPerson,
  setSelectedTeam,
  clearSelection,
} from '../slices/userContextSlice';
import {
  IC_DASHBOARD_SCREEN_ID,
  TEAM_VIEW_SCREEN_ID,
  MY_DASHBOARD_SCREEN_ID,
  EXECUTIVE_VIEW_SCREEN_ID,
} from '../ids';

export const initializeInsightNavigationEffects = (dispatch: AppDispatch): void => {
  eventBus.on(InsightNavigationEvents.SelectionRequested, (req) => {
    switch (req.screen) {
      case IC_DASHBOARD_SCREEN_ID:
        dispatch(setSelectedPerson(req.personId));
        return;
      case TEAM_VIEW_SCREEN_ID:
        dispatch(setSelectedTeam(req.teamRef));
        return;
      case MY_DASHBOARD_SCREEN_ID:
      case EXECUTIVE_VIEW_SCREEN_ID:
        dispatch(clearSelection());
        return;
    }
  });
};
