/**
 * Team View Actions
 * Emit events AND interact with APIs (Flux pattern)
 * Following Flux: Actions emit events for effects to update Redux, and call APIs
 */

import { eventBus, apiRegistry } from '@hai3/react';
import { TeamViewEvents } from '../events/teamViewEvents';
import { InsightApiService } from '../api/insightApiService';
import type { PeriodValue } from '../types';

/**
 * Load team view data for the given period
 */
export const loadTeamView = (teamId: string, period: PeriodValue): void => {
  eventBus.emit(TeamViewEvents.TeamViewLoadStarted);
  void apiRegistry.getService(InsightApiService).getTeamViewData(teamId, period)
    .then((data) => { eventBus.emit(TeamViewEvents.TeamViewLoaded, data); })
    .catch((err: unknown) => { eventBus.emit(TeamViewEvents.TeamViewLoadFailed, String(err)); });
};
