/**
 * Insight UI Actions
 * Emits cross-screen UI preference events (viewMode, etc).
 */

import { eventBus } from '@hai3/react';
import { InsightUiEvents } from '../events/insightUiEvents';
import type { ViewMode } from '../types';

export const changeViewMode = (mode: ViewMode): void => {
  eventBus.emit(InsightUiEvents.ViewModeChanged, mode);
};
