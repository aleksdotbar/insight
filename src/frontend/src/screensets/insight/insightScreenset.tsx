/**
 * insight Screenset
 * Template screenset with full structure
 */

import {
  type ScreensetConfig,
  ScreensetCategory,
  registerSlice,
  I18nRegistry,
  Language,
  screensetRegistry,
  i18nRegistry,
} from '@hai3/react';
import { INSIGHT_SCREENSET_ID, DOCUMENTATION_SCREEN_ID, DASHBOARD_SCREEN_ID, SPEED_SCREEN_ID, UIKIT_SCREEN_ID, EXECUTIVE_VIEW_SCREEN_ID, TEAM_VIEW_SCREEN_ID, IC_DASHBOARD_SCREEN_ID, MY_DASHBOARD_SCREEN_ID } from './ids';
import { withAuth } from '@/app/auth/withAuth';
import insightSlice from './slices/insightSlice';
import { initializeInsightEffects } from './effects/insightEffects';
import periodSlice from './slices/periodSlice';
import { initializePeriodEffects } from './effects/periodEffects';
import executiveViewSlice from './slices/executiveViewSlice';
import { initializeExecutiveViewEffects } from './effects/executiveViewEffects';
import teamViewSlice from './slices/teamViewSlice';
import { initializeTeamViewEffects } from './effects/teamViewEffects';
import icDashboardSlice from './slices/icDashboardSlice';
import { initializeIcDashboardEffects } from './effects/icDashboardEffects';
import currentUserSlice from './slices/currentUserSlice';
import { initializeCurrentUserEffects } from './effects/currentUserEffects';

// Import for side effect - register API service
import './api/insightApiService';

// NOTE: Mocks are now registered globally via MockPlugin in main.tsx
// If this screenset needs mocks, add them to the global mockMap in main.tsx
// or create a screenset-specific MockPlugin registration here.

/**
 * Screenset-level translations
 * Register directly with i18nRegistry (not via screenset config)
 * All 36 languages must be provided for type safety
 */
i18nRegistry.registerLoader(
  `screenset.${INSIGHT_SCREENSET_ID}`,
  I18nRegistry.createLoader({
    [Language.English]: () => import('./i18n/en.json'),
    [Language.Arabic]: () => import('./i18n/ar.json'),
    [Language.Bengali]: () => import('./i18n/bn.json'),
    [Language.Czech]: () => import('./i18n/cs.json'),
    [Language.Danish]: () => import('./i18n/da.json'),
    [Language.German]: () => import('./i18n/de.json'),
    [Language.Greek]: () => import('./i18n/el.json'),
    [Language.Spanish]: () => import('./i18n/es.json'),
    [Language.Persian]: () => import('./i18n/fa.json'),
    [Language.Finnish]: () => import('./i18n/fi.json'),
    [Language.French]: () => import('./i18n/fr.json'),
    [Language.Hebrew]: () => import('./i18n/he.json'),
    [Language.Hindi]: () => import('./i18n/hi.json'),
    [Language.Hungarian]: () => import('./i18n/hu.json'),
    [Language.Indonesian]: () => import('./i18n/id.json'),
    [Language.Italian]: () => import('./i18n/it.json'),
    [Language.Japanese]: () => import('./i18n/ja.json'),
    [Language.Korean]: () => import('./i18n/ko.json'),
    [Language.Malay]: () => import('./i18n/ms.json'),
    [Language.Dutch]: () => import('./i18n/nl.json'),
    [Language.Norwegian]: () => import('./i18n/no.json'),
    [Language.Polish]: () => import('./i18n/pl.json'),
    [Language.Portuguese]: () => import('./i18n/pt.json'),
    [Language.Romanian]: () => import('./i18n/ro.json'),
    [Language.Russian]: () => import('./i18n/ru.json'),
    [Language.Swedish]: () => import('./i18n/sv.json'),
    [Language.Swahili]: () => import('./i18n/sw.json'),
    [Language.Tamil]: () => import('./i18n/ta.json'),
    [Language.Thai]: () => import('./i18n/th.json'),
    [Language.Tagalog]: () => import('./i18n/tl.json'),
    [Language.Turkish]: () => import('./i18n/tr.json'),
    [Language.Ukrainian]: () => import('./i18n/uk.json'),
    [Language.Urdu]: () => import('./i18n/ur.json'),
    [Language.Vietnamese]: () => import('./i18n/vi.json'),
    [Language.ChineseSimplified]: () => import('./i18n/zh.json'),
    [Language.ChineseTraditional]: () => import('./i18n/zh-TW.json'),
  })
);

/**
 * Register slices with effects
 */
registerSlice(insightSlice, (dispatch) => {
  initializeInsightEffects(dispatch);
});
registerSlice(periodSlice, (dispatch) => {
  initializePeriodEffects(dispatch);
});
registerSlice(executiveViewSlice, (dispatch) => {
  initializeExecutiveViewEffects(dispatch);
});
registerSlice(teamViewSlice, (dispatch) => {
  initializeTeamViewEffects(dispatch);
});
registerSlice(icDashboardSlice, (dispatch) => {
  initializeIcDashboardEffects(dispatch);
});
registerSlice(currentUserSlice, (dispatch) => {
  initializeCurrentUserEffects(dispatch);
});

/**
 * insight Screenset Configuration
 * Translations are registered directly with i18nRegistry above
 */
export const insightScreenset: ScreensetConfig = {
  id: INSIGHT_SCREENSET_ID,
  name: 'insight',
  category: ScreensetCategory.Production,
  defaultScreen: EXECUTIVE_VIEW_SCREEN_ID,
  menu: [
    {
      menuItem: {
        id: EXECUTIVE_VIEW_SCREEN_ID,
        label: `screenset.${INSIGHT_SCREENSET_ID}:menu_items.executive-view.label`,
        icon: 'lucide:bar-chart-2',
      },
      screen: withAuth(() => import('./screens/executive-view/ExecutiveViewScreen')),
    },
    {
      menuItem: {
        id: TEAM_VIEW_SCREEN_ID,
        label: `screenset.${INSIGHT_SCREENSET_ID}:menu_items.team-view.label`,
        icon: 'lucide:users',
      },
      screen: withAuth(() => import('./screens/team-view/TeamViewScreen')),
    },
    {
      menuItem: {
        id: IC_DASHBOARD_SCREEN_ID,
        label: `screenset.${INSIGHT_SCREENSET_ID}:menu_items.ic-dashboard.label`,
        icon: 'lucide:user',
      },
      screen: withAuth(() => import('./screens/ic-dashboard/IcDashboardScreen')),
    },
    {
      menuItem: {
        id: MY_DASHBOARD_SCREEN_ID,
        label: `screenset.${INSIGHT_SCREENSET_ID}:menu_items.my-dashboard.label`,
        icon: 'lucide:user-circle',
      },
      screen: withAuth(() => import('./screens/ic-dashboard/IcDashboardScreen')),
    },
    {
      menuItem: {
        id: DOCUMENTATION_SCREEN_ID,
        label: `screenset.${INSIGHT_SCREENSET_ID}:menu_items.documentation.label`,
        icon: 'lucide:book-open',
      },
      screen: withAuth(() => import('./screens/documentation/DocumentationScreen')),
    },
    {
      menuItem: {
        id: DASHBOARD_SCREEN_ID,
        label: `screenset.${INSIGHT_SCREENSET_ID}:menu_items.dashboard.label`,
        icon: 'lucide:layout-dashboard',
      },
      screen: withAuth(() => import('./screens/dashboard/DashboardScreen')),
    },
    {
      menuItem: {
        id: SPEED_SCREEN_ID,
        label: `screenset.${INSIGHT_SCREENSET_ID}:menu_items.speed.label`,
        icon: 'lucide:gauge',
      },
      screen: withAuth(() => import('./screens/speed/SpeedScreen')),
    },
    {
      menuItem: {
        id: UIKIT_SCREEN_ID,
        label: `screenset.${INSIGHT_SCREENSET_ID}:menu_items.uikit.label`,
        icon: 'lucide:palette',
      },
      screen: withAuth(() => import('./screens/uikit/UIKitElementsScreen')),
    },
  ],
};

/**
 * Self-register screenset
 */
screensetRegistry.register(insightScreenset);
