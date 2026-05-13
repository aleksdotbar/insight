/**
 * IcDashboardScreen — thin orchestrator. Delegates display to one of two
 * dedicated components based on the viewer's department:
 *
 *   - sales department  → <SalesDashboard />
 *   - everyone else     → <EngineeringDashboard />
 *
 * Branching is intentionally inside the page rather than at the route level
 * so the URL (My Dashboard) stays stable across personas. Each variant owns
 * its own data-fetch action, slice, and layout — see `crmDashboardActions.ts`
 * + `crmDashboardSlice.ts` (sales) and `icDashboardActions.ts` +
 * `icDashboardSlice.ts` (engineering).
 *
 * The i18n loader registers here (once per dashboard mount) so both variants
 * share the same translation namespace.
 */

import React from 'react';
import { useAppSelector, useScreenTranslations, I18nRegistry, Language } from '@hai3/react';
import { selectSelectedPersonId } from '../../slices/icDashboardSlice';
import { selectCurrentUser } from '../../slices/currentUserSlice';
import { isSalesDepartment } from '../../utils/branching';
import { INSIGHT_SCREENSET_ID, IC_DASHBOARD_SCREEN_ID } from '../../ids';
import EngineeringDashboard from './components/EngineeringDashboard';
import SalesDashboard from './components/SalesDashboard';

const translations = I18nRegistry.createLoader({
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
});

const IcDashboardScreen: React.FC = () => {
  useScreenTranslations(INSIGHT_SCREENSET_ID, IC_DASHBOARD_SCREEN_ID, translations);

  // `selectSelectedPersonId` resolves to the selected IC, falling back to
  // the viewer themself (My Dashboard semantics) via userContext. Returns
  // `null` until identity has loaded — we render nothing in that window
  // rather than mounting a dashboard that will immediately re-render.
  const personId = useAppSelector(selectSelectedPersonId);
  const department = useAppSelector(selectCurrentUser)._identity?.department;

  if (!personId) return null;

  return isSalesDepartment(department)
    ? <SalesDashboard personId={personId} />
    : <EngineeringDashboard personId={personId} />;
};

export default IcDashboardScreen;
