/**
 * Executive View Screen
 * Orchestration-only: no inline components, no inline data arrays.
 */

import React, { useEffect } from 'react';
import { useAppSelector, useScreenTranslations, useTranslation, I18nRegistry, Language } from '@hai3/react';
import { INSIGHT_SCREENSET_ID, EXECUTIVE_VIEW_SCREEN_ID } from '../../ids';
import { usePeriod } from '../../hooks/usePeriod';
import { loadExecutiveView } from '../../actions/executiveViewActions';
import { changePeriod, setDateRange } from '../../actions/periodActions';
import { selectTeams, selectOrgKpis, selectExecLoading, selectExecViewConfig } from '../../slices/executiveViewSlice';
import { selectCustomRange } from '../../slices/periodSlice';
import { resolveDateRange } from '../../utils/periodToDateRange';
import { OrgKpiCards } from './components/OrgKpiCards';
import { OrgHealthRadar } from './components/OrgHealthRadar';
import { TeamMetricsBar } from './components/TeamMetricsBar';
import { TeamsTable } from './components/TeamsTable';
import { PeriodSelectorBar } from '../../uikit/composite/PeriodSelectorBar';
import type { CustomRange } from '../../types';

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

const ExecutiveViewScreen: React.FC = () => {
  useScreenTranslations(INSIGHT_SCREENSET_ID, EXECUTIVE_VIEW_SCREEN_ID, translations);
  const { t } = useTranslation();
  const period = usePeriod();
  const customRange = useAppSelector(selectCustomRange);
  const teams = useAppSelector(selectTeams);
  const orgKpis = useAppSelector(selectOrgKpis);
  const loading = useAppSelector(selectExecLoading);
  const execConfig = useAppSelector(selectExecViewConfig);

  useEffect(() => {
    loadExecutiveView(resolveDateRange(period, customRange));
  }, [period, customRange]);

  const handleRangeChange = (range: CustomRange | null): void => {
    if (range) setDateRange(range);
  };

  return (
    <div className="flex flex-col gap-4 p-6">
      {/* Sticky header — same pattern as IcDashboard / TeamView. */}
      <div className="sticky top-0 z-20 -mx-6 -mt-6 px-6 pt-6 pb-3 bg-background/95 backdrop-blur-sm border-b border-border/60 flex flex-wrap items-center justify-between gap-3">
        <div>
          <div className="text-lg font-bold text-gray-900 leading-tight">
            {(() => { const v = t('header.title'); return v === 'header.title' ? 'Executive View' : v; })()}
          </div>
          <div className="text-sm text-gray-500">
            {(() => { const v = t('header.subtitle'); return v === 'header.subtitle' ? 'All teams · Organization overview' : v; })()}
          </div>
        </div>
        <PeriodSelectorBar
          period={period}
          customRange={customRange}
          onPeriodChange={changePeriod}
          onRangeChange={handleRangeChange}
        />
      </div>
      <OrgKpiCards teams={teams} orgKpis={orgKpis} columnThresholds={execConfig?.column_thresholds ?? []} />

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <div className="bg-white border border-gray-200 rounded-lg shadow-sm p-4">
          {orgKpis ? (
            <OrgHealthRadar orgKpis={orgKpis} />
          ) : (
            <div className="h-60" />
          )}
        </div>
        <div className="bg-white border border-gray-200 rounded-lg shadow-sm p-4">
          <TeamMetricsBar teams={teams} />
        </div>
      </div>

      <TeamsTable teams={teams} loading={loading} columnThresholds={execConfig?.column_thresholds ?? []} />
    </div>
  );
};

ExecutiveViewScreen.displayName = 'ExecutiveViewScreen';

export default ExecutiveViewScreen;
