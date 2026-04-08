/**
 * Team View Screen
 * Orchestration-only: no inline components, no inline data arrays.
 */

import React, { useEffect, useState } from 'react';
import { useAppSelector, useNavigation, useScreenTranslations, I18nRegistry, Language } from '@hai3/react';
import { usePeriod } from '../../hooks/usePeriod';
import { loadTeamView } from '../../actions/teamViewActions';
import { selectIcPerson } from '../../actions/icDashboardActions';
import { changePeriod, setDateRange } from '../../actions/periodActions';
import { selectMembers, selectTeamKpis, selectBulletSections, selectTeamViewLoading, selectTeamName, selectTeamViewConfig } from '../../slices/teamViewSlice';
import { selectCurrentUser } from '../../slices/currentUserSlice';
import { selectCustomRange } from '../../slices/periodSlice';
import { TeamHeroStrip } from './components/TeamHeroStrip';
import { AttentionNeeded } from './components/AttentionNeeded';
import { MembersTable } from './components/MembersTable';
import { TeamBulletSections } from './components/TeamBulletSections';
import { PeriodSelectorBar } from '../../uikit/composite/PeriodSelectorBar';
import { ViewModeToggle } from '../../uikit/composite/ViewModeToggle';
import DrillModal from '../../uikit/composite/DrillModal';
import { INSIGHT_SCREENSET_ID, IC_DASHBOARD_SCREEN_ID, TEAM_VIEW_SCREEN_ID } from '../../ids';
import { apiRegistry } from '@hai3/react';
import { InsightApiService } from '../../api/insightApiService';
import type { ViewMode, CustomRange, DrillData } from '../../types';

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

const TeamViewScreen: React.FC = () => {
  useScreenTranslations(INSIGHT_SCREENSET_ID, TEAM_VIEW_SCREEN_ID, translations);
  const period = usePeriod();
  const customRange = useAppSelector(selectCustomRange);
  const loading = useAppSelector(selectTeamViewLoading);
  const allMembers = useAppSelector(selectMembers);
  const currentUser = useAppSelector(selectCurrentUser);
  // Team Lead sees their own data via "My Dashboard" — exclude from the team table
  const members = currentUser.role === 'team_lead'
    ? allMembers.filter((m) => m.person_id !== currentUser.personId)
    : allMembers;
  const teamKpis = useAppSelector(selectTeamKpis);
  const bulletSections = useAppSelector(selectBulletSections);
  const teamName = useAppSelector(selectTeamName);
  const teamViewConfig = useAppSelector(selectTeamViewConfig);
  const [viewMode, setViewMode] = useState<ViewMode>('chart');
  const [drillData, setDrillData] = useState<DrillData | null>(null);
  const [drillOpen, setDrillOpen] = useState(false);
  const { navigateToScreen } = useNavigation();

  useEffect(() => {
    loadTeamView(period);
  }, [period]);

  const handleNavigateToIc = (personId: string): void => {
    selectIcPerson(personId);
    navigateToScreen(INSIGHT_SCREENSET_ID, IC_DASHBOARD_SCREEN_ID);
  };

  const handleRangeChange = (range: CustomRange | null): void => {
    if (range) setDateRange(range);
  };

  const handleDrillClick = async (drillId: string): Promise<void> => {
    const api = apiRegistry.getService(InsightApiService);
    const data = await api.getTeamDrillData(drillId, period);
    setDrillData(data);
    setDrillOpen(true);
  };

  const handleMembersDrill = async (): Promise<void> => {
    await handleDrillClick('team-members');
  };

  const handleCellDrill = async (personId: string, drillId: string): Promise<void> => {
    const api = apiRegistry.getService(InsightApiService);
    const data = await api.getIcDrillData(personId, drillId);
    setDrillData(data);
    setDrillOpen(true);
  };

  const handleCloseDrill = (): void => {
    setDrillOpen(false);
    setDrillData(null);
  };

  return (
    <div className="flex flex-col gap-4 p-6">
      {/* Screen header: team name left, controls right */}
      <div className="flex items-center justify-between gap-3">
        <div className="flex items-center gap-3">
          {teamName && (
            <>
              <div className="w-8 h-8 bg-indigo-50 rounded-full flex items-center justify-center flex-shrink-0">
                <span className="text-[13px] font-extrabold text-indigo-600">
                  {teamName.split(' ').filter(Boolean).slice(0, 2).map((w) => w[0].toUpperCase()).join('')}
                </span>
              </div>
              <div>
                <div className="text-[15px] font-bold text-gray-900 leading-tight">{teamName}</div>
                <div className="text-[10px] text-gray-400">Team Dashboard</div>
              </div>
            </>
          )}
        </div>
        <div className="flex items-center gap-2 flex-shrink-0">
          <PeriodSelectorBar
            period={period}
            customRange={customRange}
            onPeriodChange={changePeriod}
            onRangeChange={handleRangeChange}
          />
          <ViewModeToggle mode={viewMode} onChange={setViewMode} />
        </div>
      </div>

      <TeamHeroStrip teamKpis={teamKpis} />

      {teamViewConfig && (
        <AttentionNeeded
          members={members}
          alertThresholds={teamViewConfig.alert_thresholds}
          onNavigate={handleNavigateToIc}
        />
      )}

      <MembersTable
        members={members}
        columnThresholds={teamViewConfig?.column_thresholds ?? []}
        loading={loading}
        onRowClick={handleNavigateToIc}
        onDetailsDrill={handleMembersDrill}
        onCellDrill={handleCellDrill}
      />

      <TeamBulletSections
        bulletSections={bulletSections}
        viewMode={viewMode}
        onDrillClick={handleDrillClick}
      />

      <DrillModal open={drillOpen} drill={drillData} onClose={handleCloseDrill} />
    </div>
  );
};

TeamViewScreen.displayName = 'TeamViewScreen';

export default TeamViewScreen;
