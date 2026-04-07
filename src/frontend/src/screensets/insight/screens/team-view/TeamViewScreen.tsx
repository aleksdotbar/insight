/**
 * Team View Screen
 * Orchestration-only: no inline components, no inline data arrays.
 */

import React, { useEffect, useState } from 'react';
import { useAppSelector, useNavigation } from '@hai3/react';
import { usePeriod } from '../../hooks/usePeriod';
import { loadTeamView } from '../../actions/teamViewActions';
import { selectIcPerson } from '../../actions/icDashboardActions';
import { changePeriod, setDateRange } from '../../actions/periodActions';
import { selectMembers, selectTeamKpis, selectBulletSections, selectTeamViewLoading, selectTeamName } from '../../slices/teamViewSlice';
import { selectCurrentUser } from '../../slices/currentUserSlice';
import { selectCustomRange } from '../../slices/periodSlice';
import { TeamHeroStrip } from './components/TeamHeroStrip';
import { AttentionNeeded } from './components/AttentionNeeded';
import { MembersTable } from './components/MembersTable';
import { TeamBulletSections } from './components/TeamBulletSections';
import { PeriodSelectorBar } from '../../uikit/composite/PeriodSelectorBar';
import { ViewModeToggle } from '../../uikit/composite/ViewModeToggle';
import DrillModal from '../../uikit/composite/DrillModal';
import { INSIGHT_SCREENSET_ID, IC_DASHBOARD_SCREEN_ID } from '../../ids';
import { apiRegistry } from '@hai3/react';
import { InsightApiService } from '../../api/insightApiService';
import type { ViewMode, CustomRange, DrillData } from '../../types';

const TeamViewScreen: React.FC = () => {
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

      <AttentionNeeded members={members} onNavigate={handleNavigateToIc} />

      <MembersTable
        members={members}
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
