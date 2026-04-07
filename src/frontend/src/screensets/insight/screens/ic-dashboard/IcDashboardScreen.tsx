/**
 * IcDashboardScreen — IC (Individual Contributor) personal dashboard screen.
 * Orchestration only — no direct state/API logic.
 */

import React, { useEffect, useState } from 'react';
import { useAppSelector, useNavigation } from '@hai3/react';
import { changePeriod, setDateRange } from '../../actions/periodActions';
import { selectCustomRange } from '../../slices/periodSlice';
import { PeriodSelectorBar } from '../../uikit/composite/PeriodSelectorBar';
import { ViewModeToggle } from '../../uikit/composite/ViewModeToggle';
import type { CustomRange } from '../../types';
import { usePeriod } from '../../hooks/usePeriod';
import { loadIcDashboard, openDrill, closeDrill } from '../../actions/icDashboardActions';
import {
  selectPerson,
  selectIcKpis,
  selectBulletMetrics,
  selectIcCharts,
  selectTimeOffNotice,
  selectDrillId,
  selectDrillData,
  selectIcLoading,
  selectSelectedPersonId,
} from '../../slices/icDashboardSlice';
import { selectCurrentUser } from '../../slices/currentUserSlice';
import { MY_DASHBOARD_SCREEN_ID } from '../../ids';
import type { ViewMode } from '../../types';
import KpiStrip from '../../uikit/composite/KpiStrip';
import MetricCard from '../../uikit/composite/MetricCard';
import CollapsibleSection from '../../uikit/composite/CollapsibleSection';
import LocStackedBar from '../../uikit/composite/LocStackedBar';
import DeliveryTrends from '../../uikit/composite/DeliveryTrends';
import DrillModal from '../../uikit/composite/DrillModal';
import PersonHeader from './components/PersonHeader';
import TimeOffBanner from './components/TimeOffBanner';
import AiToolsSection from './components/AiToolsSection';
import CollaborationSection from './components/CollaborationSection';
import PrivacyFooter from './components/PrivacyFooter';

const IcDashboardScreen: React.FC = () => {
  const { currentScreen } = useNavigation();
  const currentUser = useAppSelector(selectCurrentUser);
  const selectedPersonId = useAppSelector(selectSelectedPersonId);
  // "My Dashboard" always shows the current user's own data
  const personId = currentScreen === MY_DASHBOARD_SCREEN_ID
    ? currentUser.personId
    : selectedPersonId;
  const period = usePeriod();
  const customRange = useAppSelector(selectCustomRange);
  const [viewMode, setViewMode] = useState<ViewMode>('chart');

  const handleRangeChange = (range: CustomRange | null): void => {
    if (range) setDateRange(range);
  };

  const person = useAppSelector(selectPerson);
  const kpis = useAppSelector(selectIcKpis);
  const bulletMetrics = useAppSelector(selectBulletMetrics);
  const charts = useAppSelector(selectIcCharts);
  const timeOffNotice = useAppSelector(selectTimeOffNotice);
  const drillId = useAppSelector(selectDrillId);
  const drillData = useAppSelector(selectDrillData);
  const loading = useAppSelector(selectIcLoading);

  useEffect(() => {
    loadIcDashboard(personId, period);
  }, [personId, period]);

  // Filter bullet metrics by section
  const taskMetrics = bulletMetrics.filter((m) => m.section === 'task_delivery');
  const gitMetrics = bulletMetrics.filter((m) => m.section === 'git_output');
  const codeMetrics = bulletMetrics.filter((m) => m.section === 'code_quality');
  const aiToolsMetrics = bulletMetrics.filter((m) => m.section === 'ai_tools');
  const collabMetrics = bulletMetrics.filter((m) => m.section === 'collab');

  // API returns period-filtered kpis — use directly
  const currentKpis = kpis;

  const handleDrillClick = (drillIdVal: string): void => {
    openDrill(personId, drillIdVal);
  };

  // Not-found state
  if (!loading && !person) {
    return (
      <div className="flex items-center justify-center p-6">
        <div className="bg-white border border-gray-200 rounded-xl px-12 py-8 text-center">
          <div className="text-[32px] mb-3">👤</div>
          <div className="text-base font-bold text-gray-900 mb-1.5">Person not found</div>
          <div className="text-[13px] text-gray-500">
            No data available for person ID: {personId}
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-4 p-4">
      {/* 0+1. Person header + controls in one row */}
      <div className="flex items-center justify-between gap-3">
        <PersonHeader person={person} inline />
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

      {/* 2. KPI strip + time-off banner */}
      <div className="bg-white border border-gray-200 rounded-xl overflow-hidden">
        <KpiStrip kpis={currentKpis} plain={true} />
        <TimeOffBanner notice={timeOffNotice} />
      </div>

      {/* 3. Task Delivery + Git Output */}
      <div className="grid grid-cols-2 gap-4">
        <MetricCard
          title="Task Delivery"
          metrics={taskMetrics}
          columns={1}
          onDrillClick={handleDrillClick}
          mode={viewMode}
          personName={person?.name}
        />
        <MetricCard
          title="Git Output"
          metrics={gitMetrics}
          columns={1}
          onDrillClick={handleDrillClick}
          mode={viewMode}
          personName={person?.name}
        />
      </div>

      {/* 4. Code Quality */}
      <MetricCard
        title="Code Quality"
        metrics={codeMetrics}
        columns={3}
        onDrillClick={handleDrillClick}
        mode={viewMode}
        personName={person?.name}
      />

      {/* 5. LOC Breakdown */}
      <CollapsibleSection
        title="LOC Breakdown"
        subtitle="Bitbucket · lines added per period · AI-assisted vs manual vs spec/config"
      >
        <div className="p-4">
          {charts && <LocStackedBar data={charts.locTrend} period={period} />}
        </div>
      </CollapsibleSection>

      {/* 6. Delivery Trends */}
      <CollapsibleSection
        title="Delivery Trends"
        subtitle="Jira + Bitbucket · activity counts · Commits, PRs and Tasks are independent signals — not directly comparable"
      >
        <div className="p-4">
          {charts && <DeliveryTrends data={charts.deliveryTrend} period={period} />}
        </div>
      </CollapsibleSection>

      {/* 7. AI Dev Tools & AI Chat */}
      <CollapsibleSection title="AI Dev Tools & AI Chat">
        <AiToolsSection
          metrics={aiToolsMetrics}
          viewMode={viewMode}
          onDrillClick={handleDrillClick}
          personName={person?.name}
        />
      </CollapsibleSection>

      {/* 8. Collaboration */}
      <CollapsibleSection title="Collaboration">
        <CollaborationSection
          metrics={collabMetrics}
          viewMode={viewMode}
          onDrillClick={handleDrillClick}
          personName={person?.name}
        />
      </CollapsibleSection>

      {/* 9. Privacy footer */}
      <PrivacyFooter />

      {/* 10. Drill modal */}
      <DrillModal open={!!drillId} drill={drillData} onClose={closeDrill} />
    </div>
  );
};

export default IcDashboardScreen;
