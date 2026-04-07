/**
 * Executive View Screen
 * Orchestration-only: no inline components, no inline data arrays.
 */

import React, { useEffect } from 'react';
import { useAppSelector } from '@hai3/react';
import { usePeriod } from '../../hooks/usePeriod';
import { loadExecutiveView } from '../../actions/executiveViewActions';
import { changePeriod, setDateRange } from '../../actions/periodActions';
import { selectTeams, selectOrgKpis, selectExecLoading } from '../../slices/executiveViewSlice';
import { selectCustomRange } from '../../slices/periodSlice';
import { OrgKpiCards } from './components/OrgKpiCards';
import { OrgHealthRadar } from './components/OrgHealthRadar';
import { TeamMetricsBar } from './components/TeamMetricsBar';
import { TeamsTable } from './components/TeamsTable';
import { PeriodSelectorBar } from '../../uikit/composite/PeriodSelectorBar';
import type { CustomRange } from '../../types';

const ExecutiveViewScreen: React.FC = () => {
  const period = usePeriod();
  const customRange = useAppSelector(selectCustomRange);
  const teams = useAppSelector(selectTeams);
  const orgKpis = useAppSelector(selectOrgKpis);
  const loading = useAppSelector(selectExecLoading);

  useEffect(() => {
    loadExecutiveView(period);
  }, [period]);

  const handleRangeChange = (range: CustomRange | null): void => {
    if (range) setDateRange(range);
  };

  return (
    <div className="flex flex-col gap-4 p-6">
      <div className="flex items-center justify-between gap-3">
        <div>
          <div className="text-[15px] font-bold text-gray-900 leading-tight">Executive View</div>
          <div className="text-[10px] text-gray-400">All teams · Organization overview</div>
        </div>
        <PeriodSelectorBar
          period={period}
          customRange={customRange}
          onPeriodChange={changePeriod}
          onRangeChange={handleRangeChange}
        />
      </div>
      <OrgKpiCards teams={teams} orgKpis={orgKpis} />

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
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

      <TeamsTable teams={teams} loading={loading} />
    </div>
  );
};

ExecutiveViewScreen.displayName = 'ExecutiveViewScreen';

export default ExecutiveViewScreen;
