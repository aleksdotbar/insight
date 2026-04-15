/**
 * Dashboard Screen
 * Analytics dashboard with stats cards, chart, and metrics
 */

import React, { useEffect } from 'react';
import {
  useTranslation,
  useScreenTranslations,
  useAppSelector,
  I18nRegistry,
  Language,
} from '@hai3/react';
import { TextLoader } from '@/app/components/TextLoader';
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  Spinner,
  ResponsiveContainer,
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  ChartTooltip,
} from '@hai3/uikit';
import { INSIGHT_SCREENSET_ID, DASHBOARD_SCREEN_ID } from '../../ids';
import { selectDashboard } from '../../slices/insightSlice';
import { fetchDashboard } from '../../actions/insightActions';

/**
 * Screen-level translations (loaded lazily when screen mounts)
 */
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

/**
 * Dashboard Screen Component
 */
export const DashboardScreen: React.FC = () => {
  useScreenTranslations(INSIGHT_SCREENSET_ID, DASHBOARD_SCREEN_ID, translations);

  const { t } = useTranslation();
  const ns = `screen.${INSIGHT_SCREENSET_ID}.${DASHBOARD_SCREEN_ID}`;
  const dashboard = useAppSelector(selectDashboard);

  useEffect(() => {
    fetchDashboard();
  }, []);

  if (!dashboard) {
    return (
      <div className="flex items-center justify-center p-16">
        <Spinner />
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-6 p-4 sm:p-8">
      {/* Stats Cards Row */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {dashboard.stats.map((stat) => (
          <Card key={stat.key}>
            <CardContent className="pt-6">
              <TextLoader skeletonClassName="h-4 w-24">
                <p className="text-sm text-muted-foreground">{t(`${ns}:${stat.key}`)}</p>
              </TextLoader>
              <p className="text-3xl font-bold mt-1">{stat.value}</p>
              <TextLoader skeletonClassName="h-3 w-20">
                <p className="text-xs text-muted-foreground mt-1">{t(`${ns}:${stat.sub}`, { defaultValue: stat.sub })}</p>
              </TextLoader>
            </CardContent>
          </Card>
        ))}
      </div>

      {/* Chart Section */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <TextLoader skeletonClassName="h-6 w-64">
              {t(`${ns}:chart_title`)}
            </TextLoader>
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="h-[18.75rem] w-full">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={dashboard.chartData} margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
                <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                <XAxis dataKey="date" className="text-xs" />
                <YAxis className="text-xs" />
                <ChartTooltip />
                <Line
                  type="monotone"
                  dataKey="aiLoc"
                  stroke="hsl(var(--primary))"
                  strokeWidth={2}
                  dot={{ r: 4 }}
                />
                <Line
                  type="monotone"
                  dataKey="commitLoc"
                  stroke="hsl(var(--muted-foreground))"
                  strokeWidth={2}
                  dot={{ r: 4 }}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </CardContent>
      </Card>

      {/* Bottom Metrics */}
      <Card>
        <CardContent className="pt-6">
          <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-5 lg:grid-cols-9 gap-4">
            {dashboard.bottomMetrics.map((metric) => (
              <div key={metric.key} className="flex flex-col items-center gap-1">
                <TextLoader skeletonClassName="h-3 w-16">
                  <p className="text-xs text-muted-foreground text-center">{t(`${ns}:${metric.key}`)}</p>
                </TextLoader>
                <p className="text-lg font-semibold text-primary">{metric.value}</p>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      {/* Period Split Comparison */}
      <Card>
        <CardContent className="pt-6">
          <TextLoader skeletonClassName="h-5 w-40">
            <p className="text-sm font-medium mb-4">{t(`${ns}:period_split`)}</p>
          </TextLoader>
          <div className="grid grid-cols-2 sm:grid-cols-3 gap-4 sm:gap-8">
            {dashboard.periodSplits.map((split) => (
              <div key={split.label} className="flex flex-col">
                <p className="text-xs text-muted-foreground">{split.label}</p>
                <p className="text-2xl font-bold">{split.value}</p>
              </div>
            ))}
            <div className="flex flex-col">
              <p className="text-xs text-muted-foreground">{t(`${ns}:delta`)}</p>
              <p className="text-2xl font-bold text-destructive">{dashboard.periodDelta}</p>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  );
};

DashboardScreen.displayName = 'DashboardScreen';

// Default export for lazy loading
export default DashboardScreen;
