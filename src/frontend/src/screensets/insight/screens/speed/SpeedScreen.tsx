/**
 * Speed Screen
 * Speedometer gauge displaying current speed from API data
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
  Badge,
  Spinner,
} from '@hai3/uikit';
import { INSIGHT_SCREENSET_ID, SPEED_SCREEN_ID } from '../../ids';
import { selectSpeed } from '../../slices/insightSlice';
import { fetchSpeed } from '../../actions/insightActions';
import { SpeedGauge } from './components/SpeedGauge';

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
 * Speed Screen Component
 */
export const SpeedScreen: React.FC = () => {
  useScreenTranslations(INSIGHT_SCREENSET_ID, SPEED_SCREEN_ID, translations);

  const { t } = useTranslation();
  const ns = `screen.${INSIGHT_SCREENSET_ID}.${SPEED_SCREEN_ID}`;
  const speed = useAppSelector(selectSpeed);

  useEffect(() => {
    fetchSpeed();
  }, []);

  if (!speed) {
    return (
      <div className="flex items-center justify-center p-16">
        <Spinner />
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-6 p-8">
      <div className="flex flex-col gap-2">
        <TextLoader skeletonClassName="h-10 w-64">
          <h1 className="text-4xl font-bold">
            {t(`${ns}:title`)}
          </h1>
        </TextLoader>
        <TextLoader skeletonClassName="h-6 w-96">
          <p className="text-muted-foreground text-lg">
            {t(`${ns}:description`)}
          </p>
        </TextLoader>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <TextLoader skeletonClassName="h-6 w-40">
              {t(`${ns}:gauge_title`)}
            </TextLoader>
            <Badge variant="outline">
              {speed.min} – {speed.max} {speed.unit}
            </Badge>
          </CardTitle>
        </CardHeader>
        <CardContent>
          <SpeedGauge data={speed} />
        </CardContent>
      </Card>

      <div className="grid grid-cols-3 gap-4">
        <Card>
          <CardContent className="pt-6 text-center">
            <TextLoader skeletonClassName="h-4 w-12">
              <p className="text-sm text-muted-foreground">{t(`${ns}:min_label`)}</p>
            </TextLoader>
            <p className="text-2xl font-bold mt-1">{speed.min}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6 text-center">
            <TextLoader skeletonClassName="h-4 w-12">
              <p className="text-sm text-muted-foreground">{t(`${ns}:value_label`)}</p>
            </TextLoader>
            <p className="text-2xl font-bold text-primary mt-1">{speed.value} {speed.unit}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6 text-center">
            <TextLoader skeletonClassName="h-4 w-12">
              <p className="text-sm text-muted-foreground">{t(`${ns}:max_label`)}</p>
            </TextLoader>
            <p className="text-2xl font-bold mt-1">{speed.max}</p>
          </CardContent>
        </Card>
      </div>
    </div>
  );
};

SpeedScreen.displayName = 'SpeedScreen';

// Default export for lazy loading
export default SpeedScreen;
