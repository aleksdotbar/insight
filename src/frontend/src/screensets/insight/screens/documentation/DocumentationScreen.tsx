/**
 * Documentation Screen
 * Instructions and documentation on how to add pages via HAI3
 */

import React from 'react';
import {
  useTranslation,
  useScreenTranslations,
  I18nRegistry,
  Language,
} from '@hai3/react';
import { TextLoader } from '@/app/components/TextLoader';
import { Card, CardContent, CardHeader, CardTitle, Badge, Separator } from '@hai3/uikit';
import { INSIGHT_SCREENSET_ID, DOCUMENTATION_SCREEN_ID } from '../../ids';

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
 * Documentation Screen Component
 */
export const DocumentationScreen: React.FC = () => {
  useScreenTranslations(INSIGHT_SCREENSET_ID, DOCUMENTATION_SCREEN_ID, translations);

  const { t } = useTranslation();
  const ns = `screen.${INSIGHT_SCREENSET_ID}.${DOCUMENTATION_SCREEN_ID}`;

  return (
    <div className="flex flex-col gap-8 p-8 max-w-4xl">
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
            <TextLoader skeletonClassName="h-6 w-48">
              {t(`${ns}:quick_start_title`)}
            </TextLoader>
            <Badge variant="secondary">
              <TextLoader skeletonClassName="h-4 w-12" inheritColor>
                {t(`${ns}:quick_start_badge`)}
              </TextLoader>
            </Badge>
          </CardTitle>
        </CardHeader>
        <CardContent className="flex flex-col gap-4">
          <TextLoader skeletonClassName="h-5 w-full">
            <p>{t(`${ns}:quick_start_desc`)}</p>
          </TextLoader>
          <div className="rounded-md bg-muted p-4 font-mono text-sm whitespace-pre-line">
            <TextLoader skeletonClassName="h-24 w-full">
              {t(`${ns}:quick_start_command`)}
            </TextLoader>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>
            <TextLoader skeletonClassName="h-6 w-56">
              {t(`${ns}:structure_title`)}
            </TextLoader>
          </CardTitle>
        </CardHeader>
        <CardContent className="flex flex-col gap-3">
          <TextLoader skeletonClassName="h-5 w-full">
            <p className="text-muted-foreground">{t(`${ns}:structure_desc`)}</p>
          </TextLoader>
          <div className="rounded-md bg-muted p-4 font-mono text-sm whitespace-pre-line">
            <TextLoader skeletonClassName="h-40 w-full">
              {t(`${ns}:structure_tree`)}
            </TextLoader>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>
            <TextLoader skeletonClassName="h-6 w-48">
              {t(`${ns}:adding_screen_title`)}
            </TextLoader>
          </CardTitle>
        </CardHeader>
        <CardContent className="flex flex-col gap-4">
          <div className="flex flex-col gap-3">
            <TextLoader skeletonClassName="h-5 w-full">
              <p><strong>{t(`${ns}:step1_label`)}</strong> {t(`${ns}:step1_text`)}</p>
            </TextLoader>
            <Separator />
            <TextLoader skeletonClassName="h-5 w-full">
              <p><strong>{t(`${ns}:step2_label`)}</strong> {t(`${ns}:step2_text`)}</p>
            </TextLoader>
            <Separator />
            <TextLoader skeletonClassName="h-5 w-full">
              <p><strong>{t(`${ns}:step3_label`)}</strong> {t(`${ns}:step3_text`)}</p>
            </TextLoader>
            <Separator />
            <TextLoader skeletonClassName="h-5 w-full">
              <p><strong>{t(`${ns}:step4_label`)}</strong> {t(`${ns}:step4_text`)}</p>
            </TextLoader>
            <Separator />
            <TextLoader skeletonClassName="h-5 w-full">
              <p><strong>{t(`${ns}:step5_label`)}</strong> {t(`${ns}:step5_text`)}</p>
            </TextLoader>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>
            <TextLoader skeletonClassName="h-6 w-40">
              {t(`${ns}:rules_title`)}
            </TextLoader>
          </CardTitle>
        </CardHeader>
        <CardContent className="flex flex-col gap-2">
          <TextLoader skeletonClassName="h-5 w-full">
            <p>{t(`${ns}:rule_uikit`)}</p>
          </TextLoader>
          <TextLoader skeletonClassName="h-5 w-full">
            <p>{t(`${ns}:rule_events`)}</p>
          </TextLoader>
          <TextLoader skeletonClassName="h-5 w-full">
            <p>{t(`${ns}:rule_i18n`)}</p>
          </TextLoader>
          <TextLoader skeletonClassName="h-5 w-full">
            <p>{t(`${ns}:rule_no_inline`)}</p>
          </TextLoader>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>
            <TextLoader skeletonClassName="h-6 w-56">
              {t(`${ns}:commands_title`)}
            </TextLoader>
          </CardTitle>
        </CardHeader>
        <CardContent className="flex flex-col gap-6">
          <TextLoader skeletonClassName="h-5 w-full">
            <p className="text-muted-foreground">{t(`${ns}:commands_desc`)}</p>
          </TextLoader>

          <div className="flex flex-col gap-4">
            <TextLoader skeletonClassName="h-5 w-32">
              <h3 className="text-lg font-semibold">{t(`${ns}:commands_category_scaffold`)}</h3>
            </TextLoader>
            <div className="flex flex-col gap-3">
              <div className="flex flex-col gap-1">
                <TextLoader skeletonClassName="h-5 w-48">
                  <p className="font-mono text-sm font-medium">{t(`${ns}:cmd_new_screenset_name`)}</p>
                </TextLoader>
                <TextLoader skeletonClassName="h-4 w-full">
                  <p className="text-muted-foreground text-sm">{t(`${ns}:cmd_new_screenset_desc`)}</p>
                </TextLoader>
              </div>
              <Separator />
              <div className="flex flex-col gap-1">
                <TextLoader skeletonClassName="h-5 w-48">
                  <p className="font-mono text-sm font-medium">{t(`${ns}:cmd_duplicate_screenset_name`)}</p>
                </TextLoader>
                <TextLoader skeletonClassName="h-4 w-full">
                  <p className="text-muted-foreground text-sm">{t(`${ns}:cmd_duplicate_screenset_desc`)}</p>
                </TextLoader>
              </div>
              <Separator />
              <div className="flex flex-col gap-1">
                <TextLoader skeletonClassName="h-5 w-48">
                  <p className="font-mono text-sm font-medium">{t(`${ns}:cmd_new_screen_name`)}</p>
                </TextLoader>
                <TextLoader skeletonClassName="h-4 w-full">
                  <p className="text-muted-foreground text-sm">{t(`${ns}:cmd_new_screen_desc`)}</p>
                </TextLoader>
              </div>
              <Separator />
              <div className="flex flex-col gap-1">
                <TextLoader skeletonClassName="h-5 w-48">
                  <p className="font-mono text-sm font-medium">{t(`${ns}:cmd_new_component_name`)}</p>
                </TextLoader>
                <TextLoader skeletonClassName="h-4 w-full">
                  <p className="text-muted-foreground text-sm">{t(`${ns}:cmd_new_component_desc`)}</p>
                </TextLoader>
              </div>
              <Separator />
              <div className="flex flex-col gap-1">
                <TextLoader skeletonClassName="h-5 w-48">
                  <p className="font-mono text-sm font-medium">{t(`${ns}:cmd_new_action_name`)}</p>
                </TextLoader>
                <TextLoader skeletonClassName="h-4 w-full">
                  <p className="text-muted-foreground text-sm">{t(`${ns}:cmd_new_action_desc`)}</p>
                </TextLoader>
              </div>
              <Separator />
              <div className="flex flex-col gap-1">
                <TextLoader skeletonClassName="h-5 w-48">
                  <p className="font-mono text-sm font-medium">{t(`${ns}:cmd_new_api_service_name`)}</p>
                </TextLoader>
                <TextLoader skeletonClassName="h-4 w-full">
                  <p className="text-muted-foreground text-sm">{t(`${ns}:cmd_new_api_service_desc`)}</p>
                </TextLoader>
              </div>
            </div>
          </div>

          <Separator />

          <div className="flex flex-col gap-4">
            <TextLoader skeletonClassName="h-5 w-32">
              <h3 className="text-lg font-semibold">{t(`${ns}:commands_category_quality`)}</h3>
            </TextLoader>
            <div className="flex flex-col gap-3">
              <div className="flex flex-col gap-1">
                <TextLoader skeletonClassName="h-5 w-48">
                  <p className="font-mono text-sm font-medium">{t(`${ns}:cmd_validate_name`)}</p>
                </TextLoader>
                <TextLoader skeletonClassName="h-4 w-full">
                  <p className="text-muted-foreground text-sm">{t(`${ns}:cmd_validate_desc`)}</p>
                </TextLoader>
              </div>
              <Separator />
              <div className="flex flex-col gap-1">
                <TextLoader skeletonClassName="h-5 w-48">
                  <p className="font-mono text-sm font-medium">{t(`${ns}:cmd_fix_violation_name`)}</p>
                </TextLoader>
                <TextLoader skeletonClassName="h-4 w-full">
                  <p className="text-muted-foreground text-sm">{t(`${ns}:cmd_fix_violation_desc`)}</p>
                </TextLoader>
              </div>
              <Separator />
              <div className="flex flex-col gap-1">
                <TextLoader skeletonClassName="h-5 w-48">
                  <p className="font-mono text-sm font-medium">{t(`${ns}:cmd_review_pr_name`)}</p>
                </TextLoader>
                <TextLoader skeletonClassName="h-4 w-full">
                  <p className="text-muted-foreground text-sm">{t(`${ns}:cmd_review_pr_desc`)}</p>
                </TextLoader>
              </div>
            </div>
          </div>

          <Separator />

          <div className="flex flex-col gap-4">
            <TextLoader skeletonClassName="h-5 w-32">
              <h3 className="text-lg font-semibold">{t(`${ns}:commands_category_reference`)}</h3>
            </TextLoader>
            <div className="flex flex-col gap-3">
              <div className="flex flex-col gap-1">
                <TextLoader skeletonClassName="h-5 w-48">
                  <p className="font-mono text-sm font-medium">{t(`${ns}:cmd_rules_name`)}</p>
                </TextLoader>
                <TextLoader skeletonClassName="h-4 w-full">
                  <p className="text-muted-foreground text-sm">{t(`${ns}:cmd_rules_desc`)}</p>
                </TextLoader>
              </div>
              <Separator />
              <div className="flex flex-col gap-1">
                <TextLoader skeletonClassName="h-5 w-48">
                  <p className="font-mono text-sm font-medium">{t(`${ns}:cmd_quick_ref_name`)}</p>
                </TextLoader>
                <TextLoader skeletonClassName="h-4 w-full">
                  <p className="text-muted-foreground text-sm">{t(`${ns}:cmd_quick_ref_desc`)}</p>
                </TextLoader>
              </div>
              <Separator />
              <div className="flex flex-col gap-1">
                <TextLoader skeletonClassName="h-5 w-48">
                  <p className="font-mono text-sm font-medium">{t(`${ns}:cmd_arch_explain_name`)}</p>
                </TextLoader>
                <TextLoader skeletonClassName="h-4 w-full">
                  <p className="text-muted-foreground text-sm">{t(`${ns}:cmd_arch_explain_desc`)}</p>
                </TextLoader>
              </div>
              <Separator />
              <div className="flex flex-col gap-1">
                <TextLoader skeletonClassName="h-5 w-48">
                  <p className="font-mono text-sm font-medium">{t(`${ns}:cmd_update_guidelines_name`)}</p>
                </TextLoader>
                <TextLoader skeletonClassName="h-4 w-full">
                  <p className="text-muted-foreground text-sm">{t(`${ns}:cmd_update_guidelines_desc`)}</p>
                </TextLoader>
              </div>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  );
};

DocumentationScreen.displayName = 'DocumentationScreen';

// Default export for lazy loading
export default DocumentationScreen;
