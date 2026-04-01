/**
 * UIKit Elements Screen
 *
 * Comprehensive showcase of all UIKit components available in FrontX.
 * Features:
 * - CategoryMenu with 9 categories
 * - 56 element demos using real UIKit components
 * - Lazy loading for category components
 * - Scroll-to-element navigation
 * - i18n support for all text
 */

import React, { lazy, Suspense, useEffect, useRef, useState } from 'react';
import {
  useTranslation,
  useScreenTranslations,
  I18nRegistry,
  Language,
} from '@hai3/react';
import { CategoryMenu } from './components/CategoryMenu';
import {
  Skeleton,
  Toaster,
  TooltipProvider,
} from '@hai3/uikit';
import { INSIGHT_SCREENSET_ID, UIKIT_SCREEN_ID } from '../../ids';

// Lazy-loaded category components
const LayoutElements = lazy(() =>
  import('./components/LayoutElements').then((m) => ({ default: m.LayoutElements }))
);
const NavigationElements = lazy(() =>
  import('./components/NavigationElements').then((m) => ({ default: m.NavigationElements }))
);
const FormElements = lazy(() =>
  import('./components/FormElements').then((m) => ({ default: m.FormElements }))
);
const ActionElements = lazy(() =>
  import('./components/ActionElements').then((m) => ({ default: m.ActionElements }))
);
const FeedbackElements = lazy(() =>
  import('./components/FeedbackElements').then((m) => ({ default: m.FeedbackElements }))
);
const DataDisplayElements = lazy(() =>
  import('./components/DataDisplayElements').then((m) => ({ default: m.DataDisplayElements }))
);
const OverlayElements = lazy(() =>
  import('./components/OverlayElements').then((m) => ({ default: m.OverlayElements }))
);
const MediaElements = lazy(() =>
  import('./components/MediaElements').then((m) => ({ default: m.MediaElements }))
);
const DisclosureElements = lazy(() =>
  import('./components/DisclosureElements').then((m) => ({ default: m.DisclosureElements }))
);

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
 * UIKit Elements Screen component.
 *
 * Displays a comprehensive showcase of all UIKit components with:
 * - CategoryMenu navigation
 * - Lazy-loaded category sections
 * - Scroll-to-element functionality
 * - Full i18n support
 * - Theme and language reactivity
 */
export const UIKitElementsScreen: React.FC = () => {
  const { isLoaded } = useScreenTranslations(INSIGHT_SCREENSET_ID, UIKIT_SCREEN_ID, translations);

  const { t } = useTranslation();
  const ns = `screen.${INSIGHT_SCREENSET_ID}.${UIKIT_SCREEN_ID}`;
  const containerRef = useRef<HTMLDivElement>(null);
  const [activeElement, setActiveElement] = useState<string | undefined>();

  // Track active element on scroll (intersection observer)
  useEffect(() => {
    const observer = new IntersectionObserver(
      (entries) => {
        entries.forEach((entry) => {
          if (entry.isIntersecting && entry.target.id.startsWith('element-')) {
            setActiveElement(entry.target.id);
          }
        });
      },
      { rootMargin: '-100px 0px -50% 0px' }
    );

    const elements = document.querySelectorAll('[id^="element-"]');

    elements.forEach((el) => observer.observe(el));

    return () => observer.disconnect();
  }, []);

  const tScreen = (key: string) => t(`${ns}:${key}`);

  if (!isLoaded) {
    return (
      <div className="p-8 space-y-4">
        <Skeleton className="h-10 w-64" />
        <Skeleton className="h-6 w-96" />
        <Skeleton className="h-64 w-full" />
      </div>
    );
  }

  return (
    <TooltipProvider>
      <div ref={containerRef} className="flex gap-6 p-8">
        {/* Sidebar Menu */}
        <aside className="w-64 flex-shrink-0">
          <CategoryMenu t={tScreen} activeElement={activeElement} />
        </aside>

        {/* Main Content */}
        <main className="flex-1 min-w-0 space-y-12">
          <div>
            <h1 className="text-4xl font-bold mb-2">{tScreen('title')}</h1>
            <p className="text-lg text-muted-foreground">{tScreen('description')}</p>
          </div>

          <Suspense
            fallback={
              <div className="space-y-4">
                <Skeleton className="h-8 w-48" />
                <Skeleton className="h-64 w-full" />
              </div>
            }
          >
            <LayoutElements t={tScreen} />
          </Suspense>

          <Suspense
            fallback={
              <div className="space-y-4">
                <Skeleton className="h-8 w-48" />
                <Skeleton className="h-64 w-full" />
              </div>
            }
          >
            <NavigationElements t={tScreen} />
          </Suspense>

          <Suspense
            fallback={
              <div className="space-y-4">
                <Skeleton className="h-8 w-48" />
                <Skeleton className="h-64 w-full" />
              </div>
            }
          >
            <FormElements t={tScreen} />
          </Suspense>

          <Suspense
            fallback={
              <div className="space-y-4">
                <Skeleton className="h-8 w-48" />
                <Skeleton className="h-64 w-full" />
              </div>
            }
          >
            <ActionElements t={tScreen} />
          </Suspense>

          <Suspense
            fallback={
              <div className="space-y-4">
                <Skeleton className="h-8 w-48" />
                <Skeleton className="h-64 w-full" />
              </div>
            }
          >
            <FeedbackElements t={tScreen} />
          </Suspense>

          <Suspense
            fallback={
              <div className="space-y-4">
                <Skeleton className="h-8 w-48" />
                <Skeleton className="h-64 w-full" />
              </div>
            }
          >
            <DataDisplayElements t={tScreen} />
          </Suspense>

          <Suspense
            fallback={
              <div className="space-y-4">
                <Skeleton className="h-8 w-48" />
                <Skeleton className="h-64 w-full" />
              </div>
            }
          >
            <OverlayElements t={tScreen} />
          </Suspense>

          <Suspense
            fallback={
              <div className="space-y-4">
                <Skeleton className="h-8 w-48" />
                <Skeleton className="h-64 w-full" />
              </div>
            }
          >
            <MediaElements t={tScreen} />
          </Suspense>

          <Suspense
            fallback={
              <div className="space-y-4">
                <Skeleton className="h-8 w-48" />
                <Skeleton className="h-64 w-full" />
              </div>
            }
          >
            <DisclosureElements t={tScreen} />
          </Suspense>

        </main>

        {/* Toast container (rendered once at screen level) */}
        <Toaster />
      </div>
    </TooltipProvider>
  );
};

UIKitElementsScreen.displayName = 'UIKitElementsScreen';

// Default export for lazy loading
export default UIKitElementsScreen;
