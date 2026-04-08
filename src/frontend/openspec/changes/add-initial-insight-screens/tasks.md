# Tasks: add-initial-insight-screens

**Change:** `add-initial-insight-screens`
**Status:** Draft
**Date:** 2026-04-07

---

## 1. Prerequisites

- [x] 1.1 Run `npm install recharts` and verify `recharts` appears in `package.json` dependencies
- [x] 1.2 Verify `@hai3/react` exports `registerSlice`, `useScreenTranslations`, `I18nRegistry`, `ScreensetCategory`, and `screensetRegistry` at the versions currently installed
- [x] 1.3 Verify `@hai3/state` exports the event bus, `BaseApiService`, and Redux slice patterns required by SCREENSETS.md
- [x] 1.4 Confirm `npm run type-check` passes on the unmodified codebase (establish a clean baseline)

---

## 2. IDs & Screenset Registration

- [x] 2.1 Add `EXECUTIVE_VIEW_SCREEN_ID = 'executive-view'`, `TEAM_VIEW_SCREEN_ID = 'team-view'`, and `IC_DASHBOARD_SCREEN_ID = 'ic-dashboard'` to `src/screensets/insight/ids.ts`
- [x] 2.2 Import and register the three new screen domains (slices + effects) in `insightScreenset.tsx` once slices exist — add placeholder `registerSlice` calls with TODO comments now so the file compiles as new slices are added
- [x] 2.3 Add the three new menu items to `insightScreenset.tsx` (icons: `lucide:bar-chart-2`, `lucide:users`, `lucide:user`) with lazy screen imports pointing to the new screen files; keep existing four screens untouched
- [x] 2.4 Change `category: ScreensetCategory.Drafts` to `category: ScreensetCategory.Production` in `insightScreenset.tsx`
- [x] 2.5 Set `defaultScreen` to `EXECUTIVE_VIEW_SCREEN_ID` in `insightScreenset.tsx`

---

## 3. Types

- [ ] 3.1 Add `PeriodValue`, `CustomRange`, `ViewMode` type aliases and `PeriodState` interface to `src/screensets/insight/types/index.ts`
- [ ] 3.2 Add `ExecTeamRow`, `OrgKpis`, `ExecViewData` interfaces to `types/index.ts` (fields: all columns from executive-view spec table — `team_name`, `headcount`, `tasks_closed`, `bugs_fixed`, `build_success_pct`, `focus_time_pct`, `ai_adoption_pct`, `ai_loc_share_pct`, `pr_cycle_time_h`, `status`; plus `bugResolutionScore`, `prCycleScore` on `OrgKpis`)
- [x] 3.3 Add `TeamKpi`, `TeamMember`, `BulletMetric`, `BulletSection`, `TeamViewData` interfaces to `types/index.ts` (also added `teamName: string` to `TeamViewData`)
- [ ] 3.4 Add `PersonData`, `IcKpi`, `IcChartsData`, `LocDataPoint`, `DeliveryDataPoint`, `DrillData`, `DrillRow`, `IcDashboardData` interfaces to `types/index.ts` (all fields referenced in ic-dashboard spec)
- [ ] 3.5 Add `TimeOffNotice` interface to `types/index.ts` (fields: `days`, `dateRange`, `bambooHrUrl`)
- [x] 3.6 Add `UserRole`, `CurrentUser` types to `types/index.ts`

---

## 4. Period Domain

- [ ] 4.1 Create `src/screensets/insight/events/periodEvents.ts` — define `PeriodChanged` and `DateRangeSet` events under a `period` domain ID; no barrel export
- [ ] 4.2 Create `src/screensets/insight/slices/periodSlice.ts` — state shape `{ period: PeriodValue, customRange: CustomRange | null, scale: number }`; `initialState.period = 'month'`; export slice object as default
- [ ] 4.3 Create `src/screensets/insight/effects/periodEffects.ts` — subscribe to `PeriodChanged`, update `periodSlice.period` and `scale`; subscribe to `DateRangeSet`, update `customRange`
- [ ] 4.4 Create `src/screensets/insight/actions/periodActions.ts` — export `changePeriod(period: PeriodValue)` emitting `PeriodChanged` and `setDateRange(range)` emitting `DateRangeSet`
- [ ] 4.5 Register `periodSlice` via `registerSlice(periodSlice, initializePeriodEffects)` in `insightScreenset.tsx`
- [ ] 4.6 Add `selectPeriod` and `selectScale` selectors to `periodSlice.ts`; add `usePeriod()` convenience hook (wraps `useSelector(selectPeriod)`) to `actions/periodActions.ts` or a dedicated `hooks/usePeriod.ts`

---

## 5. InsightApiService — Types & Mock Data

- [ ] 5.1 Add mock constant `EXEC_VIEW_MOCK: Record<PeriodValue, ExecViewData>` to `src/screensets/insight/api/mocks.ts` — covers 6 teams (Platform 12, Mobile 8, Data 6, Frontend 10, DevOps 5, QA 7); count fields scaled per period (week ÷ 4, month baseline, quarter × 3, year × 12); `bugResolutionScore` and `prCycleScore` from API, not hardcoded 78/65
- [ ] 5.2 Add mock constant `TEAM_VIEW_MOCK: Record<PeriodValue, TeamViewData>` to `mocks.ts` — covers `TeamKpi[]` for 4 KPI cards and `TeamMember[]` for the full team roster with all fields; `BulletSection[]` for 5 sections with pre-computed `BulletMetric` rows including `bar_width_pct`, `median_left_pct`, `drill_id`, `status`
- [ ] 5.3 Add mock constant `IC_DASHBOARD_MOCK: Record<string, Record<PeriodValue, IcDashboardData>>` to `mocks.ts` — keyed by `personId`; includes `PersonData` (name, role, seniority), `IcKpi[]` (5 KPIs with `delta` string, `delta_type`), `BulletMetric[]` (all bullet sections: task_delivery, git_output, code_quality, ai_tools, collaboration), `TimeOffNotice`, `IcChartsData` (LOC + delivery series per period granularity)
- [ ] 5.4 Add mock drill data constant `DRILL_MOCK: Record<string, DrillData>` to `mocks.ts` — one entry per the 8 drill IDs (`tasks-completed`, `cycle-time`, `task-reopen`, `commits`, `pull-requests`, `reviews`, `builds`, `bugs-fixed`) with `filter` string, `source`, `srcColor`, and typed `rows[]`
- [ ] 5.5 Add `getExecutiveViewData(period: PeriodValue): Promise<ExecViewData>` to `InsightApiService`; register mock route `GET /api/insight/executive-view` returning `EXEC_VIEW_MOCK[period]`
- [ ] 5.6 Add `getTeamViewData(period: PeriodValue): Promise<TeamViewData>` to `InsightApiService`; register mock route `GET /api/insight/team-view`
- [ ] 5.7 Add `getIcDashboardData(personId: string, period: PeriodValue): Promise<IcDashboardData | null>` to `InsightApiService`; register mock route `GET /api/insight/ic-dashboard/:personId`; return `null` for unknown IDs
- [ ] 5.8 Add `getIcDrillData(personId: string, drillId: string): Promise<DrillData>` to `InsightApiService`; register mock route `GET /api/insight/ic-dashboard/:personId/drill/:drillId`

---

## 6. Shared uikit Composites

- [ ] 6.1 Create `src/screensets/insight/uikit/composite/BulletChart.tsx` — renders header row (label left, formatted value right with optional dotted underline when `drill_id` is set), track (24px height, value bar at `bar_width_pct`%, median line at `median_left_pct`%, status colour mapping), footer (range_min / median_label / range_max); tile mode renders large value, label, status badge; accepts `onDrillClick?: () => void`; wrap in `React.memo`
- [ ] 6.2 Create `src/screensets/insight/uikit/composite/KpiStrip.tsx` — renders a strip of KPI tile cells; each cell accepts `label`, `value`, `sublabel`, `delta`, `delta_type`, `status`; badge colour from `status`; `plain` prop suppresses badge; period suffix logic: append `/ {scale}` for count metrics, suppress for `%`, `h`, `×`, `avg`, `avg replies` units; no `@hai3/state` imports
- [ ] 6.3 Create `src/screensets/insight/uikit/composite/MetricCard.tsx` — card wrapper with `title`, optional `legend` slot, `columns` (1 | 2 | 3) grid for bullet chart children; tile mode switches to flat tile grid; no state imports
- [ ] 6.4 Create `src/screensets/insight/uikit/composite/CollapsibleSection.tsx` — trigger row with section title, item count badge (collapsed state), collapsed/expanded arrow indicator; `defaultOpen` prop (false by default); animates open/close; accepts children rendered when expanded; no state imports
- [ ] 6.5 Create `src/screensets/insight/uikit/composite/LocStackedBar.tsx` — wraps recharts `BarChart` with `Bar` stacked; 3 series (AI LOC light-blue, Code LOC blue, Spec Lines purple); x-axis labels from `granularity` prop (week → Mon-Fri, month → week-start dates, quarter → 3-letter months, year → quarter labels); no state imports
- [ ] 6.6 Create `src/screensets/insight/uikit/composite/DeliveryTrends.tsx` — wraps recharts `LineChart`; 3 series (Commits blue, PRs Merged purple, Tasks Done green); same granularity logic as `LocStackedBar`; no state imports
- [ ] 6.7 Create `src/screensets/insight/uikit/composite/DrillModal.tsx` — backdrop overlay + modal panel; header (metric title, source badge with `srcColor`, formatted value, ✕ button); filter bar (verbatim query string); data table (columns per `drill_id` schema from ic-dashboard spec, 8 drill IDs); footer ("Open all in {source} ↗" link + row count); closes on backdrop click or ✕; no state imports; columns for all 8 drill schemas defined as a constant map inside the file
- [x] 6.8 Create `src/screensets/insight/uikit/composite/ViewModeToggle.tsx` — chart/tile toggle button; `mode: ViewMode` prop + `onChange` callback; no state imports
- [x] 6.9 Create `src/screensets/insight/uikit/composite/PeriodSelectorBar.tsx` — period tabs (week/month/quarter/year) + optional custom date range picker; `period`, `customRange`, `onPeriodChange`, `onRangeChange` props; no state imports

---

## 7. Executive View Screen

- [ ] 7.1 Create `src/screensets/insight/events/executiveViewEvents.ts` — `ExecutiveViewLoaded`, `ExecutiveViewLoadFailed` events; no barrel export
- [ ] 7.2 Create `src/screensets/insight/slices/executiveViewSlice.ts` — state: `{ teams: ExecTeamRow[], orgKpis: OrgKpis | null, loading: boolean, error: string | null }`; export slice as default with `RootState` augmentation
- [ ] 7.3 Create `src/screensets/insight/effects/executiveViewEffects.ts` — subscribe to `ExecutiveViewLoaded` → set teams, orgKpis, loading false; subscribe to `ExecutiveViewLoadFailed` → set error, loading false
- [ ] 7.4 Create `src/screensets/insight/actions/executiveViewActions.ts` — export `loadExecutiveView(period: PeriodValue)`: sets loading true (via `ExecutiveViewLoadStarted` or direct), calls `insightApiService.getExecutiveViewData(period)`, emits `Loaded` or `LoadFailed`; export `selectTeams`, `selectOrgKpis`, `selectExecLoading` selectors
- [ ] 7.5 Create `src/screensets/insight/screens/executive-view/components/OrgKpiCards.tsx` — 4 KPI cards (Teams at Risk, Avg Build Success, Avg AI Adoption, Avg Focus Time); threshold colour logic per spec (green/amber); `grid-cols-2 sm:grid-cols-4`; no state imports
- [ ] 7.6 Create `src/screensets/insight/screens/executive-view/components/OrgHealthRadar.tsx` — recharts `RadarChart` with 5 axes; single "Org Average" series; stroke `#2563EB`, fill `#2563EB` 15% opacity; all 5 values from props (never hardcoded); no state imports
- [ ] 7.7 Create `src/screensets/insight/screens/executive-view/components/TeamMetricsBar.tsx` — recharts `BarChart` grouped; 3 series (Build % `#2563EB`, AI Adoption `#7C3AED`, Focus % `#16A34A`); x-axis `team_name`, y-axis domain `[0, 100]`; no state imports
- [ ] 7.8 Create `src/screensets/insight/screens/executive-view/components/TeamsTable.tsx` — 10 columns per spec; status `Badge` with variant `outline` and status-to-class map; `build_success_pct`/`focus_time_pct`/`ai_adoption_pct` threshold colour; PR Cycle rendered as `{value}h`; loading skeleton rows; no state imports
- [x] 7.9 Create `src/screensets/insight/screens/executive-view/ExecutiveViewScreen.tsx` — reads `period` via `usePeriod()`; header row with title+subtitle on left, `PeriodSelectorBar` on right; renders `OrgKpiCards`, `OrgHealthRadar`, `TeamMetricsBar`, `TeamsTable`; responsive `grid-cols-1 lg:grid-cols-2` charts layout
- [ ] 7.10 Register `executiveViewSlice` + `initializeExecutiveViewEffects` in `insightScreenset.tsx`

---

## 8. Team View Screen

- [ ] 8.1 Create `src/screensets/insight/events/teamViewEvents.ts` — `TeamViewLoaded`, `TeamViewLoadFailed` events; no barrel export
- [x] 8.2 Create `src/screensets/insight/slices/teamViewSlice.ts` — state includes `teamName: string`; `selectTeamName` selector added; `setTeamViewData` action sets all fields including `teamName`
- [ ] 8.3 Create `src/screensets/insight/effects/teamViewEffects.ts` — subscribe to `TeamViewLoaded` → populate all three data arrays, set loading false; `TeamViewLoadFailed` → set error
- [x] 8.4 Create `src/screensets/insight/actions/teamViewActions.ts` — export `loadTeamView(period: PeriodValue)`; navigation uses `selectIcPerson(personId)` + `navigateToScreen` (URL param pattern resolved — see design Decision 7)
- [ ] 8.5 Create `src/screensets/insight/screens/team-view/components/TeamHeroStrip.tsx` — 4 KPI cards (At Risk, Team Dev Time, Focus ≥ 60%, Not Using AI); status badge colour mapping (good/warn/bad); 4-column desktop, 2×2 mobile grid with separator logic per spec; no state imports
- [ ] 8.6 Create `src/screensets/insight/screens/team-view/components/AttentionNeeded.tsx` — computes alert rows from `TeamMember[]` prop (3 threshold rules); severity logic (bad if >20% below threshold, warn otherwise); per-alert entry with member name, metric value, severity badge, IC dashboard link; empty state when no alerts; no state imports
- [ ] 8.7 Create `src/screensets/insight/screens/team-view/components/MembersTable.tsx` — 9 columns per spec; `hi()`/`lo()` colour coding helper (inline or imported utility); Focus Time column renders mini progress bar + text; AI Tools column renders outline badges (excludes `"—"` values); clickable rows with `cursor-pointer hover:bg-blue-50/40`; loading skeleton; horizontal scroll on mobile; no state imports
- [ ] 8.8 Create `src/screensets/insight/screens/team-view/components/TeamBulletSections.tsx` — 5 `CollapsibleSection` wrappers (Task Delivery, Code & Quality, Estimation, AI Adoption, Collaboration); `defaultOpen=false`; legend label "Company median"; Task Delivery + Code & Quality side-by-side on desktop; Estimation 3-column grid on desktop; all collapse to single column on mobile; passes `viewMode` to `BulletChart`; no state imports
- [x] 8.9 Create `src/screensets/insight/screens/team-view/TeamViewScreen.tsx` — reads `period` via `usePeriod()`; dispatches `loadTeamView(period)` on mount and period change; filters self from members when role is `team_lead`; inline team name + avatar in header row; `ViewModeToggle` in header; drill state via `useState` (not Redux); `handleCellDrill(personId, drillId)` + `handleMembersDrill()`; calls `selectIcPerson` before navigation
- [ ] 8.10 Register `teamViewSlice` + `initializeTeamViewEffects` in `insightScreenset.tsx`

---

## 9. IC Dashboard Screen

- [x] 9.1 Create `src/screensets/insight/events/icDashboardEvents.ts` — `IcDashboardLoaded`, `IcDashboardLoadFailed`, `DrillOpened`, `DrillClosed`, `IcDashboardLoadStarted`, `PersonSelected` events; `layout/menu/itemParam` added to EventPayloadMap
- [x] 9.2 Create `src/screensets/insight/slices/icDashboardSlice.ts` — state includes `selectedPersonId: string` (default `'p1'`); `setSelectedPersonId` reducer; `selectSelectedPersonId` selector
- [ ] 9.3 Create `src/screensets/insight/effects/icDashboardEffects.ts` — subscribe to `IcDashboardLoaded` → populate all fields, set loading false; `LoadFailed` → set error; `DrillOpened` → set `drillId`, trigger `loadDrillData`; `DrillClosed` → reset `drillId` and `drillData`; period change effect closes open drill modal via `DrillClosed`
- [x] 9.4 `src/screensets/insight/actions/icDashboardActions.ts` — export `selectIcPerson(personId)` which emits `PersonSelected`; `loadIcDashboard` reads `selectedPersonId` from Redux (not URL); `MY_DASHBOARD_SCREEN_ID` screen uses `currentUser.personId` directly
- [x] 9.5 Create `src/screensets/insight/screens/ic-dashboard/components/PersonHeader.tsx` — added `inline?: boolean` prop; when `inline=true` renders content without outer card wrapper (used in screen header row)
- [ ] 9.6 Create `src/screensets/insight/screens/ic-dashboard/components/TimeOffBanner.tsx`
- [ ] 9.7 Create `src/screensets/insight/screens/ic-dashboard/components/AiToolsSection.tsx`
- [ ] 9.8 Create `src/screensets/insight/screens/ic-dashboard/components/CollaborationSection.tsx`
- [ ] 9.9 Create `src/screensets/insight/screens/ic-dashboard/components/PrivacyFooter.tsx`
- [x] 9.10 Create `src/screensets/insight/screens/ic-dashboard/IcDashboardScreen.tsx` — resolves personId via `MY_DASHBOARD_SCREEN_ID` check; inline `PersonHeader` + `PeriodSelectorBar` in header row
- [x] 9.11 Register `icDashboardSlice` + `initializeIcDashboardEffects` in `insightScreenset.tsx`; register `MY_DASHBOARD_SCREEN_ID` screen entry

---

## 9a. Role System & Dynamic Menu

- [x] 9a.1 Create `src/screensets/insight/events/currentUserEvents.ts` — `UserChanged` event with `CurrentUser` payload
- [x] 9a.2 Create `src/screensets/insight/slices/currentUserSlice.ts` — state: `{ currentUser: CurrentUser }`; `setCurrentUser` reducer; `selectCurrentUser`, `selectUserRole` selectors; default executive user
- [x] 9a.3 Create `src/screensets/insight/effects/currentUserEffects.ts` — on `UserChanged`: dispatch `setCurrentUser` + `setMenuItems(buildMenu(role, user))` + `setSelectedPersonId(user.personId)`; on `layout/menu/itemParam`: dispatch `setSelectedPersonId(param)`; define `ORG: OrgTeam[]` with recursive `subTeams`; implement recursive `teamToMenuItem()`
- [x] 9a.4 Create `src/screensets/insight/actions/currentUserActions.ts` — `MOCK_USERS[]` with 4 demo users (executive, two team_leads, ic); `switchUser(user)` emits `UserChanged`
- [x] 9a.5 Create `src/screensets/insight/uikit/composite/RoleSwitcher.tsx` — dark-themed sidebar bottom widget; reads `menuState.collapsed` for compact/full mode; light popup dropdown listing MOCK_USERS with role badges; shows `✓` on active user
- [x] 9a.6 Add `MY_DASHBOARD_SCREEN_ID = 'my-dashboard'` to `src/screensets/insight/ids.ts`
- [x] 9a.7 Add `RoleSwitcher` as bottom child of `<Menu>` in `Layout.tsx`; remove `<Header />` from Layout
- [x] 9a.8 Register `currentUserSlice` + `initializeCurrentUserEffects` in `insightScreenset.tsx`
- [x] 9a.9 Update `Menu.tsx` to read `activeParam` from `insight/icDashboard.selectedPersonId`; implement recursive `renderItem(item, depth)`; handle `::` compound IDs

## 10. Localization

- [ ] 10.1 Add menu item i18n keys to `src/screensets/insight/i18n/en.json`: `menu_items.executive-view.label = "Executive View"`, `menu_items.team-view.label = "Team View"`, `menu_items.ic-dashboard.label = "IC Dashboard"`
- [ ] 10.2 Add the same three keys (with English values as placeholders) to all 35 remaining language files (`ar.json` through `zh-TW.json`) — iterate all files in `src/screensets/insight/i18n/`
- [ ] 10.3 Create `src/screensets/insight/screens/executive-view/i18n/en.json` with keys for all user-facing strings on Executive View (card titles, chart titles, table column headers, loading text, status badge labels)
- [ ] 10.4 Create `src/screensets/insight/screens/executive-view/i18n/` stubs for all 35 non-English languages with English values as placeholders; wire `useScreenTranslations(INSIGHT_SCREENSET_ID, EXECUTIVE_VIEW_SCREEN_ID, loader)` in `ExecutiveViewScreen.tsx`
- [ ] 10.5 Create `src/screensets/insight/screens/team-view/i18n/en.json` with keys for all user-facing strings on Team View (section titles, column headers, alert reasons, KPI labels, empty states, loading text)
- [ ] 10.6 Create `src/screensets/insight/screens/team-view/i18n/` stubs for all 35 non-English languages; wire `useScreenTranslations` in `TeamViewScreen.tsx`
- [ ] 10.7 Create `src/screensets/insight/screens/ic-dashboard/i18n/en.json` with keys for all user-facing strings on IC Dashboard (section titles, KPI labels, chart subtitles, drill modal titles, privacy footer text, not-found state)
- [ ] 10.8 Create `src/screensets/insight/screens/ic-dashboard/i18n/` stubs for all 35 non-English languages; wire `useScreenTranslations` in `IcDashboardScreen.tsx`

---

## 11. Validation

- [ ] 11.1 Run `npm run type-check` — zero TypeScript errors
- [ ] 11.2 Run `npm run arch:check` — zero violations (no barrel exports in events/effects, no `@hai3/state` in uikit/, no direct slice imports in screens, no CSV imports anywhere in new files)
- [ ] 11.3 Run `npm run lint` — zero ESLint errors including `local/screen-inline-components` rule
- [ ] 11.4 Navigate to Executive View in Chrome DevTools: verify 4 KPI cards render with correct values and colours, radar and bar charts render, teams table shows 6 rows, no console errors
- [ ] 11.5 Verify period switching on Executive View: change period to `quarter` and confirm all KPI card values, radar, bar chart, and table count columns update; rate columns remain stable
- [ ] 11.6 Navigate to Team View: verify hero strip shows 4 KPI cards, attention section shows alerts for members below threshold, members table shows all rows and columns, all 5 bullet sections are collapsed on load
- [ ] 11.7 Expand all 5 bullet sections on Team View and verify bullet charts render with track, median line, legend labelled "Company median"; switch to tile mode and verify compact tile layout
- [ ] 11.8 Click a members table row and verify navigation to IC Dashboard for the correct `personId`
- [ ] 11.9 Navigate to IC Dashboard: verify person header shows initials/name/role, time-off banner appears, 3 always-visible bullet sections are expanded, LOC and Delivery Trends sections are collapsed
- [ ] 11.10 Expand LOC Breakdown and Delivery Trends on IC Dashboard; switch period and verify chart x-axis granularity changes per spec (week → 5 bars, month → 4, quarter → 3, year → 2)
- [ ] 11.11 Click a drillable bullet metric value on IC Dashboard; verify drill modal opens with correct title, source badge, column schema, and filter string; verify backdrop click closes it
- [ ] 11.12 Navigate to an unknown `personId` (e.g. `/ic/unknown-999`) and verify "Person not found" empty state renders with no KPI strip, bullet sections, or privacy footer
