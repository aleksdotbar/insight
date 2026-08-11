import { Settings2 } from "lucide-react";
import { useState } from "react";

import { AppSidebarFooter } from "@/components/app-sidebar-footer";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
} from "@/components/ui/sidebar";
import { useShellLayout } from "@/lib/portal/use-shell-layout";
import type { Zone } from "@/lib/portal/nav-model";
import { useZoneNav } from "@/lib/portal/use-zone-nav";
import { cn } from "@/lib/utils";

/**
 * The zone rail: one icon per zone, expanding to labels on hover.
 *
 * Zones that are dashboards in their own right (Person, People) link to the
 * existing dashboard routes and clear the theme-zone selection; other zones set
 * the active zone so the context pane switches. Zones the active role can't see
 * are filtered out (permission layer — FE stub over the future
 * role_section_visibility entity).
 *
 * Below 768px the rail renders nothing: 56px of icons plus a 256px pane left a
 * phone with ~60px of content. The same zones (labelled, not icon-only) live in
 * the context pane's drawer instead — see `ContextPane`. On a tablet the rail
 * stays: 56px is affordable, and it is the pane that collapses.
 *
 * ── The expansion, ported from the lite product's rail ──────────────────────
 *
 * Four things make it work, and each one is there because leaving it out broke
 * something:
 *
 * 1. The rail keeps its 56px slot in the layout and the labels open OVER the
 *    pane. Widening the element itself would shove the pane sideways every time
 *    a pointer crossed the rail on its way somewhere else.
 *
 * 2. The buttons widen to the full open width, but ONLY while it is open. A
 *    label you can read but not click is a trap: the pointer leaves the 56px
 *    column on its way to the word and the rail shuts before it arrives. People
 *    aim at what they can read. Keeping the buttons narrow while shut is what
 *    stops that from costing anything — approaching the pane from the content
 *    side never opens the rail over the row being reached for.
 *
 * 3. The buttons are inside the hover target, so a pointer resting on one keeps
 *    the rail open rather than fighting the thing that opened it.
 *
 * 4. A click collapses it until the pointer leaves. A click navigates, and the
 *    pointer is still on the rail afterwards — without this the rail reopens
 *    immediately, on top of the pane the click was aimed at. The lite product
 *    needs `sessionStorage` for this because its click reloads the page; here
 *    the navigation is client-side, so plain state survives it and is dropped
 *    the moment the pointer leaves.
 */

/**
 * How far the open rail reaches — wide enough for the longest zone label and no
 * wider. It deliberately does NOT cover the pane beside it: an overlay that
 * swallows the whole second column hides where the reader just was, and the
 * pane is what they are usually navigating towards. With an edge and a shadow
 * it reads as a panel resting over the pane rather than as a half-covered one.
 */
const OPEN_WIDTH = "12rem";

/** Where the context pane ends — the scrim reaches exactly that far. */
const PANE_EDGE = "19.5rem";

export function LensRail() {
  const layout = useShellLayout();
  const { zones, activeZone, selectZone } = useZoneNav();
  const [inside, setInside] = useState(false);
  // Set by a click, cleared on leave, so the next approach expands normally.
  const [dismissed, setDismissed] = useState(false);
  // Held in React rather than expressed as a CSS variant chain: the open state
  // is the product of two facts, one of which no selector can see, and a rule
  // that silently fails to match is a bug nothing catches. The delays below are
  // still CSS — they are about time, not about state.
  const open = inside && !dismissed;

  if (layout === "phone") return null;

  return (
    <div
      data-testid="lens-rail"
      className="relative z-20 shrink-0"
      onPointerEnter={() => setInside(true)}
      onPointerLeave={() => {
        setInside(false);
        setDismissed(false);
      }}
    >
      <Sidebar
        collapsible="none"
        className="w-14! overflow-visible border-e [&>div]:overflow-visible"
      >
        {/* A fade beside the panel, not a flat veil.
            The pane's rows do not object to being dimmed — they object to
            being CUT, and a hard edge through the middle of a word reads as a
            rendering fault whatever its brightness. So the strip the panel
            does not cover goes from fully hidden at the panel's edge to fully
            visible at the pane's, and a row dissolves instead of stopping
            mid-letter. Dimming it uniformly was tried first and did nothing:
            the cut, not the contrast, was the problem. */}
        <div
          aria-hidden
          className={cn(
            "pointer-events-none fixed inset-y-0 bg-gradient-to-r from-background to-transparent transition-opacity duration-150",
            open ? "opacity-100 delay-200" : "opacity-0 delay-100"
          )}
          style={{
            insetInlineStart: OPEN_WIDTH,
            width: `calc(${PANE_EDGE} - ${OPEN_WIDTH})`,
          }}
        />
        {/* The panel the labels sit on.
            It takes pointer events WHILE OPEN, and that is not a detail: with
            it inert the gaps between buttons belong to whatever is underneath,
            so a pointer moving from an icon towards its label crosses bare
            panel, the rail counts that as having been left, and it slams shut
            under the hand that was reaching for it.
            Delayed both ways: crossing the rail on the way elsewhere should not
            flash it open, and leaving briefly should not shut it. */}
        <div
          aria-hidden={!open}
          className={cn(
            "absolute inset-y-0 start-0 border-e bg-sidebar transition-[opacity,box-shadow] duration-150",
            open
              ? "pointer-events-auto opacity-100 shadow-lg delay-200"
              : "pointer-events-none opacity-0 delay-100"
          )}
          style={{ width: OPEN_WIDTH }}
        />
        <SidebarHeader className="relative z-10 items-start ps-3">
          <div className="flex size-8 items-center justify-center rounded-md bg-sidebar-primary text-sm font-bold text-sidebar-primary-foreground">
            I
          </div>
        </SidebarHeader>
        <SidebarContent>
          <SidebarMenu className="items-start gap-1 ps-2">
            {zones.map((z) => (
              <ZoneItem
                key={z.id}
                zone={z}
                active={activeZone === z.id}
                open={open}
                onSelect={(zone) => {
                  setDismissed(true);
                  selectZone(zone);
                }}
              />
            ))}
          </SidebarMenu>
        </SidebarContent>
        <SidebarFooter className="relative z-10 items-start gap-1 ps-2">
          <Popover>
            <PopoverTrigger
              render={
                <button
                  type="button"
                  title="Settings"
                  className="flex size-10 items-center justify-center rounded-lg text-muted-foreground transition-colors hover:bg-sidebar-accent hover:text-sidebar-accent-foreground"
                >
                  <Settings2 className="size-[19px]" aria-hidden />
                  <span className="sr-only">Settings</span>
                </button>
              }
            />
            <PopoverContent side="right" align="end" className="w-56 gap-0 p-1">
              <AppSidebarFooter />
            </PopoverContent>
          </Popover>
        </SidebarFooter>
      </Sidebar>
    </div>
  );
}

function ZoneItem({
  zone,
  active,
  open,
  onSelect,
}: {
  zone: Zone;
  active: boolean;
  open: boolean;
  onSelect: (zone: Zone) => void;
}) {
  const Icon = zone.icon;

  return (
    <SidebarMenuItem className="relative z-10">
      <SidebarMenuButton
        isActive={active}
        title={zone.label}
        // 40px shut, the full open width while open — see note 2 above. The
        // icon does not move between the two: the button starts its content at
        // the same offset either way.
        className={cn(
          "h-10 justify-start gap-2 overflow-hidden p-0 ps-[10px] transition-[width] duration-150",
          open ? "delay-200" : "w-10 delay-100"
        )}
        style={open ? { width: `calc(${OPEN_WIDTH} - 1rem)` } : undefined}
        onClick={() => onSelect(zone)}
      >
        <Icon className="shrink-0" />
        {/* Visible only while open, and never a pointer target of its own —
            the button under it is what widens, so the word IS the hit area. */}
        <span
          className={cn(
            "truncate transition-opacity duration-150",
            open ? "opacity-100 delay-200" : "opacity-0 delay-100"
          )}
        >
          {zone.label}
        </span>
      </SidebarMenuButton>
    </SidebarMenuItem>
  );
}
