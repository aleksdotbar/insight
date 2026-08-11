// @vitest-environment jsdom
/**
 * The rail's open state.
 *
 * Every case here is about the interaction rather than the look, because the
 * look is the easy half. The one that matters is the click: a click navigates
 * and leaves the pointer sitting on the rail, so without an explicit dismissal
 * the rail reopens on top of the pane the click was aimed at. That failed
 * silently once already when the state was expressed as CSS variants — the
 * rules simply never matched and nothing said so.
 */
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  layout: "desktop" as string,
  selected: [] as string[],
}));

vi.mock("@/lib/portal/use-shell-layout", () => ({
  useShellLayout: () => mocks.layout,
}));
vi.mock("@/lib/portal/use-zone-nav", () => ({
  useZoneNav: () => ({
    zones: [
      { id: "overview", label: "Overview", icon: () => null },
      { id: "people", label: "People", icon: () => null },
    ],
    activeZone: "overview",
    selectZone: (z: { id: string }) => mocks.selected.push(z.id),
  }),
}));
vi.mock("@/components/app-sidebar-footer", () => ({
  AppSidebarFooter: () => null,
}));

import { SidebarProvider } from "@/components/ui/sidebar";
import { LensRail } from "./lens-rail";

const rail = () =>
  render(
    <SidebarProvider>
      <LensRail />
    </SidebarProvider>,
  );

/** The label is present either way; what changes is whether it can be seen. */
const labelOf = (name: string) =>
  screen.getByRole("button", { name }).querySelector("span:not(.sr-only)");

beforeEach(() => {
  mocks.layout = "desktop";
  mocks.selected = [];
  window.matchMedia ??= ((query: string) => ({
    matches: false,
    media: query,
    onchange: null,
    addEventListener: () => {},
    removeEventListener: () => {},
    addListener: () => {},
    removeListener: () => {},
    dispatchEvent: () => false,
  })) as unknown as typeof window.matchMedia;
});

describe("LensRail", () => {
  it("shows labels while the pointer is on it", async () => {
    const user = userEvent.setup();
    rail();
    expect(labelOf("Overview")).toHaveClass("opacity-0");

    await user.hover(screen.getByTestId("lens-rail"));
    expect(labelOf("Overview")).toHaveClass("opacity-100");
  });

  it("collapses on a click and stays collapsed under the pointer", async () => {
    // The whole reason this state exists. The click navigates; the pointer has
    // not moved; reopening here would cover the pane that was just asked for.
    const user = userEvent.setup();
    rail();
    await user.hover(screen.getByTestId("lens-rail"));
    expect(labelOf("People")).toHaveClass("opacity-100");

    await user.click(screen.getByRole("button", { name: "People" }));
    expect(mocks.selected).toEqual(["people"]);
    expect(labelOf("People")).toHaveClass("opacity-0");
  });

  it("expands again once the pointer has left and come back", async () => {
    const user = userEvent.setup();
    rail();
    const el = screen.getByTestId("lens-rail");

    await user.hover(el);
    await user.click(screen.getByRole("button", { name: "People" }));
    expect(labelOf("People")).toHaveClass("opacity-0");

    await user.unhover(el);
    await user.hover(el);
    expect(labelOf("People")).toHaveClass("opacity-100");
  });

  it("renders nothing on a phone", () => {
    // 56px of rail plus a 256px pane left a phone with almost no content; the
    // zones live in the context pane's drawer there instead.
    mocks.layout = "phone";
    rail();
    expect(screen.queryByTestId("lens-rail")).not.toBeInTheDocument();
  });
});
