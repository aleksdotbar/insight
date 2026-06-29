import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";

import {
  PeerStorySection,
  type PeerStoryInput,
} from "@/components/widgets/v2/peer-story-section";
import type { PeerStats } from "@/lib/peers";

const settingsState = vi.hoisted(() => ({
  focusMode: "all" as "all" | "critical" | "rewards" | "neutral",
}));

vi.mock("@/hooks/use-settings", () => ({
  useSettings: () => ({ focusMode: settingsState.focusMode }),
}));

const STATS: PeerStats = {
  p25: 25,
  p50: 50,
  p75: 75,
  min: 0,
  max: 100,
  n: 9,
};

function metric(
  key: string,
  label: string,
  value: number,
  overrides: Partial<PeerStoryInput> = {},
): PeerStoryInput {
  return {
    key,
    label,
    sublabel: "Peer comparable",
    value,
    unit: "units",
    higherIsBetter: true,
    stats: STATS,
    ...overrides,
  };
}

function appearsBefore(a: HTMLElement, b: HTMLElement): boolean {
  return Boolean(a.compareDocumentPosition(b) & Node.DOCUMENT_POSITION_FOLLOWING);
}

describe("<PeerStorySection>", () => {
  beforeEach(() => {
    settingsState.focusMode = "all";
  });

  it("uses the worst bottom outlier as the all-mode hero and keeps remaining outliers red before green", () => {
    render(
      <PeerStorySection
        entries={[
          metric("hotel", "Hotel fog", 5),
          metric("alpha", "Alpha wobble", 10),
          metric("delta", "Delta static", 15),
          metric("echo", "Echo loops", 20),
          metric("bravo", "Bravo lift", 98),
          metric("charlie", "Charlie rise", 90),
        ]}
      />,
    );

    expect(screen.getByText("Top issue")).toBeInTheDocument();
    expect(screen.getByText("Hotel fog")).toBeInTheDocument();
    expect(screen.queryByText("Top win")).not.toBeInTheDocument();

    expect(appearsBefore(screen.getByText("Alpha wobble"), screen.getByText("Bravo lift"))).toBe(true);
    expect(appearsBefore(screen.getByText("Echo loops"), screen.getByText("Bravo lift"))).toBe(true);

    const chipButtons = screen.getAllByRole("button").filter((button) =>
      ["Bravo lift", "Charlie rise"].some((label) => button.textContent?.includes(label)),
    );
    expect(chipButtons).toHaveLength(2);
    expect(appearsBefore(chipButtons[0], chipButtons[1])).toBe(true);
  });

  it("filters critical and rewards focus modes and shows their empty states", () => {
    const entries = [metric("top", "Bravo lift", 98)];

    settingsState.focusMode = "critical";
    const { rerender } = render(<PeerStorySection entries={entries} />);

    expect(screen.getByText("No critical issues this period")).toBeInTheDocument();
    expect(screen.queryByText("Bravo lift")).not.toBeInTheDocument();

    settingsState.focusMode = "rewards";
    rerender(<PeerStorySection entries={entries} />);

    expect(screen.getByText("Top win")).toBeInTheDocument();
    expect(screen.getByText("Bravo lift")).toBeInTheDocument();

    settingsState.focusMode = "critical";
    rerender(<PeerStorySection entries={[metric("bottom", "Hotel fog", 5)]} />);

    expect(screen.getByText("Top issue")).toBeInTheDocument();
    expect(screen.getByText("Hotel fog")).toBeInTheDocument();

    settingsState.focusMode = "rewards";
    rerender(<PeerStorySection entries={[metric("bottom", "Hotel fog", 5)]} />);

    expect(screen.getByText("No standout wins this period")).toBeInTheDocument();
  });

  it("renders calm mode as a flat grid without hero or chips", () => {
    settingsState.focusMode = "neutral";

    render(
      <PeerStorySection
        entries={[
          metric("hotel", "Hotel fog", 5),
          metric("bravo", "Bravo lift", 98),
        ]}
      />,
    );

    expect(screen.queryByText("Top issue")).not.toBeInTheDocument();
    expect(screen.queryByText("Top win")).not.toBeInTheDocument();
    expect(screen.getByText("Hotel fog")).toBeInTheDocument();
    expect(screen.getByText("Bravo lift")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Hotel fog/ })).not.toBeInTheDocument();
  });

  it("renders percent values as whole inline percentages", () => {
    render(
      <PeerStorySection
        entries={[
          metric("acceptance", "Tool acceptance", 94.6, {
            unit: "%",
            format: "percent",
            stats: { p25: 12.2, p50: 31.8, p75: 60.4, min: 0, max: 100, n: 8 },
          }),
        ]}
      />,
    );

    expect(screen.getByText("95%")).toBeInTheDocument();
    expect(screen.getByText("32%")).toBeInTheDocument();
    expect(screen.queryByText("94.6")).not.toBeInTheDocument();
    expect(screen.queryByText("%")).not.toBeInTheDocument();
  });

  it("keeps in-pack folded rows above no-peer rows and labels missing peer context", async () => {
    const user = userEvent.setup();

    render(
      <PeerStorySection
        entries={[
          metric("bravo", "Bravo lift", 98),
          metric("kilo", "Kilo sample", 44, { stats: null }),
          metric("juliet", "Juliet balance", 54),
          metric("lima", "Lima baseline", 48),
        ]}
      />,
    );

    await user.click(screen.getByRole("button", { name: /Show 3 on-par metrics/ }));

    const fold = screen.getByText("Juliet balance").closest(".contents")?.parentElement;
    expect(fold).toBeTruthy();
    const rows = within(fold as HTMLElement);
    expect(appearsBefore(rows.getByText("Juliet balance"), rows.getByText("Kilo sample"))).toBe(true);
    expect(appearsBefore(rows.getByText("Lima baseline"), rows.getByText("Kilo sample"))).toBe(true);
    expect(rows.getByText("no peer data")).toBeInTheDocument();
  });
});
