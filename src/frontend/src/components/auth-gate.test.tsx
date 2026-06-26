import { act, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import "@/i18n";
import { authStore } from "@/auth/auth-store";
import { OidcManager } from "@/auth/oidc-manager";
import { AuthGate } from "./auth-gate";

function renderGate() {
  return render(
    <AuthGate>
      <div>protected</div>
    </AuthGate>,
  );
}

describe("<AuthGate>", () => {
  afterEach(() => {
    authStore.reset();
    vi.restoreAllMocks();
  });

  it("renders children when authenticated", () => {
    act(() => authStore.setStatus("authenticated"));
    renderGate();
    expect(screen.getByText("protected")).toBeInTheDocument();
  });

  it("renders children when auth is disabled", () => {
    act(() => authStore.setStatus("disabled", "dev_bypass"));
    renderGate();
    expect(screen.getByText("protected")).toBeInTheDocument();
  });

  it("renders children while renewing (transient, non-terminal)", () => {
    act(() => authStore.setStatus("renewing"));
    renderGate();
    expect(screen.getByText("protected")).toBeInTheDocument();
  });

  it("shows the redirect overlay instead of children when reauth is required", () => {
    act(() => authStore.setStatus("authenticated"));
    renderGate();
    act(() => authStore.setStatus("reauth_required", "refresh_failed"));
    expect(screen.queryByText("protected")).not.toBeInTheDocument();
    expect(screen.getByRole("status")).toBeInTheDocument();
  });

  it("offers a retry that re-triggers reauth when the redirect failed", () => {
    const requireReauth = vi
      .spyOn(OidcManager, "requireReauth")
      .mockResolvedValue(undefined);
    act(() => authStore.setStatus("reauth_failed", "refresh_failed"));
    renderGate();
    expect(screen.queryByText("protected")).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole("button"));
    expect(requireReauth).toHaveBeenCalledTimes(1);
  });
});
