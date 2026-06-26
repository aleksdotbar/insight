import { act, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import "@/i18n";
import { authStore } from "@/auth/auth-store";
import { OidcManager } from "@/auth/oidc-manager";
import { AuthGate } from "./auth-gate";

describe("<AuthGate>", () => {
  let signIn: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    signIn = vi.spyOn(OidcManager, "signIn").mockResolvedValue(undefined);
    authStore.reset();
  });

  afterEach(() => {
    authStore.reset();
    vi.restoreAllMocks();
  });

  it("renders children while authenticated", () => {
    act(() => authStore.setStatus("authenticated"));
    render(
      <AuthGate>
        <div>protected</div>
      </AuthGate>,
    );
    expect(screen.getByText("protected")).toBeInTheDocument();
    expect(signIn).not.toHaveBeenCalled();
  });

  it("swaps to the redirect overlay and signs in once on terminal failure", () => {
    act(() => authStore.setStatus("authenticated"));
    render(
      <AuthGate>
        <div>protected</div>
      </AuthGate>,
    );
    act(() => authStore.setStatus("unauthorized", "refresh_failed"));
    expect(screen.queryByText("protected")).not.toBeInTheDocument();
    expect(screen.getByRole("status")).toBeInTheDocument();
    expect(signIn).toHaveBeenCalledTimes(1);
  });

  it("ignores the dev / no-OIDC bypass", () => {
    render(
      <AuthGate>
        <div>protected</div>
      </AuthGate>,
    );
    act(() => authStore.setStatus("unauthorized", "missing_oidc_config"));
    expect(screen.getByText("protected")).toBeInTheDocument();
    expect(signIn).not.toHaveBeenCalled();
  });
});
