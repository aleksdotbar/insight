import { describe, expect, it } from "vitest";

import { isTerminalAuthFailure } from "./auth-policy";
import type { AuthStatus } from "./types";

describe("isTerminalAuthFailure", () => {
  it("treats unauthorized as terminal", () => {
    expect(isTerminalAuthFailure({ status: "unauthorized", error: null })).toBe(
      true,
    );
    expect(
      isTerminalAuthFailure({ status: "unauthorized", error: "refresh_failed" }),
    ).toBe(true);
    expect(
      isTerminalAuthFailure({ status: "unauthorized", error: "token_rejected" }),
    ).toBe(true);
  });

  it("excludes the dev / no-OIDC bypass", () => {
    expect(
      isTerminalAuthFailure({
        status: "unauthorized",
        error: "missing_oidc_config",
      }),
    ).toBe(false);
  });

  it("treats expired as terminal only after silent renew has failed", () => {
    expect(
      isTerminalAuthFailure({ status: "expired", error: "silent_renew_failed" }),
    ).toBe(true);
    expect(isTerminalAuthFailure({ status: "expired", error: null })).toBe(
      false,
    );
  });

  it("is never terminal for non-failure states", () => {
    const states: AuthStatus[] = ["idle", "loading", "authenticated"];
    for (const status of states) {
      expect(isTerminalAuthFailure({ status, error: null })).toBe(false);
    }
  });
});
