import { useEffect } from "react";
import { useTranslation } from "react-i18next";

import { OidcManager, isTerminalAuthFailure, useAuth } from "@/auth";
import { FullScreenLoading } from "@/components/full-screen-loading";

type AuthGateProps = {
  children: React.ReactNode;
};

/**
 * Watches auth status at runtime and, on a terminal failure (expiry that
 * silent renew can no longer recover), kicks off a full sign-in redirect.
 *
 * Rendering the overlay *instead of* `children` in the same commit keeps the
 * widget subtree — and its 401 error cells — out of the tree entirely, so no
 * error state paints before the redirect. The redirect itself is deduped
 * inside `OidcManager.signIn()`, which keeps this effect safe under
 * StrictMode's double-invoke and concurrent status flips.
 */
export function AuthGate({ children }: AuthGateProps): React.ReactNode {
  const { t } = useTranslation();
  const { status, error } = useAuth();
  const terminal = isTerminalAuthFailure({ status, error });

  useEffect(() => {
    if (terminal) void OidcManager.signIn();
  }, [terminal]);

  if (terminal) {
    return <FullScreenLoading message={t("auth.reauth.redirecting")} />;
  }

  return children;
}
