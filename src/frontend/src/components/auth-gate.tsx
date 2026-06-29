import { useTranslation } from "react-i18next";

import { OidcManager, useAuth } from "@/auth";
import { AuthError } from "@/components/auth-error";
import { FullScreenLoading } from "@/components/full-screen-loading";

type AuthGateProps = {
  children: React.ReactNode;
};

/**
 * Presents the runtime auth state. The redirect itself is owned by
 * `OidcManager.requireReauth()` (called from the 401 path and the silent-renew
 * failure), so this gate only renders:
 *   - `reauth_required` → the redirect overlay, rendered instead of `children`
 *     so the 401'd subtree never enters the tree and no error cells paint.
 *   - `reauth_failed`   → a recoverable error with a retry, so a failed
 *     redirect never pins the app behind a permanent overlay.
 */
export function AuthGate({ children }: AuthGateProps): React.ReactNode {
  const { t } = useTranslation();
  const { status } = useAuth();

  if (status === "reauth_required") {
    return <FullScreenLoading message={t("auth.reauth.redirecting")} />;
  }

  if (status === "reauth_failed") {
    return (
      <AuthError
        title={t("auth.reauth.failed_title")}
        message={t("auth.reauth.failed_message")}
        onRetry={() => void OidcManager.requireReauth()}
      />
    );
  }

  return children;
}
