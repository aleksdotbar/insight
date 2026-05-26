import { TriangleAlertIcon } from "lucide-react";
import { Trans, useTranslation } from "react-i18next";

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";

export function DevImpersonationHint(): React.ReactElement | null {
  // DEV-only diagnostic. In prod the root guard either lets a real auth'd
  // user through or redirects to the IdP — this branch is unreachable, so
  // the whole component tree-shakes out via Vite's import.meta.env.DEV
  // constant. Hook call stays above the early-return so React-Hooks ESLint
  // is happy and per-render order is stable in dev.
  const { t } = useTranslation();
  if (!import.meta.env.DEV) return null;
  return (
    <div className="flex min-h-svh items-center justify-center p-6">
      <Alert variant="warning" className="max-w-xl">
        <TriangleAlertIcon />
        <AlertTitle>{t("dev_impersonation_hint.title")}</AlertTitle>
        <AlertDescription>
          <Trans
            i18nKey="dev_impersonation_hint.description_html"
            components={{ code: <code className="font-mono" /> }}
          />
        </AlertDescription>
      </Alert>
    </div>
  );
}
