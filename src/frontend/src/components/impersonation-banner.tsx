import { UserCogIcon } from "lucide-react";
import { Trans, useTranslation } from "react-i18next";

import { clearOverride, useViewer } from "@/auth";
import {
  Alert,
  AlertAction,
  AlertDescription,
  AlertTitle,
} from "@/components/ui/alert";
import { Button } from "@/components/ui/button";

function handleStop(): void {
  clearOverride();
  window.location.assign("/");
}

export function ImpersonationBanner(): React.ReactElement | null {
  const { email, source } = useViewer();
  const { t } = useTranslation();

  if (source !== "override" || !email) return null;

  return (
    <Alert variant="warning" className="rounded-none border-0 border-b">
      <UserCogIcon />
      <AlertTitle>{t("impersonation_banner.title")}</AlertTitle>
      <AlertDescription>
        <Trans
          i18nKey="impersonation_banner.description_html"
          values={{ email }}
          components={{ strong: <strong className="font-semibold" /> }}
        />
      </AlertDescription>
      <AlertAction>
        <Button variant="outline" size="sm" onClick={handleStop}>
          {t("impersonation_banner.stop")}
        </Button>
      </AlertAction>
    </Alert>
  );
}
