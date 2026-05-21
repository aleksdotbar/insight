import { createFileRoute } from "@tanstack/react-router";
import { useEffect } from "react";
import { useTranslation } from "react-i18next";

import { OidcManager, getStartUrl } from "@/auth";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Spinner } from "@/components/ui/spinner";

type CallbackResult =
  | { ok: true; returnUrl: string }
  | { ok: false; error: "missing_code" | "exchange_failed" };

export const Route = createFileRoute("/callback")({
  loader: async (): Promise<CallbackResult> => {
    const url = getStartUrl();
    if (!url) return { ok: false, error: "missing_code" };
    let hasCode: boolean;
    try {
      hasCode = new URL(url).searchParams.has("code");
    } catch {
      hasCode = false;
    }
    if (!hasCode) return { ok: false, error: "missing_code" };
    try {
      const returnUrl = await OidcManager.handleCallback(url);
      return { ok: true, returnUrl };
    } catch {
      return { ok: false, error: "exchange_failed" };
    }
  },
  component: CallbackScreen,
});

function CallbackScreen() {
  const data = Route.useLoaderData();
  const { t } = useTranslation();

  useEffect(() => {
    if (data.ok) {
      window.location.replace(data.returnUrl);
    }
  }, [data]);

  if (data.ok) {
    return (
      <div className="flex min-h-svh items-center justify-center p-6">
        <Card className="w-full max-w-sm">
          <CardContent className="flex flex-col items-center gap-3 text-center">
            <Spinner className="size-6" />
            <p className="text-sm text-muted-foreground">
              {t("auth.callback.completing")}
            </p>
          </CardContent>
        </Card>
      </div>
    );
  }

  return (
    <div className="flex min-h-svh items-center justify-center p-6">
      <Card className="w-full max-w-md">
        <CardContent className="flex flex-col gap-4 text-center">
          <h1 className="text-base font-semibold text-foreground">
            {t("auth.callback.failed_title")}
          </h1>
          <p className="text-sm text-muted-foreground">
            {data.error === "missing_code"
              ? t("auth.callback.missing_code")
              : t("auth.callback.exchange_failed")}
          </p>
          <Button
            onClick={() => {
              void OidcManager.signIn();
            }}
          >
            {t("common.actions.try_again")}
          </Button>
        </CardContent>
      </Card>
    </div>
  );
}
