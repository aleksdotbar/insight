import { useTranslation } from "react-i18next";

import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";

type AuthErrorProps = {
  title: string;
  message: string;
  onRetry: () => void;
};

export function AuthError({
  title,
  message,
  onRetry,
}: AuthErrorProps): React.ReactElement {
  const { t } = useTranslation();
  return (
    <div className="flex min-h-svh items-center justify-center p-6">
      <Card className="w-full max-w-md">
        <CardContent className="flex flex-col gap-4 text-center">
          <h1 className="text-base font-semibold text-foreground">{title}</h1>
          <p className="text-sm text-muted-foreground">{message}</p>
          <Button onClick={onRetry}>{t("common.actions.try_again")}</Button>
        </CardContent>
      </Card>
    </div>
  );
}
