import { createFileRoute } from "@tanstack/react-router";

import { useViewer } from "@/auth";
import { DevImpersonationHint } from "@/components/dev-impersonation-hint";
import { IcDashboardScreen } from "@/screens/ic-dashboard";

export const Route = createFileRoute("/")({
  component: IndexRoute,
});

function IndexRoute() {
  const { email } = useViewer();
  if (email) return <IcDashboardScreen personId={email} />;
  // Dev-only state — guard filters prod into signIn().
  return <DevImpersonationHint />;
}
