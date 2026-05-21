import { createFileRoute } from "@tanstack/react-router";

import { IcDashboardScreen } from "@/screens/ic-dashboard";

export const Route = createFileRoute("/ic/$person/personal")({
  component: PersonalRoute,
});

function PersonalRoute() {
  const { person } = Route.useParams();
  return <IcDashboardScreen personId={person} />;
}
