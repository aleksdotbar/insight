import { createFileRoute } from "@tanstack/react-router";

import { useViewer } from "@/auth";
import { useMetricsV2Enabled } from "@/lib/feature-flags";
import { TeamViewScreen } from "@/screens/team-view";
import { TeamViewV2Screen } from "@/screens/team-view-v2";

export const Route = createFileRoute("/ic/$person/team")({
  component: TeamScreen,
});

function TeamScreen() {
  const { person } = Route.useParams();
  const { email: viewerEmail } = useViewer();
  const viewer = viewerEmail ?? person;
  const v2 = useMetricsV2Enabled();
  if (v2) {
    return <TeamViewV2Screen teamId={person} viewerEmail={viewer} />;
  }
  return <TeamViewScreen teamId={person} viewerEmail={viewer} />;
}
