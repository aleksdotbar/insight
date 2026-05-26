import { createFileRoute } from "@tanstack/react-router";

import { useViewer } from "@/auth";
import { TeamViewScreen } from "@/screens/team-view";

export const Route = createFileRoute("/ic/$person/team")({
  component: TeamScreen,
});

function TeamScreen() {
  // TanStack Router decodes path params automatically — do not decode again.
  const { person } = Route.useParams();
  const { email: viewerEmail } = useViewer();
  return (
    <TeamViewScreen teamId={person} viewerEmail={viewerEmail ?? person} />
  );
}
