import { createFileRoute } from "@tanstack/react-router";

import { ExecutiveViewScreen } from "@/screens/executive-view";

export const Route = createFileRoute("/ic/$person/exec")({
  component: ExecutiveViewScreen,
});
