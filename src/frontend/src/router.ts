import { createRouter } from "@tanstack/react-router";

import { AppBootSpinner } from "@/components/app-boot-spinner";
import { routeTree } from "./routeTree.gen";

// The runtime <base> tag (index.html) is the single source of the serving
// prefix: "/" normally, "/exp/<name>/" inside a preview experiment.
const basepath = new URL(document.baseURI).pathname;

export const router = createRouter({
  routeTree,
  basepath,
  defaultPreload: "intent",
  defaultPendingComponent: AppBootSpinner,
  defaultPendingMs: 200,
});

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}
