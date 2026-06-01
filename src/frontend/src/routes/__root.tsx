import { Outlet, createRootRoute } from "@tanstack/react-router";

import { getPerson } from "@/api/identity-client";
import { OidcManager, authStore, getViewerEmail } from "@/auth";
import { AppSidebar } from "@/components/app-sidebar";
import { ImpersonationBanner } from "@/components/impersonation-banner";
import { MockBanner } from "@/components/mock-banner";
import { SidebarInset, SidebarProvider } from "@/components/ui/sidebar";
import { queryClient } from "@/query-client";

async function prefetchViewerIdentity(): Promise<void> {
  const email = getViewerEmail();
  if (!email) return;
  const key = email.toLowerCase();
  await queryClient.prefetchQuery({
    queryKey: ["identity", "person", key],
    queryFn: () => getPerson(email),
  });
}

export const Route = createRootRoute({
  beforeLoad: async ({ location }) => {
    if (location.pathname === "/callback") return;
    await OidcManager.init();

    if (authStore.getSnapshot().status === "authenticated") {
      await prefetchViewerIdentity();
      return;
    }

    await OidcManager.signIn();
  },
  component: RootLayout,
});

function RootLayout() {
  return (
    <SidebarProvider>
      <AppSidebar />
      <SidebarInset className="min-w-0 overflow-x-clip">
        <ImpersonationBanner />
        <MockBanner />
        <Outlet />
      </SidebarInset>
    </SidebarProvider>
  );
}
