import { Outlet, createRootRoute } from "@tanstack/react-router";

import { TooltipProvider } from "@/components/ui/tooltip";
import { getPerson } from "@/api/identity-client";
import { OidcManager, authStore, getViewerEmail } from "@/auth";
import { AppSidebar } from "@/components/app-sidebar";
import { AuthGate } from "@/components/auth-gate";
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

    const { status } = authStore.getSnapshot();
    if (status === "authenticated") {
      await prefetchViewerIdentity();
      return;
    }
    // No interactive login to perform when OIDC is inactive — let the app
    // render (dev bypass works; an unconfigured deploy fails closed downstream).
    if (status === "disabled") return;

    await OidcManager.signIn();
  },
  component: RootLayout,
});

function RootLayout() {
  return (
    <TooltipProvider>
      <AuthGate>
        <SidebarProvider>
          <AppSidebar />
          <SidebarInset className="min-w-0 overflow-x-clip">
            <ImpersonationBanner />
            <MockBanner />
            <Outlet />
          </SidebarInset>
        </SidebarProvider>
      </AuthGate>
    </TooltipProvider>
  );
}
