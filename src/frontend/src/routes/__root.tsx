import { Outlet, createRootRoute } from "@tanstack/react-router";

import { OidcManager, authStore, isDevImpersonating } from "@/auth";
import { AppSidebar } from "@/components/app-sidebar";
import { MockBanner } from "@/components/mock-banner";
import { SidebarInset, SidebarProvider } from "@/components/ui/sidebar";

export const Route = createRootRoute({
  beforeLoad: async ({ location }) => {
    if (location.pathname === "/callback") return;
    await OidcManager.init();
    const snap = authStore.getSnapshot();

    // Real auth or restored OIDC session.
    if (snap.status === "authenticated" && snap.user?.email) return;

    // Dev impersonation via VITE_DEV_USER_EMAIL (DEV builds only).
    if (snap.status === "authenticated" && isDevImpersonating()) return;

    // No session yet, token expired, or backend rejected the token —
    // kick off the OIDC redirect. `unauthorized` lands here too because
    // the AppErrorBoundary can't catch data-state (it only catches throws
    // + rejected query promises), so without an explicit redirect the
    // user would sit on a half-rendered dashboard with every fetch 401'ing
    // in a loop.
    if (
      snap.status === "idle" ||
      snap.status === "expired" ||
      snap.status === "unauthorized"
    ) {
      await OidcManager.signIn();
      return;
    }

    // Dev bypass without VITE_DEV_USER_EMAIL set: let screens render
    // <DevImpersonationHint />. Unreachable in prod because dev bypass
    // doesn't fire there — leave the branch but force-redirect for safety.
    if (snap.status === "authenticated") {
      if (import.meta.env.DEV) return;
      await OidcManager.signIn();
      return;
    }
  },
  component: RootLayout,
});

function RootLayout() {
  return (
    <SidebarProvider>
      <AppSidebar />
      <SidebarInset className="min-w-0 overflow-x-clip">
        <MockBanner />
        <Outlet />
      </SidebarInset>
    </SidebarProvider>
  );
}
