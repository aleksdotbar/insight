/**
 * withAuth — HOC that wraps a screen loader to require authentication.
 * Usage in screenset config:
 *   screen: withAuth(() => import('./screens/home/HomeScreen'))
 *
 * Awaits OidcManager.authReady. If no user → redirects to OIDC login.
 * If user → renders the wrapped screen.
 */

import React, { useState, useEffect, Suspense, lazy, type ComponentType } from 'react';
import { OidcManager } from './OidcManager';

type ScreenLoader = () => Promise<{ default: ComponentType }>;

export function withAuth(loader: ScreenLoader): ScreenLoader {
  const LazyScreen = lazy(loader);

  const AuthScreen: React.FC = () => {
    const [ready, setReady] = useState(false);

    useEffect(() => {
      void OidcManager.authReady.then((user) => {
        if (user) {
          setReady(true);
        } else {
          OidcManager.signIn();
        }
      });
    }, []);

    if (!ready) {
      return (
        <div className="flex items-center justify-center h-full w-full">
          <div className="flex flex-col items-center gap-3">
            <div className="w-8 h-8 border-2 border-primary border-t-transparent rounded-full animate-spin" />
            <span className="text-sm text-muted-foreground">Loading...</span>
          </div>
        </div>
      );
    }

    return (
      <Suspense fallback={null}>
        <LazyScreen />
      </Suspense>
    );
  };

  AuthScreen.displayName = 'AuthScreen';

  // Return as a module-like loader for HAI3 screen config
  return () => Promise.resolve({ default: AuthScreen });
}
