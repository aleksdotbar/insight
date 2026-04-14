/**
 * useRequireAuth
 * Hook for protected screens.
 * Awaits OidcManager.authReady promise.
 * If authenticated → { ready: true }
 * If not → redirects to OIDC provider login
 */

import { useState, useEffect } from 'react';
import { OidcManager } from './OidcManager';

export function useRequireAuth(): { ready: boolean } {
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

  return { ready };
}
