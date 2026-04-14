/**
 * OIDC Callback Screen
 * Handles the /callback route after OIDC provider redirects back.
 * Exchanges authorization code for tokens via OidcManager action.
 *
 * If opened without code param (e.g. as default route), redirects to app root.
 */

import React, { useEffect, useRef } from 'react';
import { useAppSelector } from '@hai3/react';
import { selectAuthStatus } from '@/app/slices/authSlice';
import { handleOidcCallback } from '../../actions/callbackActions';
import { getStartUrl } from '@/app/auth/startUrl';

const CallbackScreen: React.FC = () => {
  const status = useAppSelector(selectAuthStatus);
  const attempted = useRef(false);

  useEffect(() => {
    if (attempted.current) return;
    attempted.current = true;

    // If no code in startUrl, this isn't a real OIDC callback — redirect to main app
    const url = getStartUrl();
    if (!url || !url.includes('code=')) {
      window.location.replace('/executive-view');
      return;
    }

    handleOidcCallback();
  }, []);

  if (status === 'expired') {
    return (
      <div className="flex items-center justify-center h-full w-full">
        <div className="flex flex-col items-center gap-4 max-w-md text-center">
          <div className="text-destructive text-lg font-medium">Authentication Error</div>
          <p className="text-sm text-muted-foreground">Failed to complete sign-in. Please try again.</p>
          <button
            type="button"
            onClick={() => {
              sessionStorage.clear();
              localStorage.clear();
              window.location.replace('/');
            }}
            className="px-4 py-2 bg-primary text-primary-foreground rounded-md text-sm hover:bg-primary/90 transition-colors"
          >
            Sign In Again
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="flex items-center justify-center h-full w-full">
      <div className="flex flex-col items-center gap-3">
        <div className="w-8 h-8 border-2 border-primary border-t-transparent rounded-full animate-spin" />
        <span className="text-sm text-muted-foreground">Completing sign-in...</span>
      </div>
    </div>
  );
};

export default CallbackScreen;
