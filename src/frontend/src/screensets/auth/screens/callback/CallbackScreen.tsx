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
import { AuthErrorCard } from '../../uikit/base/AuthErrorCard';
import { AuthSpinner } from '../../uikit/base/AuthSpinner';

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
    return <AuthErrorCard onRetry={() => {
      sessionStorage.clear();
      localStorage.clear();
      window.location.replace('/');
    }} />;
  }

  return <AuthSpinner label="Completing sign-in..." />;
};

export default CallbackScreen;
