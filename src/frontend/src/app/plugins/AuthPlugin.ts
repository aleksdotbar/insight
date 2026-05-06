/**
 * AuthPlugin
 * Injects Authorization Bearer token and X-Tenant-ID header on every request.
 *
 * On 401: clears the cached token, requests a fresh one via silent renew,
 * and retries the original request once. If the retry also 401s (or the
 * silent renew failed), emits AuthEvent.Unauthorized so the UI can render
 * a terminal "unauthorized" state — no further auto-refresh attempts.
 *
 * Extends RestPlugin (not ApiPluginBase) to avoid onError signature conflicts.
 * Use restProtocol.plugins.add(new AuthPlugin()) — not registerPlugin().
 */

import { getStore, TENANT_SLICE_NAME, eventBus, RestPlugin } from '@hai3/react';
import type { RestRequestContext, ApiPluginErrorContext, RestResponseContext } from '@hai3/react';
import type { TenantState } from '@hai3/react';
import { selectAuthToken } from '@/app/slices/authSlice';
import { AuthEvent } from '@/app/events/authEvents';
import { OidcManager } from '@/app/auth/OidcManager';

type HttpError = Error & { status?: number };

export class AuthPlugin extends RestPlugin {
  onRequest(context: RestRequestContext): RestRequestContext {
    const state = getStore().getState();
    const token = selectAuthToken(state);
    const tenantId = (state[TENANT_SLICE_NAME] as TenantState | undefined)?.tenant?.id ?? null;

    if (!token && !tenantId) return context;

    const extraHeaders: Record<string, string> = {};
    if (token) extraHeaders['Authorization'] = `Bearer ${token}`;
    if (tenantId) extraHeaders['X-Tenant-ID'] = tenantId;

    if (import.meta.env.DEV) {
      console.log(`[AuthPlugin] ${context.method ?? 'GET'} ${context.url} → Bearer ${token ? 'present' : 'none'}`);
    }

    return { ...context, headers: { ...context.headers, ...extraHeaders } };
  }

  async onError(context: ApiPluginErrorContext): Promise<Error | RestResponseContext> {
    const status = (context.error as HttpError).status;
    if (status !== 401) return context.error;

    if (context.retryCount === 0) {
      const newToken = await OidcManager.refresh();
      if (!newToken) {
        // Silent renew failed (session at IdP is gone, network error, etc.).
        eventBus.emit(AuthEvent.Unauthorized);
        return context.error;
      }
      // Token was just dispatched into the store by refresh(); onRequest
      // re-runs as part of retry() and will pick it up. We pass the header
      // explicitly too as belt-and-suspenders against any store-update race.
      return context.retry({
        headers: { ...context.request.headers, Authorization: `Bearer ${newToken}` },
      });
    }

    // Already retried with a fresh token and STILL got 401 — that's a server
    // policy issue (token rejected for non-expiry reasons, e.g. revoked
    // grant, audience mismatch). Stop trying.
    eventBus.emit(AuthEvent.Unauthorized);
    return context.error;
  }
}
