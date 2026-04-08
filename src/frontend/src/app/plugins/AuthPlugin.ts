/**
 * AuthPlugin
 * Injects Authorization Bearer token and X-Tenant-ID header on every request.
 * Handles 401 responses by emitting auth:sessionExpired.
 *
 * Extends RestPlugin (not ApiPluginBase) to avoid onError signature conflicts.
 * Use restProtocol.plugins.add(new AuthPlugin()) — not registerPlugin().
 */

import { getStore, TENANT_SLICE_NAME, eventBus, RestPlugin } from '@hai3/react';
import type { RestRequestContext, ApiPluginErrorContext, RestResponseContext } from '@hai3/react';
import type { TenantState } from '@hai3/react';
import { selectAuthToken } from '@/app/slices/authSlice';
import { AuthEvent } from '@/app/events/authEvents';

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

    return { ...context, headers: { ...context.headers, ...extraHeaders } };
  }

  onError(context: ApiPluginErrorContext): Error | RestResponseContext {
    const status = (context.error as HttpError).status;
    if (status === 401 && context.retryCount === 0) {
      eventBus.emit(AuthEvent.SessionExpired);
    }
    return context.error;
  }
}
