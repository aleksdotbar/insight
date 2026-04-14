/**
 * Auth types — OIDC config and error format
 */

/** OIDC bootstrapping config returned by GET /auth/config */
export type OidcConfig = {
  issuer_url: string;
  client_id: string;
  redirect_uri: string;
  scopes: string[];
  response_type: 'code';
}

/** Settings for oidc-client-ts UserManager, derived from OidcConfig */
export type OidcManagerConfig = {
  authority: string;
  client_id: string;
  redirect_uri: string;
  post_logout_redirect_uri: string;
  scope: string;
  response_type: 'code';
  automaticSilentRenew: boolean;
}

/** RFC 9457 Problem Details — standard error format from the backend */
export type ProblemDetail = {
  type: string;
  title: string;
  status: number;
  detail?: string;
  instance?: string;
}
