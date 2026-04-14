import { toUpper, trim } from 'lodash';

/**
 * Extract initials from a person name (max 2 characters).
 * Returns '' for empty / undefined / null input.
 */
export function getInitials(name: string | null | undefined): string {
  if (!name) return '';
  return trim(name).split(/\s+/u).filter(Boolean).slice(0, 2).map((w) => toUpper(w[0])).join('');
}
