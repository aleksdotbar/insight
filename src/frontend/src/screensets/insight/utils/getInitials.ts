import { toUpper } from 'lodash';

/**
 * Extract initials from a person name (max 2 characters).
 */
export function getInitials(name: string): string {
  return name
    .split(' ')
    .filter(Boolean)
    .slice(0, 2)
    .map((word) => toUpper(word[0]))
    .join('');
}
