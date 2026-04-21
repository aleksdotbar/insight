/**
 * ComingSoon — uniform placeholder for surfaces whose backend data is absent.
 *
 * Two distinct states, both data-driven:
 *   - state='empty' (default): the backend responded successfully but has no
 *     rows for the current filter (e.g. a CPO has no commits). Rendered as
 *     "No data for this period" — informational, no retry.
 *   - state='error': the backend query rejected (network error, 5xx, etc.).
 *     Rendered as "Unable to load" with a Retry button if onRetry is supplied.
 *
 * The decision between empty and error is made in actions/slices based on
 * Promise.allSettled results — the component itself does not guess.
 */

import React from 'react';
import { Icon } from '@iconify/react';

export type ComingSoonVariant = 'card' | 'chip' | 'row';
export type ComingSoonState = 'empty' | 'error';

export interface ComingSoonProps {
  variant?: ComingSoonVariant;
  state?: ComingSoonState;
  /** Override the default label for the current state. */
  label?: string;
  /** When provided and state='error', renders a Retry button. */
  onRetry?: () => void;
}

const DEFAULT_LABELS: Record<ComingSoonState, string> = {
  empty: 'No data for this period',
  error: 'Unable to load',
};

const STATE_STYLES: Record<ComingSoonState, { border: string; bg: string; text: string; icon: string; iconName: string }> = {
  empty: {
    border: 'border-gray-200',
    bg:     'bg-slate-50/40',
    text:   'text-gray-400',
    icon:   'text-gray-400',
    iconName: 'lucide:clock',
  },
  error: {
    border: 'border-red-200',
    bg:     'bg-red-50/40',
    text:   'text-red-500',
    icon:   'text-red-400',
    iconName: 'lucide:alert-triangle',
  },
};

const ComingSoon: React.FC<ComingSoonProps> = ({
  variant = 'card',
  state = 'empty',
  label,
  onRetry,
}) => {
  const s = STATE_STYLES[state];
  const displayLabel = label ?? DEFAULT_LABELS[state];
  const showRetry = state === 'error' && !!onRetry;

  if (variant === 'chip') {
    return (
      <span
        className={`inline-flex items-center gap-1 rounded-full border border-dashed ${s.border} px-2 py-0.5 text-2xs font-medium ${s.text}`}
        role="status"
        aria-label={displayLabel}
      >
        <Icon icon={s.iconName} className={`w-3 h-3 ${s.icon}`} aria-hidden />
        {displayLabel}
      </span>
    );
  }

  if (variant === 'row') {
    return (
      <div
        className={`flex items-center gap-2 rounded-md border border-dashed ${s.border} px-3 py-2 text-xs ${s.text}`}
        role="status"
        aria-label={displayLabel}
      >
        <Icon icon={s.iconName} className={`w-3.5 h-3.5 ${s.icon}`} aria-hidden />
        <span>{displayLabel}</span>
        {showRetry && (
          <button
            type="button"
            onClick={onRetry}
            className="ml-auto text-xs font-semibold text-red-600 hover:text-red-700 focus-visible:outline-none focus-visible:underline"
          >
            Retry
          </button>
        )}
      </div>
    );
  }

  return (
    <div
      className={`flex flex-col items-center justify-center gap-1 rounded-lg border border-dashed ${s.border} ${s.bg} px-4 py-6 text-center`}
      role="status"
      aria-label={displayLabel}
    >
      <Icon icon={s.iconName} className={`w-5 h-5 ${s.icon}`} aria-hidden />
      <span className={`text-xs font-medium ${s.text}`}>{displayLabel}</span>
      {showRetry && (
        <button
          type="button"
          onClick={onRetry}
          className="mt-1 rounded-md border border-red-300 bg-white px-2.5 py-1 text-xs font-semibold text-red-600 hover:bg-red-50 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-red-400"
        >
          Retry
        </button>
      )}
    </div>
  );
};

export default React.memo(ComingSoon);
