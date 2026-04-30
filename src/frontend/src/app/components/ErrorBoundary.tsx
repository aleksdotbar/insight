import { Component, type ErrorInfo, type ReactNode } from 'react';

interface Props {
  children: ReactNode;
}

interface State {
  hasError: boolean;
}

/**
 * Top-level error boundary. Catches uncaught render-tree errors and renders a
 * generic fallback so the app does not crash to a blank screen — and so stack
 * traces never bleed into the DOM in production.
 *
 * In dev: logs the full error with component stack to the console.
 * In prod: logs only `error.message` so operators can correlate user reports
 * to a sanitized trace without leaking source paths. Wire this up to a remote
 * error reporter (Sentry, etc.) when one is added.
 */
export class ErrorBoundary extends Component<Props, State> {
  state: State = { hasError: false };

  static getDerivedStateFromError(): State {
    return { hasError: true };
  }

  componentDidCatch(error: Error, info: ErrorInfo): void {
    if (import.meta.env.DEV) {
      console.error('[ErrorBoundary]', error, info.componentStack);
    } else {
      console.error('[ErrorBoundary]', error.message);
    }
  }

  render(): ReactNode {
    if (!this.state.hasError) return this.props.children;
    return (
      <div
        role="alert"
        className="max-w-lg mx-auto mt-16 p-8 text-center bg-background text-foreground"
      >
        <h1 className="text-xl mb-4">Something went wrong</h1>
        <p className="text-muted-foreground mb-6">
          The application encountered an unexpected error. Please reload the
          page. If the problem persists, contact your administrator.
        </p>
        <button
          type="button"
          onClick={() => {
            window.location.reload();
          }}
          className="px-4 py-2 border border-border rounded bg-background text-foreground cursor-pointer hover:bg-muted"
        >
          Reload
        </button>
      </div>
    );
  }
}
