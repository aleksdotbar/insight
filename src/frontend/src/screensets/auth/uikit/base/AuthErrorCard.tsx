import React from 'react';
import { Button } from '@hai3/uikit';

export interface AuthErrorCardProps { onRetry: () => void }

export const AuthErrorCard: React.FC<AuthErrorCardProps> = ({ onRetry }) => (
  <div className="flex items-center justify-center h-full w-full">
    <div className="flex flex-col items-center gap-4 max-w-md text-center">
      <div className="text-destructive text-lg font-medium">Authentication Error</div>
      <p className="text-sm text-muted-foreground">Failed to complete sign-in. Please try again.</p>
      <Button variant="default" size="sm" onClick={onRetry}>Sign In Again</Button>
    </div>
  </div>
);
