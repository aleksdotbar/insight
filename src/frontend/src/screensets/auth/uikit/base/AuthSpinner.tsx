import React from 'react';

export interface AuthSpinnerProps { label: string }

export const AuthSpinner: React.FC<AuthSpinnerProps> = ({ label }) => (
  <div className="flex items-center justify-center h-full w-full">
    <div className="flex flex-col items-center gap-3">
      <div className="w-8 h-8 border-2 border-primary border-t-transparent rounded-full animate-spin" />
      <span className="text-sm text-muted-foreground">{label}</span>
    </div>
  </div>
);
