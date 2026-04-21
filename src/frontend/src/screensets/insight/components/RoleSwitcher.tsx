/**
 * RoleSwitcher — displays current user in the sidebar bottom.
 * No longer switches between mock users — shows real identity.
 */

import React from 'react';
import { useAppSelector, type MenuState } from '@hai3/react';
import { selectCurrentUser } from '../slices/currentUserSlice';
import { getInitials } from '../utils/getInitials';
import type { UserRole } from '../types';

const ROLE_LABEL: Record<UserRole, string> = {
  executive: 'Executive',
  team_lead: 'Team Lead',
  ic: 'IC',
};

export const RoleSwitcher: React.FC = () => {
  const currentUser = useAppSelector(selectCurrentUser);
  const menuState = useAppSelector((state) => state['layout/menu'] as MenuState | undefined);
  const collapsed = menuState?.collapsed ?? false;

  const avatar = (
    <div className="w-8 h-8 rounded-full bg-white/10 flex items-center justify-center flex-shrink-0">
      <span className="text-xs font-bold text-white">{getInitials(currentUser.name)}</span>
    </div>
  );

  const jobTitle = currentUser._identity?.job_title?.trim();
  const subtitle = jobTitle && jobTitle.length > 0 ? jobTitle : ROLE_LABEL[currentUser.role];

  return (
    <div
      className={`w-full flex items-center gap-2.5 px-3 py-2.5 ${collapsed ? 'justify-center' : ''}`}
      title={collapsed ? `${currentUser.name} \u00b7 ${subtitle}` : undefined}
    >
      {avatar}
      {!collapsed && (
        <div className="flex-1 text-left min-w-0">
          <div className="text-sm font-semibold text-white/90 truncate leading-tight">
            {currentUser.name}
          </div>
          <div
            className="text-2xs text-white/60 truncate leading-tight mt-0.5"
            title={subtitle}
          >
            {subtitle}
          </div>
        </div>
      )}
    </div>
  );
};
