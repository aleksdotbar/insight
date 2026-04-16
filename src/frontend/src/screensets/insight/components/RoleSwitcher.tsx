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

const ROLE_BADGE_DARK: Record<UserRole, string> = {
  executive: 'bg-purple-500/20 text-purple-300',
  team_lead: 'bg-blue-500/20 text-blue-300',
  ic: 'bg-white/10 text-gray-300',
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

  return (
    <div
      className={`w-full flex items-center gap-2.5 px-3 py-2.5 ${collapsed ? 'justify-center' : ''}`}
      title={collapsed ? `${currentUser.name} · ${ROLE_LABEL[currentUser.role]}` : undefined}
    >
      {avatar}
      {!collapsed && (
        <div className="flex-1 text-left min-w-0">
          <div className="text-sm font-semibold text-white/90 truncate leading-tight">
            {currentUser.name}
          </div>
          <span className={`text-2xs font-bold px-1.5 py-px rounded ${ROLE_BADGE_DARK[currentUser.role]}`}>
            {ROLE_LABEL[currentUser.role]}
          </span>
        </div>
      )}
    </div>
  );
};
