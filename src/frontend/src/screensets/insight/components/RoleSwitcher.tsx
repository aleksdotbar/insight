/**
 * RoleSwitcher — demo widget to switch between mock user roles.
 * Renders in the sidebar bottom: styled to match the dark sidebar theme.
 */

import React, { useState, useRef, useEffect } from 'react';
import { useAppSelector, type MenuState } from '@hai3/react';
import { selectCurrentUser } from '../slices/currentUserSlice';
import { switchUser, MOCK_USERS } from '../actions/currentUserActions';
import type { UserRole } from '../types';

const ROLE_LABEL: Record<UserRole, string> = {
  executive: 'Executive',
  team_lead: 'Team Lead',
  ic: 'IC',
};

// Badge colors — light popup context (dropdown is on white bg)
const ROLE_BADGE: Record<UserRole, string> = {
  executive: 'bg-purple-100 text-purple-700',
  team_lead: 'bg-blue-100 text-blue-700',
  ic: 'bg-gray-100 text-gray-600',
};

// Badge colors — dark sidebar context
const ROLE_BADGE_DARK: Record<UserRole, string> = {
  executive: 'bg-purple-500/20 text-purple-300',
  team_lead: 'bg-blue-500/20 text-blue-300',
  ic: 'bg-white/10 text-gray-300',
};

function initials(name: string): string {
  return name.split(' ').filter(Boolean).slice(0, 2).map((w) => w[0].toUpperCase()).join('');
}

export const RoleSwitcher: React.FC = () => {
  const currentUser = useAppSelector(selectCurrentUser);
  const menuState = useAppSelector((state) => state['layout/menu'] as MenuState | undefined);
  const collapsed = menuState?.collapsed ?? false;

  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, []);

  const avatar = (
    <div className="w-7 h-7 rounded-full bg-white/10 flex items-center justify-center flex-shrink-0">
      <span className="text-[10px] font-bold text-white">{initials(currentUser.name)}</span>
    </div>
  );

  return (
    <div className="relative" ref={ref}>
      {/* Trigger — dark sidebar style */}
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        title={collapsed ? `${currentUser.name} · ${ROLE_LABEL[currentUser.role]}` : undefined}
        className={`w-full flex items-center gap-2.5 px-3 py-2.5 rounded-lg hover:bg-white/10 transition-colors ${
          collapsed ? 'justify-center' : ''
        }`}
      >
        {avatar}
        {!collapsed && (
          <>
            <div className="flex-1 text-left min-w-0">
              <div className="text-[12px] font-semibold text-white/90 truncate leading-tight">
                {currentUser.name}
              </div>
              <span className={`text-[9px] font-bold px-1.5 py-px rounded ${ROLE_BADGE_DARK[currentUser.role]}`}>
                {ROLE_LABEL[currentUser.role]}
              </span>
            </div>
            <span className="text-white/30 text-[10px]">▾</span>
          </>
        )}
      </button>

      {/* Dropdown — light popup, floats above sidebar */}
      {open && (
        <div className={`absolute bottom-full mb-2 bg-white border border-gray-200 rounded-xl shadow-xl z-50 min-w-[196px] py-1 ${
          collapsed ? 'left-full ml-2' : 'left-0'
        }`}>
          <div className="px-3 py-2 text-[9px] font-bold text-gray-400 uppercase tracking-wider border-b border-gray-100">
            Switch User (Demo)
          </div>
          {MOCK_USERS.map((user) => (
            <button
              key={user.personId}
              type="button"
              onClick={() => { switchUser(user); setOpen(false); }}
              className={`w-full text-left px-3 py-2.5 flex items-center gap-2.5 hover:bg-gray-50 transition-colors ${
                user.personId === currentUser.personId ? 'bg-blue-50' : ''
              }`}
            >
              <div className="w-7 h-7 rounded-full bg-indigo-100 flex items-center justify-center flex-shrink-0">
                <span className="text-[10px] font-bold text-indigo-600">{initials(user.name)}</span>
              </div>
              <div>
                <div className="text-[12px] font-semibold text-gray-800 leading-tight">{user.name}</div>
                <span className={`text-[9px] font-bold px-1.5 py-px rounded ${ROLE_BADGE[user.role]}`}>
                  {ROLE_LABEL[user.role]}
                </span>
              </div>
              {user.personId === currentUser.personId && (
                <span className="ml-auto text-blue-500 text-[10px]">✓</span>
              )}
            </button>
          ))}
        </div>
      )}
    </div>
  );
};
