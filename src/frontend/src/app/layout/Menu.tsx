/**
 * Menu Component
 *
 * Side navigation menu displaying screenset menu items.
 * Uses @hai3/uikit Sidebar components for proper styling and collapsible behavior.
 */

import React, { useState } from 'react';
import { useAppSelector, useNavigation, useTranslation, eventBus, type MenuState, type MenuItem } from '@hai3/react';
import { requestSelection } from '@/screensets/insight/actions/insightNavigationActions';
import {
  Sidebar,
  SidebarContent,
  SidebarMenu,
  SidebarMenuItem,
  SidebarMenuButton,
  SidebarMenuIcon,
  SidebarHeader,
} from '@hai3/uikit';
import { Icon } from '@iconify/react';
import { HAI3LogoIcon } from '@/app/icons/HAI3LogoIcon';
import { HAI3LogoTextIcon } from '@/app/icons/HAI3LogoTextIcon';
import { decodeMenuItemId } from '@/screensets/insight/utils/menuItemId';

export interface MenuProps {
  children?: React.ReactNode;
  /** Called after a navigation action — use to close mobile sheet */
  onNavigate?: () => void;
}

export const Menu: React.FC<MenuProps> = ({ children, onNavigate }) => {
  const menuState = useAppSelector((state) => state['layout/menu'] as MenuState | undefined);
  const { activePersonParam, activeTeamParam } = useAppSelector((state) => {
    const s = state as Record<string, unknown>;
    const icSlice   = s['insight/icDashboard'] as { selectedPersonId?: string } | undefined;
    const teamSlice = s['insight/teamView']     as { selectedTeamId?: string }   | undefined;
    return {
      activePersonParam: icSlice?.selectedPersonId ?? null,
      activeTeamParam:   teamSlice?.selectedTeamId  ?? null,
    };
  });
  const { currentScreen, navigateToScreen, currentScreenset } = useNavigation();
  const { t } = useTranslation();
  const [expandedGroups, setExpandedGroups] = useState<Record<string, boolean>>({});

  const collapsed = menuState?.collapsed ?? false;
  const items: MenuItem[] = menuState?.items ?? [];

  const handleToggleCollapse = () => {
    eventBus.emit('layout/menu/collapsed', { collapsed: !collapsed });
  };

  const handleItemClick = (itemId: string) => {
    const { screenId, param } = decodeMenuItemId(itemId);
    requestSelection(screenId, param);
    navigateToScreen(currentScreenset ?? '', screenId);
    onNavigate?.();
  };

  const toggleGroup = (id: string) =>
    setExpandedGroups((prev) => ({ ...prev, [id]: !prev[id] }));

  /** Returns true if any descendant (at any depth) is "active" */
  const hasActiveDescendant = (item: MenuItem): boolean => {
    if (!item.children?.length) return false;
    return item.children.some((child) => {
      const { screenId, param } = decodeMenuItemId(child.id);
      const isLeaf = !child.children?.length;
      if (isLeaf && param) {
        const activeParam = activeTeamParam === param ? activeTeamParam : activePersonParam;
        return currentScreen === screenId && activeParam === param;
      }
      if (isLeaf) return currentScreen === child.id;
      return hasActiveDescendant(child);
    });
  };


  /**
   * Static Tailwind classes per depth — Tailwind JIT cannot pick up
   * `pl-${n}` template strings, so we enumerate. Depths beyond the table
   * fall back to the deepest class (rare for subordinate trees).
   */
  const INDENT_BY_DEPTH: Record<number, string> = {
    0: '',
    1: 'pl-8',
    2: 'pl-12',
    3: 'pl-16',
    4: 'pl-20',
    5: 'pl-24',
  };

  /** Recursive item renderer */
  const renderItem = (item: MenuItem, depth = 0): React.ReactNode => {
    const hasChildren = (item.children?.length ?? 0) > 0;
    // Only the top-level group expands by default (so direct reports stay
    // visible). Every nested group starts collapsed — expanding a manager
    // shouldn't cascade through their entire subtree. User toggles win via
    // expandedGroups.
    const defaultExpanded = depth === 0;
    const isExpanded = expandedGroups[item.id] ?? defaultExpanded;
    const { screenId, param } = decodeMenuItemId(item.id);
    const activeParam = param
      ? (activeTeamParam === param ? activeTeamParam : activePersonParam)
      : null;
    const isSelfActive = param
      ? currentScreen === screenId && activeParam === param
      : currentScreen === item.id;
    const isActive = isSelfActive || hasActiveDescendant(item);
    const indent = INDENT_BY_DEPTH[depth] ?? INDENT_BY_DEPTH[5];

    return (
      <SidebarMenuItem key={item.id}>
        <SidebarMenuButton
          isActive={isSelfActive}
          onClick={() => {
            if (hasChildren) {
              toggleGroup(item.id);
            }
            handleItemClick(item.id);
          }}
          tooltip={collapsed && depth === 0 ? t(item.label) : undefined}
          className={`${indent} ${depth > 0 ? 'text-xs' : ''} ${isActive && !isSelfActive ? 'text-sidebar-foreground/80' : ''}`}
        >
          {item.icon && (
            <SidebarMenuIcon>
              <Icon icon={item.icon} className={depth > 0 ? 'w-3.5 h-3.5' : 'w-5 h-5'} />
            </SidebarMenuIcon>
          )}
          <span>{t(item.label)}</span>
          {hasChildren && !collapsed && (
            <span className="ml-auto text-2xs opacity-40">
              {isExpanded ? '▾' : '▸'}
            </span>
          )}
        </SidebarMenuButton>

        {hasChildren && isExpanded && !collapsed && (
          <SidebarMenu>
            {item.children!.map((child) => renderItem(child, depth + 1))}
          </SidebarMenu>
        )}
      </SidebarMenuItem>
    );
  };

  return (
    <Sidebar collapsed={collapsed} className="h-full">
      <SidebarHeader
        logo={<HAI3LogoIcon />}
        logoText={!collapsed ? <HAI3LogoTextIcon /> : undefined}
        collapsed={collapsed}
        onClick={handleToggleCollapse}
      />
      <SidebarContent>
        <SidebarMenu>
          {items.map((item) => renderItem(item, 0))}
        </SidebarMenu>
      </SidebarContent>
      {children}
    </Sidebar>
  );
};

Menu.displayName = 'Menu';
