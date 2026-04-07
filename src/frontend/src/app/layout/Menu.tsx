/**
 * Menu Component
 *
 * Side navigation menu displaying screenset menu items.
 * Uses @hai3/uikit Sidebar components for proper styling and collapsible behavior.
 */

import React, { useState } from 'react';
import { useAppSelector, useNavigation, useTranslation, eventBus, type MenuState, type MenuItem } from '@hai3/react';
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

export interface MenuProps {
  children?: React.ReactNode;
}

export const Menu: React.FC<MenuProps> = ({ children }) => {
  const menuState = useAppSelector((state) => state['layout/menu'] as MenuState | undefined);
  const activeParam = useAppSelector((state) => {
    const s = state as Record<string, unknown>;
    const icSlice = s['insight/icDashboard'] as { selectedPersonId?: string } | undefined;
    return icSlice?.selectedPersonId ?? null;
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
    if (itemId.includes('::')) {
      const [screenId, param] = itemId.split('::');
      eventBus.emit('layout/menu/itemParam', { screenId, param });
      navigateToScreen(currentScreenset ?? '', screenId);
    } else {
      navigateToScreen(currentScreenset ?? '', itemId);
    }
  };

  const toggleGroup = (id: string) =>
    setExpandedGroups((prev) => ({ ...prev, [id]: !prev[id] }));

  /** Returns true if any descendant (at any depth) is "active" */
  const hasActiveDescendant = (item: MenuItem): boolean => {
    if (!item.children?.length) return false;
    return item.children.some((child) => {
      const [screenId, param] = child.id.split('::');
      const isLeaf = !child.children?.length;
      if (isLeaf && param) return currentScreen === screenId && activeParam === param;
      if (isLeaf) return currentScreen === child.id;
      return hasActiveDescendant(child);
    });
  };

  /** Recursive item renderer */
  const renderItem = (item: MenuItem, depth = 0): React.ReactNode => {
    const hasChildren = (item.children?.length ?? 0) > 0;
    const isExpanded = expandedGroups[item.id] ?? true;
    const [screenId, param] = item.id.split('::');
    const isSelfActive = param
      ? currentScreen === screenId && activeParam === param
      : currentScreen === item.id;
    const isActive = isSelfActive || hasActiveDescendant(item);
    const indent = depth > 0 ? `pl-${4 + depth * 4}` : '';

    return (
      <SidebarMenuItem key={item.id}>
        <SidebarMenuButton
          isActive={isSelfActive}
          onClick={() => hasChildren ? toggleGroup(item.id) : handleItemClick(item.id)}
          tooltip={collapsed && depth === 0 ? t(item.label) : undefined}
          className={`${indent} ${depth > 0 ? 'text-[12px]' : ''} ${isActive && !isSelfActive ? 'text-sidebar-foreground/80' : ''}`}
        >
          {item.icon && (
            <SidebarMenuIcon>
              <Icon icon={item.icon} className={depth > 0 ? 'w-3 h-3' : 'w-4 h-4'} />
            </SidebarMenuIcon>
          )}
          <span>{t(item.label)}</span>
          {hasChildren && !collapsed && (
            <span className="ml-auto text-[10px] opacity-40">
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
    <Sidebar collapsed={collapsed}>
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
