import { Filter, HelpCircle, Sparkles } from "lucide-react";
import { useTranslation } from "react-i18next";

import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import {
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
} from "@/components/ui/sidebar";
import { Switch } from "@/components/ui/switch";
import { useSettings } from "@/hooks/use-settings";
import {
  setMetricsV2Enabled,
  useMetricsV2Enabled,
} from "@/lib/feature-flags";
import type { FocusMode } from "@/lib/peers";

const FOCUS_MODES: ReadonlyArray<FocusMode> = [
  "all",
  "rewards",
  "critical",
  "neutral",
];

export function SidebarV2Settings() {
  const { t } = useTranslation();
  const v2 = useMetricsV2Enabled();
  const { focusMode, showExplanations, setFocusMode, setShowExplanations } =
    useSettings();

  return (
    <SidebarMenu>
      {v2 ? (
        <>
          <SidebarMenuItem>
            <DropdownMenu>
              <DropdownMenuTrigger
                render={
                  <SidebarMenuButton aria-label={t("settings.focus_mode.label")}>
                    <Filter className="size-4" />
                    <span>{t(`settings.focus_mode.${focusMode}`)}</span>
                  </SidebarMenuButton>
                }
              />
              <DropdownMenuContent side="top" align="start">
                {FOCUS_MODES.map((mode) => (
                  <DropdownMenuCheckboxItem
                    key={mode}
                    checked={focusMode === mode}
                    onCheckedChange={() => setFocusMode(mode)}
                  >
                    {t(`settings.focus_mode.${mode}`)}
                  </DropdownMenuCheckboxItem>
                ))}
              </DropdownMenuContent>
            </DropdownMenu>
          </SidebarMenuItem>
          <SidebarMenuItem>
            <SidebarMenuButton
              onClick={() => setShowExplanations(!showExplanations)}
              aria-pressed={showExplanations}
              className="justify-between"
            >
              <span className="flex items-center gap-2">
                <HelpCircle className="size-4" />
                <span>{t("settings.explanations.label")}</span>
              </span>
              <Switch
                checked={showExplanations}
                onCheckedChange={setShowExplanations}
                size="sm"
                tabIndex={-1}
              />
            </SidebarMenuButton>
          </SidebarMenuItem>
        </>
      ) : null}
      <SidebarMenuItem>
        <SidebarMenuButton
          onClick={() => setMetricsV2Enabled(!v2)}
          aria-pressed={v2}
          className="justify-between"
        >
          <span className="flex items-center gap-2">
            <Sparkles className="size-4" />
            <span>{t("settings.metrics_v2.label")}</span>
          </span>
          <Switch
            checked={v2}
            onCheckedChange={setMetricsV2Enabled}
            size="sm"
            tabIndex={-1}
          />
        </SidebarMenuButton>
      </SidebarMenuItem>
    </SidebarMenu>
  );
}
