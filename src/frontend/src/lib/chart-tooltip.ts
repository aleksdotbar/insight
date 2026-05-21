import type { CSSProperties } from "react";

export const tooltipContentStyle: CSSProperties = {
  fontSize: 12,
  background: "var(--popover)",
  color: "var(--popover-foreground)",
  border: "1px solid var(--border)",
  borderRadius: 6,
};

export const tooltipLabelStyle: CSSProperties = {
  color: "var(--popover-foreground)",
  fontWeight: 600,
};

export const tooltipItemStyle: CSSProperties = {
  color: "var(--popover-foreground)",
};
