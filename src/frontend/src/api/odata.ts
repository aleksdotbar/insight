import type { DateRange } from "@/api/types";

export function odataEscapeValue(value: string): string {
  return value.replace(/'/g, "''");
}

export function odataDateFilter(range: DateRange): string {
  return `metric_date ge '${range.from}' and metric_date le '${range.to}'`;
}

export function personScopeFilter(personId: string): string {
  return `person_id eq '${odataEscapeValue(personId.toLowerCase())}'`;
}

export function andFilters(...parts: Array<string | undefined>): string {
  return parts.filter((p): p is string => Boolean(p)).join(" and ");
}
