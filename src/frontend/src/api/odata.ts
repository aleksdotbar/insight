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

export function orgScopeFilter(orgUnitId: string): string {
  return `org_unit_id eq '${odataEscapeValue(orgUnitId)}'`;
}

export function andFilters(...parts: Array<string | undefined>): string {
  return parts.filter((p): p is string => Boolean(p)).join(" and ");
}

const EQ_VALUE_RE = /([a-zA-Z_][a-zA-Z0-9_]*)\s+(eq|ge|le|lt|gt)\s+'([^']*)'/g;

export type ParsedFilter = {
  dateFrom: string | null;
  dateTo: string | null;
  personId: string | null;
  orgUnitId: string | null;
  equals: Record<string, string>;
};

export function parseODataFilter(filter: string | undefined): ParsedFilter {
  const result: ParsedFilter = {
    dateFrom: null,
    dateTo: null,
    personId: null,
    orgUnitId: null,
    equals: {},
  };
  if (!filter) return result;

  for (const m of filter.matchAll(EQ_VALUE_RE)) {
    const [, field, op, value] = m;
    if (field === "metric_date" && op === "ge") result.dateFrom = value;
    else if (field === "metric_date" && (op === "le" || op === "lt")) result.dateTo = value;
    else if (field === "person_id" && op === "eq") result.personId = value;
    else if (field === "org_unit_id" && op === "eq") result.orgUnitId = value;
    else if (op === "eq") result.equals[field] = value;
  }
  return result;
}
