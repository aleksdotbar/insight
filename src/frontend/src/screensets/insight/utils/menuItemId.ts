/**
 * MenuItem.id codec — the insight screenset encodes per-item context as
 * `${screenId}::${param}` so the framework's menu tree (which accepts only
 * string ids) can still carry a disambiguator (an email, an org_unit name).
 *
 * Keep every split/join of this format in one place so callers never parse
 * the id inline.
 */

const SEPARATOR = '::';

export function encodeMenuItemId(screenId: string, param?: string): string {
  return param && param.length > 0 ? `${screenId}${SEPARATOR}${param}` : screenId;
}

export function decodeMenuItemId(id: string): { screenId: string; param?: string } {
  const idx = id.indexOf(SEPARATOR);
  if (idx < 0) return { screenId: id };
  return {
    screenId: id.slice(0, idx),
    param: id.slice(idx + SEPARATOR.length),
  };
}
