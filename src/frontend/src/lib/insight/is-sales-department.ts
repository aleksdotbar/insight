export function isSalesDepartment(dept?: string | null): boolean {
  return /\bsales\b/i.test(dept ?? "");
}
