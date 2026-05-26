import { Avatar, AvatarFallback } from "@/components/ui/avatar";
import { Card, CardContent } from "@/components/ui/card";
import { getInitials } from "@/lib/insight/get-initials";
import type { IdentityPerson, PersonData } from "@/types/insight";

export interface PersonHeaderProps {
  person: PersonData | IdentityPerson | null;
  /** Rendered when `person` is null — keeps the avatar/name slot from collapsing. */
  fallbackEmail?: string;
  /** When true, renders without outer card wrapper (for embedding in a header row). */
  inline?: boolean;
}

function personDisplayName(p: PersonData | IdentityPerson): string {
  if ("display_name" in p && p.display_name) return p.display_name;
  if ("name" in p && p.name) return p.name;
  return "";
}

function personSubtitle(p: PersonData | IdentityPerson): string {
  if ("role" in p) {
    const seniority = "seniority" in p && p.seniority ? p.seniority : "";
    return seniority ? `${p.role} · ${seniority}` : p.role;
  }
  const jobTitle = "job_title" in p && p.job_title ? p.job_title : "";
  const department = "department" in p && p.department ? p.department : "";
  return [jobTitle, department].filter(Boolean).join(" · ");
}

export function PersonHeader({
  person,
  fallbackEmail,
  inline = false,
}: PersonHeaderProps) {
  if (!person && !fallbackEmail) return null;
  const name = person ? personDisplayName(person) : (fallbackEmail ?? "");
  const subtitle = person ? personSubtitle(person) : "";

  const content = (
    <div className="flex min-w-0 items-center gap-3">
      <Avatar className="size-10 shrink-0">
        <AvatarFallback className="bg-primary/10 text-primary text-base font-extrabold">
          {getInitials(name)}
        </AvatarFallback>
      </Avatar>
      <div className="min-w-0">
        <div className="text-foreground truncate text-lg leading-tight font-bold">
          {name}
        </div>
        {subtitle ? (
          <div className="text-muted-foreground truncate text-sm">
            {subtitle}
          </div>
        ) : null}
      </div>
    </div>
  );

  if (inline) return content;

  return (
    <Card>
      <CardContent className="p-3">{content}</CardContent>
    </Card>
  );
}
