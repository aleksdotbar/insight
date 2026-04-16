/**
 * PersonHeader — displays person identity (avatar, name, role/seniority).
 * Uses @hai3/uikit Avatar with initials fallback.
 * No state imports.
 */

import React from 'react';
import { Avatar, AvatarFallback, Card, CardContent } from '@hai3/uikit';
import { getInitials } from '../../../utils/getInitials';
import type { IdentityPerson } from '@/app/types/identity';

export interface PersonHeaderProps {
  person: IdentityPerson | null;
  /** When true, renders without outer card wrapper (for embedding in a header row) */
  inline?: boolean;
}

const PersonHeader: React.FC<PersonHeaderProps> = ({ person, inline = false }) => {
  if (!person) return null;

  const content = (
    <div className="flex items-center gap-3">
      <Avatar className="w-8 h-8 flex-shrink-0">
        <AvatarFallback className="bg-blue-50 text-blue-600 text-sm font-extrabold">
          {getInitials(person.name)}
        </AvatarFallback>
      </Avatar>
      <div>
        <div className="text-base font-bold text-gray-900 leading-tight">{person.name}</div>
        <div className="text-xs text-gray-400">
          {person.role} · {person.seniority}
        </div>
      </div>
    </div>
  );

  if (inline) return content;

  return (
    <Card>
      <CardContent className="p-3">
        {content}
      </CardContent>
    </Card>
  );
};

export default React.memo(PersonHeader);
