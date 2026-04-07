/**
 * PersonHeader — displays person identity (avatar, name, role/seniority).
 * No state imports.
 */

import React from 'react';
import type { PersonData } from '../../../types';

export interface PersonHeaderProps {
  person: PersonData | null;
  /** When true, renders without outer card wrapper (for embedding in a header row) */
  inline?: boolean;
}

function getInitials(name: string): string {
  return name
    .split(' ')
    .filter(Boolean)
    .slice(0, 2)
    .map((word) => word[0].toUpperCase())
    .join('');
}

const PersonHeader: React.FC<PersonHeaderProps> = ({ person, inline = false }) => {
  if (!person) return null;

  const content = (
    <div className="flex items-center gap-3">
      <div className="w-8 h-8 bg-blue-50 rounded-full flex items-center justify-center flex-shrink-0">
        <span className="text-[13px] font-extrabold text-blue-600">
          {getInitials(person.name)}
        </span>
      </div>
      <div>
        <div className="text-[15px] font-bold text-gray-900 leading-tight">{person.name}</div>
        <div className="text-[10px] text-gray-400">
          {person.role} · {person.seniority}
        </div>
      </div>
    </div>
  );

  if (inline) return content;

  return (
    <div className="bg-white border border-gray-200 rounded-xl shadow-sm p-3">
      {content}
    </div>
  );
};

export default React.memo(PersonHeader);
