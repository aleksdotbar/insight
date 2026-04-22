/**
 * Persistent visual warning that the app is running with mock API plugins
 * registered. Renders a non-dismissible yellow strip fixed to the top of
 * the viewport whenever VITE_ENABLE_MOCKS=true — makes it impossible to
 * forget that the numbers on screen are synthetic.
 *
 * Tree-shaken out of prod bundles: mocksEnabled() is a pure
 * `import.meta.env.*` check, so the banner + its DOM never ship when
 * mocks are disabled.
 */

import React from 'react';
import { mocksEnabled } from '@/app/config/mocksEnabled';

const MockBanner: React.FC = () => {
  if (!mocksEnabled()) return null;

  return (
    <div
      role="alert"
      className="fixed top-0 left-0 right-0 z-[9999] px-3 py-1.5 bg-amber-300 text-amber-900 text-xs font-bold text-center tracking-wider border-b-2 border-amber-700 pointer-events-none"
    >
      ⚠ MOCK DATA — values on this screen are synthetic, not from the real backend
    </div>
  );
};

export default MockBanner;
