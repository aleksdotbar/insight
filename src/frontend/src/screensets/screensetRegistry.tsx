/**
 * Screenset Registry - Auto-Discovery
 *
 * Screensets are automatically discovered via Vite glob pattern matching.
 * No manual imports or registration needed!
 *
 * Convention: Screenset files MUST end with 'Screenset.tsx' or 'screenset.tsx'
 * Examples:
 *   - src/screensets/chat/chatScreenset.tsx  ✅
 *   - src/screensets/demo/DemoScreenset.tsx  ✅
 *   - src/screensets/chat/index.tsx  ❌ (won't be discovered)
 *
 * How it works:
 * 1. Vite processes glob pattern at build time (zero runtime overhead)
 * 2. Each matched file is imported eagerly
 * 3. Side effects in each file (screensetRegistry.register() calls) execute automatically
 * 4. All screensets registered before app renders
 *
 * To add a new screenset:
 * 1. Create folder: src/screensets/my-screenset/
 * 2. Create file: src/screensets/my-screenset/myScreenset.tsx
 * 3. Export screenset config and call screensetRegistry.register()
 * 4. Done! Auto-discovered and registered.
 *
 * To disable a screenset:
 * - Rename to .disabled.screenset.tsx, OR
 * - Move outside src/screensets/ directory
 */

/**
 * Auto-discover all screensets
 * Pattern: ./*Screenset.tsx matches chatScreenset.tsx, demoScreenset.tsx, etc.
 *
 * Eager loading ensures all side effects (registration) run immediately
 */
const screensetModules = import.meta.glob('./*/*[Ss]creenset.tsx', { eager: true });

// Log discovered screensets in development
if (import.meta.env.DEV) {
  console.log(`📦 Auto-discovered ${Object.keys(screensetModules).length} screenset(s):`,
    Object.keys(screensetModules).map(path => path.replace('./', '').replace(/\/.*Screenset\.tsx$/, '')));
}

/**
 * No manual imports or registration needed!
 * Each screenset file calls screensetRegistry.register() as a side effect.
 *
 * Project-level registry that only knows about screensets, not individual screens.
 * Each screenset is self-contained and manages its own screens internally.
 *
 * All IDs (screenset, screen, icon) are well-known constants defined where they belong:
 * - Screenset ID: in ids.ts (e.g., CHAT_SCREENSET_ID in chat/ids.ts)
 * - Screen IDs: in ids.ts or screenIds.ts
 * - Icon IDs: in icon files (e.g., MESSAGE_SQUARE_ICON_ID in MessageSquareIcon.tsx)
 *
 * This pattern prevents circular imports and follows vertical slice architecture.
 */
