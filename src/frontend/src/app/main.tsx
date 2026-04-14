/// <reference types="vite/client" />
import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { HAI3Provider, apiRegistry, createHAI3App, registerSlice, type ThemeApplyFn } from '@hai3/react';
import { Toaster, applyTheme } from '@hai3/uikit';
// Side-effect imports — services self-register with apiRegistry
import '@/app/api/AccountsApiService';
import '@/app/api/AuthApiService';
import '@/app/api/IdentityApiService';
import '@hai3/uikit/styles'; // UI Kit styles
import '@/screensets/screensetRegistry'; // Auto-registers screensets (includes API services + mocks + i18n loaders)
import '@/app/events/bootstrapEvents'; // Register app-level events (type augmentation)
import { registerBootstrapEffects } from '@/app/effects/bootstrapEffects'; // Register app-level effects
import authSlice from '@/app/slices/authSlice';
import { initAuthEffects } from '@/app/effects/authEffects';
import App from './App';

// Import all themes
import { DEFAULT_THEME_ID, defaultTheme } from '@/app/themes/default';
import { DARK_THEME_ID, darkTheme } from '@/app/themes/dark';
import { LIGHT_THEME_ID, lightTheme } from '@/app/themes/light';
import { DRACULA_THEME_ID, draculaTheme } from '@/app/themes/dracula';
import { DRACULA_LARGE_THEME_ID, draculaLargeTheme } from '@/app/themes/dracula-large';

// Capture the initial URL before any router modifies it
import { storeStartUrl } from '@/app/auth/startUrl';
storeStartUrl();

// Initialize API services (services self-register via module side-effects above)
apiRegistry.initialize({});

// Create HAI3 app instance with theme apply function (constructor injection)
const app = createHAI3App({
  themes: { applyFn: applyTheme as ThemeApplyFn },
});

// Register app-level slices and effects
registerSlice(authSlice, initAuthEffects);
registerBootstrapEffects(app.store.dispatch);

// Register all themes (default theme first, becomes the default selection)
app.themeRegistry.register(DEFAULT_THEME_ID, defaultTheme);
app.themeRegistry.register(LIGHT_THEME_ID, lightTheme);
app.themeRegistry.register(DARK_THEME_ID, darkTheme);
app.themeRegistry.register(DRACULA_THEME_ID, draculaTheme);
app.themeRegistry.register(DRACULA_LARGE_THEME_ID, draculaLargeTheme);

// Apply default theme
app.themeRegistry.apply(DEFAULT_THEME_ID);

/**
 * Render application
 * Auth flow:
 * - Layout mounts → initAuth() action → restore session or redirect to Okta
 * - /callback screen → handleOidcCallback() action → exchange code for tokens
 * - authEffects listen → dispatch to authSlice → components react
 *
 * Note: Mock API is controlled via the HAI3 Studio panel.
 */
createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <HAI3Provider app={app}>
      <App />
      <Toaster />
    </HAI3Provider>
  </StrictMode>
);
