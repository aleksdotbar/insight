---
cypilot: true
type: project-rule
topic: patterns
generated-by: auto-config
version: 1.0
---
# Patterns

<!-- toc -->

- [State Management](#state-management)
  - [Slice Pattern](#slice-pattern)
  - [Selector Pattern](#selector-pattern)
  - [Slice Registration](#slice-registration)
- [Event-Driven Flow](#event-driven-flow)
  - [Action → Event → Effect → Slice](#action--event--effect--slice)
  - [Event Declaration](#event-declaration)
  - [Effects Initialization](#effects-initialization)
- [API Services](#api-services)
  - [Service Pattern](#service-pattern)
  - [Service Registration](#service-registration)
  - [Mock Pattern](#mock-pattern)
- [Plugins](#plugins)
  - [REST Plugin Pattern](#rest-plugin-pattern)
- [Hooks](#hooks)
  - [Custom Hook Pattern](#custom-hook-pattern)

<!-- /toc -->

## State Management

### Slice Pattern
Use `createSlice()` with: SLICE_KEY constant, typed State interface, explicit initialState, reducers object. Export destructured actions and default slice. Add `RootState` module augmentation.
Evidence: `src/app/slices/authSlice.ts:10-47`

### Selector Pattern
Export selector functions from slice file with default fallback values for safety.
Evidence: `src/app/slices/authSlice.ts:49-56`

### Slice Registration
Register via `registerSlice(slice, initEffectsFn)` — ties slice to its effects initializer.
Evidence: `src/app/main.tsx:13`, `src/screensets/insight/insightScreenset.tsx:86-103`

## Event-Driven Flow

### Action → Event → Effect → Slice
Actions emit events via eventBus. Effects listen to events and dispatch to slices. Slices are pure reducers. No direct slice dispatch from components.
Evidence: `src/screensets/insight/actions/executiveViewActions.ts:15-22`, `src/screensets/insight/effects/executiveViewEffects.ts:15-29`

### Event Declaration
Declare events as const object with `${SCOPE}/${DOMAIN}/eventName` keys. Augment `EventPayloadMap` for type-safe payloads.
Evidence: `src/screensets/insight/events/executiveViewEvents.ts:10-29`

### Effects Initialization
Export `initialize{Domain}Effects(dispatch: AppDispatch)` function. Subscribe to events via `eventBus.on()`, dispatch reducers inside callbacks.
Evidence: `src/screensets/insight/effects/executiveViewEffects.ts:15-29`

## API Services

### Service Pattern
Extend `BaseApiService`. Constructor: pass baseURL + `new RestProtocol()`. Register `RestMockPlugin` with mockMap. Add `AuthPlugin` to protocol.
Evidence: `src/screensets/insight/api/insightApiService.ts:15-31`

### Service Registration
App-level services: register in `main.tsx` via `apiRegistry.register()`. Screenset services: register during screenset initialization.
Evidence: `src/app/main.tsx:25-30`

### Mock Pattern
Mock maps keyed by `'METHOD /path'` returning response objects. Controlled via HAI3 Studio panel.
Evidence: `src/screensets/insight/api/insightApiService.ts:22-28`

## Plugins

### REST Plugin Pattern
Extend `RestPlugin`. Override `onRequest()` for request modification, `onError()` for error handling. Add to `restProtocol.plugins.add()`.
Evidence: `src/app/plugins/AuthPlugin.ts:18-39`

## Hooks

### Custom Hook Pattern
Thin wrappers around selectors or composed state. Place in `hooks/` directory within screenset.
Evidence: `src/screensets/insight/hooks/usePeriod.ts:10`
