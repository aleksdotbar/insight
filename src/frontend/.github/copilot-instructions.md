# HAI3 Development Guidelines for GitHub Copilot

Always read `.ai/GUIDELINES.md` before making changes.

## Quick Reference

For detailed guidance, use these resources:
- **Architecture**: See `.ai/GUIDELINES.md` and target files in `.ai/targets/`
- **Event-driven patterns**: `.ai/targets/EVENTS.md`
- **Screensets**: `.ai/targets/SCREENSETS.md`
- **API services**: `.ai/targets/API.md`
- **Styling**: `.ai/targets/STYLING.md`
- **Themes**: `.ai/targets/THEMES.md`

## Critical Rules

1. **REQUIRED**: Read the appropriate target file before changing code
2. **REQUIRED**: Event-driven architecture only (dispatch events, handle in actions)
3. **FORBIDDEN**: Direct slice dispatch from UI components
4. **FORBIDDEN**: Hardcoded colors or inline styles
5. **REQUIRED**: Use `@hai3/uikit` components for all UI
6. **REQUIRED**: Run `npm run arch:check` before committing

## Available Commands

Use `.ai/commands/` for detailed workflows:
- `hai3-new-screenset` - Create new screenset
- `hai3-new-screen` - Add screen to screenset
- `hai3-new-action` - Create action handler
- `hai3-new-api-service` - Add API service
- `hai3-new-component` - Add UI component
- `hai3-validate` - Validate changes
- `hai3-quick-ref` - Quick reference guide

## Routing

Always consult `.ai/GUIDELINES.md` ROUTING section to find the correct target file for your task.
