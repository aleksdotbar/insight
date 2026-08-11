# Connector Specifications

> Version 1.1 — March 2026

Per-source deep-dive specifications for Constructor Insight connectors. Each file expands on the corresponding source in [`../../../inbox/CONNECTORS_REFERENCE.md`](../../../inbox/CONNECTORS_REFERENCE.md) with full table schemas, identity mapping, Silver/Gold pipeline notes, and open questions.

<!-- toc -->

- [Index](#index)
  - [Version Control](#version-control)
  - [Task Tracking](#task-tracking)
  - [Collaboration](#collaboration)
  - [Wiki / Knowledge Base](#wiki--knowledge-base)
  - [Support / Helpdesk](#support--helpdesk)
  - [AI Dev Tools](#ai-dev-tools)
  - [AI Tools](#ai-tools)
  - [HR / Directory](#hr--directory)
  - [CRM](#crm)
  - [Design Tools](#design-tools)
  - [Quality / Testing](#quality--testing)
- [Unified Streams](#unified-streams)
- [How to Use](#how-to-use)

<!-- /toc -->

---

## Index

### Version Control

| Source | Spec | Status |
|--------|------|--------|
| Git (unified schema) | [`git/README.md`](git/README.md) | Draft |
| GitHub | [`git/github/specs/DESIGN.md`](git/github/specs/DESIGN.md) | Draft |
| Bitbucket | [`git/bitbucket-server/README.md`](git/bitbucket-server/README.md) | Draft |
| GitLab | [`git/gitlab/specs/DESIGN.md`](git/gitlab/specs/DESIGN.md) | Draft |

### Task Tracking

| Source | Spec | Status |
|--------|------|--------|
| Task Tracking (unified schema) | [`task-tracking/README.md`](task-tracking/README.md) | Draft |
| YouTrack | [`task-tracking/youtrack/youtrack.md`](task-tracking/youtrack/youtrack.md) | Proposed |
| Jira | [`task-tracking/jira/jira.md`](task-tracking/jira/jira.md) | Proposed |

### Collaboration

| Source | Spec | Status |
|--------|------|--------|
| Collaboration (unified schema) | [`collaboration/README.md`](collaboration/README.md) | Draft |
| Microsoft 365 | [`collaboration/m365/README.md`](collaboration/m365/README.md) | Proposed |
| Zulip | [`collaboration/zulip/zulip.md`](collaboration/zulip/zulip.md) | Proposed |
| Slack | [`collaboration/slack/slack.md`](collaboration/slack/slack.md) | Draft |
| Zoom | [`collaboration/zoom/zoom.md`](collaboration/zoom/zoom.md) | Draft |

### Wiki / Knowledge Base

| Source | Spec | Status |
|--------|------|--------|
| Wiki (unified schema) | [`wiki/README.md`](wiki/README.md) | Draft |
| Confluence | [`wiki/confluence/confluence.md`](wiki/confluence/confluence.md) | Draft |
| Outline | [`wiki/outline/outline.md`](wiki/outline/outline.md) | Draft |

### Support / Helpdesk

| Source | Spec | Status |
|--------|------|--------|
| Support (unified schema) | [`support/README.md`](support/README.md) | Draft |
| Zendesk | [`support/zendesk/zendesk.md`](support/zendesk/zendesk.md) | Draft |
| Jira Service Management | [`support/jsm/jsm.md`](support/jsm/jsm.md) | Draft |

### AI Dev Tools

| Source | Spec | Status |
|--------|------|--------|
| Cursor | [`ai/cursor/cursor.md`](ai/cursor/cursor.md) | Proposed |
| Windsurf | [`ai/windsurf/windsurf.md`](ai/windsurf/windsurf.md) | Proposed |
| GitHub Copilot | [`ai/github-copilot/github-copilot.md`](ai/github-copilot/github-copilot.md) | Proposed |
| JetBrains | [`ai/jetbrains/jetbrains.md`](ai/jetbrains/jetbrains.md) | Draft |

### AI Tools

| Source | Spec | Status |
|--------|------|--------|
| Claude Admin | [`ai/claude-admin/README.md`](ai/claude-admin/README.md) | Proposed |
| OpenAI API | [`ai/openai-api/specs/DESIGN.md`](ai/openai-api/specs/DESIGN.md) | Proposed |
| ChatGPT Team | [`ai/chatgpt-team/specs/DESIGN.md`](ai/chatgpt-team/specs/DESIGN.md) | Proposed |

### HR / Directory

| Source | Spec | Status |
|--------|------|--------|
| HR Directory (unified schema) | [`hr-directory/README.md`](hr-directory/README.md) | Draft |
| BambooHR | [`hr-directory/bamboohr/specs/DESIGN.md`](hr-directory/bamboohr/specs/DESIGN.md) | Proposed |
| Workday | [`hr-directory/workday/workday.md`](hr-directory/workday/workday.md) | Proposed |
| LDAP / Active Directory | [`hr-directory/ldap/ldap.md`](hr-directory/ldap/ldap.md) | Proposed |

### CRM

| Source | Spec | Status |
|--------|------|--------|
| CRM (unified schema) | [`crm/README.md`](crm/README.md) | Draft |
| HubSpot | [`crm/hubspot/hubspot.md`](crm/hubspot/hubspot.md) | Proposed |
| Salesforce | [`crm/salesforce/specs/DESIGN.md`](crm/salesforce/specs/DESIGN.md) | Proposed |

### Design Tools

| Source | Spec | Status |
|--------|------|--------|
| Design Tools (unified schema) | [`ui-design/README.md`](ui-design/README.md) | Draft |
| Figma | [`ui-design/figma/figma.md`](ui-design/figma/figma.md) | Draft |

### Quality / Testing

| Source | Spec | Status |
|--------|------|--------|
| Allure TestOps | [`allure.md`](testing/allure/allure.md) | Proposed |

---

## Unified Streams

| Stream | Sources | Spec |
|--------|---------|------|
| `class_communication_metrics` | M365 + Zulip + Slack + Zoom | [`collaboration/README.md`](collaboration/README.md) |
| `class_document_metrics` | M365 (OneDrive + SharePoint) | [`collaboration/README.md`](collaboration/README.md) — planned |
| `class_wiki_pages` | Confluence + Outline | [`wiki/README.md`](wiki/README.md) |
| `class_wiki_activity` | Confluence + Outline | [`wiki/README.md`](wiki/README.md) |
| `class_support_activity` | Zendesk + JSM | [`support/README.md`](support/README.md) |
| `class_design_activity` | Figma | [`ui-design/README.md`](ui-design/README.md) |
| Task Tracker unified schema | YouTrack + Jira | [`task-tracking/README.md`](task-tracking/README.md) |
| `class_people` + `class_org_units` | BambooHR + Workday + LDAP | [`hr-directory/README.md`](hr-directory/README.md) |
| `class_ai_dev_usage` | Cursor + Windsurf + Copilot + JetBrains + Claude Code | [`ai/`](ai/) |

---

## How to Use

- **Main reference** — [`../../../inbox/CONNECTORS_REFERENCE.md`](../../../inbox/CONNECTORS_REFERENCE.md) is the canonical index of all Bronze table schemas and the Bronze → Silver → Gold pipeline overview.
- **Per-source specs** (this directory) — expand on individual sources with additional detail: complete field lists, API notes, identity mapping, Silver channel mappings, and open questions.
- **Generate a new spec** — `/cf-generate Connector spec for {Source Name}`
