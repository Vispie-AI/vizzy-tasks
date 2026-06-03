# Lark → Task ingress (Vizzy fork)

This fork adds a native endpoint that turns an **@mention of the bot in a Lark
thread** into a Multica issue assigned to a triage agent. Reply to a thread with
`@TaskBot ...` → the whole thread (root message + all replies + image/file
attachments) is imported as one issue, assigned to a configured agent that
triages it (writes a ticket / implementation plan / follow-up questions — it
does **not** start the work; that behaviour is controlled by the agent's own
instructions in Multica). The bot replies in-thread with the created task id.

Everything runs **in-process** in the Multica Go backend — no sidecar, no PAT.

## Pieces added

| File | What it does |
|------|--------------|
| `server/internal/lark/client.go` | Minimal Lark Open Platform client (tenant token, get message, list thread, download resource). Stdlib-only, no new deps. |
| `server/internal/handler/lark_webhook.go` | `HandleLarkWebhook` — verifies the Lark token, pulls the thread, re-uploads attachments to Multica storage, creates the issue assigned to the triage agent (auto-enqueues it). |
| `server/internal/handler/handler.go` | `LarkConfig` on `Config`, `larkAPI()` accessor on `Handler`. |
| `server/cmd/server/router.go` | Registers `POST /api/webhooks/lark` (public) + reads `MULTICA_LARK_*` env. |

The endpoint is **disabled (404)** until all required env vars are set.

## Configuration

Set these on the Multica backend (same place as your other `MULTICA_*` vars):

```bash
# Lark custom app (dedicated task-tracking app — see below)
MULTICA_LARK_APP_ID=cli_xxxxxxxxxxxxxxxx
MULTICA_LARK_APP_SECRET=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
MULTICA_LARK_VERIFICATION_TOKEN=xxxxxxxxxxxxxxxxxxxxxx
# larksuite.com (global) or feishu.cn (Feishu); default is larksuite.com
MULTICA_LARK_BASE_URL=https://open.larksuite.com

# Where imported issues land + who handles them
MULTICA_LARK_WORKSPACE_ID=<workspace uuid>
MULTICA_LARK_AGENT_ID=<triage agent uuid>
# A real workspace member recorded as the issue creator (webhooks have no
# logged-in user). Use a dedicated "system" / "lark-bot" member account.
MULTICA_LARK_CREATOR_USER_ID=<member user uuid>

# Optional. The bot's own open_id. When set, only @mentions of THIS id trigger
# an import; when empty, any @mention of the app does. Leave empty if the bot
# is the only app likely to be @mentioned in your chats.
MULTICA_LARK_BOT_OPEN_ID=

# Optional. none|low|medium|high|urgent (default medium)
MULTICA_LARK_PRIORITY=medium
```

Get the UUIDs from the CLI: `multica workspace list --output json`,
`multica agent list --output json`, `multica workspace member list --output json`.

> Encryption: this handler expects **plain-JSON** events. Leave the Lark app's
> *Encrypt Key* blank.

## Lark app setup (one-time)

1. **Create a custom app** in the [Lark Developer Console](https://open.larksuite.com/app)
   → "Custom App". Copy its **App ID** and **App Secret** into the env vars.
2. **Enable the bot** capability (Features → Bot).
3. **Permissions (scopes)** — add:
   - `im:message` *(or)* `im:message:readonly` — read messages
   - `im:message.history:readonly` — read full thread replies
   - `im:resource` — download images/files
   - `im:message:send_as_bot` — post the "Created task …" reply back in-thread
   Then **add the bot to the chat(s)** you'll use it in — the bot can only
   read/reply in groups it belongs to.
4. **Event subscription**: set the app's request URL to
   `https://tasks.vizzylabs.ai/api/webhooks/lark`. Lark sends a
   `url_verification` challenge — the handler answers it automatically once
   `MULTICA_LARK_VERIFICATION_TOKEN` matches the **Verification Token** in the
   console. Leave **Encrypt Key blank**.
5. **Subscribe to the event** `im.message.receive_v1` (Receive message). This
   is what fires when someone @mentions the bot.
6. **Publish / release** a version of the app and (for internal use) approve it.

## How it behaves

- A user @mentions the bot in a message (typically a reply in a thread):
  `@TaskBot make this a ticket`. Lark delivers an `im.message.receive_v1`
  event.
- The handler verifies the bot was @mentioned (and the sender isn't a bot),
  finds the message's `thread_id`, and pulls every message in the thread
  (oldest-first, capped at 200).
- Text (`text` / rich-text `post`) is rendered into the issue description;
  images and files are downloaded from Lark and re-uploaded as real Multica
  attachments linked to the issue.
- The issue is created with status `todo` and assigned to the triage agent, so
  Multica enqueues the agent immediately.
- The bot replies in the thread: `✅ Created task ABC-123` (or
  `ℹ️ Task already exists: ABC-123` on an active-duplicate title).

## Keeping up with upstream

```bash
git fetch upstream
git merge upstream/main      # our changes are additive (new files + a couple
                             # of small insertions) to minimise conflicts
```

Conflict-prone spots are limited to the two insertion points in
`server/cmd/server/router.go` and `server/internal/handler/handler.go`.
