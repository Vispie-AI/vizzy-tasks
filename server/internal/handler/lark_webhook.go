package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/issueguard"
	"github.com/multica-ai/multica/server/internal/issueposition"
	"github.com/multica-ai/multica/server/internal/lark"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// maxLarkThreadMessages caps how many messages we pull from a thread. A Lark
// topic can in theory grow without bound; the cap keeps a runaway thread from
// blowing up the triage agent's context window (and our memory). 200 messages
// is far beyond any realistic task-tracking thread.
const maxLarkThreadMessages = 200

// larkEventDedupeTTL is how long a processed Lark event_id is remembered so
// Lark's at-least-once retries (it resends if it doesn't see a fast 200) don't
// create duplicate issues / duplicate replies.
const larkEventDedupeTTL = 10 * time.Minute

// larkDedupe is a tiny in-memory TTL set of Lark event_ids we've already
// accepted. Single-node by design (matches the rest of the self-host stack);
// a brief double-fire across a restart is harmless because the issue
// duplicate-guard still catches it.
type larkDedupe struct {
	mu   sync.Mutex
	seen map[string]time.Time
}

func newLarkDedupe() *larkDedupe { return &larkDedupe{seen: map[string]time.Time{}} }

// seenBefore returns true if id was already accepted within the TTL; otherwise
// it records id and returns false. Also opportunistically evicts expired ids.
func (d *larkDedupe) seenBefore(id string) bool {
	if id == "" {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	now := time.Now()
	if ts, ok := d.seen[id]; ok && now.Sub(ts) < larkEventDedupeTTL {
		return true
	}
	for k, ts := range d.seen {
		if now.Sub(ts) >= larkEventDedupeTTL {
			delete(d.seen, k)
		}
	}
	d.seen[id] = now
	return false
}

// larkChallenge is the URL-verification handshake body Lark POSTs when you
// first set the request URL in the developer console.
type larkChallenge struct {
	Challenge string `json:"challenge"`
	Token     string `json:"token"`
	Type      string `json:"type"`
}

// larkCallback is the (plain-JSON) callback envelope. We support the v2 event
// schema (used by im.message.receive_v1), where the verification token lives
// under `header.token` and the typed event payload under `event`. The legacy
// v1 top-level `token` is also accepted for the url_verification handshake.
type larkCallback struct {
	// v1 fields (url_verification handshake)
	Token string `json:"token"`
	Type  string `json:"type"`
	// v2 fields
	Header struct {
		Token     string `json:"token"`
		EventID   string `json:"event_id"`
		EventType string `json:"event_type"`
	} `json:"header"`
	Event larkEvent `json:"event"`
}

// larkEvent is the slice of an im.message.receive_v1 event we consume. The bot
// receives this when it is @mentioned in (or DM'd) a chat it belongs to.
type larkEvent struct {
	Sender struct {
		SenderID struct {
			OpenID string `json:"open_id"`
		} `json:"sender_id"`
		SenderType string `json:"sender_type"`
	} `json:"sender"`
	Message struct {
		MessageID   string `json:"message_id"`
		RootID      string `json:"root_id"`
		ParentID    string `json:"parent_id"`
		ThreadID    string `json:"thread_id"`
		ChatID      string `json:"chat_id"`
		ChatType    string `json:"chat_type"`
		MessageType string `json:"message_type"`
		Content     string `json:"content"`
		Mentions    []struct {
			Key string `json:"key"`
			ID  struct {
				OpenID string `json:"open_id"`
			} `json:"id"`
			Name string `json:"name"`
		} `json:"mentions"`
	} `json:"message"`
}

// HandleLarkWebhook is the public ingress for the Lark "@mention the bot in a
// thread → create an issue" flow. It runs OUTSIDE the authenticated route
// group: the Lark verification token in the event body IS the credential.
//
// Trigger: a user @mentions the bot in a message (typically a reply inside a
// thread). Lark delivers an im.message.receive_v1 event; we pull the whole
// thread (root + replies + attachments) into one issue assigned to the triage
// agent, which Multica auto-enqueues.
//
// Flow:
//  1. Per-IP rate limit (reuse the autopilot webhook limiter).
//  2. Read + cap the body.
//  3. url_verification handshake → echo the challenge.
//  4. Verify the Lark verification token (header.token, v2 schema).
//  5. Only act on im.message.receive_v1 where the bot was @mentioned (and the
//     sender is not the bot itself). Resolve the thread and pull all replies.
//  6. Re-upload each image/file resource into Multica storage.
//  7. Create an issue (same tx as CreateIssue) assigned to the triage agent.
//
// We ACK every event with 200 quickly (Lark retries non-2xx and dedupes by
// event_id). Errors are logged, and a best-effort reply is posted back into
// the thread so the user gets feedback.
func (h *Handler) HandleLarkWebhook(w http.ResponseWriter, r *http.Request) {
	if !h.cfg.Lark.Enabled() {
		writeError(w, http.StatusNotFound, "lark webhook not configured")
		return
	}

	// 1. Per-IP rate limit before any work.
	if h.WebhookIPRateLimiter != nil {
		if ip := h.clientIPForRateLimit(r); ip != "" {
			if !h.WebhookIPRateLimiter.Allow(r.Context(), ip) {
				writeError(w, http.StatusTooManyRequests, "rate limit exceeded")
				return
			}
		}
	}

	// 2. Body cap.
	r.Body = http.MaxBytesReader(w, r.Body, maxWebhookBodyBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "failed to read request body")
		return
	}
	body = stripBOM(body)

	// 3. URL verification handshake.
	var probe larkChallenge
	if err := json.Unmarshal(body, &probe); err == nil && probe.Type == "url_verification" {
		if probe.Token != h.cfg.Lark.VerificationToken {
			writeError(w, http.StatusUnauthorized, "invalid token")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"challenge": probe.Challenge})
		return
	}

	// 4. Parse + authenticate (v2 header token, fall back to v1 top-level).
	var cb larkCallback
	if err := json.Unmarshal(body, &cb); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}
	token := cb.Header.Token
	if token == "" {
		token = cb.Token
	}
	if token != h.cfg.Lark.VerificationToken {
		writeError(w, http.StatusUnauthorized, "invalid token")
		return
	}

	// 5. Only handle inbound message events where the bot was @mentioned.
	//    Anything else (other event types, non-mention messages, the bot's
	//    own messages) is ACKed and ignored so Lark stops retrying.
	if cb.Header.EventType != "" && cb.Header.EventType != "im.message.receive_v1" {
		writeJSON(w, http.StatusOK, map[string]string{"msg": "ignored"})
		return
	}
	msg := cb.Event.Message
	if msg.MessageID == "" {
		writeJSON(w, http.StatusOK, map[string]string{"msg": "no message"})
		return
	}
	if cb.Event.Sender.SenderType == "app" || cb.Event.Sender.SenderType == "bot" {
		// Never react to our own / another bot's messages — avoids loops.
		writeJSON(w, http.StatusOK, map[string]string{"msg": "ignored bot sender"})
		return
	}
	if !larkMentionsBot(cb.Event, h.cfg.Lark.BotOpenID) {
		writeJSON(w, http.StatusOK, map[string]string{"msg": "not mentioned"})
		return
	}

	// Dedupe Lark's at-least-once retries by event_id. Lark resends an event
	// when it doesn't observe a fast 200; without this a slow import would
	// fire twice (one "created", one "already exists"). We ensure the client
	// (and dedupe set) are initialised via larkAPI() first.
	h.larkAPI()
	if h.larkDedup != nil && h.larkDedup.seenBefore(cb.Header.EventID) {
		writeJSON(w, http.StatusOK, map[string]string{"msg": "duplicate event"})
		return
	}

	// ACK immediately so Lark never retries, then do the heavy work (thread
	// fetch + attachment upload + issue create + reply) in the background.
	// We detach from the request context (which is cancelled once we return)
	// and cap the work with our own timeout.
	messageID := msg.MessageID
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		h.processLarkImport(ctx, messageID)
	}()
	writeJSON(w, http.StatusOK, map[string]string{"msg": "accepted"})
}

// processLarkImport runs the full import for one @mention and posts the result
// reply back into the thread. Runs in a background goroutine after the webhook
// has already ACKed Lark.
func (h *Handler) processLarkImport(ctx context.Context, messageID string) {
	issue, dup, err := h.importLarkThread(ctx, messageID)
	if err != nil {
		slog.Error("lark webhook: import failed", "message_id", messageID, "error", err)
		h.larkReply(ctx, messageID, lark.PostText("❌ Failed to create task: "+err.Error()))
		return
	}

	prefix := h.getIssuePrefix(ctx, issue.WorkspaceID)
	ident := fmt.Sprintf("%s-%d", prefix, issue.Number)
	link := h.larkIssueURL(ctx, issue)
	verb := "✅ Created "
	if dup {
		verb = "ℹ️ Task already exists: "
	}
	slog.Info("lark webhook: issue ready", "identifier", ident, "duplicate", dup)
	// Rich-text reply: make the identifier itself the clickable link.
	if link != "" {
		h.larkReply(ctx, messageID, lark.PostLink(verb, ident, link))
	} else {
		h.larkReply(ctx, messageID, lark.PostText(verb+ident))
	}
}

// larkIssueURL builds the public web-app URL for an issue, or "" when AppURL
// is not configured. Format: {AppURL}/{workspaceSlug}/issues/{issueUUID}. The
// detail page resolves either the UUID or the PREFIX-N identifier; we use the
// UUID since it's always unambiguous.
func (h *Handler) larkIssueURL(ctx context.Context, issue db.Issue) string {
	base := strings.TrimRight(h.cfg.Lark.AppURL, "/")
	if base == "" {
		return ""
	}
	ws, err := h.Queries.GetWorkspace(ctx, issue.WorkspaceID)
	if err != nil || ws.Slug == "" {
		return ""
	}
	return fmt.Sprintf("%s/%s/issues/%s", base, ws.Slug, uuidToString(issue.ID))
}

// larkMentionsBot reports whether the bot was @mentioned in the message. When
// botOpenID is configured we match it exactly; otherwise (open id unset) we
// treat ANY mention as "for us" — fine when the bot is the only app likely to
// be @mentioned, and avoids a hard dependency on knowing the open id.
func larkMentionsBot(ev larkEvent, botOpenID string) bool {
	if len(ev.Message.Mentions) == 0 {
		return false
	}
	if botOpenID == "" {
		return true
	}
	for _, m := range ev.Message.Mentions {
		if m.ID.OpenID == botOpenID {
			return true
		}
	}
	return false
}

// importLarkThread fetches the message + its thread, re-uploads attachments,
// and creates the triage issue. Returns (issue, isDuplicate, error).
func (h *Handler) importLarkThread(ctx context.Context, messageID string) (db.Issue, bool, error) {
	client := h.larkAPI()
	if client == nil {
		return db.Issue{}, false, fmt.Errorf("lark client unavailable")
	}

	clicked, err := client.GetMessage(ctx, messageID)
	if err != nil {
		return db.Issue{}, false, fmt.Errorf("get message: %w", err)
	}

	// A threaded/topic message has a thread_id; pull the whole thread. A plain
	// message is its own single-element "thread".
	messages := []lark.Message{clicked}
	if clicked.ThreadID != "" {
		thread, terr := client.ListThread(ctx, clicked.ThreadID, maxLarkThreadMessages)
		if terr != nil {
			return db.Issue{}, false, fmt.Errorf("list thread: %w", terr)
		}
		if len(thread) > 0 {
			messages = thread
		}
	}

	wsUUID, err := util.ParseUUID(h.cfg.Lark.WorkspaceID)
	if err != nil {
		return db.Issue{}, false, fmt.Errorf("invalid MULTICA_LARK_WORKSPACE_ID: %w", err)
	}

	// Re-upload attachments before creating the issue so we can link them at
	// creation time. A single bad file is logged and skipped rather than
	// sinking the whole import.
	var attachmentIDs []pgtype.UUID
	for _, m := range messages {
		for _, res := range m.Resources {
			attID, upErr := h.uploadLarkResource(ctx, client, wsUUID, res)
			if upErr != nil {
				slog.Warn("lark webhook: attachment skipped", "file_key", res.FileKey, "error", upErr)
				continue
			}
			attachmentIDs = append(attachmentIDs, attID)
		}
	}

	title := larkTitle(messages)
	description := larkDescription(messages)

	issue, dup, err := h.createLarkIssue(ctx, wsUUID, title, description, attachmentIDs)
	if err != nil {
		return db.Issue{}, false, err
	}
	return issue, dup, nil
}

// uploadLarkResource downloads a Lark resource and stores it as a Multica
// attachment owned by the configured creator. Returns the attachment id.
func (h *Handler) uploadLarkResource(ctx context.Context, client *lark.Client, wsUUID pgtype.UUID, res lark.Resource) (pgtype.UUID, error) {
	if h.Storage == nil {
		return pgtype.UUID{}, fmt.Errorf("storage not configured")
	}
	data, contentType, err := client.DownloadResource(ctx, res)
	if err != nil {
		return pgtype.UUID{}, err
	}

	id, err := uuid.NewV7()
	if err != nil {
		return pgtype.UUID{}, err
	}
	filename := res.FileName
	if filename == "" {
		filename = id.String()
	}
	key := "workspaces/" + h.cfg.Lark.WorkspaceID + "/" + id.String() + "-" + sanitizeFilename(filename)

	link, err := h.Storage.Upload(ctx, key, data, contentType, filename)
	if err != nil {
		return pgtype.UUID{}, fmt.Errorf("upload: %w", err)
	}

	att, err := h.Queries.CreateAttachment(ctx, db.CreateAttachmentParams{
		ID:           pgtype.UUID{Bytes: id, Valid: true},
		WorkspaceID:  wsUUID,
		UploaderType: "member",
		UploaderID:   parseUUID(h.cfg.Lark.CreatorUserID),
		Filename:     filename,
		Url:          link,
		ContentType:  contentType,
		SizeBytes:    int64(len(data)),
	})
	if err != nil {
		return pgtype.UUID{}, fmt.Errorf("create attachment: %w", err)
	}
	return att.ID, nil
}

// createLarkIssue runs the same guarded create+enqueue transaction as
// CreateIssue, hard-wired to the configured creator + triage agent. Returns
// (issue, isDuplicate, error).
func (h *Handler) createLarkIssue(ctx context.Context, wsUUID pgtype.UUID, title, description string, attachmentIDs []pgtype.UUID) (db.Issue, bool, error) {
	priority := h.cfg.Lark.Priority
	if priority == "" {
		priority = "medium"
	}
	creatorUUID, err := util.ParseUUID(h.cfg.Lark.CreatorUserID)
	if err != nil {
		return db.Issue{}, false, fmt.Errorf("invalid MULTICA_LARK_CREATOR_USER_ID: %w", err)
	}
	agentUUID, err := util.ParseUUID(h.cfg.Lark.AgentID)
	if err != nil {
		return db.Issue{}, false, fmt.Errorf("invalid MULTICA_LARK_AGENT_ID: %w", err)
	}

	// Validate the creator is a real member and the agent a real, non-archived
	// agent of the target workspace — same trust boundary CreateIssue enforces.
	if _, err := h.Queries.GetMemberByUserAndWorkspace(ctx, db.GetMemberByUserAndWorkspaceParams{
		UserID:      creatorUUID,
		WorkspaceID: wsUUID,
	}); err != nil {
		return db.Issue{}, false, fmt.Errorf("creator is not a member of the workspace")
	}
	agent, err := h.Queries.GetAgentInWorkspace(ctx, db.GetAgentInWorkspaceParams{
		ID:          agentUUID,
		WorkspaceID: wsUUID,
	})
	if err != nil {
		return db.Issue{}, false, fmt.Errorf("triage agent not found in workspace")
	}
	if agent.ArchivedAt.Valid {
		return db.Issue{}, false, fmt.Errorf("triage agent is archived")
	}

	tx, err := h.TxStarter.Begin(ctx)
	if err != nil {
		return db.Issue{}, false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)
	qtx := h.Queries.WithTx(tx)

	// Active-duplicate guard (top-level issue: no project / parent).
	duplicate, found, err := issueguard.LockAndFindActiveDuplicate(ctx, qtx, wsUUID, pgtype.UUID{}, pgtype.UUID{}, title, false)
	if err != nil {
		return db.Issue{}, false, fmt.Errorf("duplicate guard: %w", err)
	}
	if found {
		return duplicate, true, nil
	}

	number, err := qtx.IncrementIssueCounter(ctx, wsUUID)
	if err != nil {
		return db.Issue{}, false, fmt.Errorf("increment counter: %w", err)
	}

	position, err := issueposition.NextTopPosition(ctx, tx, wsUUID, "todo")
	if err != nil {
		return db.Issue{}, false, fmt.Errorf("next position: %w", err)
	}

	issue, err := qtx.CreateIssue(ctx, db.CreateIssueParams{
		WorkspaceID:  wsUUID,
		Title:        title,
		Description:  strToText(description),
		Status:       "todo", // todo (not backlog) so the agent picks it up immediately
		Priority:     priority,
		AssigneeType: pgtype.Text{String: "agent", Valid: true},
		AssigneeID:   agentUUID,
		CreatorType:  "member",
		CreatorID:    creatorUUID,
		Position:     position,
		Number:       number,
	})
	if err != nil {
		return db.Issue{}, false, fmt.Errorf("create issue: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return db.Issue{}, false, fmt.Errorf("commit: %w", err)
	}

	// Link attachments (post-commit, best-effort — mirrors CreateIssue).
	if len(attachmentIDs) > 0 {
		h.linkAttachmentsByIssueIDs(ctx, issue.ID, issue.WorkspaceID, attachmentIDs)
	}

	// Assigned-to-agent + status=todo ⇒ enqueue the triage agent now.
	if _, err := h.TaskService.EnqueueTaskForIssue(ctx, issue); err != nil {
		// The issue exists and is assigned; a failed enqueue is recoverable
		// (re-assign in the UI). Log loudly but don't fail the import.
		slog.Error("lark webhook: enqueue agent task failed",
			"issue_id", uuidToString(issue.ID), "error", err)
	}

	return issue, false, nil
}

// ── rendering ────────────────────────────────────────────────────────────---

func larkTitle(messages []lark.Message) string {
	root := ""
	if len(messages) > 0 {
		root = strings.TrimSpace(messages[0].Text)
	}
	firstLine := root
	if i := strings.IndexByte(root, '\n'); i >= 0 {
		firstLine = root[:i]
	}
	firstLine = strings.TrimSpace(firstLine)
	if firstLine == "" {
		firstLine = "thread"
	}
	title := "[Lark] " + firstLine
	if len(title) > 250 {
		title = title[:250]
	}
	return title
}

func larkDescription(messages []lark.Message) string {
	var b strings.Builder
	b.WriteString("> Imported from Lark (bot @mention).\n\n---\n")
	for i, m := range messages {
		sender := m.SenderID
		if sender == "" {
			sender = m.SenderType
		}
		role := "Reply"
		if i == 0 {
			role = "Root message"
		}
		b.WriteString("\n**")
		b.WriteString(role)
		b.WriteString("** — ")
		b.WriteString(sender)
		if ts := formatLarkTime(m.CreateTimeMs); ts != "" {
			b.WriteString(" · ")
			b.WriteString(ts)
		}
		b.WriteString("\n\n")
		text := strings.TrimSpace(m.Text)
		if text == "" {
			text = "_(no text)_"
		}
		b.WriteString(text)
		b.WriteString("\n")
	}
	return b.String()
}

func formatLarkTime(ms int64) string {
	if ms == 0 {
		return ""
	}
	return time.UnixMilli(ms).UTC().Format("2006-01-02 15:04 UTC")
}

// larkReply posts a best-effort reply back into the thread so the user gets
// feedback. Failures are logged, never fatal.
func (h *Handler) larkReply(ctx context.Context, messageID string, payload lark.ReplyPayload) {
	client := h.larkAPI()
	if client == nil || messageID == "" {
		return
	}
	if err := client.Reply(ctx, messageID, payload); err != nil {
		slog.Warn("lark webhook: reply failed", "message_id", messageID, "error", err)
	}
}

// ── small local helpers ──────────────────────────────────────────────────---

// sanitizeFilename keeps S3 keys tame: strip path separators and spaces.
func sanitizeFilename(name string) string {
	name = strings.ReplaceAll(name, "/", "_")
	name = strings.ReplaceAll(name, "\\", "_")
	name = strings.ReplaceAll(name, " ", "_")
	return name
}
