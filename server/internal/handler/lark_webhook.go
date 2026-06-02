package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
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

// larkChallenge is the URL-verification handshake body Lark POSTs when you
// first set the request URL in the developer console.
type larkChallenge struct {
	Challenge string `json:"challenge"`
	Token     string `json:"token"`
	Type      string `json:"type"`
}

// larkCallback is the (decrypted, plain-JSON) callback envelope. We only read
// the fields needed to authenticate the request and locate the message; the
// rest of the (large, schema-drifting) payload is ignored.
type larkCallback struct {
	Token  string `json:"token"`
	Type   string `json:"type"`
	Header struct {
		Token string `json:"token"`
	} `json:"header"`
	Event map[string]any `json:"event"`
	// Some message-action shapes put the id at top level.
	OpenMessageID string `json:"open_message_id"`
	MessageID     string `json:"message_id"`
}

// HandleLarkWebhook is the public ingress for the Lark message-shortcut → issue
// flow. It runs OUTSIDE the authenticated route group: the Lark verification
// token in the body IS the credential.
//
// Flow:
//  1. Per-IP rate limit (reuse the autopilot webhook limiter).
//  2. Read + cap the body.
//  3. url_verification handshake → echo the challenge.
//  4. Verify the Lark verification token.
//  5. Extract the clicked message id, resolve its thread, pull all replies.
//  6. Re-upload each image/file resource into Multica storage.
//  7. Create an issue (same tx as CreateIssue) assigned to the triage agent —
//     which auto-enqueues the agent — and return a card "toast" to Lark.
//
// Responses are always 200 with a Lark card-action toast body on the happy
// path so the user sees inline feedback; failures still return 200 with an
// error toast so Lark doesn't retry an un-actionable callback.
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

	// 4. Parse + authenticate.
	var cb larkCallback
	if err := json.Unmarshal(body, &cb); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}
	token := cb.Token
	if token == "" {
		token = cb.Header.Token
	}
	if token != h.cfg.Lark.VerificationToken {
		writeError(w, http.StatusUnauthorized, "invalid token")
		return
	}

	// 5. Locate the clicked message id.
	messageID := extractLarkMessageID(cb)
	if messageID == "" {
		slog.Warn("lark webhook: no message id in callback", "body", truncate(string(body), 500))
		// Ack so Lark stops retrying a payload we can't act on.
		writeJSON(w, http.StatusOK, larkToast("error", "Could not find a message to import"))
		return
	}

	issue, dup, err := h.importLarkThread(r.Context(), messageID)
	if err != nil {
		slog.Error("lark webhook: import failed", "message_id", messageID, "error", err)
		writeJSON(w, http.StatusOK, larkToast("error", "Failed to create task: "+err.Error()))
		return
	}

	prefix := h.getIssuePrefix(r.Context(), issue.WorkspaceID)
	ident := fmt.Sprintf("%s-%d", prefix, issue.Number)
	msg := "Created task " + ident
	if dup {
		msg = "Task already exists: " + ident
	}
	slog.Info("lark webhook: issue ready", "identifier", ident, "duplicate", dup)
	writeJSON(w, http.StatusOK, larkToast("success", msg))
}

// extractLarkMessageID digs the clicked message id out of whichever callback
// shape arrived (card.action.trigger, message action shortcut, or top-level).
func extractLarkMessageID(cb larkCallback) string {
	if cb.OpenMessageID != "" {
		return cb.OpenMessageID
	}
	if cb.MessageID != "" {
		return cb.MessageID
	}
	ev := cb.Event
	if ev == nil {
		return ""
	}
	for _, key := range []string{"open_message_id", "message_id"} {
		if v, ok := ev[key].(string); ok && v != "" {
			return v
		}
	}
	// Nested under event.action (card callbacks) or event.context.
	if action, ok := ev["action"].(map[string]any); ok {
		if v, ok := action["open_message_id"].(string); ok && v != "" {
			return v
		}
	}
	if ctx, ok := ev["context"].(map[string]any); ok {
		if v, ok := ctx["open_message_id"].(string); ok && v != "" {
			return v
		}
	}
	return ""
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
	b.WriteString("> Imported from Lark via message shortcut.\n\n---\n")
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

// larkToast builds a Lark card-action response body. Lark renders the `toast`
// field as an inline notification to the user who clicked the shortcut.
func larkToast(kind, content string) map[string]any {
	return map[string]any{"toast": map[string]string{"type": kind, "content": content}}
}

// ── small local helpers ──────────────────────────────────────────────────---

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// sanitizeFilename keeps S3 keys tame: strip path separators and spaces.
func sanitizeFilename(name string) string {
	name = strings.ReplaceAll(name, "/", "_")
	name = strings.ReplaceAll(name, "\\", "_")
	name = strings.ReplaceAll(name, " ", "_")
	return name
}
