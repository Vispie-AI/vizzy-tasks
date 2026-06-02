// Package lark is a minimal client for the Lark (Feishu) Open Platform,
// scoped to exactly what the Lark→issue webhook needs:
//
//   - tenant_access_token (cached until shortly before expiry)
//   - GetMessage          — fetch one message (to discover its thread id)
//   - ListThread          — page through every message in a thread (the replies)
//   - DownloadResource     — fetch an image/file resource as bytes
//
// It deliberately uses only the standard library so the fork picks up no new
// third-party dependency. Construct one Client per process and reuse it; the
// token cache is goroutine-safe.
package lark

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

// Client talks to one Lark tenant. BaseURL is e.g. https://open.larksuite.com
// (global) or https://open.feishu.cn (Feishu).
type Client struct {
	appID     string
	appSecret string
	baseURL   string
	http      *http.Client

	mu        sync.Mutex
	token     string
	tokenExpr time.Time
}

func NewClient(appID, appSecret, baseURL string) *Client {
	if baseURL == "" {
		baseURL = "https://open.larksuite.com"
	}
	return &Client{
		appID:     appID,
		appSecret: appSecret,
		baseURL:   baseURL,
		http:      &http.Client{Timeout: 30 * time.Second},
	}
}

// Resource is a downloadable attachment referenced inside a message.
type Resource struct {
	MessageID string
	FileKey   string
	FileName  string
	Type      string // "image" | "file"
}

// Message is the normalized shape this package exposes to the handler. The raw
// Lark body is parsed into plain Text plus a list of downloadable Resources.
type Message struct {
	MessageID    string
	SenderID     string
	SenderType   string
	MsgType      string
	Text         string
	CreateTimeMs int64
	ThreadID     string
	RootID       string
	Resources    []Resource
}

// ── auth ────────────────────────────────────────────────────────────────────

func (c *Client) tenantToken(ctx context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.token != "" && time.Now().Before(c.tokenExpr.Add(-60*time.Second)) {
		return c.token, nil
	}
	body, _ := json.Marshal(map[string]string{
		"app_id":     c.appID,
		"app_secret": c.appSecret,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+"/open-apis/auth/v3/tenant_access_token/internal",
		bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var out struct {
		Code   int    `json:"code"`
		Msg    string `json:"msg"`
		Token  string `json:"tenant_access_token"`
		Expire int    `json:"expire"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	if out.Code != 0 {
		return "", fmt.Errorf("lark token: code=%d msg=%s", out.Code, out.Msg)
	}
	c.token = out.Token
	c.tokenExpr = time.Now().Add(time.Duration(out.Expire) * time.Second)
	return c.token, nil
}

func (c *Client) authedGet(ctx context.Context, endpoint string, query url.Values) (*http.Response, error) {
	token, err := c.tenantToken(ctx)
	if err != nil {
		return nil, err
	}
	u := c.baseURL + endpoint
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	return c.http.Do(req)
}

// ── messages ──────────────────────────────────────────────────────────────--

// rawMessage mirrors the subset of the Lark message object we consume.
type rawMessage struct {
	MessageID  string `json:"message_id"`
	RootID     string `json:"root_id"`
	ThreadID   string `json:"thread_id"`
	MsgType    string `json:"msg_type"`
	CreateTime string `json:"create_time"`
	Sender     struct {
		ID         string `json:"id"`
		SenderType string `json:"sender_type"`
	} `json:"sender"`
	Body struct {
		Content string `json:"content"`
	} `json:"body"`
}

type listResponse struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
	Data struct {
		HasMore   bool         `json:"has_more"`
		PageToken string       `json:"page_token"`
		Items     []rawMessage `json:"items"`
	} `json:"data"`
}

// GetMessage fetches a single message by id.
func (c *Client) GetMessage(ctx context.Context, messageID string) (Message, error) {
	resp, err := c.authedGet(ctx, "/open-apis/im/v1/messages/"+url.PathEscape(messageID), nil)
	if err != nil {
		return Message{}, err
	}
	defer resp.Body.Close()
	var lr listResponse
	if err := json.NewDecoder(resp.Body).Decode(&lr); err != nil {
		return Message{}, err
	}
	if lr.Code != 0 {
		return Message{}, fmt.Errorf("lark get_message: code=%d msg=%s", lr.Code, lr.Msg)
	}
	if len(lr.Data.Items) == 0 {
		return Message{}, fmt.Errorf("lark get_message: message %s not found", messageID)
	}
	return parseMessage(lr.Data.Items[0]), nil
}

// ListThread returns every message in a thread, oldest first, paging through
// all results. maxMessages caps the total to keep an enormous thread from
// blowing up the agent's context window (0 = no cap).
func (c *Client) ListThread(ctx context.Context, threadID string, maxMessages int) ([]Message, error) {
	var out []Message
	pageToken := ""
	for {
		q := url.Values{}
		q.Set("container_id_type", "thread")
		q.Set("container_id", threadID)
		q.Set("sort_type", "ByCreateTimeAsc")
		q.Set("page_size", "50")
		if pageToken != "" {
			q.Set("page_token", pageToken)
		}
		resp, err := c.authedGet(ctx, "/open-apis/im/v1/messages", q)
		if err != nil {
			return nil, err
		}
		var lr listResponse
		if decErr := json.NewDecoder(resp.Body).Decode(&lr); decErr != nil {
			resp.Body.Close()
			return nil, decErr
		}
		resp.Body.Close()
		if lr.Code != 0 {
			return nil, fmt.Errorf("lark list_thread: code=%d msg=%s", lr.Code, lr.Msg)
		}
		for _, item := range lr.Data.Items {
			out = append(out, parseMessage(item))
			if maxMessages > 0 && len(out) >= maxMessages {
				return out, nil
			}
		}
		if lr.Data.HasMore && lr.Data.PageToken != "" {
			pageToken = lr.Data.PageToken
			continue
		}
		return out, nil
	}
}

// DownloadResource fetches the bytes of an image/file resource. Returns the
// bytes and the response Content-Type.
func (c *Client) DownloadResource(ctx context.Context, res Resource) ([]byte, string, error) {
	q := url.Values{}
	q.Set("type", res.Type)
	endpoint := "/open-apis/im/v1/messages/" + url.PathEscape(res.MessageID) +
		"/resources/" + url.PathEscape(res.FileKey)
	resp, err := c.authedGet(ctx, endpoint, q)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		// The resource endpoint returns a JSON error envelope on failure.
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, "", fmt.Errorf("lark download_resource: http %d: %s", resp.StatusCode, string(b))
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", err
	}
	ct := resp.Header.Get("Content-Type")
	if ct == "" {
		ct = "application/octet-stream"
	}
	return data, ct, nil
}

// ── parsing ──────────────────────────────────────────────────────────────---

func parseMessage(rm rawMessage) Message {
	text, resources := extractContent(rm.MsgType, rm.Body.Content, rm.MessageID)
	createMs, _ := strconv.ParseInt(rm.CreateTime, 10, 64)
	return Message{
		MessageID:    rm.MessageID,
		SenderID:     rm.Sender.ID,
		SenderType:   rm.Sender.SenderType,
		MsgType:      rm.MsgType,
		Text:         text,
		CreateTimeMs: createMs,
		ThreadID:     rm.ThreadID,
		RootID:       rm.RootID,
		Resources:    resources,
	}
}

// extractContent turns a message body (a JSON string whose shape depends on
// msg_type) into plain text plus any downloadable resources.
func extractContent(msgType, rawContent, messageID string) (string, []Resource) {
	var resources []Resource
	var content map[string]any
	_ = json.Unmarshal([]byte(rawContent), &content)

	switch msgType {
	case "text":
		return asString(content["text"]), resources

	case "image":
		if fk := asString(content["image_key"]); fk != "" {
			resources = append(resources, Resource{MessageID: messageID, FileKey: fk, FileName: fk + ".png", Type: "image"})
		}
		return "[image]", resources

	case "file", "audio", "media":
		fk := asString(content["file_key"])
		name := asString(content["file_name"])
		if name == "" {
			name = fk
		}
		if fk != "" {
			resources = append(resources, Resource{MessageID: messageID, FileKey: fk, FileName: name, Type: "file"})
		}
		return fmt.Sprintf("[%s: %s]", msgType, name), resources

	case "post":
		return extractPost(content, messageID, &resources), resources

	default:
		return fmt.Sprintf("[%s] %s", msgType, rawContent), resources
	}
}

// extractPost walks a rich-text ("post") body: a title plus a list of lines,
// each a list of segments (text / link / mention / inline image).
func extractPost(content map[string]any, messageID string, resources *[]Resource) string {
	var lines []string
	if title := asString(content["title"]); title != "" {
		lines = append(lines, title)
	}
	blocks, _ := content["content"].([]any)
	for _, blockAny := range blocks {
		segs, _ := blockAny.([]any)
		var line string
		for _, segAny := range segs {
			seg, _ := segAny.(map[string]any)
			switch asString(seg["tag"]) {
			case "text":
				line += asString(seg["text"])
			case "a":
				line += fmt.Sprintf("%s(%s)", asString(seg["text"]), asString(seg["href"]))
			case "at":
				name := asString(seg["user_name"])
				if name == "" {
					name = asString(seg["user_id"])
				}
				line += "@" + name
			case "img":
				if fk := asString(seg["image_key"]); fk != "" {
					*resources = append(*resources, Resource{MessageID: messageID, FileKey: fk, FileName: fk + ".png", Type: "image"})
				}
				line += "[image]"
			}
		}
		lines = append(lines, line)
	}
	out := ""
	for i, l := range lines {
		if i > 0 {
			out += "\n"
		}
		out += l
	}
	return out
}

func asString(v any) string {
	s, _ := v.(string)
	return s
}
