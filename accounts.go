package main

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"
)

var accountIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{2,31}$`)

func requestAccount(r *http.Request) string {
	id := strings.TrimSpace(r.Header.Get("X-WhatsApp-Account"))
	if id == "" {
		id = strings.TrimSpace(r.URL.Query().Get("account"))
	}
	if !accountIDPattern.MatchString(id) {
		return "primary"
	}
	return id
}

func handleAccounts(w http.ResponseWriter, r *http.Request, db *sql.DB) {
	ctx := r.Context()
	if strings.HasPrefix(r.URL.Path, "/v1/accounts/") {
		parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/accounts/"), "/")
		if len(parts) == 1 && accountIDPattern.MatchString(parts[0]) && r.Method == http.MethodDelete {
			var body struct { Confirm bool `json:"confirm"` }
			if json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&body) != nil || !body.Confirm {
				respond(w, 400, map[string]string{"error": "confirmation_required"}); return
			}
			id, err := enqueueRaw(ctx, db, parts[0], "delete_account", []byte(`{}`))
			if err != nil { respond(w, 409, map[string]string{"error": "worker_required_for_deletion"}); return }
			respond(w, 202, map[string]string{"id": id, "status": "pending"}); return
		}
		if len(parts) == 1 && accountIDPattern.MatchString(parts[0]) && r.Method == http.MethodPatch {
			var body struct {
				Name string `json:"name"`
			}
			if json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&body) != nil {
				respond(w, 400, map[string]string{"error": "invalid_account"})
				return
			}
			body.Name = strings.TrimSpace(body.Name)
			if len([]rune(body.Name)) < 2 || len([]rune(body.Name)) > 60 {
				respond(w, 400, map[string]string{"error": "invalid_account_name"})
				return
			}
			result, err := db.ExecContext(ctx, "UPDATE whatsapp_accounts SET name=$2,updated_at=now() WHERE id=$1 AND enabled=true", parts[0], body.Name)
			if err != nil {
				respond(w, 503, map[string]string{"error": "account_update_failed"})
				return
			}
			if n, _ := result.RowsAffected(); n == 0 {
				respond(w, 404, map[string]string{"error": "account_not_found"})
				return
			}
			respond(w, 200, map[string]any{"id": parts[0], "name": body.Name})
			return
		}
		if len(parts) != 2 || !accountIDPattern.MatchString(parts[0]) || (parts[1] != "connect" && parts[1] != "disconnect") || r.Method != http.MethodPost {
			respond(w, 404, map[string]string{"error": "not_found"})
			return
		}
		var exists bool
		if err := db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM whatsapp_accounts WHERE id=$1 AND enabled=true)", parts[0]).Scan(&exists); err != nil || !exists {
			respond(w, 404, map[string]string{"error": "account_not_found"})
			return
		}
		id, err := enqueueRaw(ctx, db, parts[0], parts[1], []byte(`{}`))
		if err != nil {
			respondQueueError(w, err, func() { respond(w, 503, map[string]string{"error": "account_command_failed"}) })
			return
		}
		respond(w, 202, map[string]string{"id": id, "status": "pending", "poll": "/v1/commands/" + id})
		return
	}
	if r.Method == http.MethodGet && r.URL.Path == "/v1/accounts" {
		rows, err := db.QueryContext(ctx, `SELECT id,name,device_jid,status,heartbeat,enabled,created_at FROM whatsapp_accounts WHERE enabled=true ORDER BY created_at,id`)
		if err != nil {
			respond(w, 503, map[string]string{"error": "database_unavailable_or_not_initialized"})
			return
		}
		defer rows.Close()
		items := []map[string]any{}
		for rows.Next() {
			var id, name, jid, status string
			var heartbeat, created time.Time
			var enabled bool
			if rows.Scan(&id, &name, &jid, &status, &heartbeat, &enabled, &created) != nil {
				respond(w, 503, map[string]string{"error": "database_unavailable_or_not_initialized"})
				return
			}
			phone := ""
			if at := strings.IndexByte(jid, '@'); at > 0 {
				phone = jid[:at]
			}
			items = append(items, map[string]any{"id": id, "name": name, "phone": phone, "status": status, "worker_alive": time.Since(heartbeat) < 20*time.Second, "enabled": enabled, "created_at": created})
		}
		respond(w, 200, map[string]any{"accounts": items})
		return
	}
	if r.Method == http.MethodPost && r.URL.Path == "/v1/accounts" {
		var body struct {
			Name string `json:"name"`
		}
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
		dec.DisallowUnknownFields()
		if dec.Decode(&body) != nil || dec.Decode(new(any)) != io.EOF {
			respond(w, 400, map[string]string{"error": "invalid_account"})
			return
		}
		body.Name = strings.TrimSpace(body.Name)
		if len([]rune(body.Name)) < 2 || len([]rune(body.Name)) > 60 {
			respond(w, 400, map[string]string{"error": "name deve ter entre 2 e 60 caracteres"})
			return
		}
		random := make([]byte, 8)
		if _, err := rand.Read(random); err != nil {
			respond(w, 503, map[string]string{"error": "account_creation_failed"})
			return
		}
		id := "wa_" + hex.EncodeToString(random)
		if _, err := db.ExecContext(ctx, `INSERT INTO whatsapp_accounts(id,name,status) VALUES($1,$2,'offline')`, id, body.Name); err != nil {
			respond(w, 503, map[string]string{"error": "account_creation_failed"})
			return
		}
		respond(w, 201, map[string]any{"id": id, "name": body.Name, "status": "offline", "enabled": true})
		return
	}
	respond(w, 405, map[string]string{"error": "method_not_allowed"})
}

