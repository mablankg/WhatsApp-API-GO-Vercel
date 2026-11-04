package main

import (
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"
)

func handleComments(w http.ResponseWriter, r *http.Request, db *sql.DB) {
	ctx := r.Context()
	entityType := r.URL.Query().Get("entity_type")
	entityID := strings.TrimSpace(r.URL.Query().Get("entity_id"))
	if (entityType != "contact" && entityType != "group") || entityID == "" || len(entityID) > 180 {
		respond(w, 400, map[string]string{"error": "invalid_entity"})
		return
	}
	if r.Method == http.MethodGet {
		rows, err := db.QueryContext(ctx, `SELECT id,body,created_at FROM entity_comments WHERE account_id=$1 AND entity_type=$2 AND entity_id=$3 ORDER BY created_at DESC LIMIT 200`, requestAccount(r), entityType, entityID)
		if err != nil {
			respond(w, 503, map[string]string{"error": "comments_unavailable"})
			return
		}
		defer rows.Close()
		items := []map[string]any{}
		for rows.Next() {
			var id int64
			var body string
			var created time.Time
			if rows.Scan(&id, &body, &created) != nil {
				respond(w, 503, map[string]string{"error": "comments_unavailable"})
				return
			}
			items = append(items, map[string]any{"id": id, "body": body, "created_at": created})
		}
		respond(w, 200, map[string]any{"comments": items})
		return
	}
	if r.Method == http.MethodPost {
		var body struct {
			Body string `json:"body"`
		}
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16384))
		dec.DisallowUnknownFields()
		if dec.Decode(&body) != nil || dec.Decode(new(any)) != io.EOF {
			respond(w, 400, map[string]string{"error": "invalid_comment"})
			return
		}
		body.Body = strings.TrimSpace(body.Body)
		if body.Body == "" || len(body.Body) > 8000 {
			respond(w, 400, map[string]string{"error": "invalid_comment"})
			return
		}
		var id int64
		var created time.Time
		if err := db.QueryRowContext(ctx, `INSERT INTO entity_comments(account_id,entity_type,entity_id,body) VALUES($1,$2,$3,$4) RETURNING id,created_at`, requestAccount(r), entityType, entityID, body.Body).Scan(&id, &created); err != nil {
			respond(w, 503, map[string]string{"error": "comment_not_saved"})
			return
		}
		respond(w, 201, map[string]any{"id": id, "body": body.Body, "created_at": created})
		return
	}
	w.Header().Set("Allow", "GET, POST")
	respond(w, 405, map[string]string{"error": "method_not_allowed"})
}

