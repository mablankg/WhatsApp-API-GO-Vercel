package main

import (
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"
)

var lawyerPathPattern = regexp.MustCompile(`^/v1/lawyers/([1-9][0-9]{0,18})$`)

func handleLawyers(w http.ResponseWriter, r *http.Request, db *sql.DB) {
	ctx := r.Context()
	if r.URL.Path == "/v1/lawyers" && r.Method == http.MethodGet {
		rows, err := db.QueryContext(ctx, `SELECT id,name,color,active,created_at,updated_at FROM lawyers WHERE active=true ORDER BY lower(name),id`)
		if err != nil {
			respond(w, 503, map[string]string{"error": "lawyers_unavailable"})
			return
		}
		defer rows.Close()
		items := []map[string]any{}
		for rows.Next() {
			var id int64
			var name, color string
			var active bool
			var created, updated time.Time
			if rows.Scan(&id, &name, &color, &active, &created, &updated) != nil {
				respond(w, 503, map[string]string{"error": "lawyers_unavailable"})
				return
			}
			items = append(items, map[string]any{"id": id, "name": name, "color": color, "active": active, "created_at": created, "updated_at": updated})
		}
		respond(w, 200, map[string]any{"lawyers": items})
		return
	}
	if r.URL.Path == "/v1/lawyers" && r.Method == http.MethodPost {
		var body struct {
			Name  string `json:"name"`
			Color string `json:"color"`
		}
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
		dec.DisallowUnknownFields()
		if dec.Decode(&body) != nil || dec.Decode(new(any)) != io.EOF {
			respond(w, 400, map[string]string{"error": "invalid_lawyer"})
			return
		}
		body.Name = strings.TrimSpace(body.Name)
		if len([]rune(body.Name)) < 2 || len([]rune(body.Name)) > 120 || !stageColorPattern.MatchString(body.Color) {
			respond(w, 400, map[string]string{"error": "invalid_lawyer"})
			return
		}
		var id int64
		var created time.Time
		if err := db.QueryRowContext(ctx, `INSERT INTO lawyers(name,color) VALUES($1,$2) RETURNING id,created_at`, body.Name, body.Color).Scan(&id, &created); err != nil {
			respond(w, 409, map[string]string{"error": "lawyer_not_saved"})
			return
		}
		respond(w, 201, map[string]any{"id": id, "name": body.Name, "color": body.Color, "active": true, "created_at": created})
		return
	}
	match := lawyerPathPattern.FindStringSubmatch(r.URL.Path)
	if match != nil && r.Method == http.MethodPatch {
		var body struct {
			Name   *string `json:"name"`
			Color  *string `json:"color"`
			Active *bool   `json:"active"`
		}
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
		dec.DisallowUnknownFields()
		if dec.Decode(&body) != nil || dec.Decode(new(any)) != io.EOF {
			respond(w, 400, map[string]string{"error": "invalid_lawyer"})
			return
		}
		var name, color string
		var active bool
		if err := db.QueryRowContext(ctx, "SELECT name,color,active FROM lawyers WHERE id=$1", match[1]).Scan(&name, &color, &active); err == sql.ErrNoRows {
			respond(w, 404, map[string]string{"error": "not_found"})
			return
		} else if err != nil {
			respond(w, 503, map[string]string{"error": "lawyers_unavailable"})
			return
		}
		if body.Name != nil {
			name = strings.TrimSpace(*body.Name)
		}
		if body.Color != nil {
			color = *body.Color
		}
		if body.Active != nil {
			active = *body.Active
		}
		if len([]rune(name)) < 2 || len([]rune(name)) > 120 || !stageColorPattern.MatchString(color) {
			respond(w, 400, map[string]string{"error": "invalid_lawyer"})
			return
		}
		if _, err := db.ExecContext(ctx, "UPDATE lawyers SET name=$2,color=$3,active=$4,updated_at=now() WHERE id=$1", match[1], name, color, active); err != nil {
			respond(w, 409, map[string]string{"error": "lawyer_not_saved"})
			return
		}
		respond(w, 200, map[string]any{"id": match[1], "name": name, "color": color, "active": active})
		return
	}
	w.Header().Set("Allow", "GET, POST, PATCH")
	respond(w, 405, map[string]string{"error": "method_not_allowed"})
}

