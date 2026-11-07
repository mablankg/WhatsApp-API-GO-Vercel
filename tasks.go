package main

import (
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type entityTask struct {
	ID         int64     `json:"id"`
	EntityType string    `json:"entity_type"`
	EntityID   string    `json:"entity_id"`
	Title      string    `json:"title"`
	DueAt      time.Time `json:"due_at"`
	Status     string    `json:"status"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

func taskEntity(r *http.Request) (string, string, bool) {
	entityType := strings.TrimSpace(r.URL.Query().Get("entity_type"))
	entityID := strings.TrimSpace(r.URL.Query().Get("entity_id"))
	return entityType, entityID, (entityType == "contact" || entityType == "group") && entityID != "" && len(entityID) <= 180
}

func scanTask(scanner interface{ Scan(...any) error }) (entityTask, error) {
	var task entityTask
	err := scanner.Scan(&task.ID, &task.EntityType, &task.EntityID, &task.Title, &task.DueAt, &task.Status, &task.CreatedAt, &task.UpdatedAt)
	return task, err
}

func handleTasks(w http.ResponseWriter, r *http.Request, db *sql.DB) {
	ctx := r.Context()
	accountID := requestAccount(r)
	pathID := strings.TrimPrefix(r.URL.Path, "/v1/tasks/")
	if pathID == r.URL.Path {
		pathID = ""
	}

	if r.Method == http.MethodGet && pathID == "" {
		entityType, entityID, valid := taskEntity(r)
		if !valid {
			respond(w, http.StatusBadRequest, map[string]string{"error": "invalid_entity"})
			return
		}
		rows, err := db.QueryContext(ctx, `
			SELECT id,entity_type,entity_id,title,due_at,status,created_at,updated_at
			FROM entity_tasks
			WHERE account_id=$1 AND entity_type=$2 AND entity_id=$3
			ORDER BY CASE status WHEN 'pending' THEN 0 ELSE 1 END,due_at,created_at
			LIMIT 500
		`, accountID, entityType, entityID)
		if err != nil {
			respond(w, http.StatusServiceUnavailable, map[string]string{"error": "tasks_unavailable"})
			return
		}
		defer rows.Close()
		tasks := []entityTask{}
		for rows.Next() {
			task, err := scanTask(rows)
			if err != nil {
				respond(w, http.StatusServiceUnavailable, map[string]string{"error": "tasks_unavailable"})
				return
			}
			tasks = append(tasks, task)
		}
		respond(w, http.StatusOK, map[string]any{"tasks": tasks})
		return
	}

	if r.Method == http.MethodPost && pathID == "" {
		entityType, entityID, valid := taskEntity(r)
		if !valid {
			respond(w, http.StatusBadRequest, map[string]string{"error": "invalid_entity"})
			return
		}
		var body struct {
			Title string    `json:"title"`
			DueAt time.Time `json:"due_at"`
		}
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16384))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&body) != nil || decoder.Decode(new(any)) != io.EOF {
			respond(w, http.StatusBadRequest, map[string]string{"error": "invalid_task"})
			return
		}
		body.Title = strings.TrimSpace(body.Title)
		if body.Title == "" || len(body.Title) > 300 || body.DueAt.IsZero() {
			respond(w, http.StatusBadRequest, map[string]string{"error": "invalid_task"})
			return
		}
		task, err := scanTask(db.QueryRowContext(ctx, `
			INSERT INTO entity_tasks(account_id,entity_type,entity_id,title,due_at)
			VALUES($1,$2,$3,$4,$5)
			RETURNING id,entity_type,entity_id,title,due_at,status,created_at,updated_at
		`, accountID, entityType, entityID, body.Title, body.DueAt))
		if err != nil {
			respond(w, http.StatusServiceUnavailable, map[string]string{"error": "task_not_saved"})
			return
		}
		respond(w, http.StatusCreated, task)
		return
	}

	id, err := strconv.ParseInt(pathID, 10, 64)
	if err != nil || id <= 0 {
		respond(w, http.StatusNotFound, map[string]string{"error": "not_found"})
		return
	}
	if r.Method == http.MethodPatch {
		var body struct {
			Status string `json:"status"`
		}
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&body) != nil || decoder.Decode(new(any)) != io.EOF || (body.Status != "pending" && body.Status != "done" && body.Status != "cancelled") {
			respond(w, http.StatusBadRequest, map[string]string{"error": "invalid_task_status"})
			return
		}
		task, err := scanTask(db.QueryRowContext(ctx, `
			UPDATE entity_tasks SET status=$3,updated_at=now()
			WHERE id=$1 AND account_id=$2
			RETURNING id,entity_type,entity_id,title,due_at,status,created_at,updated_at
		`, id, accountID, body.Status))
		if err == sql.ErrNoRows {
			respond(w, http.StatusNotFound, map[string]string{"error": "task_not_found"})
			return
		}
		if err != nil {
			respond(w, http.StatusServiceUnavailable, map[string]string{"error": "task_not_saved"})
			return
		}
		respond(w, http.StatusOK, task)
		return
	}
	if r.Method == http.MethodDelete {
		result, err := db.ExecContext(ctx, `DELETE FROM entity_tasks WHERE id=$1 AND account_id=$2`, id, accountID)
		if err != nil {
			respond(w, http.StatusServiceUnavailable, map[string]string{"error": "task_not_deleted"})
			return
		}
		rows, _ := result.RowsAffected()
		if rows == 0 {
			respond(w, http.StatusNotFound, map[string]string{"error": "task_not_found"})
			return
		}
		respond(w, http.StatusOK, map[string]bool{"deleted": true})
		return
	}
	w.Header().Set("Allow", "GET, POST, PATCH, DELETE")
	respond(w, http.StatusMethodNotAllowed, map[string]string{"error": "method_not_allowed"})
}
