package main

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type taskAttachment struct {
	Type     string `json:"type"`
	MimeType string `json:"mime_type"`
	FileName string `json:"file_name"`
	Caption  string `json:"caption"`
	Data     string `json:"data"`
}

type entityTask struct {
	ID             int64          `json:"id"`
	EntityType     string         `json:"entity_type"`
	EntityID       string         `json:"entity_id"`
	Title          string         `json:"title"`
	Description    string         `json:"description"`
	LinkURL        string         `json:"link_url"`
	Attachment     taskAttachment `json:"attachment"`
	DueAt          time.Time      `json:"due_at"`
	Status         string         `json:"status"`
	DeliveryStatus string         `json:"delivery_status"`
	DeliveryError  string         `json:"delivery_error,omitempty"`
	SentAt         *time.Time     `json:"sent_at,omitempty"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
}

func taskEntity(r *http.Request) (string, string, bool) {
	entityType := strings.TrimSpace(r.URL.Query().Get("entity_type"))
	entityID := strings.TrimSpace(r.URL.Query().Get("entity_id"))
	return entityType, entityID, (entityType == "contact" || entityType == "group") && entityID != "" && len(entityID) <= 180
}

func scanTask(scanner interface{ Scan(...any) error }) (entityTask, error) {
	var task entityTask
	var attachment []byte
	err := scanner.Scan(&task.ID, &task.EntityType, &task.EntityID, &task.Title, &task.Description, &task.LinkURL, &attachment, &task.DueAt, &task.Status, &task.DeliveryStatus, &task.DeliveryError, &task.SentAt, &task.CreatedAt, &task.UpdatedAt)
	if err == nil && len(attachment) > 0 {
		_ = json.Unmarshal(attachment, &task.Attachment)
		task.Attachment.Data = ""
	}
	return task, err
}

const taskColumns = `id,entity_type,entity_id,title,description,link_url,attachment,due_at,status,delivery_status,delivery_error,sent_at,created_at,updated_at`

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
			SELECT `+taskColumns+`
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
			Title       string         `json:"title"`
			Description string         `json:"description"`
			LinkURL     string         `json:"link_url"`
			Attachment  taskAttachment `json:"attachment"`
			DueAt       time.Time      `json:"due_at"`
		}
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 72*1024*1024))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&body) != nil || decoder.Decode(new(any)) != io.EOF {
			respond(w, http.StatusBadRequest, map[string]string{"error": "invalid_task"})
			return
		}
		body.Title = strings.TrimSpace(body.Title)
		body.Description = strings.TrimSpace(body.Description)
		body.LinkURL = strings.TrimSpace(body.LinkURL)
		if body.Title == "" || len(body.Title) > 300 || len(body.Description) > 8000 || len(body.LinkURL) > 2000 || body.DueAt.IsZero() {
			respond(w, http.StatusBadRequest, map[string]string{"error": "invalid_task"})
			return
		}
		if body.LinkURL != "" && !strings.HasPrefix(body.LinkURL, "https://") && !strings.HasPrefix(body.LinkURL, "http://") {
			respond(w, http.StatusBadRequest, map[string]string{"error": "invalid_task_link"})
			return
		}
		if body.Attachment.Data != "" {
			raw, err := base64.StdEncoding.DecodeString(body.Attachment.Data)
			if err != nil || len(raw) > 50*1024*1024 || len(body.Attachment.FileName) > 255 {
				respond(w, http.StatusBadRequest, map[string]string{"error": "invalid_task_attachment"})
				return
			}
		}
		attachment, _ := json.Marshal(body.Attachment)
		task, err := scanTask(db.QueryRowContext(ctx, `
			INSERT INTO entity_tasks(account_id,entity_type,entity_id,title,description,link_url,attachment,due_at)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8)
			RETURNING `+taskColumns+`
		`, accountID, entityType, entityID, body.Title, body.Description, body.LinkURL, string(attachment), body.DueAt))
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
			RETURNING `+taskColumns+`
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
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			respond(w, http.StatusServiceUnavailable, map[string]string{"error": "task_not_deleted"})
			return
		}
		defer tx.Rollback()
		var commandID string
		if err = tx.QueryRowContext(ctx, `SELECT command_id FROM entity_tasks WHERE id=$1 AND account_id=$2 FOR UPDATE`, id, accountID).Scan(&commandID); err == sql.ErrNoRows {
			respond(w, http.StatusNotFound, map[string]string{"error": "task_not_found"})
			return
		} else if err != nil {
			respond(w, http.StatusServiceUnavailable, map[string]string{"error": "task_not_deleted"})
			return
		}
		if commandID != "" {
			_, _ = tx.ExecContext(ctx, `DELETE FROM commands WHERE id=$1 AND status='pending'`, commandID)
		}
		result, err := tx.ExecContext(ctx, `DELETE FROM entity_tasks WHERE id=$1 AND account_id=$2`, id, accountID)
		if err != nil {
			respond(w, http.StatusServiceUnavailable, map[string]string{"error": "task_not_deleted"})
			return
		}
		rows, _ := result.RowsAffected()
		if rows == 0 {
			respond(w, http.StatusNotFound, map[string]string{"error": "task_not_found"})
			return
		}
		if err = tx.Commit(); err != nil {
			respond(w, http.StatusServiceUnavailable, map[string]string{"error": "task_not_deleted"})
			return
		}
		respond(w, http.StatusOK, map[string]bool{"deleted": true})
		return
	}
	w.Header().Set("Allow", "GET, POST, PATCH, DELETE")
	respond(w, http.StatusMethodNotAllowed, map[string]string{"error": "method_not_allowed"})
}
