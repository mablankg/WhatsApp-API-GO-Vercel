package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var caseNumberPattern = regexp.MustCompile(`^[0-9.\-/]{0,40}$`)

type legalProcess struct {
	ID             int64     `json:"id"`
	CaseNumber     string    `json:"case_number"`
	Title          string    `json:"title"`
	ClientName     string    `json:"client_name"`
	Status         string    `json:"status"`
	DriveFolderID  string    `json:"drive_folder_id"`
	DriveFolderURL string    `json:"drive_folder_url"`
	Notes          string    `json:"notes"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

const legalProcessColumns = `id,case_number,title,client_name,status,drive_folder_id,drive_folder_url,notes,created_at,updated_at`

func scanLegalProcess(scanner interface{ Scan(...any) error }) (legalProcess, error) {
	var item legalProcess
	err := scanner.Scan(&item.ID, &item.CaseNumber, &item.Title, &item.ClientName, &item.Status, &item.DriveFolderID, &item.DriveFolderURL, &item.Notes, &item.CreatedAt, &item.UpdatedAt)
	return item, err
}

func requestAppAccount(r *http.Request, db *sql.DB) (string, error) {
	ctx := r.Context()
	whatsAppAccountID := requestAccount(r)
	var appAccountID string
	err := db.QueryRowContext(ctx, `
		SELECT wa.app_account_id
		FROM whatsapp_accounts wa
		JOIN app_accounts aa ON aa.id = wa.app_account_id AND aa.enabled = true
		WHERE wa.id = $1 AND wa.enabled = true
	`, whatsAppAccountID).Scan(&appAccountID)
	if err == nil && appAccountID != "" {
		return appAccountID, nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}

	// Instalações antigas podem ter a conexão do WhatsApp criada antes do
	// cadastro da conta jurídica. Quando existe uma única conta ativa, o vínculo
	// é inequívoco e pode ser reparado automaticamente.
	var activeAccounts int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*), COALESCE(MIN(id), '')
		FROM app_accounts
		WHERE enabled = true
	`).Scan(&activeAccounts, &appAccountID); err != nil {
		return "", err
	}
	if activeAccounts != 1 || appAccountID == "" {
		return "", sql.ErrNoRows
	}

	result, err := db.ExecContext(ctx, `
		UPDATE whatsapp_accounts
		SET app_account_id = $2, updated_at = now()
		WHERE id = $1 AND enabled = true
	`, whatsAppAccountID, appAccountID)
	if err != nil {
		return "", err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return "", err
	}
	if rows != 1 {
		return "", sql.ErrNoRows
	}
	return appAccountID, nil
}

func validProcessStatus(status string) bool {
	return map[string]bool{"active": true, "suspended": true, "closed": true, "archived": true}[status]
}

func normalizeDriveFolder(folderID, folderURL string) (string, string, bool) {
	folderID = strings.TrimSpace(folderID)
	folderURL = strings.TrimSpace(folderURL)
	if folderID == "" && folderURL == "" {
		return "", "", true
	}
	if folderID != "" && !regexp.MustCompile(`^[A-Za-z0-9_-]{10,200}$`).MatchString(folderID) {
		return "", "", false
	}
	if folderURL != "" {
		u, err := url.Parse(folderURL)
		if err != nil || u.Scheme != "https" || (u.Host != "drive.google.com" && u.Host != "docs.google.com") {
			return "", "", false
		}
		if folderID == "" {
			parts := strings.Split(strings.Trim(u.Path, "/"), "/")
			for i := 0; i+1 < len(parts); i++ {
				if parts[i] == "folders" {
					folderID = parts[i+1]
					break
				}
			}
		}
	}
	if folderID != "" && folderURL == "" {
		folderURL = "https://drive.google.com/drive/folders/" + folderID
	}
	return folderID, folderURL, true
}

func handleProcesses(w http.ResponseWriter, r *http.Request, db *sql.DB) {
	ctx := r.Context()
	appAccountID, err := requestAppAccount(r, db)
	if err != nil {
		respond(w, http.StatusForbidden, map[string]string{"error": "account_not_available"})
		return
	}

	if r.URL.Path == "/v1/process-settings" {
		handleProcessSettings(w, r, db, appAccountID)
		return
	}
	if r.URL.Path == "/v1/process-links" {
		handleProcessLinks(w, r, db, appAccountID)
		return
	}
	if r.URL.Path == "/v1/processes" {
		switch r.Method {
		case http.MethodGet:
			q := strings.TrimSpace(r.URL.Query().Get("q"))
			rows, err := db.QueryContext(ctx, `SELECT `+legalProcessColumns+` FROM legal_processes
				WHERE app_account_id=$1 AND ($2='' OR case_number ILIKE '%'||$2||'%' OR title ILIKE '%'||$2||'%' OR client_name ILIKE '%'||$2||'%')
				ORDER BY CASE status WHEN 'active' THEN 0 WHEN 'suspended' THEN 1 WHEN 'closed' THEN 2 ELSE 3 END, updated_at DESC,id DESC LIMIT 200`, appAccountID, q)
			if err != nil {
				respond(w, 503, map[string]string{"error": "processes_unavailable"})
				return
			}
			defer rows.Close()
			items := []legalProcess{}
			for rows.Next() {
				item, err := scanLegalProcess(rows)
				if err != nil {
					respond(w, 503, map[string]string{"error": "processes_unavailable"})
					return
				}
				items = append(items, item)
			}
			respond(w, 200, map[string]any{"processes": items})
		case http.MethodPost:
			var body legalProcess
			dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
			dec.DisallowUnknownFields()
			if dec.Decode(&body) != nil || dec.Decode(new(any)) != io.EOF {
				respond(w, 400, map[string]string{"error": "invalid_process"})
				return
			}
			body.CaseNumber = strings.TrimSpace(body.CaseNumber)
			body.Title = strings.TrimSpace(body.Title)
			body.ClientName = strings.TrimSpace(body.ClientName)
			body.Notes = strings.TrimSpace(body.Notes)
			if body.Status == "" {
				body.Status = "active"
			}
			if len([]rune(body.Title)) < 2 || len([]rune(body.Title)) > 200 || len([]rune(body.ClientName)) > 160 || len(body.Notes) > 20000 || !caseNumberPattern.MatchString(body.CaseNumber) || !validProcessStatus(body.Status) {
				respond(w, 400, map[string]string{"error": "invalid_process"})
				return
			}
			folderID, folderURL, ok := normalizeDriveFolder(body.DriveFolderID, body.DriveFolderURL)
			if !ok {
				respond(w, 400, map[string]string{"error": "invalid_drive_folder"})
				return
			}
			item, err := scanLegalProcess(db.QueryRowContext(ctx, `INSERT INTO legal_processes(app_account_id,case_number,title,client_name,status,drive_folder_id,drive_folder_url,notes)
				VALUES($1,$2,$3,$4,$5,$6,$7,$8) RETURNING `+legalProcessColumns, appAccountID, body.CaseNumber, body.Title, body.ClientName, body.Status, folderID, folderURL, body.Notes))
			if err != nil {
				respond(w, 409, map[string]string{"error": "process_not_created"})
				return
			}
			respond(w, 201, item)
		default:
			w.Header().Set("Allow", "GET, POST")
			respond(w, 405, map[string]string{"error": "method_not_allowed"})
		}
		return
	}

	idText := strings.TrimPrefix(r.URL.Path, "/v1/processes/")
	id, parseErr := strconv.ParseInt(idText, 10, 64)
	if id < 1 || parseErr != nil {
		respond(w, 404, map[string]string{"error": "not_found"})
		return
	}
	switch r.Method {
	case http.MethodGet:
		item, err := scanLegalProcess(db.QueryRowContext(ctx, `SELECT `+legalProcessColumns+` FROM legal_processes WHERE id=$1 AND app_account_id=$2`, id, appAccountID))
		if err == sql.ErrNoRows {
			respond(w, 404, map[string]string{"error": "not_found"})
			return
		}
		if err != nil {
			respond(w, 503, map[string]string{"error": "processes_unavailable"})
			return
		}
		respond(w, 200, item)
	case http.MethodPatch:
		var body legalProcess
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
		dec.DisallowUnknownFields()
		if dec.Decode(&body) != nil || dec.Decode(new(any)) != io.EOF {
			respond(w, 400, map[string]string{"error": "invalid_process"})
			return
		}
		body.CaseNumber = strings.TrimSpace(body.CaseNumber)
		body.Title = strings.TrimSpace(body.Title)
		body.ClientName = strings.TrimSpace(body.ClientName)
		body.Notes = strings.TrimSpace(body.Notes)
		folderID, folderURL, ok := normalizeDriveFolder(body.DriveFolderID, body.DriveFolderURL)
		if !ok || len([]rune(body.Title)) < 2 || len([]rune(body.Title)) > 200 || len([]rune(body.ClientName)) > 160 || len(body.Notes) > 20000 || !caseNumberPattern.MatchString(body.CaseNumber) || !validProcessStatus(body.Status) {
			respond(w, 400, map[string]string{"error": "invalid_process"})
			return
		}
		item, err := scanLegalProcess(db.QueryRowContext(ctx, `UPDATE legal_processes SET case_number=$3,title=$4,client_name=$5,status=$6,drive_folder_id=$7,drive_folder_url=$8,notes=$9,updated_at=now()
			WHERE id=$1 AND app_account_id=$2 RETURNING `+legalProcessColumns, id, appAccountID, body.CaseNumber, body.Title, body.ClientName, body.Status, folderID, folderURL, body.Notes))
		if err == sql.ErrNoRows {
			respond(w, 404, map[string]string{"error": "not_found"})
			return
		}
		if err != nil {
			respond(w, 409, map[string]string{"error": "process_not_saved"})
			return
		}
		respond(w, 200, item)
	default:
		w.Header().Set("Allow", "GET, PATCH")
		respond(w, 405, map[string]string{"error": "method_not_allowed"})
	}
}

func handleProcessLinks(w http.ResponseWriter, r *http.Request, db *sql.DB, appAccountID string) {
	ctx := r.Context()
	entityType := strings.TrimSpace(r.URL.Query().Get("entity_type"))
	entityID := strings.TrimSpace(r.URL.Query().Get("entity_id"))
	if entityType != "contact" && entityType != "group" || entityID == "" || len(entityID) > 100 {
		respond(w, 400, map[string]string{"error": "invalid_entity"})
		return
	}
	waAccountID := requestAccount(r)
	switch r.Method {
	case http.MethodGet:
		rows, err := db.QueryContext(ctx, `SELECT `+legalProcessColumns+` FROM legal_processes p JOIN legal_process_entities l ON l.process_id=p.id
			WHERE p.app_account_id=$1 AND l.whatsapp_account_id=$2 AND l.entity_type=$3 AND l.entity_id=$4 ORDER BY p.updated_at DESC`, appAccountID, waAccountID, entityType, entityID)
		if err != nil {
			respond(w, 503, map[string]string{"error": "process_links_unavailable"})
			return
		}
		defer rows.Close()
		items := []legalProcess{}
		for rows.Next() {
			item, err := scanLegalProcess(rows)
			if err != nil {
				respond(w, 503, map[string]string{"error": "process_links_unavailable"})
				return
			}
			items = append(items, item)
		}
		respond(w, 200, map[string]any{"processes": items})
	case http.MethodPost:
		var body struct {
			ProcessID int64 `json:"process_id"`
		}
		if json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&body) != nil || body.ProcessID < 1 {
			respond(w, 400, map[string]string{"error": "invalid_process"})
			return
		}
		result, err := db.ExecContext(ctx, `INSERT INTO legal_process_entities(process_id,whatsapp_account_id,entity_type,entity_id)
			SELECT id,$2,$3,$4 FROM legal_processes WHERE id=$1 AND app_account_id=$5 ON CONFLICT DO NOTHING`, body.ProcessID, waAccountID, entityType, entityID, appAccountID)
		if err != nil {
			respond(w, 503, map[string]string{"error": "process_link_not_saved"})
			return
		}
		if n, _ := result.RowsAffected(); n == 0 {
			var exists bool
			_ = db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM legal_processes WHERE id=$1 AND app_account_id=$2)", body.ProcessID, appAccountID).Scan(&exists)
			if !exists {
				respond(w, 404, map[string]string{"error": "process_not_found"})
				return
			}
		}
		respond(w, 200, map[string]any{"process_id": body.ProcessID, "linked": true})
	case http.MethodDelete:
		processID, err := strconv.ParseInt(r.URL.Query().Get("process_id"), 10, 64)
		if err != nil || processID < 1 {
			respond(w, 400, map[string]string{"error": "invalid_process"})
			return
		}
		_, err = db.ExecContext(ctx, `DELETE FROM legal_process_entities l USING legal_processes p
			WHERE l.process_id=p.id AND l.process_id=$1 AND p.app_account_id=$2 AND l.whatsapp_account_id=$3 AND l.entity_type=$4 AND l.entity_id=$5`, processID, appAccountID, waAccountID, entityType, entityID)
		if err != nil {
			respond(w, 503, map[string]string{"error": "process_link_not_removed"})
			return
		}
		respond(w, 200, map[string]any{"process_id": processID, "linked": false})
	default:
		w.Header().Set("Allow", "GET, POST, DELETE")
		respond(w, 405, map[string]string{"error": "method_not_allowed"})
	}
}

func handleProcessSettings(w http.ResponseWriter, r *http.Request, db *sql.DB, appAccountID string) {
	ctx := r.Context()
	if r.Method == http.MethodGet {
		var folderID sql.NullString
		if err := db.QueryRowContext(ctx, "SELECT drive_root_folder_id FROM app_accounts WHERE id=$1 AND enabled=true", appAccountID).Scan(&folderID); err != nil {
			respond(w, 404, map[string]string{"error": "account_not_found"})
			return
		}
		folderURL := ""
		if folderID.String != "" {
			folderURL = "https://drive.google.com/drive/folders/" + folderID.String
		}
		respond(w, 200, map[string]any{"drive_root_folder_id": folderID.String, "drive_root_folder_url": folderURL})
		return
	}
	if r.Method == http.MethodPatch {
		var body struct {
			DriveRootFolderID  string `json:"drive_root_folder_id"`
			DriveRootFolderURL string `json:"drive_root_folder_url"`
		}
		if json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&body) != nil {
			respond(w, 400, map[string]string{"error": "invalid_drive_folder"})
			return
		}
		folderID, folderURL, ok := normalizeDriveFolder(body.DriveRootFolderID, body.DriveRootFolderURL)
		if !ok || folderID == "" {
			respond(w, 400, map[string]string{"error": "invalid_drive_folder"})
			return
		}
		if _, err := db.ExecContext(ctx, "UPDATE app_accounts SET drive_root_folder_id=$2 WHERE id=$1 AND enabled=true", appAccountID, folderID); err != nil {
			respond(w, 503, map[string]string{"error": "drive_settings_not_saved"})
			return
		}
		respond(w, 200, map[string]any{"drive_root_folder_id": folderID, "drive_root_folder_url": folderURL})
		return
	}
	w.Header().Set("Allow", "GET, PATCH")
	respond(w, 405, map[string]string{"error": "method_not_allowed"})
}
