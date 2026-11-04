package main

import (
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/lib/pq"
)

var contactIDPattern = regexp.MustCompile(`^[1-9][0-9]{0,18}$`)
var contactMessagesPattern = regexp.MustCompile(`^/v1/contacts/([1-9][0-9]{0,18})/messages$`)

type contact struct {
	ID             int64          `json:"id"`
	WhatsAppID     string         `json:"whatsapp_id"`
	Phone          string         `json:"phone"`
	Name           string         `json:"name"`
	Nickname       string         `json:"nickname"`
	Email          string         `json:"email"`
	TaxID          string         `json:"tax_id"`
	ContactType    string         `json:"contact_type"`
	Status         string         `json:"status"`
	Responsible    string         `json:"responsible"`
	Tags           pq.StringArray `json:"tags"`
	Notes          string         `json:"notes"`
	Extra          map[string]any `json:"extra"`
	FirstContactAt time.Time      `json:"first_contact_at"`
	LastContactAt  time.Time      `json:"last_contact_at"`
	Archived       bool           `json:"archived"`
	UpdatedAt      time.Time      `json:"updated_at"`
}

const contactColumns = `id,whatsapp_id,phone,name,nickname,email,tax_id,contact_type,status,responsible,tags,notes,extra,first_contact_at,last_contact_at,archived,updated_at`

func scanContact(scanner interface{ Scan(...any) error }) (contact, error) {
	var item contact
	var extra []byte
	err := scanner.Scan(&item.ID, &item.WhatsAppID, &item.Phone, &item.Name, &item.Nickname, &item.Email, &item.TaxID, &item.ContactType, &item.Status, &item.Responsible, &item.Tags, &item.Notes, &extra, &item.FirstContactAt, &item.LastContactAt, &item.Archived, &item.UpdatedAt)
	if len(extra) > 0 {
		_ = json.Unmarshal(extra, &item.Extra)
	}
	if item.Extra == nil {
		item.Extra = map[string]any{}
	}
	if item.Tags == nil {
		item.Tags = pq.StringArray{}
	}
	return item, err
}

func handleContacts(w http.ResponseWriter, r *http.Request, db *sql.DB) {
	ctx := r.Context()
	if match := contactMessagesPattern.FindStringSubmatch(r.URL.Path); match != nil {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			respond(w, 405, map[string]string{"error": "method_not_allowed"})
			return
		}
		var phone string
		if err := db.QueryRowContext(ctx, "SELECT phone FROM contacts WHERE id=$1", match[1]).Scan(&phone); err == sql.ErrNoRows {
			respond(w, 404, map[string]string{"error": "not_found"})
			return
		} else if err != nil {
			respond(w, 503, map[string]string{"error": "contacts_unavailable"})
			return
		}
		rows, err := db.QueryContext(ctx, `SELECT recent.seq,recent.message_id,recent.chat,recent.sender,recent.text,recent.timestamp,recent.from_me,
			COALESCE(mm.id,0),COALESCE(mm.media_type,''),COALESCE(mm.mime_type,''),COALESCE(mm.file_name,''),COALESCE(mm.caption,''),COALESCE(mm.file_size,0),COALESCE(mm.duration_seconds,0)
		FROM (
			SELECT seq,message_id,chat,sender,text,timestamp,from_me FROM messages
			WHERE chat=$1 OR sender=$1 OR chat=(SELECT lid||'@lid' FROM whatsmeow_lid_map WHERE pn=$2)
			OR sender=(SELECT lid||'@lid' FROM whatsmeow_lid_map WHERE pn=$2)
			ORDER BY seq DESC LIMIT 200
		) recent LEFT JOIN message_media mm ON mm.message_id=recent.message_id AND mm.chat=recent.chat ORDER BY recent.seq`, phone+"@s.whatsapp.net", phone)
		if err != nil {
			respond(w, 503, map[string]string{"error": "messages_unavailable"})
			return
		}
		defer rows.Close()
		items := []map[string]any{}
		for rows.Next() {
			var seq int64
			var messageID, chat, sender, text string
			var timestamp time.Time
			var fromMe bool
			var mediaID, fileSize int64
			var mediaType, mimeType, fileName, caption string
			var duration int
			if err = rows.Scan(&seq, &messageID, &chat, &sender, &text, &timestamp, &fromMe, &mediaID, &mediaType, &mimeType, &fileName, &caption, &fileSize, &duration); err != nil {
				respond(w, 503, map[string]string{"error": "messages_unavailable"})
				return
			}
			items = append(items, map[string]any{"seq": seq, "id": messageID, "text": text, "timestamp": timestamp, "from_me": fromMe, "media": mediaJSON(mediaID, mediaType, mimeType, fileName, caption, fileSize, duration)})
		}
		respond(w, 200, map[string]any{"messages": items})
		return
	}
	if r.URL.Path == "/v1/contacts/sync" && r.Method == http.MethodPost {
		id, err := enqueueRaw(ctx, db, "sync_contacts", []byte(`{}`))
		if err != nil {
			respondQueueError(w, err, func() { respond(w, 503, map[string]string{"error": "contacts_unavailable"}) })
			return
		}
		respond(w, 202, map[string]string{"id": id, "status": "pending", "poll": "/v1/commands/" + id})
		return
	}
	if r.URL.Path == "/v1/contacts" && r.Method == http.MethodGet {
		q := strings.TrimSpace(r.URL.Query().Get("q"))
		archived := r.URL.Query().Get("archived") == "true"
		rows, err := db.QueryContext(ctx, `SELECT `+contactColumns+` FROM contacts
			WHERE archived=$1 AND ($2='' OR name ILIKE '%'||$2||'%' OR nickname ILIKE '%'||$2||'%' OR phone LIKE '%'||$2||'%' OR email ILIKE '%'||$2||'%')
			ORDER BY last_contact_at DESC,id DESC LIMIT 200`, archived, q)
		if err != nil {
			respond(w, 503, map[string]string{"error": "contacts_unavailable"})
			return
		}
		defer rows.Close()
		items := []contact{}
		for rows.Next() {
			item, err := scanContact(rows)
			if err != nil {
				respond(w, 503, map[string]string{"error": "contacts_unavailable"})
				return
			}
			items = append(items, item)
		}
		respond(w, 200, map[string]any{"contacts": items})
		return
	}
	if r.URL.Path == "/v1/contacts" && r.Method == http.MethodPost {
		var body struct {
			Phone string `json:"phone"`
			Name  string `json:"name"`
		}
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8192))
		dec.DisallowUnknownFields()
		if dec.Decode(&body) != nil || dec.Decode(new(any)) != io.EOF {
			respond(w, 400, map[string]string{"error": "invalid_contact"})
			return
		}
		body.Phone = strings.TrimSpace(body.Phone)
		body.Name = strings.TrimSpace(body.Name)
		if !phonePattern.MatchString(body.Phone) || len(body.Name) > 160 {
			respond(w, 400, map[string]string{"error": "invalid_contact"})
			return
		}
		item, err := scanContact(db.QueryRowContext(ctx, `INSERT INTO contacts(whatsapp_id,phone,name) VALUES($1,$2,$3)
			ON CONFLICT(phone) DO UPDATE SET name=CASE WHEN excluded.name<>'' THEN excluded.name ELSE contacts.name END,updated_at=now()
			RETURNING `+contactColumns, body.Phone+"@s.whatsapp.net", body.Phone, body.Name))
		if err != nil {
			respond(w, 503, map[string]string{"error": "contact_not_saved"})
			return
		}
		respond(w, 201, item)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/v1/contacts/")
	if !contactIDPattern.MatchString(id) {
		respond(w, 404, map[string]string{"error": "not_found"})
		return
	}
	if r.Method == http.MethodGet {
		item, err := scanContact(db.QueryRowContext(ctx, "SELECT "+contactColumns+" FROM contacts WHERE id=$1", id))
		if err == sql.ErrNoRows {
			respond(w, 404, map[string]string{"error": "not_found"})
			return
		}
		if err != nil {
			respond(w, 503, map[string]string{"error": "contacts_unavailable"})
			return
		}
		respond(w, 200, item)
		return
	}
	if r.Method == http.MethodPatch {
		item, err := scanContact(db.QueryRowContext(ctx, "SELECT "+contactColumns+" FROM contacts WHERE id=$1", id))
		if err == sql.ErrNoRows {
			respond(w, 404, map[string]string{"error": "not_found"})
			return
		}
		if err != nil {
			respond(w, 503, map[string]string{"error": "contacts_unavailable"})
			return
		}
		var body struct {
			Name        *string         `json:"name"`
			Nickname    *string         `json:"nickname"`
			Email       *string         `json:"email"`
			TaxID       *string         `json:"tax_id"`
			ContactType *string         `json:"contact_type"`
			Status      *string         `json:"status"`
			Responsible *string         `json:"responsible"`
			Tags        *[]string       `json:"tags"`
			Notes       *string         `json:"notes"`
			Extra       *map[string]any `json:"extra"`
			Archived    *bool           `json:"archived"`
		}
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 65536))
		dec.DisallowUnknownFields()
		if dec.Decode(&body) != nil || dec.Decode(new(any)) != io.EOF {
			respond(w, 400, map[string]string{"error": "invalid_contact"})
			return
		}
		if body.Name != nil {
			item.Name = strings.TrimSpace(*body.Name)
		}
		if body.Nickname != nil {
			item.Nickname = strings.TrimSpace(*body.Nickname)
		}
		if body.Email != nil {
			item.Email = strings.TrimSpace(*body.Email)
		}
		if body.TaxID != nil {
			item.TaxID = strings.TrimSpace(*body.TaxID)
		}
		if body.ContactType != nil {
			item.ContactType = strings.TrimSpace(*body.ContactType)
		}
		if body.Status != nil {
			item.Status = strings.TrimSpace(*body.Status)
		}
		if body.Responsible != nil {
			item.Responsible = strings.TrimSpace(*body.Responsible)
		}
		if body.Tags != nil {
			item.Tags = pq.StringArray(*body.Tags)
		}
		if body.Notes != nil {
			item.Notes = *body.Notes
		}
		if body.Extra != nil {
			item.Extra = *body.Extra
		}
		if body.Archived != nil {
			item.Archived = *body.Archived
		}
		if len(item.Name) > 160 || len(item.Nickname) > 80 || len(item.Email) > 254 || len(item.TaxID) > 32 || len(item.ContactType) > 40 || len(item.Status) > 40 || len(item.Responsible) > 160 || len(item.Tags) > 30 || len(item.Notes) > 20000 {
			respond(w, 400, map[string]string{"error": "invalid_contact"})
			return
		}
		extra, _ := json.Marshal(item.Extra)
		item, err = scanContact(db.QueryRowContext(ctx, `UPDATE contacts SET name=$2,nickname=$3,email=$4,tax_id=$5,contact_type=$6,status=$7,responsible=$8,tags=$9,notes=$10,extra=$11,archived=$12,updated_at=now() WHERE id=$1 RETURNING `+contactColumns, id, item.Name, item.Nickname, item.Email, item.TaxID, item.ContactType, item.Status, item.Responsible, pq.Array(item.Tags), item.Notes, string(extra), item.Archived))
		if err != nil {
			respond(w, 503, map[string]string{"error": "contact_not_saved"})
			return
		}
		respond(w, 200, item)
		return
	}
	w.Header().Set("Allow", "GET, PATCH")
	respond(w, 405, map[string]string{"error": "method_not_allowed"})
}

