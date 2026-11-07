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

	"github.com/lib/pq"
)

var contactAvatarPattern = regexp.MustCompile(`^/v1/contacts/([1-9][0-9]{0,18})/avatar$`)

var contactIDPattern = regexp.MustCompile(`^[1-9][0-9]{0,18}$`)
var contactMessagesPattern = regexp.MustCompile(`^/v1/contacts/([1-9][0-9]{0,18})/messages$`)
var stagePathPattern = regexp.MustCompile(`^/v1/contacts/stages/([a-z0-9_]{1,40})$`)
var stageColorPattern = regexp.MustCompile(`^#[0-9a-fA-F]{6}$`)

type contact struct {
	ID              int64          `json:"id"`
	WhatsAppID      string         `json:"whatsapp_id"`
	Phone           string         `json:"phone"`
	Name            string         `json:"name"`
	AvatarURL       string         `json:"avatar_url"`
	Nickname        string         `json:"nickname"`
	Email           string         `json:"email"`
	TaxID           string         `json:"tax_id"`
	ContactType     string         `json:"contact_type"`
	Status          string         `json:"status"`
	AttendanceStage string         `json:"attendance_stage"`
	Priority        string         `json:"priority"`
	LawyerID        int64          `json:"lawyer_id"`
	LawyerName      string         `json:"lawyer_name"`
	LawyerColor     string         `json:"lawyer_color"`
	Responsible     string         `json:"responsible"`
	Tags            pq.StringArray `json:"tags"`
	Notes           string         `json:"notes"`
	Extra           map[string]any `json:"extra"`
	FirstContactAt  time.Time      `json:"first_contact_at"`
	LastContactAt   time.Time      `json:"last_contact_at"`
	Archived        bool           `json:"archived"`
	UpdatedAt       time.Time      `json:"updated_at"`
}

const contactColumns = `id,whatsapp_id,phone,name,avatar_url,nickname,email,tax_id,contact_type,status,attendance_stage,priority,COALESCE(lawyer_id,0),COALESCE((SELECT name FROM lawyers WHERE id=contacts.lawyer_id),''),COALESCE((SELECT color FROM lawyers WHERE id=contacts.lawyer_id),''),responsible,tags,notes,extra,first_contact_at,last_contact_at,archived,updated_at`

func scanContact(scanner interface{ Scan(...any) error }) (contact, error) {
	var item contact
	var extra []byte
	err := scanner.Scan(&item.ID, &item.WhatsAppID, &item.Phone, &item.Name, &item.AvatarURL, &item.Nickname, &item.Email, &item.TaxID, &item.ContactType, &item.Status, &item.AttendanceStage, &item.Priority, &item.LawyerID, &item.LawyerName, &item.LawyerColor, &item.Responsible, &item.Tags, &item.Notes, &extra, &item.FirstContactAt, &item.LastContactAt, &item.Archived, &item.UpdatedAt)
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
	if match := contactAvatarPattern.FindStringSubmatch(r.URL.Path); match != nil {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			respond(w, 405, map[string]string{"error": "method_not_allowed"})
			return
		}
		var raw []byte
		var mime string
		err := db.QueryRowContext(ctx, "SELECT avatar_data,avatar_mime FROM contacts WHERE id=$1 AND account_id=$2 AND avatar_data IS NOT NULL", match[1], requestAccount(r)).Scan(&raw, &mime)
		if err == sql.ErrNoRows {
			respond(w, 404, map[string]string{"error": "avatar_not_found"})
			return
		}
		if err != nil {
			respond(w, 503, map[string]string{"error": "avatar_unavailable"})
			return
		}
		w.Header().Set("Cache-Control", "private, no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Content-Type", mime)
		_, _ = w.Write(raw)
		return
	}
	if r.URL.Path == "/v1/contacts/stages" {
		if r.Method == http.MethodGet {
			rows, err := db.QueryContext(ctx, "SELECT id,name,color,position FROM attendance_stages WHERE active=true ORDER BY position,id")
			if err != nil {
				respond(w, 503, map[string]string{"error": "stages_unavailable"})
				return
			}
			defer rows.Close()
			items := []map[string]any{}
			for rows.Next() {
				var id, name, color string
				var position int
				if rows.Scan(&id, &name, &color, &position) != nil {
					respond(w, 503, map[string]string{"error": "stages_unavailable"})
					return
				}
				items = append(items, map[string]any{"id": id, "name": name, "color": color, "position": position})
			}
			respond(w, 200, map[string]any{"stages": items})
			return
		}
		if r.Method == http.MethodPost {
			var body struct {
				Name  string `json:"name"`
				Color string `json:"color"`
			}
			dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
			dec.DisallowUnknownFields()
			if dec.Decode(&body) != nil || dec.Decode(new(any)) != io.EOF {
				respond(w, 400, map[string]string{"error": "invalid_stage"})
				return
			}
			body.Name = strings.TrimSpace(body.Name)
			body.Color = strings.TrimSpace(body.Color)
			if len(body.Name) < 1 || len(body.Name) > 60 || !stageColorPattern.MatchString(body.Color) {
				respond(w, 400, map[string]string{"error": "invalid_stage"})
				return
			}
			random := make([]byte, 8)
			if _, err := rand.Read(random); err != nil {
				respond(w, 503, map[string]string{"error": "stage_not_saved"})
				return
			}
			id := "stage_" + hex.EncodeToString(random)
			var position int
			err := db.QueryRowContext(ctx, `INSERT INTO attendance_stages(id,name,color,position) VALUES($1,$2,$3,(SELECT COALESCE(max(position),0)+10 FROM attendance_stages)) RETURNING position`, id, body.Name, body.Color).Scan(&position)
			if err != nil {
				respond(w, 503, map[string]string{"error": "stage_not_saved"})
				return
			}
			respond(w, 201, map[string]any{"id": id, "name": body.Name, "color": body.Color, "position": position})
			return
		}
		w.Header().Set("Allow", "GET, POST")
		respond(w, 405, map[string]string{"error": "method_not_allowed"})
		return
	}
	if match := stagePathPattern.FindStringSubmatch(r.URL.Path); match != nil {
		if r.Method != http.MethodPatch {
			w.Header().Set("Allow", "PATCH")
			respond(w, 405, map[string]string{"error": "method_not_allowed"})
			return
		}
		var body struct {
			Name  string `json:"name"`
			Color string `json:"color"`
		}
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
		dec.DisallowUnknownFields()
		if dec.Decode(&body) != nil || dec.Decode(new(any)) != io.EOF {
			respond(w, 400, map[string]string{"error": "invalid_stage"})
			return
		}
		body.Name = strings.TrimSpace(body.Name)
		body.Color = strings.TrimSpace(body.Color)
		if len(body.Name) < 1 || len(body.Name) > 60 || !stageColorPattern.MatchString(body.Color) {
			respond(w, 400, map[string]string{"error": "invalid_stage"})
			return
		}
		var position int
		err := db.QueryRowContext(ctx, "UPDATE attendance_stages SET name=$2,color=$3,updated_at=now() WHERE id=$1 AND active=true RETURNING position", match[1], body.Name, body.Color).Scan(&position)
		if err == sql.ErrNoRows {
			respond(w, 404, map[string]string{"error": "not_found"})
			return
		}
		if err != nil {
			respond(w, 503, map[string]string{"error": "stage_not_saved"})
			return
		}
		respond(w, 200, map[string]any{"id": match[1], "name": body.Name, "color": body.Color, "position": position})
		return
	}
	if match := contactMessagesPattern.FindStringSubmatch(r.URL.Path); match != nil {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			respond(w, 405, map[string]string{"error": "method_not_allowed"})
			return
		}
		var phone string
		if err := db.QueryRowContext(ctx, "SELECT phone FROM contacts WHERE id=$1 AND account_id=$2", match[1], requestAccount(r)).Scan(&phone); err == sql.ErrNoRows {
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
			WHERE account_id=$1 AND (chat=$2 OR sender=$2 OR chat=(SELECT lid||'@lid' FROM whatsmeow_lid_map WHERE pn=$3)
			OR sender=(SELECT lid||'@lid' FROM whatsmeow_lid_map WHERE pn=$3)
			)
			ORDER BY seq DESC LIMIT 200
		) recent LEFT JOIN message_media mm ON mm.account_id=$1 AND mm.message_id=recent.message_id AND mm.chat=recent.chat ORDER BY recent.seq`, requestAccount(r), phone+"@s.whatsapp.net", phone)
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
			items = append(items, map[string]any{"seq": seq, "id": messageID, "sender": sender, "text": text, "timestamp": timestamp, "from_me": fromMe, "media": mediaJSON(mediaID, mediaType, mimeType, fileName, caption, fileSize, duration)})
		}
		respond(w, 200, map[string]any{"messages": items})
		return
	}
	if r.URL.Path == "/v1/contacts/sync" && r.Method == http.MethodPost {
		id, err := enqueueRaw(ctx, db, requestAccount(r), "sync_contacts", []byte(`{}`))
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
			WHERE account_id=$1 AND archived=$2 AND ($3='' OR name ILIKE '%'||$3||'%' OR nickname ILIKE '%'||$3||'%' OR phone LIKE '%'||$3||'%' OR email ILIKE '%'||$3||'%')
			ORDER BY last_contact_at DESC,id DESC LIMIT 200`, requestAccount(r), archived, q)
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
		item, err := scanContact(db.QueryRowContext(ctx, `INSERT INTO contacts(account_id,whatsapp_id,phone,name) VALUES($1,$2,$3,$4)
			ON CONFLICT(account_id,phone) DO UPDATE SET name=CASE WHEN excluded.name<>'' THEN excluded.name ELSE contacts.name END,updated_at=now()
			RETURNING `+contactColumns, requestAccount(r), body.Phone+"@s.whatsapp.net", body.Phone, body.Name))
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
		item, err := scanContact(db.QueryRowContext(ctx, "SELECT "+contactColumns+" FROM contacts WHERE id=$1 AND account_id=$2", id, requestAccount(r)))
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
		item, err := scanContact(db.QueryRowContext(ctx, "SELECT "+contactColumns+" FROM contacts WHERE id=$1 AND account_id=$2", id, requestAccount(r)))
		if err == sql.ErrNoRows {
			respond(w, 404, map[string]string{"error": "not_found"})
			return
		}
		if err != nil {
			respond(w, 503, map[string]string{"error": "contacts_unavailable"})
			return
		}
		var body struct {
			Name            *string         `json:"name"`
			Nickname        *string         `json:"nickname"`
			Email           *string         `json:"email"`
			TaxID           *string         `json:"tax_id"`
			ContactType     *string         `json:"contact_type"`
			Status          *string         `json:"status"`
			AttendanceStage *string         `json:"attendance_stage"`
			Priority        *string         `json:"priority"`
			LawyerID        *int64          `json:"lawyer_id"`
			Responsible     *string         `json:"responsible"`
			Tags            *[]string       `json:"tags"`
			Notes           *string         `json:"notes"`
			Extra           *map[string]any `json:"extra"`
			Archived        *bool           `json:"archived"`
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
		if body.AttendanceStage != nil {
			item.AttendanceStage = strings.TrimSpace(*body.AttendanceStage)
		}
		if body.Priority != nil {
			item.Priority = strings.TrimSpace(*body.Priority)
		}
		if body.LawyerID != nil {
			item.LawyerID = *body.LawyerID
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
		validPriorities := map[string]bool{"low": true, "normal": true, "high": true, "urgent": true}
		validLawyer := item.LawyerID == 0
		if !validLawyer {
			_ = db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM lawyers WHERE id=$1 AND active=true)", item.LawyerID).Scan(&validLawyer)
		}
		validStage := item.AttendanceStage == ""
		if !validStage {
			_ = db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM attendance_stages WHERE id=$1 AND active=true)", item.AttendanceStage).Scan(&validStage)
		}
		if len(item.Name) > 160 || len(item.Nickname) > 80 || len(item.Email) > 254 || len(item.TaxID) > 32 || len(item.ContactType) > 40 || len(item.Status) > 40 || !validStage || !validPriorities[item.Priority] || !validLawyer || len(item.Responsible) > 160 || len(item.Tags) > 30 || len(item.Notes) > 20000 {
			respond(w, 400, map[string]string{"error": "invalid_contact"})
			return
		}
		extra, _ := json.Marshal(item.Extra)
		item, err = scanContact(db.QueryRowContext(ctx, `UPDATE contacts SET name=$3,nickname=$4,email=$5,tax_id=$6,contact_type=$7,status=$8,attendance_stage=$9,priority=$10,lawyer_id=NULLIF($11,0),responsible=$12,tags=$13,notes=$14,extra=$15,archived=$16,updated_at=now() WHERE id=$1 AND account_id=$2 RETURNING `+contactColumns, id, requestAccount(r), item.Name, item.Nickname, item.Email, item.TaxID, item.ContactType, item.Status, item.AttendanceStage, item.Priority, item.LawyerID, item.Responsible, pq.Array(item.Tags), item.Notes, string(extra), item.Archived))
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

