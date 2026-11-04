package main

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/mablankg/WhatsApp-API-GO-Vercel/internal/data"
	qrcode "github.com/skip2/go-qrcode"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

var phonePattern = regexp.MustCompile(`^[1-9][0-9]{7,14}$`)
var groupPattern = regexp.MustCompile(`^[0-9-]+@g\.us$`)

func main() {
	token := os.Getenv("API_TOKEN")
	if len(token) < 32 {
		log.Fatal("API_TOKEN exige pelo menos 32 caracteres")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	db, err := data.Open(ctx, false)
	cancel()
	if err != nil {
		log.Fatal("não foi possível conectar ao banco; confira DATABASE_URL e inicialização do worker")
	}
	defer db.Close()
	port := os.Getenv("PORT")
	if port == "" {
		port = "3000"
	}
	server := &http.Server{Addr: ":" + port, Handler: handler(db, token), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 20 * time.Second, IdleTimeout: 60 * time.Second}
	log.Fatal(server.ListenAndServe())
}

func respond(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
func handler(db *sql.DB, token string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if len(token) < 32 || subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+token)) != 1 {
			respond(w, 401, map[string]string{"error": "unauthorized"})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		r = r.WithContext(ctx)
		fail := func() { respond(w, 503, map[string]string{"error": "database_unavailable_or_not_initialized"}) }
		if strings.HasPrefix(r.URL.Path, "/v1/contacts") {
			handleContacts(w, r, db)
			return
		}
		if r.URL.Path == "/v1/comments" {
			handleComments(w, r, db)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/v1/media/") {
			handleMedia(w, r, db)
			return
		}
		switch {
		case r.Method == "GET" && r.URL.Path == "/v1/status":
			var status string
			var heartbeat time.Time
			if db.QueryRowContext(ctx, "SELECT status,heartbeat FROM state WHERE id=1").Scan(&status, &heartbeat) != nil {
				fail()
				return
			}
			alive := time.Since(heartbeat) < 20*time.Second
			if !alive {
				status = "offline"
			}
			respond(w, 200, map[string]any{"status": status, "worker_alive": alive, "heartbeat": heartbeat})
		case r.Method == "GET" && r.URL.Path == "/v1/qr":
			var code string
			err := db.QueryRowContext(ctx, "SELECT qr FROM state WHERE id=1 AND qr<>'' AND qr_expires>now() AND heartbeat>now()-interval '20 seconds'").Scan(&code)
			if err == sql.ErrNoRows {
				respond(w, 409, map[string]string{"error": "qr_not_available", "hint": "POST /v1/connect e consulte novamente"})
				return
			}
			if err != nil {
				fail()
				return
			}
			png, err := qrcode.Encode(code, qrcode.Medium, 320)
			if err != nil {
				fail()
				return
			}
			w.Header().Set("Content-Type", "image/png")
			w.Write(png)
		case r.Method == "GET" && r.URL.Path == "/v1/groups":
			rows, err := db.QueryContext(ctx, "SELECT group_id,name,topic,participant_count,is_announce,attendance_stage,priority,updated_at FROM groups ORDER BY lower(name),group_id")
			if err != nil {
				fail()
				return
			}
			defer rows.Close()
			items := []map[string]any{}
			for rows.Next() {
				var id, name, topic, stage, priority string
				var count int
				var announce bool
				var updated time.Time
				if rows.Scan(&id, &name, &topic, &count, &announce, &stage, &priority, &updated) != nil {
					fail()
					return
				}
				items = append(items, map[string]any{"id": id, "name": name, "topic": topic, "participant_count": count, "is_announce": announce, "attendance_stage": stage, "priority": priority, "updated_at": updated})
			}
			if rows.Err() != nil {
				fail()
				return
			}
			respond(w, 200, map[string]any{"groups": items})
		case r.Method == "POST" && r.URL.Path == "/v1/groups/sync":
			id, err := enqueueRaw(ctx, db, "sync_groups", []byte(`{}`))
			if err != nil {
				respondQueueError(w, err, fail)
				return
			}
			respond(w, 202, map[string]string{"id": id, "status": "pending", "poll": "/v1/commands/" + id})
		case r.Method == "POST" && r.URL.Path == "/v1/groups":
			var body struct {
				Name         string   `json:"name"`
				Participants []string `json:"participants"`
			}
			dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 32768))
			dec.DisallowUnknownFields()
			if dec.Decode(&body) != nil || dec.Decode(new(any)) != io.EOF {
				respond(w, 400, map[string]string{"error": "invalid_group"})
				return
			}
			body.Name = strings.TrimSpace(body.Name)
			if utf8.RuneCountInString(body.Name) < 1 || utf8.RuneCountInString(body.Name) > 25 || len(body.Participants) < 1 || len(body.Participants) > 100 {
				respond(w, 400, map[string]string{"error": "use name com 1 a 25 caracteres e participants com 1 a 100 números"})
				return
			}
			seen := map[string]bool{}
			participants := make([]string, 0, len(body.Participants))
			for _, participant := range body.Participants {
				if !phonePattern.MatchString(participant) {
					respond(w, 400, map[string]string{"error": "participants devem conter DDI e somente dígitos"})
					return
				}
				if !seen[participant] {
					seen[participant] = true
					participants = append(participants, participant)
				}
			}
			body.Participants = participants
			id, err := enqueue(ctx, db, "create_group", body)
			if err != nil {
				respondQueueError(w, err, fail)
				return
			}
			respond(w, 202, map[string]string{"id": id, "status": "pending", "poll": "/v1/commands/" + id})
		case r.Method == "PATCH" && strings.HasPrefix(r.URL.Path, "/v1/groups/") && !isGroupMessagePath(r.URL.Path):
			groupID := strings.TrimPrefix(r.URL.Path, "/v1/groups/")
			if !groupPattern.MatchString(groupID) {
				respond(w, 400, map[string]string{"error": "invalid_group_id"})
				return
			}
			var body struct {
				AttendanceStage string `json:"attendance_stage"`
				Priority        string `json:"priority"`
			}
			dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
			dec.DisallowUnknownFields()
			if dec.Decode(&body) != nil || dec.Decode(new(any)) != io.EOF {
				respond(w, 400, map[string]string{"error": "invalid_group"})
				return
			}
			validPriority := map[string]bool{"low": true, "normal": true, "high": true, "urgent": true}[body.Priority]
			var validStage bool
			_ = db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM attendance_stages WHERE id=$1 AND active=true)", body.AttendanceStage).Scan(&validStage)
			if !validStage || !validPriority {
				respond(w, 400, map[string]string{"error": "invalid_group_classification"})
				return
			}
			result, err := db.ExecContext(ctx, "UPDATE groups SET attendance_stage=$2,priority=$3 WHERE group_id=$1", groupID, body.AttendanceStage, body.Priority)
			if err != nil {
				fail()
				return
			}
			affected, _ := result.RowsAffected()
			if affected == 0 {
				respond(w, 404, map[string]string{"error": "not_found"})
				return
			}
			respond(w, 200, map[string]any{"id": groupID, "attendance_stage": body.AttendanceStage, "priority": body.Priority})
		case r.Method == "POST" && isGroupMessagePath(r.URL.Path):
			groupID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/groups/"), "/messages")
			if !groupPattern.MatchString(groupID) {
				respond(w, 400, map[string]string{"error": "invalid_group_id"})
				return
			}
			var body struct {
				Text string `json:"text"`
			}
			dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16384))
			dec.DisallowUnknownFields()
			if dec.Decode(&body) != nil || dec.Decode(new(any)) != io.EOF || strings.TrimSpace(body.Text) == "" || len(body.Text) > 8000 {
				respond(w, 400, map[string]string{"error": "text deve ter entre 1 e 8000 bytes"})
				return
			}
			payload := struct {
				GroupID string `json:"group_id"`
				Text    string `json:"text"`
			}{GroupID: groupID, Text: body.Text}
			id, err := enqueue(ctx, db, "send_group", payload)
			if err != nil {
				respondQueueError(w, err, fail)
				return
			}
			respond(w, 202, map[string]string{"id": id, "status": "pending", "poll": "/v1/commands/" + id})
		case r.Method == "GET" && isGroupMessagePath(r.URL.Path):
			groupID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/groups/"), "/messages")
			if !groupPattern.MatchString(groupID) {
				respond(w, 400, map[string]string{"error": "invalid_group_id"})
				return
			}
			rows, err := db.QueryContext(ctx, `SELECT recent.seq,recent.message_id,recent.sender,recent.text,recent.timestamp,recent.from_me,
				COALESCE(mm.id,0),COALESCE(mm.media_type,''),COALESCE(mm.mime_type,''),COALESCE(mm.file_name,''),COALESCE(mm.caption,''),COALESCE(mm.file_size,0),COALESCE(mm.duration_seconds,0)
				FROM (SELECT seq,message_id,chat,sender,text,timestamp,from_me FROM messages WHERE chat=$1 ORDER BY seq DESC LIMIT 200) recent
			LEFT JOIN message_media mm ON mm.message_id=recent.message_id AND mm.chat=recent.chat ORDER BY recent.seq`, groupID)
			if err != nil {
				fail()
				return
			}
			defer rows.Close()
			items := []map[string]any{}
			for rows.Next() {
				var seq int64
				var messageID, sender, text string
				var timestamp time.Time
				var fromMe bool
				var mediaID, fileSize int64
				var mediaType, mimeType, fileName, caption string
				var duration int
				if rows.Scan(&seq, &messageID, &sender, &text, &timestamp, &fromMe, &mediaID, &mediaType, &mimeType, &fileName, &caption, &fileSize, &duration) != nil {
					fail()
					return
				}
				items = append(items, map[string]any{"seq": seq, "id": messageID, "sender": sender, "text": text, "timestamp": timestamp, "from_me": fromMe, "media": mediaJSON(mediaID, mediaType, mimeType, fileName, caption, fileSize, duration)})
			}
			respond(w, 200, map[string]any{"messages": items})
		case r.Method == "POST" && (r.URL.Path == "/v1/connect" || r.URL.Path == "/v1/messages"):
			kind := "connect"
			payload := []byte(`{}`)
			if r.URL.Path == "/v1/messages" {
				kind = "send"
				var body struct {
					To   string `json:"to"`
					Text string `json:"text"`
				}
				dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16384))
				dec.DisallowUnknownFields()
				if dec.Decode(&body) != nil || dec.Decode(new(any)) != io.EOF || !phonePattern.MatchString(body.To) || strings.TrimSpace(body.Text) == "" || len(body.Text) > 8000 {
					respond(w, 400, map[string]string{"error": "use to com DDI, apenas dígitos, e text entre 1 e 8000 bytes"})
					return
				}
				payload, _ = json.Marshal(body)
			}
			id, err := enqueueRaw(ctx, db, kind, payload)
			if err != nil {
				respondQueueError(w, err, fail)
				return
			}
			respond(w, 202, map[string]string{"id": id, "status": "pending", "poll": "/v1/commands/" + id})
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/v1/commands/"):
			var status string
			var result []byte
			err := db.QueryRowContext(ctx, "SELECT status,result FROM commands WHERE id=$1", strings.TrimPrefix(r.URL.Path, "/v1/commands/")).Scan(&status, &result)
			if err == sql.ErrNoRows {
				respond(w, 404, map[string]string{"error": "not_found"})
				return
			}
			if err != nil {
				fail()
				return
			}
			var value any
			if len(result) > 0 {
				_ = json.Unmarshal(result, &value)
			}
			respond(w, 200, map[string]any{"status": status, "result": value})
		case r.Method == "GET" && r.URL.Path == "/v1/messages":
			after := r.URL.Query().Get("after")
			if after == "" {
				after = "0"
			}
			if after == "latest" {
				var latest int64
				if db.QueryRowContext(ctx, "SELECT COALESCE(max(seq),0) FROM messages").Scan(&latest) != nil {
					fail()
					return
				}
				respond(w, 200, map[string]any{"messages": []any{}, "next_after": fmtCursor(latest)})
				return
			}
			if !regexp.MustCompile(`^[0-9]{1,18}$`).MatchString(after) {
				respond(w, 400, map[string]string{"error": "invalid_cursor"})
				return
			}
			rows, err := db.QueryContext(ctx, `SELECT m.seq,m.message_id,m.chat,m.sender,m.text,m.timestamp,m.from_me,
				CASE WHEN m.chat LIKE '%@s.whatsapp.net' THEN split_part(m.chat,'@',1)
				WHEN m.chat LIKE '%@lid' THEN COALESCE((SELECT pn FROM whatsmeow_lid_map WHERE lid=split_part(m.chat,'@',1)),'') ELSE '' END
				FROM messages m WHERE m.seq>$1 ORDER BY m.seq LIMIT 100`, after)
			if err != nil {
				fail()
				return
			}
			defer rows.Close()
			items := []map[string]any{}
			for rows.Next() {
				var seq int64
				var id, chat, sender, text string
				var contactPhone string
				var ts time.Time
				var fromMe bool
				if rows.Scan(&seq, &id, &chat, &sender, &text, &ts, &fromMe, &contactPhone) != nil {
					fail()
					return
				}
				items = append(items, map[string]any{"seq": seq, "id": id, "chat": chat, "sender": sender, "text": text, "timestamp": ts, "from_me": fromMe, "contact_phone": contactPhone})
				after = fmtCursor(seq)
			}
			if rows.Err() != nil {
				fail()
				return
			}
			respond(w, 200, map[string]any{"messages": items, "next_after": after})
		default:
			respond(w, 404, map[string]string{"error": "not_found"})
		}
	})
}
func fmtCursor(n int64) string { return strconv.FormatInt(n, 10) }

var errWorkerOffline = errors.New("worker offline")

func enqueue(ctx context.Context, db *sql.DB, kind string, payload any) (string, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return enqueueRaw(ctx, db, kind, raw)
}

func enqueueRaw(ctx context.Context, db *sql.DB, kind string, payload []byte) (string, error) {
	var alive bool
	if err := db.QueryRowContext(ctx, "SELECT heartbeat>now()-interval '20 seconds' FROM state WHERE id=1").Scan(&alive); err != nil {
		return "", err
	}
	if !alive {
		return "", errWorkerOffline
	}
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	id := hex.EncodeToString(b)
	_, err := db.ExecContext(ctx, "INSERT INTO commands(id,kind,payload) VALUES($1,$2,$3)", id, kind, string(payload))
	return id, err
}

func respondQueueError(w http.ResponseWriter, err error, databaseFailure func()) {
	if err == errWorkerOffline {
		respond(w, 503, map[string]string{"error": "worker_offline"})
		return
	}
	databaseFailure()
}

func isGroupMessagePath(path string) bool {
	return strings.HasPrefix(path, "/v1/groups/") && strings.HasSuffix(path, "/messages")
}

