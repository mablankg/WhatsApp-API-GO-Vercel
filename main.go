package main

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"database/sql"
	"encoding/base64"
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
		if strings.HasPrefix(r.URL.Path, "/v1/accounts") {
			handleAccounts(w, r, db)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/v1/lawyers") {
			handleLawyers(w, r, db)
			return
		}
		if r.Method == http.MethodPost && r.URL.Path == "/v1/directory/reset" {
			id, err := enqueueRaw(ctx, db, requestAccount(r), "reset_directory", []byte(`{}`))
			if err != nil {
				respondQueueError(w, err, fail)
				return
			}
			respond(w, http.StatusAccepted, map[string]string{"id": id, "status": "pending", "poll": "/v1/commands/" + id})
			return
		}
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
			accountID := requestAccount(r)
			var status string
			var heartbeat time.Time
			if db.QueryRowContext(ctx, "SELECT status,heartbeat FROM whatsapp_accounts WHERE id=$1 AND enabled=true", accountID).Scan(&status, &heartbeat) != nil {
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
			err := db.QueryRowContext(ctx, "SELECT qr FROM whatsapp_accounts WHERE id=$1 AND qr<>'' AND qr_expires>now() AND heartbeat>now()-interval '20 seconds'", requestAccount(r)).Scan(&code)
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
			if r.URL.Query().Get("format") == "json" {
				respond(w, 200, map[string]string{"image": "data:image/png;base64," + base64.StdEncoding.EncodeToString(png)})
			} else {
				w.Header().Set("Content-Type", "image/png")
				w.Write(png)
			}
		case r.Method == "GET" && r.URL.Path == "/v1/groups":
			rows, err := db.QueryContext(ctx, `SELECT group_id,name,topic,participant_count,is_announce,attendance_stage,priority,updated_at,COALESCE(lawyer_id,0),COALESCE((SELECT name FROM lawyers WHERE id=groups.lawyer_id),''),COALESCE((SELECT color FROM lawyers WHERE id=groups.lawyer_id),''),archived FROM groups WHERE account_id=$1 AND archived=false ORDER BY lower(name),group_id`, requestAccount(r))
			if err != nil {
				fail()
				return
			}
			defer rows.Close()
			items := []map[string]any{}
			for rows.Next() {
				var id, name, topic, stage, priority, lawyerName, lawyerColor string
				var count int
				var lawyerID int64
				var announce bool
				var archived bool
				var updated time.Time
				if rows.Scan(&id, &name, &topic, &count, &announce, &stage, &priority, &updated, &lawyerID, &lawyerName, &lawyerColor, &archived) != nil {
					fail()
					return
				}
				items = append(items, map[string]any{"id": id, "name": name, "topic": topic, "participant_count": count, "is_announce": announce, "attendance_stage": stage, "priority": priority, "lawyer_id": lawyerID, "lawyer_name": lawyerName, "lawyer_color": lawyerColor, "archived": archived, "updated_at": updated})
			}
			if rows.Err() != nil {
				fail()
				return
			}
			respond(w, 200, map[string]any{"groups": items})
		case r.Method == "POST" && r.URL.Path == "/v1/groups/sync":
			id, err := enqueueRaw(ctx, db, requestAccount(r), "sync_groups", []byte(`{}`))
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
			id, err := enqueueAccount(ctx, db, requestAccount(r), "create_group", body)
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
				LawyerID        int64  `json:"lawyer_id"`
				Archived        bool   `json:"archived"`
			}
			dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
			dec.DisallowUnknownFields()
			if dec.Decode(&body) != nil || dec.Decode(new(any)) != io.EOF {
				respond(w, 400, map[string]string{"error": "invalid_group"})
				return
			}
			validPriority := map[string]bool{"low": true, "normal": true, "high": true, "urgent": true}[body.Priority]
			validLawyer := body.LawyerID == 0
			if !validLawyer {
				_ = db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM lawyers WHERE id=$1 AND active=true)", body.LawyerID).Scan(&validLawyer)
			}
			var validStage bool
			_ = db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM attendance_stages WHERE id=$1 AND active=true)", body.AttendanceStage).Scan(&validStage)
			if !validStage || !validPriority || !validLawyer {
				respond(w, 400, map[string]string{"error": "invalid_group_classification"})
				return
			}
			result, err := db.ExecContext(ctx, "UPDATE groups SET attendance_stage=$3,priority=$4,lawyer_id=NULLIF($5,0),archived=$6 WHERE account_id=$1 AND group_id=$2", requestAccount(r), groupID, body.AttendanceStage, body.Priority, body.LawyerID, body.Archived)
			if err != nil {
				fail()
				return
			}
			affected, _ := result.RowsAffected()
			if affected == 0 {
				respond(w, 404, map[string]string{"error": "not_found"})
				return
			}
			respond(w, 200, map[string]any{"id": groupID, "attendance_stage": body.AttendanceStage, "priority": body.Priority, "lawyer_id": body.LawyerID, "archived": body.Archived})
		case r.Method == "POST" && isGroupMessagePath(r.URL.Path):
			groupID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/groups/"), "/messages")
			if !groupPattern.MatchString(groupID) {
				respond(w, 400, map[string]string{"error": "invalid_group_id"})
				return
			}
			var body struct {
				Text  string `json:"text"`
				Media struct {
					Type     string `json:"type"`
					MimeType string `json:"mime_type"`
					FileName string `json:"file_name"`
					Caption  string `json:"caption"`
					Data     string `json:"data"`
				} `json:"media"`
			}
			dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 35<<20))
			dec.DisallowUnknownFields()
			if dec.Decode(&body) != nil || dec.Decode(new(any)) != io.EOF || (strings.TrimSpace(body.Text) == "" && body.Media.Data == "") || len(body.Text) > 8000 || len(body.Media.Data) > 34<<20 {
				respond(w, 400, map[string]string{"error": "text deve ter entre 1 e 8000 bytes"})
				return
			}
			payload := struct {
				GroupID string `json:"group_id"`
				Text    string `json:"text"`
				Media   any    `json:"media,omitempty"`
			}{GroupID: groupID, Text: body.Text, Media: body.Media}
			id, err := enqueueAccount(ctx, db, requestAccount(r), "send_group", payload)
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
				FROM (SELECT seq,message_id,chat,sender,text,timestamp,from_me FROM messages WHERE account_id=$1 AND chat=$2 ORDER BY seq DESC LIMIT 200) recent
			LEFT JOIN message_media mm ON mm.account_id=$1 AND mm.message_id=recent.message_id AND mm.chat=recent.chat ORDER BY recent.seq`, requestAccount(r), groupID)
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
		case r.Method == "POST" && (r.URL.Path == "/v1/connect" || r.URL.Path == "/v1/disconnect" || r.URL.Path == "/v1/messages"):
			kind := "connect"
			payload := []byte(`{}`)
			if r.URL.Path == "/v1/disconnect" {
				kind = "disconnect"
			}
			if r.URL.Path == "/v1/messages" {
				kind = "send"
				var body struct {
					To    string `json:"to"`
					Text  string `json:"text"`
					Media struct {
						Type     string `json:"type"`
						MimeType string `json:"mime_type"`
						FileName string `json:"file_name"`
						Caption  string `json:"caption"`
						Data     string `json:"data"`
					} `json:"media"`
				}
				dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 35<<20))
				dec.DisallowUnknownFields()
				if dec.Decode(&body) != nil || dec.Decode(new(any)) != io.EOF || !phonePattern.MatchString(body.To) || (strings.TrimSpace(body.Text) == "" && body.Media.Data == "") || len(body.Text) > 8000 || len(body.Media.Data) > 34<<20 {
					respond(w, 400, map[string]string{"error": "use to com DDI, apenas dígitos, e text entre 1 e 8000 bytes"})
					return
				}
				payload, _ = json.Marshal(body)
			}
			id, err := enqueueRaw(ctx, db, requestAccount(r), kind, payload)
			if err != nil {
				respondQueueError(w, err, fail)
				return
			}
			respond(w, 202, map[string]string{"id": id, "status": "pending", "poll": "/v1/commands/" + id})
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/v1/commands/"):
			var status string
			var result []byte
			err := db.QueryRowContext(ctx, "SELECT status,result FROM commands WHERE id=$1 AND account_id=$2", strings.TrimPrefix(r.URL.Path, "/v1/commands/"), requestAccount(r)).Scan(&status, &result)
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
				if db.QueryRowContext(ctx, "SELECT COALESCE(max(seq),0) FROM messages WHERE account_id=$1", requestAccount(r)).Scan(&latest) != nil {
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
				FROM messages m WHERE m.account_id=$1 AND m.seq>$2 ORDER BY m.seq LIMIT 100`, requestAccount(r), after)
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
	return enqueueAccount(ctx, db, "primary", kind, payload)
}

func enqueueAccount(ctx context.Context, db *sql.DB, accountID, kind string, payload any) (string, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return enqueueRaw(ctx, db, accountID, kind, raw)
}

func enqueueRaw(ctx context.Context, db *sql.DB, accountID, kind string, payload []byte) (string, error) {
	var alive bool
	if err := db.QueryRowContext(ctx, "SELECT heartbeat>now()-interval '20 seconds' FROM whatsapp_accounts WHERE id=$1 AND enabled=true", accountID).Scan(&alive); err != nil {
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
	_, err := db.ExecContext(ctx, "INSERT INTO commands(id,account_id,kind,payload) VALUES($1,$2,$3,$4)", id, accountID, kind, string(payload))
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

