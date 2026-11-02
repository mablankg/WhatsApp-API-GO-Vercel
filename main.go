package main

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"encoding/json"
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
)

var phonePattern = regexp.MustCompile(`^[1-9][0-9]{7,14}$`)

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
			var alive bool
			if db.QueryRowContext(ctx, "SELECT heartbeat>now()-interval '20 seconds' FROM state WHERE id=1").Scan(&alive) != nil {
				fail()
				return
			}
			if !alive {
				respond(w, 503, map[string]string{"error": "worker_offline"})
				return
			}
			b := make([]byte, 16)
			if _, err := rand.Read(b); err != nil {
				fail()
				return
			}
			id := hex.EncodeToString(b)
			if _, err := db.ExecContext(ctx, "INSERT INTO commands(id,kind,payload) VALUES($1,$2,$3)", id, kind, string(payload)); err != nil {
				fail()
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
			if !regexp.MustCompile(`^[0-9]{1,18}$`).MatchString(after) {
				respond(w, 400, map[string]string{"error": "invalid_cursor"})
				return
			}
			rows, err := db.QueryContext(ctx, "SELECT seq,message_id,chat,sender,text,timestamp FROM messages WHERE seq>$1 ORDER BY seq LIMIT 100", after)
			if err != nil {
				fail()
				return
			}
			defer rows.Close()
			items := []map[string]any{}
			for rows.Next() {
				var seq int64
				var id, chat, sender, text string
				var ts time.Time
				if rows.Scan(&seq, &id, &chat, &sender, &text, &ts) != nil {
					fail()
					return
				}
				items = append(items, map[string]any{"seq": seq, "id": id, "chat": chat, "sender": sender, "text": text, "timestamp": ts})
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
