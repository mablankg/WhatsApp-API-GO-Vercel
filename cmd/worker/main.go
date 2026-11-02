package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"github.com/mablankg/WhatsApp-API-GO-Vercel/internal/data"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type worker struct {
	db      *sql.DB
	client  *whatsmeow.Client
	mu      sync.Mutex
	pairing bool
	ctx     context.Context
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	boot, stop := context.WithTimeout(ctx, 30*time.Second)
	db, err := data.Open(boot, true)
	stop()
	if err != nil {
		log.Fatal("falha ao inicializar banco; confira conexão Session Pooler, SSL e permissões")
	}
	defer db.Close()
	// A dedicated session lock prevents overlapping deployments from using the same WhatsApp session.
	lock, err := db.Conn(ctx)
	if err != nil {
		log.Fatal("falha ao reservar conexão")
	}
	defer lock.Close()
	for ctx.Err() == nil {
		var held bool
		err = lock.QueryRowContext(ctx, "SELECT pg_try_advisory_lock(728104562)").Scan(&held)
		if err != nil {
			log.Fatal("falha no lock do worker")
		}
		if held {
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
	if ctx.Err() != nil {
		return
	}
	defer lock.ExecContext(context.Background(), "SELECT pg_advisory_unlock(728104562)")
	store := sqlstore.NewWithDB(db, "postgres", nil)
	if err = store.Upgrade(ctx); err != nil {
		log.Fatal("falha ao criar tabelas whatsmeow")
	}
	device, err := store.GetFirstDevice(ctx)
	if err != nil {
		log.Fatal("falha ao recuperar sessão")
	}
	w := &worker{db: db, ctx: ctx, client: whatsmeow.NewClient(device, nil)}
	w.client.AddEventHandler(w.event)
	if _, err = db.ExecContext(ctx, "UPDATE commands SET status='unknown',result='{"+`"error":"worker_restarted_check_delivery_before_retry"`+"}' WHERE status='running'"); err != nil {
		log.Fatal("falha ao recuperar comandos")
	}
	w.state("disconnected", "", 0)
	defer w.client.Disconnect()
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				check, done := context.WithTimeout(ctx, 4*time.Second)
				// Use the lock connection itself: losing this session must stop the client.
				_, err := lock.ExecContext(check, "UPDATE state SET heartbeat=now() WHERE id=1")
				done()
				if err != nil {
					log.Print("conexão de coordenação perdida; encerrando worker")
					cancel()
					w.client.Disconnect()
					return
				}
			}
		}
	}()
	if device.ID != nil {
		if err = w.client.Connect(); err != nil {
			log.Print("reconexão falhou; use POST /v1/connect")
		}
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.next()
		}
	}
}

func (w *worker) state(status, qr string, ttl time.Duration) {
	ctx, cancel := context.WithTimeout(w.ctx, 5*time.Second)
	defer cancel()
	_, err := w.db.ExecContext(ctx, "UPDATE state SET status=$1,qr=$2,qr_expires=$3,heartbeat=now() WHERE id=1", status, qr, time.Now().Add(ttl))
	if err != nil {
		log.Print("falha ao atualizar status")
	}
}
func (w *worker) event(event any) {
	switch e := event.(type) {
	case *events.Connected:
		w.state("connected", "", 0)
	case *events.Disconnected:
		w.state("disconnected", "", 0)
	case *events.LoggedOut:
		w.state("logged_out", "", 0)
	case *events.Message:
		if e.Info.IsFromMe {
			return
		}
		text := e.Message.GetConversation()
		if text == "" {
			text = e.Message.GetExtendedTextMessage().GetText()
		}
		ctx, cancel := context.WithTimeout(w.ctx, 5*time.Second)
		defer cancel()
		_, err := w.db.ExecContext(ctx, "INSERT INTO messages(message_id,chat,sender,text,timestamp) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING", e.Info.ID, e.Info.Chat.String(), e.Info.Sender.String(), text, e.Info.Timestamp)
		if err != nil {
			log.Print("falha ao guardar mensagem recebida")
		}
	}
}
func (w *worker) connect() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.client.IsConnected() || w.pairing {
		return nil
	}
	if w.client.Store.ID != nil {
		return w.client.Connect()
	}
	ch, err := w.client.GetQRChannel(w.ctx)
	if err != nil {
		return err
	}
	if err = w.client.Connect(); err != nil {
		return err
	}
	w.pairing = true
	go func() {
		defer func() { w.mu.Lock(); w.pairing = false; w.mu.Unlock() }()
		for ev := range ch {
			if ev.Event == "code" {
				w.state("qr", ""+ev.Code, ev.Timeout)
			} else if ev.Event == "success" {
				w.state("connected", "", 0)
			} else {
				w.state(ev.Event, "", 0)
			}
		}
	}()
	return nil
}
func (w *worker) next() {
	ctx, cancel := context.WithTimeout(w.ctx, 30*time.Second)
	defer cancel()
	var id, kind string
	var raw []byte
	err := w.db.QueryRowContext(ctx, `UPDATE commands SET status='running' WHERE id=(SELECT id FROM commands WHERE status='pending' ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED) RETURNING id,kind,payload`).Scan(&id, &kind, &raw)
	if err == sql.ErrNoRows {
		return
	}
	if err != nil {
		log.Print("falha ao buscar comando")
		return
	}
	result := map[string]string{}
	status := "done"
	switch kind {
	case "connect":
		if w.client.Store.ID == nil && w.client.Store.Initialized {
			result["error"] = "session_logged_out_restart_worker"
			status = "failed"
		} else if w.connect() != nil {
			result["error"] = "connection_failed"
			status = "failed"
		}
	case "send":
		var p struct {
			To   string
			Text string
		}
		if json.Unmarshal(raw, &p) != nil || !w.client.IsLoggedIn() {
			result["error"] = "whatsapp_not_connected"
			status = "failed"
			break
		}
		response, err := w.client.SendMessage(ctx, types.NewJID(p.To, types.DefaultUserServer), &waE2E.Message{Conversation: proto.String(p.Text)})
		if err != nil {
			result["error"] = "delivery_unknown_check_before_retry"
			status = "unknown"
		} else {
			result["message_id"] = response.ID
		}
	default:
		result["error"] = "unsupported_command"
		status = "failed"
	}
	encoded, _ := json.Marshal(result)
	saveCtx, saveCancel := context.WithTimeout(w.ctx, 5*time.Second)
	defer saveCancel()
	if _, err = w.db.ExecContext(saveCtx, "UPDATE commands SET status=$2,result=$3 WHERE id=$1", id, status, string(encoded)); err != nil {
		log.Print("falha ao salvar resultado; comando não será reenviado automaticamente")
	}
}
