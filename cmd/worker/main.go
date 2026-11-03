package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
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
	"regexp"
	"strconv"
	"strings"
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
	groupTicker := time.NewTicker(time.Minute)
	defer groupTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.next()
		case <-groupTicker.C:
			go w.refreshGroups()
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
		go func() { _, _ = w.refreshGroups() }()
		go func() { _, _ = w.refreshContacts() }()
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
		if e.Info.Chat.Server == types.DefaultUserServer {
			_, err = w.db.ExecContext(ctx, `INSERT INTO contacts(whatsapp_id,phone,name,first_contact_at,last_contact_at)
				VALUES($1,$2,$3,$4,$4) ON CONFLICT(whatsapp_id) DO UPDATE SET
				name=CASE WHEN contacts.name='' THEN excluded.name ELSE contacts.name END,
				last_contact_at=GREATEST(contacts.last_contact_at,excluded.last_contact_at),updated_at=now()`,
				e.Info.Chat.String(), e.Info.Chat.User, e.Info.PushName, e.Info.Timestamp)
			if err != nil {
				log.Print("falha ao atualizar contato")
			}
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
	ctx, cancel := context.WithTimeout(w.ctx, 60*time.Second)
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
	case "create_group":
		var p struct {
			Name         string
			Participants []string
		}
		if json.Unmarshal(raw, &p) != nil || !w.client.IsLoggedIn() {
			result["error"] = "whatsapp_not_connected"
			status = "failed"
			break
		}
		participants := make([]types.JID, len(p.Participants))
		for i, participant := range p.Participants {
			participants[i] = types.NewJID(participant, types.DefaultUserServer)
		}
		group, err := w.client.CreateGroup(ctx, whatsmeow.ReqCreateGroup{Name: p.Name, Participants: participants})
		if err != nil {
			result["error"] = "group_creation_failed"
			status = "failed"
		} else {
			result["group_id"] = group.JID.String()
			result["name"] = group.Name
			w.upsertGroup(group)
		}
	case "send_group":
		var p struct {
			GroupID string `json:"group_id"`
			Text    string `json:"text"`
		}
		if json.Unmarshal(raw, &p) != nil {
			result["error"] = "invalid_group_or_not_connected"
			status = "failed"
			break
		}
		groupJID, parseErr := types.ParseJID(p.GroupID)
		if parseErr != nil || groupJID.Server != types.GroupServer || !w.client.IsLoggedIn() {
			result["error"] = "invalid_group_or_not_connected"
			status = "failed"
			break
		}
		response, err := w.client.SendMessage(ctx, groupJID, &waE2E.Message{Conversation: proto.String(p.Text)})
		if err != nil {
			result["error"] = "delivery_unknown_check_before_retry"
			status = "unknown"
		} else {
			result["message_id"] = response.ID
		}
	case "sync_groups":
		if !w.client.IsLoggedIn() {
			result["error"] = "whatsapp_not_connected"
			status = "failed"
			break
		}
		count, err := w.refreshGroups()
		if err != nil {
			result["error"] = "group_sync_failed"
			status = "failed"
		} else {
			result["group_count"] = strconv.Itoa(count)
		}
	case "sync_contacts":
		if !w.client.IsLoggedIn() {
			result["error"] = "whatsapp_not_connected"
			status = "failed"
			break
		}
		count, err := w.refreshContacts()
		if err != nil {
			result["error"] = "contact_sync_failed"
			status = "failed"
		} else {
			result["contact_count"] = strconv.Itoa(count)
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

func (w *worker) refreshGroups() (int, error) {
	if !w.client.IsLoggedIn() {
		return 0, errors.New("whatsapp not connected")
	}
	ctx, cancel := context.WithTimeout(w.ctx, 45*time.Second)
	defer cancel()
	groups, err := w.client.GetJoinedGroups(ctx)
	if err != nil {
		log.Printf("falha ao atualizar grupos: %v", err)
		return 0, err
	}
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "DELETE FROM groups"); err != nil {
		return 0, err
	}
	for _, group := range groups {
		if _, err = tx.ExecContext(ctx, `INSERT INTO groups(group_id,name,topic,participant_count,is_announce,updated_at) VALUES($1,$2,$3,$4,$5,now())`, group.JID.String(), group.Name, group.Topic, len(group.Participants), group.IsAnnounce); err != nil {
			return 0, err
		}
	}
	if err = tx.Commit(); err != nil {
		log.Printf("falha ao salvar grupos: %v", err)
		return 0, err
	}
	return len(groups), nil
}

func (w *worker) upsertGroup(group *types.GroupInfo) {
	ctx, cancel := context.WithTimeout(w.ctx, 5*time.Second)
	defer cancel()
	_, err := w.db.ExecContext(ctx, `INSERT INTO groups(group_id,name,topic,participant_count,is_announce,updated_at) VALUES($1,$2,$3,$4,$5,now()) ON CONFLICT(group_id) DO UPDATE SET name=excluded.name,topic=excluded.topic,participant_count=excluded.participant_count,is_announce=excluded.is_announce,updated_at=now()`, group.JID.String(), group.Name, group.Topic, len(group.Participants), group.IsAnnounce)
	if err != nil {
		log.Printf("falha ao salvar grupo criado: %v", err)
	}
}

func (w *worker) refreshContacts() (int, error) {
	ctx, cancel := context.WithTimeout(w.ctx, 45*time.Second)
	defer cancel()
	items, err := w.client.Store.Contacts.GetAllContacts(ctx)
	if err != nil {
		return 0, err
	}
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	count := 0
	for jid, info := range items {
		phoneJID := jid
		if jid.Server == types.HiddenUserServer {
			phoneJID, err = w.client.Store.LIDs.GetPNForLID(ctx, jid)
			if err != nil {
				continue
			}
		}
		if phoneJID.Server != types.DefaultUserServer || !phonePatternWorker.MatchString(phoneJID.User) {
			continue
		}
		name := firstContactName(info.FullName, info.BusinessName, info.PushName, info.FirstName)
		_, err = tx.ExecContext(ctx, `INSERT INTO contacts(whatsapp_id,phone,name) VALUES($1,$2,$3)
			ON CONFLICT(phone) DO UPDATE SET name=CASE WHEN contacts.name='' THEN excluded.name ELSE contacts.name END,updated_at=now()`, phoneJID.String(), phoneJID.User, name)
		if err != nil {
			return 0, err
		}
		count++
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return count, nil
}

var phonePatternWorker = regexp.MustCompile(`^[1-9][0-9]{7,14}$`)

func firstContactName(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

