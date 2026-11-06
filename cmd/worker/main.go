package main

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"github.com/mablankg/WhatsApp-API-GO-Vercel/internal/data"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store"
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
	accountID string
	db        *sql.DB
	client    *whatsmeow.Client
	mu        sync.Mutex
	pairing   bool
	ctx       context.Context
}

type outboundMedia struct {
	Type     string `json:"type"`
	MimeType string `json:"mime_type"`
	FileName string `json:"file_name"`
	Caption  string `json:"caption"`
	Data     string `json:"data"`
}

func (w *worker) sendMessage(ctx context.Context, to types.JID, text string, media outboundMedia) (types.MessageID, error) {
	if media.Data == "" {
		response, err := w.client.SendMessage(ctx, to, &waE2E.Message{Conversation: proto.String(text)})
		return response.ID, err
	}
	raw, err := base64.StdEncoding.DecodeString(media.Data)
	if err != nil {
		return "", err
	}
	if len(raw) > 50*1024*1024 {
		return "", errors.New("media_too_large")
	}
	appInfo := whatsmeow.MediaDocument
	switch media.Type {
	case "image":
		appInfo = whatsmeow.MediaImage
	case "audio":
		appInfo = whatsmeow.MediaAudio
	case "video":
		appInfo = whatsmeow.MediaVideo
	}
	upload, err := w.client.Upload(ctx, raw, appInfo)
	if err != nil {
		return "", err
	}
	mime := media.MimeType
	if mime == "" {
		mime = "application/octet-stream"
	}
	caption := media.Caption
	if caption == "" {
		caption = text
	}
	var msg *waE2E.Message
	if media.Type == "image" {
		msg = &waE2E.Message{ImageMessage: &waE2E.ImageMessage{URL: &upload.URL, DirectPath: &upload.DirectPath, MediaKey: upload.MediaKey, FileEncSHA256: upload.FileEncSHA256, FileSHA256: upload.FileSHA256, FileLength: &upload.FileLength, Mimetype: proto.String(mime), Caption: proto.String(caption)}}
	} else if media.Type == "audio" {
		msg = &waE2E.Message{AudioMessage: &waE2E.AudioMessage{URL: &upload.URL, DirectPath: &upload.DirectPath, MediaKey: upload.MediaKey, FileEncSHA256: upload.FileEncSHA256, FileSHA256: upload.FileSHA256, FileLength: &upload.FileLength, Mimetype: proto.String(mime), PTT: proto.Bool(false), StreamingSidecar: upload.StreamingSidecar}}
	} else if media.Type == "video" {
		msg = &waE2E.Message{VideoMessage: &waE2E.VideoMessage{URL: &upload.URL, DirectPath: &upload.DirectPath, MediaKey: upload.MediaKey, FileEncSHA256: upload.FileEncSHA256, FileSHA256: upload.FileSHA256, FileLength: &upload.FileLength, Mimetype: proto.String(mime), Caption: proto.String(caption)}}
	} else {
		msg = &waE2E.Message{DocumentMessage: &waE2E.DocumentMessage{URL: &upload.URL, DirectPath: &upload.DirectPath, MediaKey: upload.MediaKey, FileEncSHA256: upload.FileEncSHA256, FileSHA256: upload.FileSHA256, FileLength: &upload.FileLength, Mimetype: proto.String(mime), FileName: proto.String(media.FileName), Title: proto.String(media.FileName), Caption: proto.String(caption)}}
	}
	response, err := w.client.SendMessage(ctx, to, msg)
	return response.ID, err
}

type manager struct {
	db       *sql.DB
	store    *sqlstore.Container
	ctx      context.Context
	mu       sync.Mutex
	sessions map[string]*worker
	devices  map[string]*store.Device
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
	container := sqlstore.NewWithDB(db, "postgres", nil)
	if err = container.Upgrade(ctx); err != nil {
		log.Fatal("falha ao criar tabelas whatsmeow")
	}
	devices, err := container.GetAllDevices(ctx)
	if err != nil {
		log.Fatal("falha ao recuperar sessões")
	}
	m := &manager{db: db, store: container, ctx: ctx, sessions: map[string]*worker{}, devices: map[string]*store.Device{}}
	for _, device := range devices {
		if device.ID != nil {
			m.devices[device.ID.String()] = device
		}
	}
	if _, err = db.ExecContext(ctx, "UPDATE commands SET status='unknown',result='{"+`"error":"worker_restarted_check_delivery_before_retry"`+"}' WHERE status='running'"); err != nil {
		log.Fatal("falha ao recuperar comandos")
	}
	if err = m.syncAccounts(); err != nil {
		log.Fatal("falha ao carregar conexões")
	}
	defer m.disconnectAll()
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
				_, err := lock.ExecContext(check, "UPDATE state SET heartbeat=now() WHERE id=1; UPDATE whatsapp_accounts SET heartbeat=now() WHERE enabled=true")
				done()
				if err != nil {
					log.Print("conexão de coordenação perdida; encerrando worker")
					cancel()
					m.disconnectAll()
					return
				}
			}
		}
	}()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	groupTicker := time.NewTicker(30 * time.Minute)
	defer groupTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := m.syncAccounts(); err != nil {
				log.Print("falha ao atualizar conexões")
			}
			m.next()
		case <-groupTicker.C:
			m.refreshGroups()
		}
	}
}

func (m *manager) syncAccounts() error {
	rows, err := m.db.QueryContext(m.ctx, `SELECT id,device_jid FROM whatsapp_accounts WHERE enabled=true ORDER BY created_at,id`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id, deviceJID string
		if err = rows.Scan(&id, &deviceJID); err != nil {
			return err
		}
		m.mu.Lock()
		_, exists := m.sessions[id]
		m.mu.Unlock()
		if exists {
			continue
		}
		var device *store.Device
		if deviceJID != "" {
			device = m.devices[deviceJID]
		}
		if device == nil && id == "primary" {
			for jid, candidate := range m.devices {
				device, deviceJID = candidate, jid
				break
			}
			if device != nil {
				_, _ = m.db.ExecContext(m.ctx, "UPDATE whatsapp_accounts SET device_jid=$2 WHERE id=$1", id, deviceJID)
			}
		}
		if device == nil {
			device = m.store.NewDevice()
		}
		w := &worker{accountID: id, db: m.db, ctx: m.ctx, client: whatsmeow.NewClient(device, nil)}
		w.client.AddEventHandler(w.event)
		w.state("disconnected", "", 0)
		m.mu.Lock()
		m.sessions[id] = w
		m.mu.Unlock()
		if device.ID != nil {
			go func() {
				if err := w.client.Connect(); err != nil {
					log.Printf("reconexão %s falhou", w.accountID)
				}
			}()
		}
	}
	return rows.Err()
}

func (m *manager) workers() []*worker {
	m.mu.Lock()
	defer m.mu.Unlock()
	items := make([]*worker, 0, len(m.sessions))
	for _, w := range m.sessions {
		items = append(items, w)
	}
	return items
}
func (m *manager) next() {
	for _, w := range m.workers() {
		w.next()
	}
}
func (m *manager) refreshGroups() {
	for _, w := range m.workers() {
		go w.refreshGroups()
	}
}
func (m *manager) disconnectAll() {
	for _, w := range m.workers() {
		w.client.Disconnect()
	}
}

func (w *worker) state(status, qr string, ttl time.Duration) {
	ctx, cancel := context.WithTimeout(w.ctx, 5*time.Second)
	defer cancel()
	var err error
	if w.accountID == "primary" {
		_, err = w.db.ExecContext(ctx, "UPDATE state SET status=$1,qr=$2,qr_expires=$3,heartbeat=now() WHERE id=1", status, qr, time.Now().Add(ttl))
	}
	if err == nil {
		deviceJID := ""
		if w.client.Store.ID != nil {
			deviceJID = w.client.Store.ID.String()
		}
		_, err = w.db.ExecContext(ctx, `UPDATE whatsapp_accounts SET status=$2,qr=$3,qr_expires=$4,heartbeat=now(),
			device_jid=CASE WHEN $5<>'' THEN $5 ELSE device_jid END,updated_at=now() WHERE id=$1`, w.accountID, status, qr, time.Now().Add(ttl), deviceJID)
	}
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
		text := e.Message.GetConversation()
		if text == "" {
			text = e.Message.GetExtendedTextMessage().GetText()
		}
		ctx, cancel := context.WithTimeout(w.ctx, 5*time.Second)
		defer cancel()
		_, err := w.db.ExecContext(ctx, "INSERT INTO messages(account_id,message_id,chat,sender,text,timestamp,from_me) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING", w.accountID, e.Info.ID, e.Info.Chat.String(), e.Info.Sender.String(), text, e.Info.Timestamp, e.Info.IsFromMe)
		if err != nil {
			log.Print("falha ao guardar mensagem recebida")
		}
		go w.storeMedia(e)
		contactJID := e.Info.Chat
		if contactJID.Server == types.HiddenUserServer {
			if phoneJID, mapErr := w.client.Store.LIDs.GetPNForLID(ctx, contactJID); mapErr == nil && !phoneJID.IsEmpty() {
				contactJID = phoneJID
			}
		}
		if contactJID.Server == types.DefaultUserServer {
			_, err = w.db.ExecContext(ctx, `INSERT INTO contacts(account_id,whatsapp_id,phone,name,first_contact_at,last_contact_at)
				VALUES($1,$2,$3,$4,$5,$5) ON CONFLICT(account_id,whatsapp_id) DO UPDATE SET
				name=CASE WHEN contacts.name='' THEN excluded.name ELSE contacts.name END,
				last_contact_at=GREATEST(contacts.last_contact_at,excluded.last_contact_at),updated_at=now()`,
				w.accountID, contactJID.String(), contactJID.User, e.Info.PushName, e.Info.Timestamp)
			if err != nil {
				log.Print("falha ao atualizar contato")
			}
		}
	}
}

const maxMediaBytes = 16 * 1024 * 1024

func (w *worker) storeMedia(event *events.Message) {
	var downloadable whatsmeow.DownloadableMessage
	mediaType, mimeType, fileName, caption := "", "application/octet-stream", "", ""
	var declaredLength uint64
	var duration uint32
	message := event.Message
	switch {
	case message.GetImageMessage() != nil:
		item := message.GetImageMessage()
		downloadable, mediaType, mimeType, caption, declaredLength = item, "image", item.GetMimetype(), item.GetCaption(), item.GetFileLength()
	case message.GetAudioMessage() != nil:
		item := message.GetAudioMessage()
		downloadable, mediaType, mimeType, declaredLength, duration = item, "audio", item.GetMimetype(), item.GetFileLength(), item.GetSeconds()
	case message.GetVideoMessage() != nil:
		item := message.GetVideoMessage()
		downloadable, mediaType, mimeType, caption, declaredLength, duration = item, "video", item.GetMimetype(), item.GetCaption(), item.GetFileLength(), item.GetSeconds()
	case message.GetDocumentMessage() != nil:
		item := message.GetDocumentMessage()
		downloadable, mediaType, mimeType, fileName, caption, declaredLength = item, "document", item.GetMimetype(), item.GetFileName(), item.GetCaption(), item.GetFileLength()
		if fileName == "" {
			fileName = item.GetTitle()
		}
	case message.GetStickerMessage() != nil:
		item := message.GetStickerMessage()
		downloadable, mediaType, mimeType, declaredLength = item, "sticker", item.GetMimetype(), item.GetFileLength()
	default:
		return
	}
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}
	if declaredLength > maxMediaBytes {
		log.Printf("anexo %s ignorado: excede 16 MB", event.Info.ID)
		return
	}
	ctx, cancel := context.WithTimeout(w.ctx, 90*time.Second)
	defer cancel()
	content, err := w.client.Download(ctx, downloadable)
	if err != nil {
		log.Printf("falha ao baixar anexo %s: %v", event.Info.ID, err)
		return
	}
	if len(content) > maxMediaBytes {
		log.Printf("anexo %s ignorado após download: excede 16 MB", event.Info.ID)
		return
	}
	_, err = w.db.ExecContext(ctx, `INSERT INTO message_media(account_id,message_id,chat,media_type,mime_type,file_name,caption,file_size,duration_seconds,data)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT(account_id,message_id,chat) DO NOTHING`,
		w.accountID, event.Info.ID, event.Info.Chat.String(), mediaType, mimeType, fileName, caption, len(content), duration, content)
	if err != nil {
		log.Printf("falha ao guardar anexo %s: %v", event.Info.ID, err)
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
	err := w.db.QueryRowContext(ctx, `UPDATE commands SET status='running' WHERE id=(SELECT id FROM commands WHERE status='pending' AND account_id=$1 ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED) RETURNING id,kind,payload`, w.accountID).Scan(&id, &kind, &raw)
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
	case "disconnect":
		w.client.Disconnect()
		w.state("disconnected", "", 0)
	case "send":
		var p struct {
			To    string
			Text  string
			Media outboundMedia
		}
		if json.Unmarshal(raw, &p) != nil || !w.client.IsLoggedIn() {
			result["error"] = "whatsapp_not_connected"
			status = "failed"
			break
		}
		messageID, err := w.sendMessage(ctx, types.NewJID(p.To, types.DefaultUserServer), p.Text, p.Media)
		if err != nil {
			result["error"] = "delivery_unknown_check_before_retry"
			status = "unknown"
		} else {
			result["message_id"] = messageID
			_, _ = w.db.ExecContext(ctx, `INSERT INTO messages(account_id,message_id,chat,sender,text,timestamp,from_me) VALUES($1,$2,$3,$4,$5,now(),true) ON CONFLICT DO NOTHING`, w.accountID, messageID, types.NewJID(p.To, types.DefaultUserServer).String(), "me", p.Text)
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
			GroupID string        `json:"group_id"`
			Text    string        `json:"text"`
			Media   outboundMedia `json:"media"`
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
		messageID, err := w.sendMessage(ctx, groupJID, p.Text, p.Media)
		if err != nil {
			result["error"] = "delivery_unknown_check_before_retry"
			status = "unknown"
		} else {
			result["message_id"] = messageID
			_, _ = w.db.ExecContext(ctx, `INSERT INTO messages(account_id,message_id,chat,sender,text,timestamp,from_me) VALUES($1,$2,$3,$4,$5,now(),true) ON CONFLICT DO NOTHING`, w.accountID, messageID, groupJID.String(), "me", p.Text)
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
			log.Printf("falha ao sincronizar contatos: %v", err)
			result["error"] = "contact_sync_failed"
			status = "failed"
		} else {
			result["contact_count"] = strconv.Itoa(count)
		}
	case "reset_directory":
		// Separate statements are required by PostgreSQL's parameterized protocol.
		// Keep both deletions atomic and scoped to the selected WhatsApp account.
		tx, err := w.db.BeginTx(ctx, nil)
		if err == nil {
			_, err = tx.ExecContext(ctx, "DELETE FROM contacts WHERE account_id=$1", w.accountID)
			if err == nil {
				_, err = tx.ExecContext(ctx, "DELETE FROM groups WHERE account_id=$1", w.accountID)
			}
			if err == nil {
				err = tx.Commit()
			} else {
				_ = tx.Rollback()
			}
		}
		if err != nil {
			log.Printf("directory reset failed for account %s: %v", w.accountID, err)
			result["error"] = "directory_reset_failed"
			status = "failed"
			break
		}
		result["contact_count"] = "0"
		result["group_count"] = "0"
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
	for _, group := range groups {
		if _, err = tx.ExecContext(ctx, `INSERT INTO groups(account_id,group_id,name,topic,participant_count,is_announce,updated_at) VALUES($1,$2,$3,$4,$5,$6,now())
			ON CONFLICT(account_id,group_id) DO UPDATE SET name=excluded.name,topic=excluded.topic,participant_count=excluded.participant_count,is_announce=excluded.is_announce,updated_at=now()`, w.accountID, group.JID.String(), group.Name, group.Topic, len(group.Participants), group.IsAnnounce); err != nil {
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
	_, err := w.db.ExecContext(ctx, `INSERT INTO groups(account_id,group_id,name,topic,participant_count,is_announce,updated_at) VALUES($1,$2,$3,$4,$5,$6,now()) ON CONFLICT(account_id,group_id) DO UPDATE SET name=excluded.name,topic=excluded.topic,participant_count=excluded.participant_count,is_announce=excluded.is_announce,updated_at=now()`, w.accountID, group.JID.String(), group.Name, group.Topic, len(group.Participants), group.IsAnnounce)
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
	lidToPhone := map[string]string{}
	rows, err := w.db.QueryContext(ctx, `SELECT lid,pn FROM whatsmeow_lid_map`)
	if err != nil {
		return 0, err
	}
	for rows.Next() {
		var lid, phone string
		if err = rows.Scan(&lid, &phone); err != nil {
			rows.Close()
			return 0, err
		}
		lidToPhone[lid] = phone
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return 0, err
	}
	if err = rows.Close(); err != nil {
		return 0, err
	}
	type contactImport struct {
		WhatsAppID string `json:"whatsapp_id"`
		Phone      string `json:"phone"`
		Name       string `json:"name"`
	}
	imports := make([]contactImport, 0, len(items))
	seen := map[string]bool{}
	for jid, info := range items {
		phoneJID := jid
		if jid.Server == types.HiddenUserServer {
			phone, found := lidToPhone[jid.User]
			if !found {
				continue
			}
			phoneJID = types.NewJID(phone, types.DefaultUserServer)
		}
		if phoneJID.Server != types.DefaultUserServer || !phonePatternWorker.MatchString(phoneJID.User) {
			continue
		}
		if seen[phoneJID.User] {
			continue
		}
		seen[phoneJID.User] = true
		name := firstContactName(info.FullName, info.BusinessName, info.PushName, info.FirstName)
		imports = append(imports, contactImport{WhatsAppID: phoneJID.String(), Phone: phoneJID.User, Name: name})
	}
	if len(imports) == 0 {
		return 0, nil
	}
	payload, err := json.Marshal(imports)
	if err != nil {
		return 0, err
	}
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `WITH input AS (
		SELECT * FROM jsonb_to_recordset($1::jsonb) AS x(whatsapp_id text,phone text,name text)
	) INSERT INTO contacts(account_id,whatsapp_id,phone,name)
	SELECT $2,whatsapp_id,phone,name FROM input ON CONFLICT DO NOTHING`, string(payload), w.accountID)
	if err != nil {
		return 0, err
	}
	_, err = tx.ExecContext(ctx, `WITH input AS (
		SELECT * FROM jsonb_to_recordset($1::jsonb) AS x(whatsapp_id text,phone text,name text)
	) UPDATE contacts SET name=CASE WHEN contacts.name='' THEN input.name ELSE contacts.name END,updated_at=now()
	FROM input WHERE contacts.account_id=$2 AND contacts.phone=input.phone`, string(payload), w.accountID)
	if err != nil {
		return 0, err
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return len(imports), nil
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

