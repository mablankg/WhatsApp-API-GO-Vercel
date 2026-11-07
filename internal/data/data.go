package data

import (
	"context"
	"database/sql"
	"errors"
	_ "github.com/lib/pq"
	"net/url"
	"os"
	"time"
)

func Open(ctx context.Context, migrate bool) (*sql.DB, error) {
	raw := os.Getenv("DATABASE_URL")
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		return nil, errors.New("configure DATABASE_URL PostgreSQL")
	}
	q := u.Query()
	if q.Get("sslmode") != "require" && q.Get("sslmode") != "verify-full" {
		return nil, errors.New("DATABASE_URL exige SSL")
	}
	if migrate {
		bootstrap, err := sql.Open("postgres", raw)
		if err != nil {
			return nil, err
		}
		_, err = bootstrap.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS wa_private")
		bootstrap.Close()
		if err != nil {
			return nil, err
		}
	}
	q.Set("search_path", "wa_private")
	u.RawQuery = q.Encode()
	db, err := sql.Open("postgres", u.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(10 * time.Minute)
	if err = db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	if migrate {
		_, err = db.ExecContext(ctx, schema)
		if err != nil {
			db.Close()
			return nil, err
		}
	}
	return db, nil
}

const schema = `
CREATE TABLE IF NOT EXISTS whatsapp_accounts (
 id TEXT PRIMARY KEY,
 name TEXT NOT NULL,
 device_jid TEXT NOT NULL DEFAULT '',
 status TEXT NOT NULL DEFAULT 'offline',
 qr TEXT NOT NULL DEFAULT '',
 qr_expires TIMESTAMPTZ,
 heartbeat TIMESTAMPTZ NOT NULL DEFAULT now(),
 enabled BOOLEAN NOT NULL DEFAULT true,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO whatsapp_accounts(id,name) VALUES('primary','Principal') ON CONFLICT(id) DO NOTHING;
CREATE TABLE IF NOT EXISTS app_accounts (
 id TEXT PRIMARY KEY,
 name TEXT NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 enabled BOOLEAN NOT NULL DEFAULT true
);
ALTER TABLE app_accounts ADD COLUMN IF NOT EXISTS drive_root_folder_id TEXT;
CREATE TABLE IF NOT EXISTS app_users (
 id BIGSERIAL PRIMARY KEY,
 account_id TEXT NOT NULL REFERENCES app_accounts(id) ON DELETE CASCADE,
 username TEXT NOT NULL,
 email TEXT NOT NULL,
 password_hash TEXT NOT NULL,
 role TEXT NOT NULL DEFAULT 'user' CHECK(role IN ('master','admin','user')),
 enabled BOOLEAN NOT NULL DEFAULT true,
 must_change_password BOOLEAN NOT NULL DEFAULT true,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 UNIQUE(account_id, username),
 UNIQUE(account_id, email)
);
CREATE TABLE IF NOT EXISTS app_sessions (
 id BIGSERIAL PRIMARY KEY,
 user_id BIGINT NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
 token_hash TEXT NOT NULL UNIQUE,
 expires_at TIMESTAMPTZ NOT NULL,
 revoked_at TIMESTAMPTZ,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS app_sessions_active_idx ON app_sessions(token_hash, expires_at) WHERE revoked_at IS NULL;
INSERT INTO app_accounts(id,name) VALUES('bbadvocacia','BBAdvocacia') ON CONFLICT(id) DO NOTHING;
CREATE TABLE IF NOT EXISTS state (
 id INTEGER PRIMARY KEY CHECK(id=1), status TEXT NOT NULL DEFAULT 'offline',
 qr TEXT NOT NULL DEFAULT '', qr_expires TIMESTAMPTZ,
 heartbeat TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO state(id) VALUES(1) ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS commands (
 id TEXT PRIMARY KEY, kind TEXT NOT NULL, payload JSONB NOT NULL DEFAULT '{}',
 status TEXT NOT NULL DEFAULT 'pending', result JSONB, created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE commands ADD COLUMN IF NOT EXISTS account_id TEXT NOT NULL DEFAULT 'primary';
CREATE INDEX IF NOT EXISTS commands_account_pending ON commands(account_id,created_at) WHERE status='pending';
CREATE INDEX IF NOT EXISTS commands_pending ON commands(created_at) WHERE status='pending';
CREATE TABLE IF NOT EXISTS messages (
 seq BIGSERIAL PRIMARY KEY, message_id TEXT NOT NULL, chat TEXT NOT NULL,
 sender TEXT NOT NULL, text TEXT NOT NULL, timestamp TIMESTAMPTZ NOT NULL,
 from_me BOOLEAN NOT NULL DEFAULT false,
 UNIQUE(message_id,chat)
);
ALTER TABLE messages ADD COLUMN IF NOT EXISTS from_me BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE messages ADD COLUMN IF NOT EXISTS account_id TEXT NOT NULL DEFAULT 'primary';
ALTER TABLE messages DROP CONSTRAINT IF EXISTS messages_message_id_chat_key;
CREATE UNIQUE INDEX IF NOT EXISTS messages_account_message_chat_key ON messages(account_id,message_id,chat);
CREATE INDEX IF NOT EXISTS messages_account_chat ON messages(account_id,chat,seq DESC);
CREATE TABLE IF NOT EXISTS message_media (
 id BIGSERIAL PRIMARY KEY,
 message_id TEXT NOT NULL,
 chat TEXT NOT NULL,
 media_type TEXT NOT NULL,
 mime_type TEXT NOT NULL DEFAULT 'application/octet-stream',
 file_name TEXT NOT NULL DEFAULT '',
 caption TEXT NOT NULL DEFAULT '',
 file_size BIGINT NOT NULL DEFAULT 0,
 duration_seconds INTEGER NOT NULL DEFAULT 0,
 data BYTEA NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 UNIQUE(message_id,chat)
);
CREATE INDEX IF NOT EXISTS message_media_lookup ON message_media(message_id,chat);
ALTER TABLE message_media ADD COLUMN IF NOT EXISTS account_id TEXT NOT NULL DEFAULT 'primary';
ALTER TABLE message_media DROP CONSTRAINT IF EXISTS message_media_message_id_chat_key;
CREATE UNIQUE INDEX IF NOT EXISTS message_media_account_message_chat_key ON message_media(account_id,message_id,chat);
CREATE INDEX IF NOT EXISTS message_media_account_lookup ON message_media(account_id,message_id,chat);
CREATE TABLE IF NOT EXISTS groups (
 group_id TEXT PRIMARY KEY, name TEXT NOT NULL, topic TEXT NOT NULL DEFAULT '',
 participant_count INTEGER NOT NULL DEFAULT 0,
 is_announce BOOLEAN NOT NULL DEFAULT false,
 attendance_stage TEXT NOT NULL DEFAULT 'new',
 priority TEXT NOT NULL DEFAULT 'normal',
 updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE groups ADD COLUMN IF NOT EXISTS attendance_stage TEXT NOT NULL DEFAULT 'new';
ALTER TABLE groups ADD COLUMN IF NOT EXISTS priority TEXT NOT NULL DEFAULT 'normal';
ALTER TABLE groups ADD COLUMN IF NOT EXISTS account_id TEXT NOT NULL DEFAULT 'primary';
ALTER TABLE groups DROP CONSTRAINT IF EXISTS groups_pkey;
CREATE UNIQUE INDEX IF NOT EXISTS groups_account_group_key ON groups(account_id,group_id);
CREATE INDEX IF NOT EXISTS groups_account_name ON groups(account_id,lower(name));
CREATE TABLE IF NOT EXISTS contacts (
 id BIGSERIAL PRIMARY KEY,
 whatsapp_id TEXT NOT NULL UNIQUE,
 phone TEXT NOT NULL UNIQUE,
 name TEXT NOT NULL DEFAULT '',
 avatar_url TEXT NOT NULL DEFAULT '',
 nickname TEXT NOT NULL DEFAULT '',
 email TEXT NOT NULL DEFAULT '',
 tax_id TEXT NOT NULL DEFAULT '',
 contact_type TEXT NOT NULL DEFAULT 'lead',
 status TEXT NOT NULL DEFAULT 'active',
 attendance_stage TEXT NOT NULL DEFAULT 'new',
 priority TEXT NOT NULL DEFAULT 'normal',
 responsible TEXT NOT NULL DEFAULT '',
 tags TEXT[] NOT NULL DEFAULT '{}',
 notes TEXT NOT NULL DEFAULT '',
 extra JSONB NOT NULL DEFAULT '{}',
 first_contact_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 last_contact_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 archived BOOLEAN NOT NULL DEFAULT false,
 updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS avatar_url TEXT NOT NULL DEFAULT '';
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS avatar_data BYTEA;
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS avatar_mime TEXT NOT NULL DEFAULT 'image/jpeg';
CREATE INDEX IF NOT EXISTS contacts_avatar_updated ON contacts(updated_at DESC);
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS nickname TEXT NOT NULL DEFAULT '';
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS attendance_stage TEXT NOT NULL DEFAULT 'new';
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS priority TEXT NOT NULL DEFAULT 'normal';
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS account_id TEXT NOT NULL DEFAULT 'primary';
ALTER TABLE contacts DROP CONSTRAINT IF EXISTS contacts_whatsapp_id_key;
ALTER TABLE contacts DROP CONSTRAINT IF EXISTS contacts_phone_key;
CREATE UNIQUE INDEX IF NOT EXISTS contacts_account_whatsapp_key ON contacts(account_id,whatsapp_id);
CREATE UNIQUE INDEX IF NOT EXISTS contacts_account_phone_key ON contacts(account_id,phone);
CREATE INDEX IF NOT EXISTS contacts_account_name ON contacts(account_id,lower(name));
CREATE TABLE IF NOT EXISTS attendance_stages (
 id TEXT PRIMARY KEY,
 name TEXT NOT NULL,
 color TEXT NOT NULL DEFAULT '#3f9cd0',
 position INTEGER NOT NULL DEFAULT 0,
 active BOOLEAN NOT NULL DEFAULT true,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO attendance_stages(id,name,color,position) VALUES
 ('new','Novos','#3f9cd0',10),
 ('in_progress','Em atendimento','#8b6fd6',20),
 ('waiting_client','Aguardando cliente','#e2a51b',30),
 ('waiting_action','Aguardando providência','#e47b21',40),
 ('finished','Finalizados','#5d9b75',50)
ON CONFLICT(id) DO NOTHING;
CREATE TABLE IF NOT EXISTS lawyers (
 id BIGSERIAL PRIMARY KEY,
 name TEXT NOT NULL,
 color TEXT NOT NULL DEFAULT '#177447',
 active BOOLEAN NOT NULL DEFAULT true,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS lawyers_name_key ON lawyers(lower(name));
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS lawyer_id BIGINT;
ALTER TABLE groups ADD COLUMN IF NOT EXISTS lawyer_id BIGINT;
ALTER TABLE groups ADD COLUMN IF NOT EXISTS archived BOOLEAN NOT NULL DEFAULT false;
CREATE INDEX IF NOT EXISTS contacts_lawyer ON contacts(account_id,lawyer_id) WHERE archived=false;
CREATE INDEX IF NOT EXISTS groups_lawyer ON groups(account_id,lawyer_id) WHERE archived=false;
CREATE INDEX IF NOT EXISTS contacts_name_search ON contacts(lower(name));
CREATE INDEX IF NOT EXISTS contacts_last_contact ON contacts(last_contact_at DESC);
CREATE TABLE IF NOT EXISTS entity_comments (
 id BIGSERIAL PRIMARY KEY,
 entity_type TEXT NOT NULL CHECK(entity_type IN ('contact','group')),
 entity_id TEXT NOT NULL,
 body TEXT NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS entity_comments_lookup ON entity_comments(entity_type,entity_id,created_at DESC);
ALTER TABLE entity_comments ADD COLUMN IF NOT EXISTS account_id TEXT NOT NULL DEFAULT 'primary';
CREATE INDEX IF NOT EXISTS entity_comments_account_lookup ON entity_comments(account_id,entity_type,entity_id,created_at DESC);
INSERT INTO contacts(account_id,whatsapp_id,phone,first_contact_at,last_contact_at)
SELECT account_id,chat,split_part(chat,'@',1),min(timestamp),max(timestamp)
FROM messages WHERE chat LIKE '%@s.whatsapp.net'
GROUP BY account_id,chat
ON CONFLICT(account_id,whatsapp_id) DO UPDATE SET
 last_contact_at=GREATEST(contacts.last_contact_at,excluded.last_contact_at),updated_at=now();
ALTER TABLE state ENABLE ROW LEVEL SECURITY;
ALTER TABLE whatsapp_accounts ENABLE ROW LEVEL SECURITY;
ALTER TABLE commands ENABLE ROW LEVEL SECURITY;
ALTER TABLE messages ENABLE ROW LEVEL SECURITY;
ALTER TABLE message_media ENABLE ROW LEVEL SECURITY;
ALTER TABLE groups ENABLE ROW LEVEL SECURITY;
ALTER TABLE contacts ENABLE ROW LEVEL SECURITY;
ALTER TABLE attendance_stages ENABLE ROW LEVEL SECURITY;
ALTER TABLE lawyers ENABLE ROW LEVEL SECURITY;
ALTER TABLE entity_comments ENABLE ROW LEVEL SECURITY;
CREATE TABLE IF NOT EXISTS legal_message_tags (
 account_id TEXT NOT NULL REFERENCES whatsapp_accounts(id),
 id BIGSERIAL,
 name TEXT NOT NULL CHECK (length(trim(name)) BETWEEN 1 AND 80),
 color TEXT NOT NULL CHECK (color ~ '^#[0-9A-Fa-f]{6}$'),
 PRIMARY KEY (account_id,id)
);
CREATE UNIQUE INDEX IF NOT EXISTS legal_message_tags_name_key
 ON legal_message_tags(account_id,lower(trim(name)));
CREATE UNIQUE INDEX IF NOT EXISTS messages_account_seq_key ON messages(account_id,seq);
CREATE TABLE IF NOT EXISTS legal_message_status (
 account_id TEXT NOT NULL,
 message_seq BIGINT NOT NULL,
 status TEXT NOT NULL CHECK (status IN ('pending','in_review','done')),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 PRIMARY KEY (account_id,message_seq),
 FOREIGN KEY (account_id,message_seq) REFERENCES messages(account_id,seq) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS legal_message_tag_links (
 account_id TEXT NOT NULL,
 message_seq BIGINT NOT NULL,
 tag_id BIGINT NOT NULL,
 PRIMARY KEY (account_id,message_seq,tag_id),
 FOREIGN KEY (account_id,message_seq) REFERENCES messages(account_id,seq) ON DELETE CASCADE,
 FOREIGN KEY (account_id,tag_id) REFERENCES legal_message_tags(account_id,id) ON DELETE CASCADE
);
ALTER TABLE legal_message_tags ENABLE ROW LEVEL SECURITY;
ALTER TABLE legal_message_status ENABLE ROW LEVEL SECURITY;
ALTER TABLE legal_message_tag_links ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON SCHEMA wa_private FROM PUBLIC;
`

