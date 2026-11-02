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
CREATE INDEX IF NOT EXISTS commands_pending ON commands(created_at) WHERE status='pending';
CREATE TABLE IF NOT EXISTS messages (
 seq BIGSERIAL PRIMARY KEY, message_id TEXT NOT NULL, chat TEXT NOT NULL,
 sender TEXT NOT NULL, text TEXT NOT NULL, timestamp TIMESTAMPTZ NOT NULL,
 UNIQUE(message_id,chat)
);
CREATE TABLE IF NOT EXISTS groups (
 group_id TEXT PRIMARY KEY, name TEXT NOT NULL, topic TEXT NOT NULL DEFAULT '',
 participant_count INTEGER NOT NULL DEFAULT 0,
 is_announce BOOLEAN NOT NULL DEFAULT false,
 updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE state ENABLE ROW LEVEL SECURITY;
ALTER TABLE commands ENABLE ROW LEVEL SECURITY;
ALTER TABLE messages ENABLE ROW LEVEL SECURITY;
ALTER TABLE groups ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON SCHEMA wa_private FROM PUBLIC;
`
