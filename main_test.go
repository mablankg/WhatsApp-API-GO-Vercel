package main

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAuthorizationProtectsEveryEndpoint(t *testing.T) {
	token := strings.Repeat("a", 32)
	for _, path := range []string{"/v1/status", "/v1/qr", "/v1/messages", "/v1/connect", "/v1/groups", "/v1/groups/sync", "/v1/commands/123"} {
		for _, auth := range []string{"", "Bearer wrong", "Bearer " + strings.Repeat("b", 32)} {
			r := httptest.NewRequest("GET", path, nil)
			r.Header.Set("Authorization", auth)
			out := httptest.NewRecorder()
			handler(nil, token).ServeHTTP(out, r)
			if out.Code != 401 {
				t.Fatalf("%s returned %d", path, out.Code)
			}
			if out.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("sensitive responses must not be cached")
			}
		}
	}
}
func TestInvalidSendDoesNotReachDatabase(t *testing.T) {
	token := strings.Repeat("a", 32)
	for _, body := range []string{`{}`, `{"to":"+5511999999999","text":"oi"}`, `{"to":"5511999999999","text":" "}`, `{"to":"5511999999999","text":"oi","unexpected":true}`, `{"to":"5511999999999","text":"oi"} {}`, `{"to":"5511999999999","text":"` + strings.Repeat("x", 8001) + `"}`} {
		r := httptest.NewRequest("POST", "/v1/messages", strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+token)
		out := httptest.NewRecorder()
		handler(nil, token).ServeHTTP(out, r)
		if out.Code != 400 {
			t.Fatalf("invalid payload returned %d", out.Code)
		}
	}
}

func TestInvalidGroupDoesNotReachDatabase(t *testing.T) {
	token := strings.Repeat("a", 32)
	for _, body := range []string{
		`{}`,
		`{"name":"","participants":["5511999999999"]}`,
		`{"name":"Grupo","participants":[]}`,
		`{"name":"Grupo","participants":["+5511999999999"]}`,
		`{"name":"Nome de grupo com mais de vinte e cinco caracteres","participants":["5511999999999"]}`,
	} {
		r := httptest.NewRequest("POST", "/v1/groups", strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+token)
		out := httptest.NewRecorder()
		handler(nil, token).ServeHTTP(out, r)
		if out.Code != 400 {
			t.Fatalf("invalid group returned %d", out.Code)
		}
	}
}

func TestGroupMessagePath(t *testing.T) {
	if !isGroupMessagePath("/v1/groups/120363123456@g.us/messages") {
		t.Fatal("valid group message path was rejected")
	}
	if isGroupMessagePath("/v1/groups") || isGroupMessagePath("/v1/groups/120363123456@g.us") {
		t.Fatal("non-message group path was accepted")
	}
}

