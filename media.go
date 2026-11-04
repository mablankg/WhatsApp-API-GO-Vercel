package main

import (
	"database/sql"
	"fmt"
	"net/http"
	"regexp"
	"strings"
)

var mediaPathPattern = regexp.MustCompile(`^/v1/media/([1-9][0-9]{0,18})$`)

type mediaInfo struct {
	ID              int64  `json:"id"`
	Type            string `json:"type"`
	MimeType        string `json:"mime_type"`
	FileName        string `json:"file_name"`
	Caption         string `json:"caption"`
	FileSize        int64  `json:"file_size"`
	DurationSeconds int    `json:"duration_seconds"`
}

func mediaJSON(id int64, mediaType, mimeType, fileName, caption string, fileSize int64, duration int) any {
	if id == 0 {
		return nil
	}
	return mediaInfo{ID: id, Type: mediaType, MimeType: mimeType, FileName: fileName, Caption: caption, FileSize: fileSize, DurationSeconds: duration}
}

func handleMedia(w http.ResponseWriter, r *http.Request, db *sql.DB) {
	match := mediaPathPattern.FindStringSubmatch(r.URL.Path)
	if match == nil {
		respond(w, 404, map[string]string{"error": "not_found"})
		return
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		respond(w, 405, map[string]string{"error": "method_not_allowed"})
		return
	}
	var mimeType, fileName, mediaType string
	var content []byte
	err := db.QueryRowContext(r.Context(), "SELECT mime_type,file_name,media_type,data FROM message_media WHERE id=$1 AND account_id=$2", match[1], requestAccount(r)).Scan(&mimeType, &fileName, &mediaType, &content)
	if err == sql.ErrNoRows {
		respond(w, 404, map[string]string{"error": "not_found"})
		return
	}
	if err != nil {
		respond(w, 503, map[string]string{"error": "media_unavailable"})
		return
	}
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}
	w.Header().Set("Content-Type", mimeType)
	w.Header().Set("Content-Length", fmt.Sprint(len(content)))
	w.Header().Set("Cache-Control", "private, no-store")
	if fileName != "" {
		disposition := "inline"
		if mediaType == "document" {
			disposition = "attachment"
		}
		fileName = strings.ReplaceAll(strings.ReplaceAll(fileName, "\"", ""), "\r", "")
		fileName = strings.ReplaceAll(fileName, "\n", "")
		w.Header().Set("Content-Disposition", disposition+`; filename="`+fileName+`"`)
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(content)
}

