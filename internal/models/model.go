package models

import (
	"time"
)

// VideoMetadata contains metadata about a video
type VideoMetadata struct {
	ID          string    `json:"id"`
	Title       string    `json:"title"`
	Description string    `json:"description"`
	Duration    int64     `json:"duration"`
	ThumbnailURL string   `json:"thumbnail_url"`
	Views       int64     `json:"views"`
	CreatedAt   time.Time `json:"created_at"`
	Tags        []string  `json:"tags"`
}

// VideoFormat represents a specific format/quality of a video
type VideoFormat struct {
	Quality  string `json:"quality"`
	URL      string `json:"url"`
	Size     int64  `json:"size"`
	MimeType string `json:"mime_type"`
}

// VideoProcessingMessage is sent to Kafka for video processing
type VideoProcessingMessage struct {
	VideoID   string   `json:"video_id"`
	FilePath  string   `json:"file_path"`
	Formats   []string `json:"formats"`
	Timestamp int64    `json:"timestamp"`
}

// VideoProcessingComplete is sent to Kafka when video processing is complete
type VideoProcessingComplete struct {
	VideoID      string       `json:"video_id"`
	Success      bool         `json:"success"`
	ThumbnailURL string       `json:"thumbnail_url"`
	Formats      []VideoFormat `json:"formats"`
	Duration     int64        `json:"duration"`
	Timestamp    int64        `json:"timestamp"`
}