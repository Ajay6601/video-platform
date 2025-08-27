package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/yourusername/video-platform/internal/models"
	"github.com/yourusername/video-platform/pkg/api/video"
	"github.com/yourusername/video-platform/pkg/kafka"
	"github.com/yourusername/video-platform/pkg/redis"
	"github.com/yourusername/video-platform/pkg/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// VideoService implements the VideoService gRPC service
type VideoService struct {
	video.UnimplementedVideoServiceServer
	videoStorage   storage.Storage
	redisClient    *redis.Client
	kafkaProducer  *kafka.Producer
	uploadBasePath string
}

// NewVideoService creates a new video service
func NewVideoService(
	videoStorage storage.Storage,
	redisClient *redis.Client,
	kafkaProducer *kafka.Producer,
	uploadBasePath string,
) *VideoService {
	return &VideoService{
		videoStorage:   videoStorage,
		redisClient:    redisClient,
		kafkaProducer:  kafkaProducer,
		uploadBasePath: uploadBasePath,
	}
}

// UploadVideo implements the UploadVideo RPC method
func (s *VideoService) UploadVideo(stream video.VideoService_UploadVideoServer) error {
	var uploadID string
	var videoID string
	var title string
	var description string
	var videoPath string
	var file *os.File

	// Get the first request to get metadata
	req, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.Internal, "failed to receive initial request: %v", err)
	}

	// Get or generate upload ID
	if req.UploadId == "" {
		uploadID = uuid.New().String()
		videoID = uuid.New().String()
	} else {
		uploadID = req.UploadId
		// Retrieve videoID from Redis using uploadID
		var err error
		videoID, err = s.redisClient.Get(context.Background(), fmt.Sprintf("upload:%s:video_id", uploadID))
		if err != nil {
			videoID = uuid.New().String()
		}
	}

	title = req.Title
	description = req.Description

	// Create upload directory if it doesn't exist
	err = os.MkdirAll(s.uploadBasePath, 0755)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create upload directory: %v", err)
	}

	// Create temp file for video
	videoPath = filepath.Join(s.uploadBasePath, fmt.Sprintf("%s.mp4", videoID))
	file, err = os.OpenFile(videoPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create video file: %v", err)
	}
	defer file.Close()

	// Write first chunk
	_, err = file.Write(req.Chunk)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to write to file: %v", err)
	}

	// Store upload information in Redis
	ctx := context.Background()
	s.redisClient.Set(ctx, fmt.Sprintf("upload:%s:video_id", uploadID), videoID, 24*time.Hour)
	s.redisClient.Set(ctx, fmt.Sprintf("upload:%s:status", uploadID), "uploading", 24*time.Hour)
	s.redisClient.Set(ctx, fmt.Sprintf("video:%s:title", videoID), title, 0)
	s.redisClient.Set(ctx, fmt.Sprintf("video:%s:description", videoID), description, 0)
	s.redisClient.Set(ctx, fmt.Sprintf("video:%s:status", videoID), "uploading", 0)
	s.redisClient.Set(ctx, fmt.Sprintf("video:%s:created_at", videoID), time.Now().Format(time.RFC3339), 0)

	// Handle first chunk completion
	if req.IsLastChunk {
		if err := s.finalizeUpload(ctx, videoID, videoPath); err != nil {
			return status.Errorf(codes.Internal, "failed to finalize upload: %v", err)
		}
		return stream.SendAndClose(&video.UploadVideoResponse{
			VideoId:  videoID,
			UploadId: uploadID,
			Success:  true,
		})
	}

	// Continue receiving chunks
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return status.Errorf(codes.Internal, "failed to receive chunk: %v", err)
		}

		// Write chunk to file
		_, err = file.Write(req.Chunk)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to write to file: %v", err)
		}

		// If this is the last chunk, finalize the upload
		if req.IsLastChunk {
			if err := s.finalizeUpload(ctx, videoID, videoPath); err != nil {
				return status.Errorf(codes.Internal, "failed to finalize upload: %v", err)
			}
			break
		}
	}

	return stream.SendAndClose(&video.UploadVideoResponse{
		VideoId:  videoID,
		UploadId: uploadID,
		Success:  true,
	})
}

// finalizeUpload completes the upload process and triggers video processing
func (s *VideoService) finalizeUpload(ctx context.Context, videoID string, videoPath string) error {
	// Update status
	s.redisClient.Set(ctx, fmt.Sprintf("video:%s:status", videoID), "processing", 0)
	s.redisClient.Set(ctx, fmt.Sprintf("video:%s:path", videoID), videoPath, 0)

	// Create processing message
	message := models.VideoProcessingMessage{
		VideoID:   videoID,
		FilePath:  videoPath,
		Formats:   []string{"hd", "sd"},
		Timestamp: time.Now().Unix(),
	}

	// Send to Kafka for processing
	err := s.kafkaProducer.ProduceJSON("video-processing", videoID, message)
	if err != nil {
		log.Printf("Failed to send processing message: %v", err)
		s.redisClient.Set(ctx, fmt.Sprintf("video:%s:status", videoID), "error", 0)
		return err
	}

	return nil
}

// GetVideo implements the GetVideo RPC method
func (s *VideoService) GetVideo(ctx context.Context, req *video.GetVideoRequest) (*video.Video, error) {
	videoID := req.VideoId

	// Check if video exists
	exists, err := s.redisClient.Exists(ctx, fmt.Sprintf("video:%s:title", videoID))
	if err != nil || !exists {
		return nil, status.Errorf(codes.NotFound, "video not found")
	}

	// Get video metadata
	title, _ := s.redisClient.Get(ctx, fmt.Sprintf("video:%s:title", videoID))
	description, _ := s.redisClient.Get(ctx, fmt.Sprintf("video:%s:description", videoID))
	createdAt, _ := s.redisClient.Get(ctx, fmt.Sprintf("video:%s:created_at", videoID))
	thumbnail, _ := s.redisClient.Get(ctx, fmt.Sprintf("video:%s:thumbnail", videoID))
	durationStr, _ := s.redisClient.Get(ctx, fmt.Sprintf("video:%s:duration", videoID))
	viewsStr, _ := s.redisClient.Get(ctx, fmt.Sprintf("video:%s:views", videoID))
	tagsJSON, _ := s.redisClient.Get(ctx, fmt.Sprintf("video:%s:tags", videoID))

	// Parse metadata
	var duration int64 = 0
	if durationStr != "" {
		fmt.Sscanf(durationStr, "%d", &duration)
	}

	var views int64 = 0
	if viewsStr != "" {
		fmt.Sscanf(viewsStr, "%d", &views)
	}

	var tags []string
	if tagsJSON != "" {
		if err := json.Unmarshal([]byte(tagsJSON), &tags); err != nil {
			tags = []string{}
		}
	}

	// Get formats
	formatsJSON, _ := s.redisClient.Get(ctx, fmt.Sprintf("video:%s:formats", videoID))
	var formatList []models.VideoFormat
	if formatsJSON != "" {
		if err := json.Unmarshal([]byte(formatsJSON), &formatList); err != nil {
			formatList = []models.VideoFormat{}
		}
	}

	// Convert formats to proto message
	var formats []*video.VideoFormat
	for _, format := range formatList {
		formats = append(formats, &video.VideoFormat{
			Quality:  format.Quality,
			Url:      format.URL,
			Size:     format.Size,
			MimeType: format.MimeType,
		})
	}

	// Build video response
	videoResponse := &video.Video{
		Metadata: &video.VideoMetadata{
			Id:          videoID,
			Title:       title,
			Description: description,
			Duration:    duration,
			ThumbnailUrl: thumbnail,
			Views:       views,
			CreatedAt:   createdAt,
			Tags:        tags,
		},
		StreamUrl: s.videoStorage.GetPublicURL(videoID, "hd"),
		Formats:   formats,
	}

	// Increment view count asynchronously
	go func() {
		s.redisClient.Incr(context.Background(), fmt.Sprintf("video:%s:views", videoID))
	}()

	return videoResponse, nil
}

// StreamVideo implements the StreamVideo RPC method
func (s *VideoService) StreamVideo(req *video.StreamVideoRequest, stream video.VideoService_StreamVideoServer) error {
	videoID := req.VideoId
	quality := req.Quality
	if quality == "" {
		quality = "hd" // Default quality
	}
	startTime := req.StartTime

	// Get video path based on quality
	var videoPath string
	
	// For a real implementation, this would get the processed video path
	// Here we're using a simple approach for demonstration
	videoPath, err := s.redisClient.Get(context.Background(), fmt.Sprintf("video:%s:path:%s", videoID, quality))
	if err != nil || videoPath == "" {
		// Fallback to original path
		videoPath, err = s.redisClient.Get(context.Background(), fmt.Sprintf("video:%s:path", videoID))
		if err != nil || videoPath == "" {
			return status.Errorf(codes.NotFound, "video not found")
		}
	}

	// Open the video file
	file, err := os.Open(videoPath)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to open video file: %v", err)
	}
	defer file.Close()

	// If startTime is provided, seek to position
	// Note: In a real implementation, this would involve using ffmpeg or similar
	// to seek to the correct keyframe
	if startTime > 0 {
		// This is a simplification - actual implementation would be more complex
		log.Printf("Seeking to position %d ms (implementation simplified)", startTime)
	}

	// Read and send chunks
	buffer := make([]byte, 1024*1024) // 1MB chunks
	for {
		n, err := file.Read(buffer)
		if err == io.EOF {
			// Send last chunk
			if err := stream.Send(&video.VideoChunk{
				Data:        buffer[:n],
				Timestamp:   0, // In a real implementation, this would be the actual timestamp
				IsLastChunk: true,
			}); err != nil {
				return status.Errorf(codes.Internal, "failed to send last chunk: %v", err)
			}
			break
		}
		if err != nil {
			return status.Errorf(codes.Internal, "failed to read file: %v", err)
		}

		// Send chunk
		if err := stream.Send(&video.VideoChunk{
			Data:        buffer[:n],
			Timestamp:   0, // In a real implementation, this would be the actual timestamp
			IsLastChunk: false,
		}); err != nil {
			return status.Errorf(codes.Internal, "failed to send chunk: %v", err)
		}
	}

	return nil
}

// ListVideos implements the ListVideos RPC method
func (s *VideoService) ListVideos(ctx context.Context, req *video.ListVideosRequest) (*video.ListVideosResponse, error) {
	page := req.Page
	if page <= 0 {
		page = 1
	}
	pageSize := req.PageSize
	if pageSize <= 0 {
		pageSize = 10
	}
	sortBy := req.SortBy
	if sortBy == "" {
		sortBy = "created_at"
	}
	sortDesc := req.SortDesc

	// In a real implementation, this would query a database
	// For demonstration, we'll use Redis to get video IDs
	
	// Get all video IDs (this is inefficient but simple for demo)
	// In production, you would use a database with proper pagination
	videoIDs, err := s.redisClient.Keys(ctx, "video:*:title")
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list videos: %v", err)
	}

	// Extract actual IDs from key patterns
	var ids []string
	for _, key := range videoIDs {
		// Extract ID from pattern "video:{id}:title"
		parts := strings.Split(key, ":")
		if len(parts) == 3 && parts[0] == "video" && parts[2] == "title" {
			ids = append(ids, parts[1])
		}
	}

	// Get metadata for each video
	var videos []*video.VideoMetadata
	for _, id := range ids {
		title, _ := s.redisClient.Get(ctx, fmt.Sprintf("video:%s:title", id))
		description, _ := s.redisClient.Get(ctx, fmt.Sprintf("video:%s:description", id))
		createdAt, _ := s.redisClient.Get(ctx, fmt.Sprintf("video:%s:created_at", id))
		thumbnail, _ := s.redisClient.Get(ctx, fmt.Sprintf("video:%s:thumbnail", id))
		durationStr, _ := s.redisClient.Get(ctx, fmt.Sprintf("video:%s:duration", id))
		viewsStr, _ := s.redisClient.Get(ctx, fmt.Sprintf("video:%s:views", id))
		tagsJSON, _ := s.redisClient.Get(ctx, fmt.Sprintf("video:%s:tags", id))

		var duration int64 = 0
		if durationStr != "" {
			fmt.Sscanf(durationStr, "%d", &duration)
		}

		var views int64 = 0
		if viewsStr != "" {
			fmt.Sscanf(viewsStr, "%d", &views)
		}

		var tags []string
		if tagsJSON != "" {
			if err := json.Unmarshal([]byte(tagsJSON), &tags); err != nil {
				tags = []string{}
			}
		}

		videos = append(videos, &video.VideoMetadata{
			Id:          id,
			Title:       title,
			Description: description,
			Duration:    duration,
			ThumbnailUrl: thumbnail,
			Views:       views,
			CreatedAt:   createdAt,
			Tags:        tags,
		})
	}

	// Sort videos
	sort.Slice(videos, func(i, j int) bool {
		switch sortBy {
		case "title":
			if sortDesc {
				return videos[i].Title > videos[j].Title
			}
			return videos[i].Title < videos[j].Title
		case "views":
			if sortDesc {
				return videos[i].Views > videos[j].Views
			}
			return videos[i].Views < videos[j].Views
		case "duration":
			if sortDesc {
				return videos[i].Duration > videos[j].Duration
			}
			return videos[i].Duration < videos[j].Duration
		default: // created_at
			if sortDesc {
				return videos[i].CreatedAt > videos[j].CreatedAt
			}
			return videos[i].CreatedAt < videos[j].CreatedAt
		}
	})

	// Apply pagination
	startIndex := (int(page) - 1) * int(pageSize)
	endIndex := startIndex + int(pageSize)
	if startIndex >= len(videos) {
		return &video.ListVideosResponse{
			Videos:     []*video.VideoMetadata{},
			TotalCount: int32(len(videos)),
			Page:       page,
			PageSize:   pageSize,
		}, nil
	}
	if endIndex > len(videos) {
		endIndex = len(videos)
	}
	paginatedVideos := videos[startIndex:endIndex]

	return &video.ListVideosResponse{
		Videos:     paginatedVideos,
		TotalCount: int32(len(videos)),
		Page:       page,
		PageSize:   pageSize,
	}, nil
}

// UpdateVideo implements the UpdateVideo RPC method
func (s *VideoService) UpdateVideo(ctx context.Context, req *video.UpdateVideoRequest) (*video.UpdateVideoResponse, error) {
	videoID := req.VideoId

	// Check if video exists
	exists, err := s.redisClient.Exists(ctx, fmt.Sprintf("video:%s:title", videoID))
	if err != nil || !exists {
		return nil, status.Errorf(codes.NotFound, "video not found")
	}

	// Update metadata
	if req.Title != "" {
		s.redisClient.Set(ctx, fmt.Sprintf("video:%s:title", videoID), req.Title, 0)
	}
	if req.Description != "" {
		s.redisClient.Set(ctx, fmt.Sprintf("video:%s:description", videoID), req.Description, 0)
	}
	if len(req.Tags) > 0 {
		tagsJSON, _ := json.Marshal(req.Tags)
		s.redisClient.Set(ctx, fmt.Sprintf("video:%s:tags", videoID), string(tagsJSON), 0)
	}

	// Get updated metadata for response
	title, _ := s.redisClient.Get(ctx, fmt.Sprintf("video:%s:title", videoID))
	description, _ := s.redisClient.Get(ctx, fmt.Sprintf("video:%s:description", videoID))
	createdAt, _ := s.redisClient.Get(ctx, fmt.Sprintf("video:%s:created_at", videoID))
	thumbnail, _ := s.redisClient.Get(ctx, fmt.Sprintf("video:%s:thumbnail", videoID))
	durationStr, _ := s.redisClient.Get(ctx, fmt.Sprintf("video:%s:duration", videoID))
	viewsStr, _ := s.redisClient.Get(ctx, fmt.Sprintf("video:%s:views", videoID))
	tagsJSON, _ := s.redisClient.Get(ctx, fmt.Sprintf("video:%s:tags", videoID))

	var duration int64 = 0
	if durationStr != "" {
		fmt.Sscanf(durationStr, "%d", &duration)
	}

	var views int64 = 0
	if viewsStr != "" {
		fmt.Sscanf(viewsStr, "%d", &views)
	}

	var tags []string
	if tagsJSON != "" {
		if err := json.Unmarshal([]byte(tagsJSON), &tags); err != nil {
			tags = []string{}
		}
	}

	return &video.UpdateVideoResponse{
		Success: true,
		Metadata: &video.VideoMetadata{
			Id:          videoID,
			Title:       title,
			Description: description,
			Duration:    duration,
			ThumbnailUrl: thumbnail,
			Views:       views,
			CreatedAt:   createdAt,
			Tags:        tags,
		},
	}, nil
}

// DeleteVideo implements the DeleteVideo RPC method
func (s *VideoService) DeleteVideo(ctx context.Context, req *video.DeleteVideoRequest) (*video.DeleteVideoResponse, error) {
	videoID := req.VideoId

	// Check if video exists
	exists, err := s.redisClient.Exists(ctx, fmt.Sprintf("video:%s:title", videoID))
	if err != nil || !exists {
		return nil, status.Errorf(codes.NotFound, "video not found")
	}

	// Get video paths
	originalPath, _ := s.redisClient.Get(ctx, fmt.Sprintf("video:%s:path", videoID))
	
	// In a real implementation, this would get all formats and delete them
	// For simplicity, we're just deleting the original
	
	// Delete the file if it exists
	if originalPath != "" {
		os.Remove(originalPath)
	}

	// Delete all metadata from Redis
	keys, _ := s.redisClient.Keys(ctx, fmt.Sprintf("video:%s:*", videoID))
	for _, key := range keys {
		s.redisClient.Del(ctx, key)
	}

	return &video.DeleteVideoResponse{
		Success: true,
	}, nil
}