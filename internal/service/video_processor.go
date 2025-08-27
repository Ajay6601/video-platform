package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"

	"github.com/yourusername/video-platform/internal/models"
	"github.com/yourusername/video-platform/pkg/api/video"
	"github.com/yourusername/video-platform/pkg/kafka"
	"github.com/yourusername/video-platform/pkg/redis"
	"github.com/yourusername/video-platform/pkg/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// VideoProcessor handles video processing
type VideoProcessor struct {
	video.UnimplementedVideoProcessingServiceServer
	redisClient    *redis.Client
	kafkaProducer  *kafka.Producer
	videoStorage   storage.Storage
	outputBasePath string
}

// NewVideoProcessor creates a new video processor
func NewVideoProcessor(
	redisClient *redis.Client,
	kafkaProducer *kafka.Producer,
	videoStorage storage.Storage,
	outputBasePath string,
) *VideoProcessor {
	return &VideoProcessor{
		redisClient:    redisClient,
		kafkaProducer:  kafkaProducer,
		videoStorage:   videoStorage,
		outputBasePath: outputBasePath,
	}
}

// ProcessVideo implements the ProcessVideo RPC method
func (p *VideoProcessor) ProcessVideo(ctx context.Context, req *video.ProcessVideoRequest) (*video.ProcessVideoResponse, error) {
	videoID := req.VideoId
	inputPath := req.InputPath
	outputFormats := req.OutputFormats
	generateThumbnail := req.GenerateThumbnail

	log.Printf("Processing video %s from %s", videoID, inputPath)

	// Check if video file exists
	if _, err := os.Stat(inputPath); os.IsNotExist(err) {
		return nil, status.Errorf(codes.NotFound, "video file not found")
	}

	// Create output directory
	outputDir := filepath.Join(p.outputBasePath, videoID)
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create output directory: %v", err)
	}

	// Process video asynchronously
	go func() {
		var formats []*video.VideoFormat
		var thumbnailURL string
		var err error

		// Update status
		p.redisClient.Set(context.Background(), fmt.Sprintf("video:%s:status", videoID), "processing", 0)

		// Generate thumbnail if requested
		if generateThumbnail {
			thumbnailReq := &video.GenerateThumbnailRequest{
				VideoId:   videoID,
				VideoPath: inputPath,
				Timestamp: 0, // Take thumbnail from beginning
			}
			
			thumbnailResp, err := p.GenerateThumbnail(context.Background(), thumbnailReq)
			if err != nil {
				log.Printf("Failed to generate thumbnail: %v", err)
			} else {
				thumbnailURL = thumbnailResp.ThumbnailUrl
				p.redisClient.Set(context.Background(), fmt.Sprintf("video:%s:thumbnail", videoID), thumbnailURL, 0)
			}
		}

		// Transcode video to different formats
		if len(outputFormats) > 0 {
			transcodeReq := &video.TranscodeVideoRequest{
				VideoId:   videoID,
				InputPath: inputPath,
				Formats:   outputFormats,
			}
			
			transcodeResp, err := p.TranscodeVideo(context.Background(), transcodeReq)
			if err != nil {
				log.Printf("Failed to transcode video: %v", err)
			} else {
				formats = transcodeResp.Formats
				
				// Save formats to Redis
				formatList := make([]models.VideoFormat, 0, len(formats))
				for _, format := range formats {
					formatList = append(formatList, models.VideoFormat{
						Quality:  format.Quality,
						URL:      format.Url,
						Size:     format.Size,
						MimeType: format.MimeType,
					})
				}
				
				formatsJSON, _ := json.Marshal(formatList)
				p.redisClient.Set(context.Background(), fmt.Sprintf("video:%s:formats", videoID), string(formatsJSON), 0)
			}
		}

		// Extract video duration using ffprobe
		duration, err := p.getVideoDuration(inputPath)
		if err != nil {
			log.Printf("Failed to get video duration: %v", err)
		} else {
			p.redisClient.Set(context.Background(), fmt.Sprintf("video:%s:duration", videoID), strconv.FormatInt(duration, 10), 0)
		}

		// Update status
		if err != nil {
			p.redisClient.Set(context.Background(), fmt.Sprintf("video:%s:status", videoID), "error", 0)
		} else {
			p.redisClient.Set(context.Background(), fmt.Sprintf("video:%s:status", videoID), "ready", 0)
		}

		// Send completion message to Kafka
		completionMsg := models.VideoProcessingComplete{
			VideoID:      videoID,
			Success:      err == nil,
			ThumbnailURL: thumbnailURL,
			Formats:      formatList,
			Duration:     duration,
			Timestamp:    time.Now().Unix(),
		}
		
		if err := p.kafkaProducer.ProduceJSON("video-processing-complete", videoID, completionMsg); err != nil {
			log.Printf("Failed to send completion message: %v", err)
		}
	}()

	// Return immediate response
	return &video.ProcessVideoResponse{
		Success:  true,
		VideoId:  videoID,
		Formats:  nil, // Will be filled later
		ThumbnailUrl: "",
	}, nil
}

// GenerateThumbnail implements the GenerateThumbnail RPC method
func (p *VideoProcessor) GenerateThumbnail(ctx context.Context, req *video.GenerateThumbnailRequest) (*video.GenerateThumbnailResponse, error) {
	videoID := req.VideoId
	videoPath := req.VideoPath
	timestamp := req.Timestamp

	log.Printf("Generating thumbnail for video %s at %d ms", videoID, timestamp)

	// Create output directory
	outputDir := filepath.Join(p.outputBasePath, videoID)
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create output directory: %v", err)
	}

	// Output thumbnail path
	thumbnailPath := filepath.Join(outputDir, "thumbnail.jpg")

	// Use ffmpeg to extract thumbnail
	// If timestamp is 0, use the first frame, otherwise seek to the specified position
	var cmd *exec.Cmd
	if timestamp == 0 {
		cmd = exec.Command("ffmpeg", "-i", videoPath, "-vframes", "1", "-an", "-s", "640x360", "-ss", "0", thumbnailPath)
	} else {
		// Convert timestamp from milliseconds to seconds for ffmpeg
		seconds := fmt.Sprintf("%.3f", float64(timestamp)/1000.0)
		cmd = exec.Command("ffmpeg", "-i", videoPath, "-vframes", "1", "-an", "-s", "640x360", "-ss", seconds, thumbnailPath)
	}

	// Run the command
	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("ffmpeg error: %s", output)
		return nil, status.Errorf(codes.Internal, "failed to generate thumbnail: %v", err)
	}

	// Upload thumbnail to storage
	thumbnailURL, err := p.videoStorage.UploadFile(thumbnailPath, fmt.Sprintf("%s/thumbnail.jpg", videoID), "image/jpeg")
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to upload thumbnail: %v", err)
	}

	// Store thumbnail URL in Redis
	p.redisClient.Set(ctx, fmt.Sprintf("video:%s:thumbnail", videoID), thumbnailURL, 0)

	return &video.GenerateThumbnailResponse{
		Success:      true,
		ThumbnailUrl: thumbnailURL,
	}, nil
}

// TranscodeVideo implements the TranscodeVideo RPC method
func (p *VideoProcessor) TranscodeVideo(ctx context.Context, req *video.TranscodeVideoRequest) (*video.TranscodeVideoResponse, error) {
	videoID := req.VideoId
	inputPath := req.InputPath
	formats := req.Formats

	log.Printf("Transcoding video %s to formats: %v", videoID, formats)

	// Create output directory
	outputDir := filepath.Join(p.outputBasePath, videoID)
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create output directory: %v", err)
	}

	var videoFormats []*video.VideoFormat

	// Process each format
	for _, format := range formats {
		outputPath := filepath.Join(outputDir, fmt.Sprintf("%s.mp4", format))
		
		var cmd *exec.Cmd
		switch format {
		case "hd":
			cmd = exec.Command("ffmpeg", "-i", inputPath, "-c:v", "libx264", "-preset", "medium", "-crf", "22", 
				"-c:a", "aac", "-b:a", "128k", "-movflags", "+faststart", "-vf", "scale=-2:720", outputPath)
		case "sd":
			cmd = exec.Command("ffmpeg", "-i", inputPath, "-c:v", "libx264", "-preset", "medium", "-crf", "23", 
				"-c:a", "aac", "-b:a", "96k", "-movflags", "+faststart", "-vf", "scale=-2:480", outputPath)
		case "low":
			cmd = exec.Command("ffmpeg", "-i", inputPath, "-c:v", "libx264", "-preset", "medium", "-crf", "28", 
				"-c:a", "aac", "-b:a", "64k", "-movflags", "+faststart", "-vf", "scale=-2:360", outputPath)
		default:
			continue // Skip unknown formats
		}

		// Run the command
		output, err := cmd.CombinedOutput()
		if err != nil {
			log.Printf("ffmpeg error: %s", output)
			continue
		}

		// Get file size
		fileInfo, err := os.Stat(outputPath)
		if err != nil {
			log.Printf("Failed to get file size: %v", err)
			continue
		}

		// Upload transcoded file to storage
		url, err := p.videoStorage.UploadFile(outputPath, fmt.Sprintf("%s/%s.mp4", videoID, format), "video/mp4")
		if err != nil {
			log.Printf("Failed to upload file: %v", err)
			continue
		}

		// Store path in Redis
		p.redisClient.Set(ctx, fmt.Sprintf("video:%s:path:%s", videoID, format), outputPath, 0)

		// Add to formats list
		videoFormats = append(videoFormats, &video.VideoFormat{
			Quality:  format,
			Url:      url,
			Size:     fileInfo.Size(),
			MimeType: "video/mp4",
		})
	}

	return &video.TranscodeVideoResponse{
		Success: len(videoFormats) > 0,
		Formats: videoFormats,
	}, nil
}

// getVideoDuration extracts the duration of a video file in seconds
func (p *VideoProcessor) getVideoDuration(filePath string) (int64, error) {
	cmd := exec.Command("ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", 
		"default=noprint_wrappers=1:nokey=1", filePath)
	
	output, err := cmd.CombinedOutput()
	if err != nil {
		return 0, fmt.Errorf("ffprobe error: %v, output: %s", err, output)
	}
	
	// Parse duration (output is in seconds as a float)
	durationStr := strings.TrimSpace(string(output))
	durationFloat, err := strconv.ParseFloat(durationStr, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse duration: %v", err)
	}
	
	// Convert to seconds (integer)
	return int64(durationFloat), nil
}