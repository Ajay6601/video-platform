package main

import (
	"context"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/Ajay6601/video-platform/internal/models"
	"github.com/Ajay6601/video-platform/pkg/config"
	"github.com/Ajay6601/video-platform/pkg/kafka"
	"github.com/Ajay6601/video-platform/pkg/redis"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Create Redis client
	redisClient := redis.NewClient(
		cfg.Redis.Address,
		cfg.Redis.Password,
		cfg.Redis.DB,
	)
	defer redisClient.Close()

	// Create Kafka producer
	kafkaProducer, err := kafka.NewProducer(cfg.Kafka.BootstrapServers)
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer kafkaProducer.Close()

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle OS signals for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Received shutdown signal, gracefully shutting down...")
		cancel()
	}()

	// Create upload directory if it doesn't exist
	if err := os.MkdirAll(cfg.Storage.UploadPath, 0755); err != nil {
		log.Fatalf("Failed to create upload directory: %v", err)
	}

	// Initialize Gin router
	router := gin.Default()

	// Configure maximum file size
	router.MaxMultipartMemory = 32 << 20 // 32 MiB

	// Setup routes
	router.POST("/upload", func(c *gin.Context) {
		// Get form values
		title := c.PostForm("title")
		description := c.PostForm("description")
		tags := c.PostFormArray("tags")

		// Get file
		file, header, err := c.Request.FormFile("video")
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "No video file provided"})
			return
		}
		defer file.Close()

		// Validate file type
		if !isVideoFile(header.Filename) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid file type. Only video files are allowed"})
			return
		}

		// Generate IDs
		videoID := uuid.New().String()
		uploadPath := filepath.Join(cfg.Storage.UploadPath, videoID+filepath.Ext(header.Filename))

		// Create destination file
		dst, err := os.Create(uploadPath)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create file"})
			return
		}
		defer dst.Close()

		// Copy file to destination
		if _, err = io.Copy(dst, file); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save file"})
			return
		}

		// Store metadata in Redis
		redisClient.Set(ctx, "video:"+videoID+":title", title, 0)
		redisClient.Set(ctx, "video:"+videoID+":description", description, 0)
		redisClient.Set(ctx, "video:"+videoID+":path", uploadPath, 0)
		redisClient.Set(ctx, "video:"+videoID+":status", "uploaded", 0)
		
		// If tags are provided, store them as JSON
		if len(tags) > 0 {
			tagsJSON, _ := json.Marshal(tags)
			redisClient.Set(ctx, "video:"+videoID+":tags", string(tagsJSON), 0)
		}

		// Send message to Kafka for processing
		message := models.VideoProcessingMessage{
			VideoID:   videoID,
			FilePath:  uploadPath,
			Formats:   []string{"hd", "sd"},
			Timestamp: time.Now().Unix(),
		}

		if err := kafkaProducer.ProduceJSON("video-processing", videoID, message); err != nil {
			log.Printf("Failed to send processing message: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to queue video for processing"})
			return
		}

		// Return success response
		c.JSON(http.StatusOK, gin.H{
			"message":  "Video uploaded successfully",
			"video_id": videoID,
		})
	})

	// Start HTTP server
	log.Printf("Starting HTTP server on %s", cfg.API.UploadAddress)
	if err := router.Run(cfg.API.UploadAddress); err != nil {
		log.Fatalf("Failed to run server: %v", err)
	}
}

// isVideoFile checks if the filename has a video extension
func isVideoFile(filename string) bool {
	ext := strings.ToLower(filepath.Ext(filename))
	videoExtensions := map[string]bool{
		".mp4":  true,
		".avi":  true,
		".mov":  true,
		".wmv":  true,
		".flv":  true,
		".mkv":  true,
		".webm": true,
	}
	return videoExtensions[ext]
}