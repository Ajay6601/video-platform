package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Ajay6601/video-platform/internal/models"
	"github.com/Ajay6601/video-platform/internal/service"
	"github.com/Ajay6601/video-platform/pkg/api/video"
	"github.com/Ajay6601/video-platform/pkg/config"
	"github.com/Ajay6601/video-platform/pkg/kafka"
	"github.com/Ajay6601/video-platform/pkg/redis"
	"github.com/Ajay6601/video-platform/pkg/storage"
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

	// Create Kafka consumer
	kafkaConsumer, err := kafka.NewConsumer(
		cfg.Kafka.BootstrapServers,
		"stream-processor",
		[]string{"video-processing"},
	)
	if err != nil {
		log.Fatalf("Failed to create Kafka consumer: %v", err)
	}
	defer kafkaConsumer.Close()

	// Create storage
	videoStorage := storage.NewLocalStorage(
		cfg.Storage.BasePath,
		cfg.Storage.BaseURL,
	)

	// Create video processor
	videoProcessor := service.NewVideoProcessor(
		redisClient,
		kafkaProducer,
		videoStorage,
		cfg.Storage.ProcessedPath,
	)

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

	// Start gRPC server for processing service
	go startProcessingServer(videoProcessor, cfg.Services.ProcessingAddress)

	// Start consuming messages
	log.Println("Stream processor started, consuming from 'video-processing' topic")
	err = kafkaConsumer.Consume(ctx, func(topic string, key, value []byte) error {
		if topic == "video-processing" {
			var msg models.VideoProcessingMessage
			if err := json.Unmarshal(value, &msg); err != nil {
				log.Printf("Error unmarshaling message: %v", err)
				return err
			}

			log.Printf("Processing video: %s", msg.VideoID)

			// Process the video
			_, err := videoProcessor.ProcessVideo(context.Background(), &video.ProcessVideoRequest{
				VideoId:           msg.VideoID,
				InputPath:         msg.FilePath,
				OutputFormats:     msg.Formats,
				GenerateThumbnail: true,
			})
			if err != nil {
				log.Printf("Error processing video: %v", err)
				return err
			}
		}
		return nil
	})

	if err != nil && err != context.Canceled {
		log.Fatalf("Error consuming messages: %v", err)
	}

	log.Println("Stream processor shutting down")
}

// startProcessingServer starts the gRPC server for the processing service
func startProcessingServer(processor *service.VideoProcessor, address string) {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	
	// Register services
	video.RegisterVideoProcessingServiceServer(grpcServer, processor)
	
	log.Printf("Starting processing gRPC server on %s", address)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}