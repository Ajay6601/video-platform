package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/yourusername/video-platform/internal/service"
	"github.com/yourusername/video-platform/pkg/api/video"
	"github.com/yourusername/video-platform/pkg/config"
	"github.com/yourusername/video-platform/pkg/kafka"
	"github.com/yourusername/video-platform/pkg/redis"
	"github.com/yourusername/video-platform/pkg/storage"
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

	// Create storage
	videoStorage := storage.NewLocalStorage(
		cfg.Storage.BasePath,
		cfg.Storage.BaseURL,
	)

	// Create video service
	videoService := service.NewVideoService(
		videoStorage,
		redisClient,
		kafkaProducer,
		cfg.Storage.UploadPath,
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

	// Start gRPC server
	go startGRPCServer(videoService, cfg.API.GRPCAddress)

	// Start REST gateway
	if err := startRESTGateway(ctx, cfg.API.GRPCAddress, cfg.API.HTTPAddress); err != nil {
		log.Fatalf("Failed to start REST gateway: %v", err)
	}
}

// startGRPCServer starts the gRPC server
func startGRPCServer(videoService *service.VideoService, address string) {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	
	// Register services
	video.RegisterVideoServiceServer(grpcServer, videoService)
	
	log.Printf("Starting gRPC server on %s", address)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

// startRESTGateway starts the REST gateway
func startRESTGateway(ctx context.Context, grpcAddress, httpAddress string) error {
	mux := runtime.NewServeMux()
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

	// Register gRPC services
	if err := video.RegisterVideoServiceHandlerFromEndpoint(ctx, mux, grpcAddress, opts); err != nil {
		return err
	}

	// Serve HTTP
	log.Printf("Starting REST gateway on %s", httpAddress)
	return http.ListenAndServe(httpAddress, mux)
}