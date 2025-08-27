# Video Streaming Platform

A complete video streaming platform built with Go, gRPC, Redis, Kafka, and Docker.

## Features

- Video upload and streaming
- Video transcoding to multiple formats
- Thumbnail generation
- Metadata management
- Scalable architecture with microservices
- Event-driven processing with Kafka

## Prerequisites

- Docker and Docker Compose

## Quick Start

1. Clone the repository:
   ```bash
   git clone https://github.com/Ajay6601/video-platform.git
   cd video-platform
   ```

2. Start the services with Docker Compose:
   ```bash
   docker-compose up -d
   ```

3. Check if all services are running:
   ```bash
   docker-compose ps
   ```

## Services

- **API Gateway (http://localhost:8080)**: REST API for video management
- **Stream Ingestor**: Handles video uploads
- **Stream Processor**: Processes videos (transcoding, thumbnail generation)
- **Kafka (localhost:9092)**: Message broker for event-driven processing
- **Redis (localhost:6379)**: Cache and metadata storage
- **Kafka UI (http://localhost:8081)**: Web UI for monitoring Kafka
- **Redis Commander (http://localhost:8082)**: Web UI for monitoring Redis

## API Endpoints

### Upload a Video
```bash
curl -X POST -F "title=Test Video" -F "description=A test video" \
     -F "video=@/path/to/video.mp4" \
     http://localhost:8080/upload
```

### Get Video Details
```bash
curl http://localhost:8080/api/v1/videos/{video_id}
```

### Stream a Video
```bash
# Open in browser or video player
http://localhost:8080/api/v1/videos/{video_id}/stream?quality=hd
```

### List Videos
```bash
curl http://localhost:8080/api/v1/videos?page=1&page_size=10
```

### Update Video Metadata
```bash
curl -X PATCH -H "Content-Type: application/json" \
     -d '{"title":"Updated Title", "description":"Updated description"}' \
     http://localhost:8080/api/v1/videos/{video_id}
```

### Delete a Video
```bash
curl -X DELETE http://localhost:8080/api/v1/videos/{video_id}
```

## Architecture

This platform follows a microservices architecture with event-driven processing:

1. **API Gateway**: Exposes REST APIs and forwards requests to appropriate services
2. **Stream Ingestor**: Handles video uploads and generates processing events
3. **Stream Processor**: Processes videos based on events from Kafka
4. **Redis**: Stores metadata and caches frequently accessed data
5. **Kafka**: Enables asynchronous processing and communication between services

The system uses gRPC for efficient inter-service communication and REST APIs for external clients.

## Directory Structure

- `api/proto`: Protocol buffer definitions
- `cmd`: Service entry points
- `internal`: Internal application code
- `pkg`: Reusable packages
- `docker`: Docker-related files

## Development

### Generate Protocol Buffers

```bash
protoc -I=api/proto --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       --grpc-gateway_out=. --grpc-gateway_opt=paths=source_relative \
       api/proto/video.proto
```

## Troubleshooting

### Common Issues

1. **Services fail to start**: Check Docker logs with `docker-compose logs [service-name]`
2. **Video upload fails**: Ensure the upload directory has proper permissions
3. **Video processing hangs**: Check Kafka UI to see if messages are being consumed

### Logs

View logs for all services:
```bash
docker-compose logs -f
```

View logs for a specific service:
```bash
docker-compose logs -f api-gateway
```

### Next Steps

To extend this platform, you might consider:

Authentication & Authorization: Add user management and access control
Analytics: Track video views and user engagement
CDN Integration: Distribute content across edge locations
Search Capabilities: Add video search by metadata
Playlists & Collections: Group videos for better organization

## License

[MIT License](LICENSE)
```
