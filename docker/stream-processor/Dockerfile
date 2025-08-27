FROM golang:1.18-alpine AS build

WORKDIR /app

# Copy go.mod and go.sum files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux go build -o /stream-processor ./cmd/stream-processor

# Create final lightweight image
FROM alpine:latest

# Install ffmpeg for video processing
RUN apk --no-cache add ca-certificates ffmpeg

WORKDIR /app

COPY --from=build /stream-processor .

# Create necessary directories
RUN mkdir -p /data/processed

# Run as non-root user
RUN adduser -D appuser
RUN chown -R appuser:appuser /data
USER appuser

# Command to run the executable
CMD ["./stream-processor"]