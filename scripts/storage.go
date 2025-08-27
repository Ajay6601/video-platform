package storage

import (
	"fmt"
	"io"
	"mime"
	"os"
	"path/filepath"
)

// Storage defines an interface for storage operations
type Storage interface {
	// UploadFile uploads a file to storage and returns its URL
	UploadFile(localPath, remotePath, contentType string) (string, error)
	
	// DownloadFile downloads a file from storage
	DownloadFile(remotePath, localPath string) error
	
	// DeleteFile deletes a file from storage
	DeleteFile(remotePath string) error
	
	// GetPublicURL gets the public URL for a file
	GetPublicURL(videoID, quality string) string
}

// LocalStorage implements Storage using the local filesystem
type LocalStorage struct {
	basePath  string
	baseURL   string
}

// NewLocalStorage creates a new local storage
func NewLocalStorage(basePath, baseURL string) *LocalStorage {
	return &LocalStorage{
		basePath: basePath,
		baseURL:  baseURL,
	}
}

// UploadFile copies a file to the storage directory
func (s *LocalStorage) UploadFile(localPath, remotePath, contentType string) (string, error) {
	// Create destination directory
	destPath := filepath.Join(s.basePath, filepath.Dir(remotePath))
	if err := os.MkdirAll(destPath, 0755); err != nil {
		return "", fmt.Errorf("failed to create directory: %w", err)
	}
	
	// Open source file
	src, err := os.Open(localPath)
	if err != nil {
		return "", fmt.Errorf("failed to open source file: %w", err)
	}
	defer src.Close()
	
	// Create destination file
	dest, err := os.Create(filepath.Join(s.basePath, remotePath))
	if err != nil {
		return "", fmt.Errorf("failed to create destination file: %w", err)
	}
	defer dest.Close()
	
	// Copy the file
	if _, err := io.Copy(dest, src); err != nil {
		return "", fmt.Errorf("failed to copy file: %w", err)
	}
	
	// Return the URL
	return fmt.Sprintf("%s/%s", s.baseURL, remotePath), nil
}

// DownloadFile copies a file from storage to local path
func (s *LocalStorage) DownloadFile(remotePath, localPath string) error {
	// Open source file
	src, err := os.Open(filepath.Join(s.basePath, remotePath))
	if err != nil {
		return fmt.Errorf("failed to open source file: %w", err)
	}
	defer src.Close()
	
	// Create destination directory
	destDir := filepath.Dir(localPath)
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}
	
	// Create destination file
	dest, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("failed to create destination file: %w", err)
	}
	defer dest.Close()
	
	// Copy the file
	if _, err := io.Copy(dest, src); err != nil {
		return fmt.Errorf("failed to copy file: %w", err)
	}
	
	return nil
}

// DeleteFile removes a file from storage
func (s *LocalStorage) DeleteFile(remotePath string) error {
	return os.Remove(filepath.Join(s.basePath, remotePath))
}

// GetPublicURL returns the public URL for a video
func (s *LocalStorage) GetPublicURL(videoID, quality string) string {
	if quality == "" {
		quality = "hd"
	}
	return fmt.Sprintf("%s/%s/%s.mp4", s.baseURL, videoID, quality)
}