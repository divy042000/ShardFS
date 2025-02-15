package storage

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// ChunkMetadata stores checksum and version details for a chunk
type ChunkMetadata struct {
	Checksum string `json:"checksum"`
	Version  int    `json:"version"`
}

// WriteChunk stores a chunk on disk, generates a checksum, and saves metadata
func WriteChunk(storagePath, chunkID string, data []byte, version int) error {
	chunkPath := filepath.Join(storagePath, chunkID+".chunk")
	metaPath := filepath.Join(storagePath, chunkID+".meta")

	// Ensure storage directory exists
	if err := os.MkdirAll(storagePath, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create storage directory: %w", err)
	}

	// Compute checksum for integrity verification
	checksum := computeChecksum(data)

	// Save chunk data to disk atomically
	if err := atomicWriteFile(chunkPath, data); err != nil {
		return fmt.Errorf("failed to write chunk data: %w", err)
	}

	// Save metadata
	metadata := ChunkMetadata{Checksum: checksum, Version: version}
	if err := saveMetadata(metaPath, metadata); err != nil {
		return fmt.Errorf("failed to save metadata: %w", err)
	}

	return nil
}

// ReadChunk retrieves a chunk from disk given an offset and length
func ReadChunk(storagePath, chunkID string) ([]byte, error) {
	chunkPath := filepath.Join(storagePath, chunkID+".chunk")

	file, err := os.Open(chunkPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open chunk file: %w", err)
	}
	defer file.Close()

	// Read file contents
	data, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read chunk data: %w", err)
	}

	return data, nil
}

// computeChecksum generates an MD5 checksum for chunk integrity verification
func computeChecksum(data []byte) string {
	hash := md5.Sum(data)
	return hex.EncodeToString(hash[:])
}

// saveMetadata writes metadata to a JSON file
func saveMetadata(metaPath string, metadata ChunkMetadata) error {
	// Serialize metadata to JSON
	metaDataBytes, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	// Write metadata atomically
	return atomicWriteFile(metaPath, metaDataBytes)
}

// atomicWriteFile writes data to a file atomically (ensures data integrity)
func atomicWriteFile(filePath string, data []byte) error {
	tempPath := filePath + ".tmp"

	// Write data to a temporary file
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write temp file: %w", err)
	}

	// Rename the temp file to final file (atomic operation)
	return os.Rename(tempPath, filePath)
}

