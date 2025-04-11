package storage

import (
 //       "log"
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
	checksum := ComputeChecksum(data)

	// Save chunk data to disk atomically
	if err := AtomicWriteFile(chunkPath, data); err != nil {
		return fmt.Errorf("failed to write chunk data: %w", err)
	}

	// Save metadata
	metadata := ChunkMetadata{Checksum: checksum, Version: version}
	if err := SaveMetadata(metaPath, metadata); err != nil {
		return fmt.Errorf("failed to save metadata: %w", err)
	}

	return nil
}

// ReadChunk retrieves a chunk from disk
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

// ComputeChecksum generates an MD5 checksum for chunk integrity verification
func ComputeChecksum(data []byte) string {
	hash := md5.Sum(data)
	return hex.EncodeToString(hash[:])
}

// SaveMetadata writes metadata to a JSON file
func SaveMetadata(metaPath string, metadata ChunkMetadata) error {
	// Serialize metadata to JSON
	metaDataBytes, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	// Write metadata atomically
	return AtomicWriteFile(metaPath, metaDataBytes)
}

// AtomicWriteFile writes data to a file atomically (ensures data integrity)
func AtomicWriteFile(filePath string, data []byte) error {
	tempPath := filePath + ".tmp"

	// Write data to a temporary file
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write temp file: %w", err)
	}

	// Rename the temp file to final file (atomic operation)
	return os.Rename(tempPath, filePath)
}


// ListStoredChunks returns a list of all stored chunk IDs in the storage path
func ListStoredChunks(storagePath string) ([]string, error) {
	files, err := os.ReadDir(storagePath)
	if err != nil {
		return nil, fmt.Errorf("failed to list stored chunks: %w", err)
	}

	var chunkIDs []string
	for _, file := range files {
		if filepath.Ext(file.Name()) == ".chunk" { // ✅ Only include chunk files
			chunkID := file.Name()[:len(file.Name())-len(".chunk")] // Remove file extension
			chunkIDs = append(chunkIDs, chunkID)
		}
	}

	return chunkIDs, nil
}

