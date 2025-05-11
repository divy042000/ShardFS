package storage

import (
	//       "log"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
)

// ChunkMetadata stores checksum and version details for a chunk
type ChunkMetadata struct {
	Checksum string `json:"checksum"`
	Version  int    `json:"version"`
}

func WriteChunk(storagePath string, chunkHash string, chunkIndex int32, data []byte) error {
	// Sanitize chunkHash to avoid any unwanted characters
	sanitizedHash := strings.ReplaceAll(chunkHash, "/", "_")

	// Create file names using chunkHash and chunkIndex
	baseName := fmt.Sprintf("%s_%d", sanitizedHash, chunkIndex)
	chunkPath := filepath.Join(storagePath, baseName+".chunk")
	metaPath := filepath.Join(storagePath, baseName+".meta")

	// Ensure the storage directory exists
	if err := os.MkdirAll(storagePath, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create storage directory: %w", err)
	}

	// Compute checksum for integrity verification
	checksum := ComputeChecksum(data)

	// Save chunk data atomically
	if err := AtomicWriteFile(chunkPath, data); err != nil {
		return fmt.Errorf("failed to write chunk data: %w", err)
	}

	// Save metadata with checksum
	metadata := ChunkMetadata{Checksum: checksum}
	if err := SaveMetadata(metaPath, metadata); err != nil {
		return fmt.Errorf("failed to save metadata: %w", err)
	}

	return nil
}





// WriteChunk stores a chunk on disk, generates a checksum, and saves metadata
func ReadChunk(storagePath, chunkHash string, chunkIndex int32) ([]byte, error) {
	// Sanitize chunkHash
	sanitizedHash := strings.ReplaceAll(chunkHash, "/", "_")

	// Construct the chunk file name
	baseName := fmt.Sprintf("%s_%d", sanitizedHash, chunkIndex)
	chunkPath := filepath.Join(storagePath, baseName+".chunk")

	log.Printf("📂 Attempting to read chunk from: %s", chunkPath)

	// Open the chunk file
	file, err := os.Open(chunkPath)
	if err != nil {
		log.Printf("❌ Failed to open chunk file '%s': %v", chunkPath, err)
		return nil, fmt.Errorf("failed to open chunk file: %w", err)
	}
	defer file.Close()

	// Read the contents
	data, err := io.ReadAll(file)
	if err != nil {
		log.Printf("❌ Failed to read chunk data from '%s': %v", chunkPath, err)
		return nil, fmt.Errorf("failed to read chunk data: %w", err)
	}

	log.Printf("✅ Successfully read chunk '%s' (%d bytes)", chunkPath, len(data))
	return data, nil
}



// ComputeChecksum generates an MD5 checksum for chunk integrity verification
func ComputeChecksum(data []byte) string {
	hash := md5.Sum(data)
	return hex.EncodeToString(hash[:])
}

func SaveMetadata(metaPath string, metadata ChunkMetadata) error {
	data, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	return AtomicWriteFile(metaPath, data)
}

func AtomicWriteFile(filePath string, data []byte) error {
	tempFile := filePath + ".tmp"
	if err := os.WriteFile(tempFile, data, 0644); err != nil {
		return err
	}
	return os.Rename(tempFile, filePath)
}


func DeleteChunkFromDisk(chunkHash string, chunkIndex int32, baseDir string) error {
	chunkID := fmt.Sprintf("%s_%d", chunkHash, chunkIndex)
	chunkPath := filepath.Join(baseDir, chunkID)
	metaPath := chunkPath + ".meta"

	tempChunkPath := chunkPath + ".deleting"
	tempMetaPath := metaPath + ".deleting"

	
	if err := os.Rename(chunkPath, tempChunkPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to rename chunk file '%s': %v", chunkPath, err)
	}

	if err := os.Rename(metaPath, tempMetaPath); err != nil && !os.IsNotExist(err) {
	
		_ = os.Rename(tempChunkPath, chunkPath)
		return fmt.Errorf("failed to rename metadata file '%s': %v", metaPath, err)
	}

	
	if err := os.Remove(tempChunkPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete temp chunk file '%s': %v", tempChunkPath, err)
	}

	if err := os.Remove(tempMetaPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete temp metadata file '%s': %v", tempMetaPath, err)
	}

	return nil
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
