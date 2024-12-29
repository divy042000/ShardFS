package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
)

const (
	ChunkSize      = 64 * 1024 * 1024             // 64 MB
	MaxRetries     = 3                            // Maximum retries for chunk uploads
	ChunkServerURL = "http://chunk-server/upload" // Mock chunk server URL
)

// ChunkMetadata holds metadata for a single chunk
type ChunkMetadata struct {
	ChunkID    string
	ChunkIndex int
	Size       int
	FileName   string
	Offset     int64
	Checksum   string
}

// UploadResult tracks the result of a single chunk upload
type UploadResult struct {
	Success bool
	Error   error
}

// uploadChunk uploads a single chunk to the chunk server
func uploadChunk(chunkData []byte, metadata ChunkMetadata) UploadResult {
	retries := 0

	for retries < MaxRetries {
		// Simulate chunk upload via HTTP POST
		req, err := http.NewRequest("POST", ChunkServerURL, bytes.NewReader(chunkData))
		if err != nil {
			return UploadResult{Success: false, Error: err}
		}

		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("Chunk-ID", metadata.ChunkID)
		req.Header.Set("Chunk-Index", strconv.Itoa(metadata.ChunkIndex))
		req.Header.Set("File-Name", metadata.FileName)

		resp, err := http.DefaultClient.Do(req)
		if err != nil || resp.StatusCode != http.StatusOK {
			retries++
			continue
		}
		defer resp.Body.Close()

		// Success
		return UploadResult{Success: true, Error: nil}
	}

	// Failure after retries
	return UploadResult{Success: false, Error: fmt.Errorf("failed to upload chunk after %d retries", MaxRetries)}
}

// processFile handles streaming the file, splitting it into chunks, and uploading them in parallel
func processFile(filePath string, fileName string) error {
	// Open file
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	buffer := make([]byte, ChunkSize)
	chunkIndex := 0
	offset := int64(0)

	var wg sync.WaitGroup
	errorChannel := make(chan error, 1) // Channel to handle upload errors

	for {
		// Read data into buffer
		bytesRead, err := reader.Read(buffer)
		if err != nil && err != io.EOF {
			return fmt.Errorf("error reading file: %w", err)
		}

		if bytesRead == 0 {
			break // End of file
		}

		// Prepare chunk metadata
		metadata := ChunkMetadata{
			ChunkID:    fmt.Sprintf("%s_chunk_%d", fileName, chunkIndex),
			ChunkIndex: chunkIndex,
			Size:       bytesRead,
			FileName:   fileName,
			Offset:     offset,
		}

		// Capture the chunk data to avoid overwriting in parallel goroutines
		chunkData := make([]byte, bytesRead)
		copy(chunkData, buffer[:bytesRead])

		// Launch a goroutine to upload the chunk
		wg.Add(1)
		go func(chunkData []byte, metadata ChunkMetadata) {
			defer wg.Done()
			result := uploadChunk(chunkData, metadata)
			if !result.Success {
				errorChannel <- result.Error
			}
		}(chunkData, metadata)

		// Update offset and chunk index
		offset += int64(bytesRead)
		chunkIndex++
	}

	// Wait for all uploads to complete
	go func() {
		wg.Wait()
		close(errorChannel)
	}()

	// Check for errors
	for err := range errorChannel {
		if err != nil {
			return fmt.Errorf("upload failed: %w", err)
		}
	}

	log.Printf("File %s successfully uploaded in %d chunks", fileName, chunkIndex)
	return nil
}

func main() {
	// Mock file upload
	filePath := "largefile.txt" // Path to file to upload
	fileName := "largefile.txt"

	err := processFile(filePath, fileName)
	if err != nil {
		log.Fatalf("Upload failed: %v", err)
	}
}
