package chunking

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
)	


const ChunkSize = 64 * 1024 * 1024 // 64MB

type Chunk struct {
	Index int
	Data  []byte
	Size  int64
	Hash  string
}

func ChunkFile(filePath string) ([]Chunk, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	var chunks []Chunk
	var wg sync.WaitGroup
	chunkChan := make(chan Chunk, 10)

	worker := func(index int, data []byte) {
		defer wg.Done()
		hash := sha256.Sum256(data)
		chunkChan <- Chunk{
			Index: index,
			Data:  data,
			Size:  int64(len(data)),
			Hash:  hex.EncodeToString(hash[:]),
		}
	}

	index := 0
	for {
		buf := make([]byte, ChunkSize)
		n, err := file.Read(buf)
		if n == 0 || err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error reading file: %v", err)
		}

		wg.Add(1)
		go worker(index, buf[:n])
		index++
	}

	go func() {
		wg.Wait()
		close(chunkChan)
	}()

	for chunk := range chunkChan {
		chunks = append(chunks, chunk)
	}

	// Optimized sorting
	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].Index < chunks[j].Index
	})

	return chunks, nil
}
