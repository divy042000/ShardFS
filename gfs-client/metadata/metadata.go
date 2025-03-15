package metadata

import (
	"crypto/sha256"
	"encoding/hex"
	"time"

	pb "gfs-client/proto"
)

// FileMetadata represents client-side file metadata
type FileMetadata struct {
	FileName        string
	FileFormat      string
	TotalSize       int64
	ChunkCount      int
	ChunkSizes      []int64
	ChunkHashes     []string
	Timestamp       int64
	ClientID        string
	Priority        int
	RedundancyLevel int
	CompressionUsed bool
}

// NewFileMetadata creates a new metadata instance
func NewFileMetadata(fileName string, totalSize int64, chunkSizes []int64, chunkHashes []string, clientID string) *FileMetadata {
	return &FileMetadata{
		FileName:        fileName,
		FileFormat:      "binary", // Default, can be extended
		TotalSize:       totalSize,
		ChunkCount:      len(chunkSizes),
		ChunkSizes:      chunkSizes,
		ChunkHashes:     chunkHashes,
		Timestamp:       time.Now().Unix(),
		ClientID:        clientID,
		Priority:        1,          // Default priority
		RedundancyLevel: 3,          // Default redundancy
		CompressionUsed: false,      // Default to no compression
	}
}

// ToProto converts FileMetadata to gRPC RegisterFileRequest
func (m *FileMetadata) ToProto() *pb.RegisterFileRequest {
	return &pb.RegisterFileRequest{
		FileName:        m.FileName,
		FileFormat:      m.FileFormat,
		TotalSize:       m.TotalSize,
		ChunkCount:      int32(m.ChunkCount),
		ChunkSizes:      m.ChunkSizes,
		ChunkHashes:     m.ChunkHashes,
		Timestamp:       m.Timestamp,
		ClientId:        m.ClientID,
		Priority:        int32(m.Priority),
		RedundancyLevel: int32(m.RedundancyLevel),
		CompressionUsed: m.CompressionUsed,
	}
}

// GenerateChunkHash computes SHA-256 hash for a chunk
func GenerateChunkHash(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}