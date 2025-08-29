// internal/core/storage.go
package core

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
)

//-----------------------------------------------------------------------------
// Core System Data Models and Constants
//-----------------------------------------------------------------------------

// FileMetadata represents the rich metadata for a file.
type FileMetadata struct {
	FileID           string            `json:"fileId"`
	Name             string            `json:"name"`
	SizeBytes        int64             `json:"sizeBytes"`
	OwnerID          string            `json:"ownerId"`
	CreationDate     time.Time         `json:"creationDate"`
	LastModifiedDate time.Time         `json:"lastModifiedDate"`
	Version          int               `json:"version"`
	FileChecksum     string            `json:"fileChecksum"`
	Permissions      map[string]string `json:"permissions"`
	ChunkPointers    []ChunkPointer    `json:"chunkPointers"`
}

// ChunkPointer stores information about a data chunk and its replicas.
type ChunkPointer struct {
	ChunkID          string   `json:"chunkId"`
	Size             int      `json:"size"`
	Checksum         string   `json:"checksum"`
	ReplicaLocations []string `json:"replicaLocations"`
}

// A simple in-memory user database.
var Users = map[string]string{
	"user123": "admin",
	"user456": "contributor",
	"user789": "reader",
}

// This simulates a single token for a logged-in user to keep the prototype simple.
const LoggedInUser = "user456"

// Core system constants.
const (
	REPLICATION_FACTOR = 3           // N: Number of data replicas.
	WRITE_QUORUM       = 2           // W: Number of replicas needed for a successful write.
	READ_QUORUM        = 2           // R: Number of replicas needed for a successful read.
	CHUNK_SIZE         = 1024 * 1024 // 1MB chunk size for simplicity.
)

//-----------------------------------------------------------------------------
// In-Memory Data Stores (Simulating Distributed Databases)
//-----------------------------------------------------------------------------

// metadataService simulates the Control Plane.
var metadataService = struct {
	sync.RWMutex
	db map[string]FileMetadata
}{db: make(map[string]FileMetadata)}

// dataNodes simulates the Data Plane.
var dataNodes = struct {
	sync.RWMutex
	nodes map[string][]byte
}{nodes: make(map[string][]byte)}

// init initializes the in-memory databases.
func init() {
	metadataService.db = make(map[string]FileMetadata)
	dataNodes.nodes = make(map[string][]byte)
}

//-----------------------------------------------------------------------------
// Core Storage Logic Functions
//-----------------------------------------------------------------------------

// UploadFile handles the core logic of chunking and replicating file data.
func UploadFile(fileContent []byte, filename, ownerID string) (FileMetadata, error) {
	fileID := uuid.New().String()
	fileChecksum := calculateChecksum(fileContent)
	chunkPointers := []ChunkPointer{}

	for i := 0; i < len(fileContent); i += CHUNK_SIZE {
		end := i + CHUNK_SIZE
		if end > len(fileContent) {
			end = len(fileContent)
		}
		chunkContent := fileContent[i:end]
		chunkID := uuid.New().String()
		chunkChecksum := calculateChecksum(chunkContent)

		// Simulate quorum-based write.
		// A write is only successful if it is replicated to W out of N nodes.
		var confirmedReplicas []string
		var wg sync.WaitGroup
		var mu sync.Mutex

		for j := 0; j < REPLICATION_FACTOR; j++ {
			wg.Add(1)
			go func(replicaIndex int) {
				defer wg.Done()
				replicaID := fmt.Sprintf("%s_replica_%d", chunkID, replicaIndex)
				dataNodes.Lock()
				dataNodes.nodes[replicaID] = chunkContent
				dataNodes.Unlock()

				mu.Lock()
				confirmedReplicas = append(confirmedReplicas, replicaID)
				mu.Unlock()
			}(j)
		}
		wg.Wait()

		if len(confirmedReplicas) < WRITE_QUORUM {
			log.Printf("Write quorum not met for chunk %s. Only %d replicas confirmed.", chunkID, len(confirmedReplicas))
			return FileMetadata{}, errors.New("write quorum not met")
		}

		chunkPointers = append(chunkPointers, ChunkPointer{
			ChunkID:          chunkID,
			Size:             len(chunkContent),
			Checksum:         chunkChecksum,
			ReplicaLocations: confirmedReplicas,
		})
	}

	metadataService.Lock()
	defer metadataService.Unlock()
	
	newMetadata := FileMetadata{
		FileID:           fileID,
		Name:             filename,
		SizeBytes:        int64(len(fileContent)),
		OwnerID:          ownerID,
		CreationDate:     time.Now(),
		LastModifiedDate: time.Now(),
		Version:          1,
		FileChecksum:     fileChecksum,
		Permissions:      map[string]string{ownerID: "rw-"},
		ChunkPointers:    chunkPointers,
	}
	metadataService.db[fileID] = newMetadata
	
	return newMetadata, nil
}

// DownloadFile retrieves a file by its ID, using quorum-based reads.
func DownloadFile(fileID string) ([]byte, FileMetadata, error) {
	metadataService.RLock()
	metadata, ok := metadataService.db[fileID]
	metadataService.RUnlock()
	if !ok {
		return nil, FileMetadata{}, errors.New("file not found")
	}

	var totalFileContent bytes.Buffer
	for _, chunk := range metadata.ChunkPointers {
		var confirmedReplicas int
		var chunkData []byte
		var wg sync.WaitGroup

		dataChan := make(chan []byte, REPLICATION_FACTOR)

		for _, replicaID := range chunk.ReplicaLocations {
			wg.Add(1)
			go func(id string) {
				defer wg.Done()
				dataNodes.RLock()
				data, exists := dataNodes.nodes[id]
				dataNodes.RUnlock()
				
				if exists && calculateChecksum(data) == chunk.Checksum {
					dataChan <- data
				}
			}(replicaID)
		}
		
		wg.Wait()
		close(dataChan)

		for data := range dataChan {
			chunkData = data
			confirmedReplicas++
			if confirmedReplicas >= READ_QUORUM {
				break
			}
		}

		if confirmedReplicas < READ_QUORUM {
			return nil, FileMetadata{}, fmt.Errorf("failed to retrieve chunk %s: not enough healthy replicas found", chunk.ChunkID)
		}

		totalFileContent.Write(chunkData)
	}

	if calculateChecksum(totalFileContent.Bytes()) != metadata.FileChecksum {
		return nil, FileMetadata{}, errors.New("file checksum mismatch, possible corruption")
	}

	return totalFileContent.Bytes(), metadata, nil
}

// ListUserFiles returns a list of files for a given user.
func ListUserFiles(ownerID string) []FileMetadata {
	metadataService.RLock()
	defer metadataService.RUnlock()

	var files []FileMetadata
	for _, meta := range metadataService.db {
		if meta.OwnerID == ownerID {
			files = append(files, meta)
		}
	}
	return files
}

// RenameFile updates the name of a file in the metadata.
func RenameFile(fileID, newName string) error {
	metadataService.Lock()
	defer metadataService.Unlock()

	metadata, ok := metadataService.db[fileID]
	if !ok {
		return errors.New("file not found")
	}
	
	metadata.Name = newName
	metadata.LastModifiedDate = time.Now()
	metadataService.db[fileID] = metadata
	
	return nil
}

// DeleteFile removes a file's metadata and its data chunks.
func DeleteFile(fileID string) error {
	metadataService.Lock()
	metadata, ok := metadataService.db[fileID]
	if !ok {
		metadataService.Unlock()
		return errors.New("file not found")
	}
	
	dataNodes.Lock()
	for _, chunk := range metadata.ChunkPointers {
		for _, replicaID := range chunk.ReplicaLocations {
			delete(dataNodes.nodes, replicaID)
		}
	}
	dataNodes.Unlock()
	
	delete(metadataService.db, fileID)
	metadataService.Unlock()
	
	return nil
}

// CorruptRandomChunk simulates a data corruption event for testing fault tolerance.
func CorruptRandomChunk() (string, error) {
	dataNodes.Lock()
	defer dataNodes.Unlock()

	if len(dataNodes.nodes) > 0 {
		var firstKey string
		for k := range dataNodes.nodes {
			firstKey = k
			break
		}
		dataNodes.nodes[firstKey] = []byte("corrupted_data_that_fails_checksum")
		return firstKey, nil
	}
	return "", errors.New("no chunks to corrupt")
}

// calculateChecksum computes a SHA256 checksum of a byte slice.
func calculateChecksum(data []byte) string {
	hash := sha256.New()
	hash.Write(data)
	return hex.EncodeToString(hash.Sum(nil))
}

// hasPermission simulates the RBAC check.
func hasPermission(perm string, user string) bool {
	role, ok := Users[user]
	if !ok {
		return false
	}
	switch role {
	case "admin":
		return true
	case "contributor":
		return perm == "files:write" || perm == "files:read" || perm == "files:delete"
	case "reader":
		return perm == "files:read"
	}
	return false
}
