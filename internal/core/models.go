package core

import "time"

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

type ChunkPointer struct {
	ChunkID          string   `json:"chunkId"`
	Size             int      `json:"size"`
	Checksum         string   `json:"checksum"`
	ReplicaLocations []string `json:"replicaLocations"`
}

var Users = map[string]string{
	"user123": "admin",
	"user456": "contributor",
	"user789": "reader",
}

const LoggedInUser = "user456"

const (
	REPLICATION_FACTOR = 3
	WRITE_QUORUM       = 2
	READ_QUORUM        = 2
	CHUNK_SIZE         = 1024 * 1024
)
