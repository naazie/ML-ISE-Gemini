// main.go
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

//-----------------------------------------------------------------------------
// CORE (Business Logic)
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

// In-memory data structures to simulate the distributed system's state.
var (
	// MetadataService simulates the Control Plane.
	metadataService = struct {
		sync.RWMutex
		db map[string]FileMetadata
	}{db: make(map[string]FileMetadata)}

	// DataNodes simulates the Data Plane.
	dataNodes = struct {
		sync.RWMutex
		nodes map[string][]byte
	}{nodes: make(map[string][]byte)}
)

// init initializes the in-memory databases.
func init() {
	metadataService.db = make(map[string]FileMetadata)
	dataNodes.nodes = make(map[string][]byte)
}

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

		// Simulate quorum-based write
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

//-----------------------------------------------------------------------------
// API & Handlers
//-----------------------------------------------------------------------------

// isAuthenticated simulates a token validation check.
func isAuthenticated(r *http.Request) bool {
	return r.Header.Get("Authorization") == "Bearer my-jwt-token"
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

// uploadFileHandler handles the POST /files endpoint for file uploads.
func uploadFileHandler(w http.ResponseWriter, r *http.Request) {
	log.Println("Received request to upload file...")
	if !isAuthenticated(r) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if !hasPermission("files:write", LoggedInUser) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	r.ParseMultipartForm(100 << 20) // 100 MB max
	file, fileHeader, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "Invalid file", http.StatusBadRequest)
		return
	}
	defer file.Close()

	fileBuffer := bytes.NewBuffer(nil)
	if _, err := io.Copy(fileBuffer, file); err != nil {
		http.Error(w, "Failed to read file", http.StatusInternalServerError)
		return
	}

	meta, err := UploadFile(fileBuffer.Bytes(), fileHeader.Filename, LoggedInUser)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{
		"message":  "File uploaded successfully",
		"fileId":   meta.FileID,
		"filename": meta.Name,
	})
	log.Println("File uploaded successfully:", meta.FileID)
}

// downloadFileHandler handles the GET /files/{fileId} endpoint.
func downloadFileHandler(w http.ResponseWriter, r *http.Request) {
	log.Println("Received request to download file...")
	if !isAuthenticated(r) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if !hasPermission("files:read", LoggedInUser) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	fileID := getFileIDFromPath(r.URL.Path)
	if fileID == "" {
		http.Error(w, "Invalid file ID", http.StatusBadRequest)
		return
	}

	fileData, metadata, err := DownloadFile(fileID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", metadata.Name))
	w.Write(fileData)
	log.Println("File downloaded successfully:", fileID)
}

// listFilesHandler handles the GET /files endpoint.
func listFilesHandler(w http.ResponseWriter, r *http.Request) {
	log.Println("Received request to list files...")
	if !isAuthenticated(r) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if !hasPermission("files:read", LoggedInUser) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	files := ListUserFiles(LoggedInUser)
	json.NewEncoder(w).Encode(files)
	log.Println("Files listed successfully.")
}

// renameFileHandler handles the PUT /files/{fileId}/rename endpoint.
func renameFileHandler(w http.ResponseWriter, r *http.Request) {
	log.Println("Received request to rename file...")
	if !isAuthenticated(r) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if !hasPermission("files:write", LoggedInUser) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	fileID := getFileIDFromPath(r.URL.Path)
	if fileID == "" {
		http.Error(w, "Invalid file ID", http.StatusBadRequest)
		return
	}

	var requestBody struct {
		NewName string `json:"newName"`
	}
	if err := json.NewDecoder(r.Body).Decode(&requestBody); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	if err := RenameFile(fileID, requestBody.NewName); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"message": "File renamed successfully",
		"fileId":  fileID,
	})
	log.Println("File renamed successfully:", fileID)
}

// deleteFileHandler handles the DELETE /files/{fileId} endpoint.
func deleteFileHandler(w http.ResponseWriter, r *http.Request) {
	log.Println("Received request to delete file...")
	if !isAuthenticated(r) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if !hasPermission("files:delete", LoggedInUser) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	fileID := getFileIDFromPath(r.URL.Path)
	if fileID == "" {
		http.Error(w, "Invalid file ID", http.StatusBadRequest)
		return
	}

	if err := DeleteFile(fileID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"message": "File deleted successfully",
		"fileId":  fileID,
	})
	log.Println("File deleted successfully:", fileID)
}

// simulateFaultHandler allows for manual fault injection for testing.
func simulateFaultHandler(w http.ResponseWriter, r *http.Request) {
	log.Println("Received request to simulate fault...")
	corruptedChunkID, err := CorruptRandomChunk()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("Corrupted chunk replica with ID: %s", corruptedChunkID)))
}

// getFileIDFromPath is a helper function to extract the file ID.
func getFileIDFromPath(path string) string {
	parts := strings.Split(path, "/")
	if len(parts) >= 3 && parts[2] != "rename" {
		return parts[2]
	}
	return ""
}

// NewRouter creates a new HTTP multiplexer with all the API routes defined.
func NewRouter() *http.ServeMux {
	router := http.NewServeMux()

	router.HandleFunc("/files", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			uploadFileHandler(w, r)
		case http.MethodGet:
			listFilesHandler(w, r)
		default:
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		}
	})

	router.HandleFunc("/files/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			downloadFileHandler(w, r)
		case http.MethodPut:
			renameFileHandler(w, r)
		case http.MethodDelete:
			deleteFileHandler(w, r)
		default:
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		}
	})

	router.HandleFunc("/simulate-fault", simulateFaultHandler)

	return router
}

// main.go entry point
func main() {
	router := NewRouter()
	log.Println("Server started on http://localhost:8080")
	log.Println("To run this code, you must first initialize the Go module and download dependencies:")
	log.Println("  go mod init distributed-storage")
	log.Println("  go mod tidy")
	log.Println("Then you can run the server:")
	log.Println("  go run main.go")
	log.Println("Use the `README.md` file for instructions on how to test.")
	log.Fatal(http.ListenAndServe(":8080", router))
}
