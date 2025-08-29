// package api

// import (
// 	"net/http"
// )

// // Stub Handlers — just return 200 OK for now

// func uploadFileHandler(w http.ResponseWriter, r *http.Request) {
// 	w.WriteHeader(http.StatusCreated)
// 	w.Write([]byte("uploadFileHandler not implemented yet"))
// }

// func listFilesHandler(w http.ResponseWriter, r *http.Request) {
// 	w.WriteHeader(http.StatusOK)
// 	w.Write([]byte("listFilesHandler not implemented yet"))
// }

// func downloadFileHandler(w http.ResponseWriter, r *http.Request) {
// 	w.WriteHeader(http.StatusOK)
// 	w.Write([]byte("downloadFileHandler not implemented yet"))
// }

// func renameFileHandler(w http.ResponseWriter, r *http.Request) {
// 	w.WriteHeader(http.StatusOK)
// 	w.Write([]byte("renameFileHandler not implemented yet"))
// }

// func deleteFileHandler(w http.ResponseWriter, r *http.Request) {
// 	w.WriteHeader(http.StatusOK)
// 	w.Write([]byte("deleteFileHandler not implemented yet"))
// }

// func simulateFaultHandler(w http.ResponseWriter, r *http.Request) {
// 	w.WriteHeader(http.StatusOK)
// 	w.Write([]byte("simulateFaultHandler not implemented yet"))
// }

// new onw with upload logic
// internal/api/handlers.go
package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"distributed-storage/internal/core"
)

// uploadFileHandler handles the POST /files endpoint for file uploads.
func uploadFileHandler(w http.ResponseWriter, r *http.Request) {
	// Use middleware to handle authentication and authorization
	if !isAuthenticated(r) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if !hasPermission("files:write", core.LoggedInUser) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	// Parse the file from the request
	r.ParseMultipartForm(100 << 20) // 100 MB max
	file, fileHeader, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "Invalid file", http.StatusBadRequest)
		return
	}
	defer file.Close()

	// Copy the file content to a buffer for processing.
	fileBuffer := bytes.NewBuffer(nil)
	if _, err := io.Copy(fileBuffer, file); err != nil {
		http.Error(w, "Failed to read file", http.StatusInternalServerError)
		return
	}

	// Hand off to the core storage logic.
	meta, err := core.UploadFile(fileBuffer.Bytes(), fileHeader.Filename, core.LoggedInUser)
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
}

// downloadFileHandler handles the GET /files/{fileId} endpoint.
func downloadFileHandler(w http.ResponseWriter, r *http.Request) {
	// Use middleware to handle authentication and authorization.
	if !isAuthenticated(r) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if !hasPermission("files:read", core.LoggedInUser) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	fileID := getFileIDFromPath(r.URL.Path)
	if fileID == "" {
		http.Error(w, "Invalid file ID", http.StatusBadRequest)
		return
	}

	// Hand off to the core storage logic.
	fileData, metadata, err := core.DownloadFile(fileID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", metadata.Name))
	w.Write(fileData)
}

// listFilesHandler handles the GET /files endpoint.
func listFilesHandler(w http.ResponseWriter, r *http.Request) {
	if !isAuthenticated(r) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if !hasPermission("files:read", core.LoggedInUser) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	files := core.ListUserFiles(core.LoggedInUser)
	json.NewEncoder(w).Encode(files)
}

// renameFileHandler handles the PUT /files/{fileId}/rename endpoint.
func renameFileHandler(w http.ResponseWriter, r *http.Request) {
	if !isAuthenticated(r) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if !hasPermission("files:write", core.LoggedInUser) {
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

	if err := core.RenameFile(fileID, requestBody.NewName); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"message": "File renamed successfully",
		"fileId":  fileID,
	})
}

// deleteFileHandler handles the DELETE /files/{fileId} endpoint.
func deleteFileHandler(w http.ResponseWriter, r *http.Request) {
	if !isAuthenticated(r) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if !hasPermission("files:delete", core.LoggedInUser) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	fileID := getFileIDFromPath(r.URL.Path)
	if fileID == "" {
		http.Error(w, "Invalid file ID", http.StatusBadRequest)
		return
	}

	if err := core.DeleteFile(fileID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"message": "File deleted successfully",
		"fileId":  fileID,
	})
}

// simulateFaultHandler allows for manual fault injection for testing.
func simulateFaultHandler(w http.ResponseWriter, r *http.Request) {
	log.Println("Simulating a fault by corrupting a chunk...")
	corruptedChunkID, err := core.CorruptRandomChunk()
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