# Distributed File Storage Prototype

This project is a minimal, self-contained prototype of a distributed file storage system built in Go. It simulates the core functionality and principles of a robust, scalable system.

### Features
- **RESTful API:** For file management (upload, download, rename, list).
- **Quorum-Based Consistency:** Uses a configurable quorum to ensure strong consistency without a single point of failure.
- **Data Chunking & Replication:** Splits files into small chunks and replicates them for fault tolerance.
- **Rich Metadata:** Stores comprehensive metadata to support advanced features.
- **RBAC:** A simple role-based access control system to simulate production-grade security.

### How to Run

1.  **Initialize the Go module:**
    ```bash
    go mod tidy
    ```
2.  **Run the server:**
    ```bash
    go run ./cmd/server
    ```
3.  **Test the endpoints using cURL or Postman:**
    - The server runs on `http://localhost:8080`.
    - Use the header `Authorization: Bearer my-jwt-token` to authenticate as a "contributor" user.

### API Endpoints

- **Upload a file:**
  ```bash
  curl -X POST -H "Authorization: Bearer my-jwt-token" -F "file=@/path/to/your/file.txt" http://localhost:8080/files  
  ```

- **Download a file:**
  ```bash
  curl -X GET -H "Authorization: Bearer my-jwt-token" http://localhost:8080/files/<file-id> -o downloaded-file.txt
  ```

- **List all files:**
  ```bash
  curl -X GET -H "Authorization: Bearer my-jwt-token" http://localhost:8080/files
  ```

- **Rename a file:**
  ```bash
  curl -X PUT -H "Authorization: Bearer my-jwt-token" -H "Content-Type: application/json" -d '{"newName": "new_name.txt"}' http://localhost:8080/files/<file-id>/rename
  ```

- **Simulate a fault:**
  ```bash
  curl -X GET http://localhost:8080/simulate-fault
  ```
