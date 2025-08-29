package api

import (
	"distributed-storage/internal/core"
	"net/http"
)

func isAuthenticated(r *http.Request) bool {
	return r.Header.Get("Authorization") == "Bearer my-jwt-token"
}

func hasPermission(perm string, user string) bool {
	role, ok := core.Users[user]
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
