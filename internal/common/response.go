package common

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
)

func WriteJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("error encoding JSON response: %v", err)
	}
}

func WriteError(w http.ResponseWriter, status int, message string) {
	WriteJSON(w, status, map[string]string{"error": message})
}

// WriteAppError maps typed errors to the correct HTTP status code.
// Handlers call this instead of manually checking error types.
func WriteAppError(w http.ResponseWriter, err error) {
	var notFound *NotFoundError
	var alreadyExists *AlreadyExistsError
	var validation *ValidationError

	switch {
	case errors.As(err, &notFound):
		WriteError(w, http.StatusNotFound, err.Error())
	case errors.As(err, &alreadyExists):
		WriteError(w, http.StatusConflict, err.Error())
	case errors.As(err, &validation):
		WriteError(w, http.StatusBadRequest, err.Error())
	default:
		WriteError(w, http.StatusInternalServerError, "internal error")
	}
}
