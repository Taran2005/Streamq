package common

import "fmt"

// Typed errors let handlers pick the right HTTP status code
// without string matching. In later phases, these also help
// with retry logic (e.g., NotFound = don't retry, AlreadyExists = skip).

type NotFoundError struct {
	Resource string
	Name     string
}

func (e *NotFoundError) Error() string {
	return fmt.Sprintf("%s %q not found", e.Resource, e.Name)
}

type AlreadyExistsError struct {
	Resource string
	Name     string
}

func (e *AlreadyExistsError) Error() string {
	return fmt.Sprintf("%s %q already exists", e.Resource, e.Name)
}

type ValidationError struct {
	Message string
}

func (e *ValidationError) Error() string {
	return e.Message
}
