package bloomd

import "fmt"

// Error custom error for bloomd related actions
type Error struct {
	Message                  string
	Err                      error
	ShouldRetryWithNewClient bool
}

func (e Error) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s (%s)", e.Message, e.Err)
	}

	return e.Message
}
