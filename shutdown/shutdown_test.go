package shutdown

import (
	"log"
	"testing"
)

func TestDown(t *testing.T) {
	h := NewHook()
	h.WithSignals()
	h.Close(func() {
		log.Println("11")
	})
}
