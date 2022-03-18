package logger

import (
	"testing"
)


func TestLogger(t *testing.T) {
	log := NewLogger()
	log.ConsoleInfo("asdasdasd")
	log.ConsoleError("asdasdasd")
	log.ConsoleWarn("asdasdasd")
	log.ConsolePainc("asdasdasd")
}
