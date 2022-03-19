package logger

import (
	"context"
	"errors"
	"testing"
)


func TestLogger(t *testing.T) {
	log := NewLogger(
		WithTimeLayout("2006-01-02 15:04:05 Monday"),
		WithInfoLevel())

	test := "test"
	ConsoleInfo(test, "asdasdasd")
	ConsoleError(test, "asdasdasd")
	ConsoleWarn(test, "asdasdasd")
	ConsolePainc(test, "asdasdasd")
	log.Debug("asdasd")
	log.Info("infoLevel")

	ctx := context.Background()
	newCtx, log2 := log.SetCtx(ctx, WrapMeta(errors.New("test1"), "sql1", NewMeta("time1", "12.00s"))...)
	log2.Info("直接使用", WrapMeta(errors.New("test2"), "sql2", NewMeta("time2", "13.00s"))...)
	log.GetCtx(newCtx).Info("间接使用")
}
