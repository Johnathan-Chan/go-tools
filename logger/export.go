package logger

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"log"
)

const (
	red uint8 = iota + 31
	green
	yellow
	blue
	purple
)

const (
	info = "\u001B[%dm [INFO] %v \u001B[0m"
	warn = "\u001B[%dm [WARN] %v \u001B[0m"
	errors = "\u001B[%dm [ERROR] %v \u001B[0m"
	paincs = "\u001B[%dm [PAINC] %v \u001B[0m"
)

type Logger struct {
	*zap.Logger
	ctxKey string
	consoleLog  log.Logger
}

func NewLogger(opt ...Option) *Logger {
	logger, err := NewJSONLogger(opt...)
	if err != nil{
		panic("init logger error")
	}

	return &Logger{
		Logger: logger,
	}
}

func (l *Logger) GetCtx(ctx context.Context) *zap.Logger {
	log, ok := ctx.Value(l.ctxKey).(*zap.Logger)
	if ok {
		return log
	}
	return l.Logger
}

func (l *Logger) AddCtx(ctx context.Context, field ...zap.Field) (context.Context, *zap.Logger) {
	log := l.With(field...)
	ctx = context.WithValue(ctx, l.ctxKey, log)
	return ctx, log
}

func (l Logger) ConsoleInfo(v interface{}) {
	log.Println(fmt.Sprintf(info, green, v))
}

func (l Logger) ConsoleWarn(v interface{}) {
	log.Println(fmt.Sprintf(warn, yellow, v))
}

func (l Logger) ConsoleError(v interface{}) {
	log.Println(fmt.Sprintf(errors, red, v))
}

func (l Logger) ConsolePainc(v interface{}) {
	log.Println(fmt.Sprintf(paincs, purple, v))
}
