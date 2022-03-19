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
	infoLevel = "\u001B[%dm [INFO] [%s] %v \u001B[0m"
	warnLevel  = "\u001B[%dm [WARN] [%s] %v \u001B[0m"
	errorLevel = "\u001B[%dm [ERROR] [%s] %v \u001B[0m"
	paincLevel = "\u001B[%dm [PAINC] [%s] %v \u001B[0m"
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

func (l *Logger) SetCtx(ctx context.Context, field ...zap.Field) (context.Context, *zap.Logger) {
	log := l.With(field...)
	ctx = context.WithValue(ctx, l.ctxKey, log)
	return ctx, log
}


func ConsoleInfo(namespace string, v interface{}) {
	log.Println(fmt.Sprintf(infoLevel, green, namespace, v))
}

func ConsoleWarn(namespace string, v interface{}) {
	log.Println(fmt.Sprintf(warnLevel, yellow, namespace, v))
}

func ConsoleError(namespace string, v interface{}) {
	log.Println(fmt.Sprintf(errorLevel, red, namespace, v))
}

func ConsolePainc(namespace string, v interface{}) {
	log.Println(fmt.Sprintf(paincLevel, purple, namespace, v))
}
