package raft

import (
	"context"
	"go.uber.org/zap"
)
import "go.uber.org/zap/zapcore"

type LogContext struct {
	logger *zap.Logger
	sugar *zap.SugaredLogger
}

func (lc *LogContext) GetSugarLogger() *zap.SugaredLogger {
	return lc.sugar
}
func (lc *LogContext) GetLogger() *zap.Logger {
	return lc.logger
}


func CreateLogContext(fields... zapcore.Field) *LogContext {
	logger, sugar := WithFields(fields...)
	context.WithValue(context.Background(), "_logger", sugar)
	return &LogContext{logger: logger, sugar: sugar}

}

func WithLogContext(logContext *LogContext, fields... zapcore.Field) *LogContext {
	logger := logContext.logger//(*zap.SugaredLogger)
	newLogger := logger.With(fields...)
	return &LogContext{logger: newLogger, sugar: newLogger.Sugar()}

}

func WithFields(fields... zapcore.Field) (*zap.Logger, *zap.SugaredLogger) {
	l := createLogger().With(fields...)
	return l, l.Sugar()
}


func createLogger() *zap.Logger{
	zapConfig := zap.NewDevelopmentConfig()
	zapConfig.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	l, err := zapConfig.Build(
		zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))

	if err != nil {
		panic("create logger failed")
	}

	return l
}

