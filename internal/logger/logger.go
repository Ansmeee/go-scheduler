package logger

import (
	"context"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

var L *zap.Logger

func Init() error {
	encoder := zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())

	infoWriter := zapcore.AddSync(&lumberjack.Logger{
		Filename:   "./logs/app.log",
		MaxSize:    200,
		MaxBackups: 10,
		MaxAge:     30,
		Compress:   true,
	})

	level := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool { return lvl >= zapcore.InfoLevel })
	core := zapcore.NewCore(encoder, infoWriter, level)
	L = zap.New(core, zap.AddCaller())

	return nil
}

func Debug(ctx context.Context, msg string, fields ...zap.Field) {
	L.Debug(msg, withTrace(ctx, fields)...)
}

func Info(ctx context.Context, msg string, fields ...zap.Field) {
	L.Info(msg, withTrace(ctx, fields)...)
}

func Error(ctx context.Context, msg string, fields ...zap.Field) {
	L.Error(msg, withTrace(ctx, fields)...)
}

func Fatal(ctx context.Context, msg string, fields ...zap.Field) {
	L.Fatal(msg, withTrace(ctx, fields)...)
}

func withTrace(ctx context.Context, fields []zap.Field) []zap.Field {
	trace := ctx.Value("trace")
	if trace != nil {
		tracer := zap.String("trace", trace.(string))
		fields = append(fields, tracer)
	}

	return fields
}
