package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	husky "github.com/honeycombio/husky/otlp"
	"github.com/honeycombio/libhoney-go"
	"github.com/honeycombio/libhoney-go/transmission"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	trace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
)

func TestDecodeJSON(t *testing.T) {
	logger := logrus.New()

	reader := strings.NewReader("{}{}")

	ch := DecodeJSON(logger, reader)

	var count int

	for range ch {
		count++
	}

	assert.Equal(t, count, 2)
}

func TestTranslateCollectorTraces(t *testing.T) {
	logger := logrus.New()

	in := make(chan *json.RawMessage, 1)

	simple := `{"resourceSpans":[{"resource":{"attributes":[]},"scopeSpans":[{"scope":{"name":"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc","version":"semver:0.29.0"},"spans":[]}],"schemaUrl":"https://opentelemetry.io/schemas/v1.7.0"}]}`

	var msg json.RawMessage = []byte(simple)

	in <- &msg

	close(in)

	ch := TranslateCollectorTraces(logger, in)

	var count int

	for range ch {
		count++
	}

	assert.Equal(t, count, 1)
}

func TestTranslateTraceRequest(t *testing.T) {
	logger := logrus.New()

	in := make(chan *trace.ExportTraceServiceRequest, 1)

	msg := trace.ExportTraceServiceRequest{}

	in <- &msg

	close(in)

	ch := TranslateTraceRequest(logger, in)

	var count int

	for range ch {
		count++
	}

	assert.Equal(t, count, 1)
}

func TestProduceEvents(t *testing.T) {
	logger := logrus.New()

	tx := &transmission.MockSender{}

	client, err := libhoney.NewClient(libhoney.ClientConfig{
		APIKey:       "test",
		Transmission: tx,
	})

	assert.NoError(t, err)

	in := make(chan *husky.TranslateOTLPRequestResult, 1)

	msg := husky.TranslateOTLPRequestResult{
		RequestSize: 1,
		Batches: []husky.Batch{
			{
				Dataset: "test",
				Events: []husky.Event{
					{
						SampleRate: 1,
						Attributes: map[string]any{
							"test": "test",
						},
						Timestamp: time.Now(),
					},
				},
				SizeBytes: 10,
			},
		},
	}

	in <- &msg

	close(in)

	ch := ProduceEvents(logger, client, "", 1, 0, 0, in)

	var count int

	for range ch {
		count++
	}

	assert.Equal(t, count, 1)

	events := tx.Events()

	assert.Equal(t, len(events), 1)
}
