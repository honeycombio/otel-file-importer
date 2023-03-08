package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	husky "github.com/honeycombio/husky/otlp"
	"github.com/honeycombio/libhoney-go"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/collector/pdata/ptrace"
	trace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

func main() {
	var path string
	var key string
	var dataset string
	var host string
	var batch int
	var sleep time.Duration
	var start time.Duration
	var verbosity uint
	var checkDIAndExit bool

	flag.StringVar(&path, "path", "", "Path to the file containing OTLP JSON formatted events")
	flag.StringVar(&key, "key", "", "The Honeycomb API key to send the events")
	flag.StringVar(&dataset, "dataset", "", "The Honeycomb dataset to send the events to, if not specified, assumes the destination is an environment")
	flag.StringVar(&host, "host", "https://api.honeycomb.io", "The Honeycomb host to send the events to, if not specified, assumes the destination is an environment")
	flag.IntVar(&batch, "batch", 200, "The number of events to send in a row before pausing")
	flag.DurationVar(&sleep, "sleep", 100*time.Millisecond, "The duration to sleep between batches")
	flag.DurationVar(&start, "start", 0, "The duration ago to start the events from")
	flag.UintVar(&verbosity, "verbosity", 4, "The verbosity level of the output")
	flag.BoolVar(&checkDIAndExit, "check-di-and-exit", false, "if present, we'll exit immediately - used in CI to check if DI is valid")

	flag.Parse()

	logger := logrus.New()

	if checkDIAndExit {
		logrus.Infoln("Flag is set, exiting instead of starting the service.")
		os.Exit(0)
	}

	if path == "" {
		logger.Fatal("File Path is required")
	}

	if key == "" {
		logger.Fatal("API Key is required")
	}

	logger.Level = logrus.Level(verbosity)

	client, err := libhoney.NewClient(libhoney.ClientConfig{
		APIKey:  key,
		APIHost: host,
		Dataset: dataset,
	})

	if err != nil {
		logger.WithError(err).Fatal("Failed to initialize honeycomb")
	}

	MonitorLibhoneyResponses(logger)

	file, err := os.Open(path)

	if err != nil {
		logger.WithError(err).Fatal("Failed to open file")
	}

	defer func() {
		if err = file.Close(); err != nil {
			logger.WithError(err).Error("Failed to close file")
		}
	}()

	signal := make(chan any)
	defer close(signal)

	Spinner("Working", signal)

	json := DecodeJSON(logger, file)
	exports := TranslateCollectorTraces(logger, json)
	translated := TranslateTraceRequest(logger, exports)
	events := ProduceEvents(logger, client, dataset, batch, start, sleep, translated)

	var count int

	for range events {
		count++
	}

	libhoney.Close()

	logger.Infof("Finished: sent %d events", count)
}

func DecodeJSON(logger *logrus.Logger, file io.Reader) <-chan *json.RawMessage {
	out := make(chan *json.RawMessage)
	decoder := json.NewDecoder(file)

	go func() {
		defer close(out)

		for {
			var rm *json.RawMessage

			if err := decoder.Decode(&rm); err != nil {
				if err == io.EOF {
					logger.WithError(err).Debug("Finished reading file")
					break
				}

				logger.WithError(err).Error("Failed to decode JSON")
				continue
			}

			out <- rm
		}
	}()

	return out
}

func TranslateCollectorTraces(logger *logrus.Logger, in <-chan *json.RawMessage) <-chan *trace.ExportTraceServiceRequest {
	out := make(chan *trace.ExportTraceServiceRequest)
	jsonUnmarshaler := ptrace.NewJSONUnmarshaler()
	protoMarshaler := ptrace.NewProtoMarshaler()

	go func() {
		defer close(out)

		for rm := range in {
			t, err := jsonUnmarshaler.UnmarshalTraces(*rm)

			if err != nil {
				logger.WithError(err).Error("Failed to unmarshal traces")
				break
			}

			b, err := protoMarshaler.MarshalTraces(t)

			if err != nil {
				logger.WithError(err).Error("Failed to marshal traces")
				break
			}

			req := &trace.ExportTraceServiceRequest{}

			if err := proto.Unmarshal(b, req); err != nil {
				logger.WithError(err).Error("Failed to unmarshal ExportTraceServiceRequest")
				break
			}

			out <- req
		}
	}()

	return out
}

func TranslateTraceRequest(logger *logrus.Logger, in <-chan *trace.ExportTraceServiceRequest) <-chan *husky.TranslateOTLPRequestResult {
	out := make(chan *husky.TranslateOTLPRequestResult)

	go func() {
		defer close(out)

		for req := range in {
			hny, err := husky.TranslateTraceRequest(req, husky.RequestInfo{
				ApiKey:      "junk", // we just need a value here. It is not used for anything
				ContentType: "application/protobuf",
			})

			if err != nil {
				logger.WithError(err).Error("Failed to translate trace")
				break
			}

			out <- hny
		}
	}()

	return out
}

func ProduceEvents(logger *logrus.Logger, client *libhoney.Client, dataset string, batch int, start, sleep time.Duration, in <-chan *husky.TranslateOTLPRequestResult) <-chan *libhoney.Event {
	out := make(chan *libhoney.Event)

	// this is the time that we want to start populating events from
	begin := time.Now().Add(-1 * start)

	// this is how much we need to adjust each events timestamp by
	var adjustment time.Duration

	var count int

	go func() {
		defer close(out)

		for hny := range in {
			for _, b := range hny.Batches {
				for _, e := range b.Events {
					// calculate the adjustment to use for the events
					// if a start has been provided and we have not done it yet
					if adjustment == 0 && start > 0 {
						// this should be the earliest event in the file
						// use this to calculate how much to adjust each timestamp by
						adjustment = begin.Sub(e.Timestamp)
					}

					if count > batch {
						time.Sleep(sleep)
						count = 0
					}

					event := client.NewEvent()
					event.Add(e.Attributes)
					event.SampleRate = uint(e.SampleRate)
					event.Timestamp = e.Timestamp.Add(adjustment)

					// if no dataset has been provided then we are sending to an environment so set the dataset for each event
					if dataset == "" {
						event.Dataset = b.Dataset
					}

					if err := event.SendPresampled(); err != nil {
						logger.WithError(err).Error("Failed to send event")
						continue
					}

					count++

					out <- event
				}
			}
		}
	}()

	return out
}

func MonitorLibhoneyResponses(logger *logrus.Logger) {
	r := libhoney.TxResponses()

	go func() {
		for resp := range r {
			if resp.Err != nil {
				logger.WithError(resp.Err).
					WithField("response", resp).
					Error("Failed to send event")
			}
		}
	}()
}

func Spinner(message string, in <-chan any) {
	go func() {
		for {
			for _, r := range `-\|/` {
				fmt.Printf("\r%s%s %c%s", message, "\x1b[92m", r, "\x1b[39m")
				select {
				case <-in:
					return
				case <-time.After(100 * time.Millisecond):
					continue
				}
			}
		}
	}()
}
