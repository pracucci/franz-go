package kgo

import (
	"context"
	"errors"
	"math/rand/v2"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/twmb/franz-go/pkg/kfake"
)

func TestMaxVersions(t *testing.T) {
	if ours, main := new(fetchRequest).MaxVersion(), new(kmsg.FetchRequest).MaxVersion(); ours != main {
		t.Errorf("our fetch request max version %d != kmsg's %d", ours, main)
	}
	if ours, main := new(produceRequest).MaxVersion(), new(kmsg.ProduceRequest).MaxVersion(); ours != main {
		t.Errorf("our produce request max version %d != kmsg's %d", ours, main)
	}
}

func TestParseBrokerAddr(t *testing.T) {
	tests := []struct {
		name     string
		addr     string
		expected hostport
	}{
		{
			"IPv4",
			"127.0.0.1:1234",
			hostport{"127.0.0.1", 1234},
		},
		{
			"IPv4 + default port",
			"127.0.0.1",
			hostport{"127.0.0.1", 9092},
		},
		{
			"host",
			"localhost:1234",
			hostport{"localhost", 1234},
		},
		{
			"host + default port",
			"localhost",
			hostport{"localhost", 9092},
		},
		{
			"IPv6",
			"[2001:1000:2000::1]:1234",
			hostport{"2001:1000:2000::1", 1234},
		},
		{
			"IPv6 + default port",
			"[2001:1000:2000::1]",
			hostport{"2001:1000:2000::1", 9092},
		},
		{
			"IPv6 literal",
			"::1",
			hostport{"::1", 9092},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := parseBrokerAddr(test.addr)
			if err != nil {
				t.Fatal(err)
			}
			if result != test.expected {
				t.Fatalf("expected %v, got %v", test.expected, result)
			}
		})
	}
}

func TestParseBrokerAddrErrors(t *testing.T) {
	tests := []struct {
		name string
		addr string
	}{
		{
			"IPv4 invalid port",
			"127.0.0.1:foo",
		},
		{
			"host invalid port",
			"localhost:foo",
		},

		{
			"IPv6 invalid port",
			"[2001:1000:2000::1]:foo",
		},
		{
			"IPv6 missing closing bracket",
			"[2001:1000:2000::1:1234",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := parseBrokerAddr(test.addr)
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestUnknownGroupOffsetFetchPinned(t *testing.T) {
	req := kmsg.NewOffsetFetchRequest()
	req.Group = "unknown-" + strconv.FormatInt(time.Now().UnixNano(), 10)

	cl, _ := newTestClient()
	defer cl.Close()
	defer func() {
		if err := recover(); err != nil {
			t.Errorf("fetch panicked: %v", err)
		}
	}()
	req.RequestWith(context.Background(), cl)
}

func TestProcessHooks(t *testing.T) {
	var (
		aHook     = Hook(&someHook{index: 10})
		xs        = []Hook{&someHook{index: 1}, &someHook{index: 2}}
		ys        = []Hook{&someHook{index: 3}, &someHook{index: 4}, &someHook{index: 5}}
		all       = append(append(xs, ys...), aHook)
		sliceHook = new(intSliceHook)
	)

	tests := []struct {
		name     string
		hooks    []Hook
		expected []Hook
	}{
		{
			name:     "all",
			hooks:    all,
			expected: all,
		},
		{
			name:     "nested slice",
			hooks:    []Hook{all},
			expected: all,
		},
		{
			name:     "hooks and slices",
			hooks:    []Hook{xs, ys, aHook},
			expected: all,
		},
		{
			name:     "slice that implements a hook",
			hooks:    []Hook{sliceHook},
			expected: []Hook{sliceHook},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			hooks, err := processHooks(test.hooks)
			if err != nil {
				t.Fatal("expected no error", err)
			}
			if !reflect.DeepEqual(hooks, test.expected) {
				t.Fatalf("didn't get expected hooks back after processing, %+v, %+v", hooks, test.expected)
			}
		})
	}
}

func TestProcessHooksErrors(t *testing.T) {
	tests := []struct {
		name  string
		hooks []Hook
	}{
		{
			name:  "useless slice",
			hooks: []Hook{&[]int{}},
		},
		{
			name:  "deep useless slice",
			hooks: []Hook{[]Hook{&[]int{}}},
		},
		{
			name:  "mixed useful and useless",
			hooks: []Hook{&someHook{}, &notAHook{}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := processHooks(test.hooks)
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

type notAHook struct{}

type someHook struct {
	index int
}

func (*someHook) OnNewClient(*Client) {
	// ignore
}

type intSliceHook []int

func (*intSliceHook) OnNewClient(*Client) {
	// ignore
}

func TestClient_Produce(t *testing.T) {
	const (
		topicName     = "test"
		numPartitions = 1
		numWorkers    = 50
		testDuration  = 3 * time.Second
	)

	var (
		done    = make(chan struct{})
		workers = sync.WaitGroup{}

		writeSuccessCount = &atomic.Int64{}
		writeFailureCount = &atomic.Int64{}
	)

	createRandomRecord := func() *Record {
		return &Record{
			Key:   []byte("test"),
			Value: []byte(strings.Repeat("x", rand.IntN(1000))),
			Topic: topicName,
		}
	}

	// If the test is successful (no WriteSync() request is in a deadlock state) then we expect the test
	// to complete shortly after the estimated test duration.
	ctx, cancel := context.WithTimeoutCause(context.Background(), 2*testDuration, errors.New("test did not complete within the expected time"))
	t.Cleanup(cancel)

	// Create fake cluster.
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(numPartitions, topicName))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)

	// Throttle a very short (random) time to increase chances of hitting race conditions.
	cluster.ControlKey(int16(kmsg.Produce), func(_ kmsg.Request) (kmsg.Response, error, bool) {
		time.Sleep(time.Duration(rand.Int64N(int64(time.Millisecond))))

		return nil, nil, false
	})

	client, err := NewClient(
		SeedBrokers(cluster.ListenAddrs()[0]),
		MaxBufferedBytes(5000),
	)
	if err != nil {
		t.Fatal(err)
	}

	// Start N workers that will concurrently write to the same partition.
	for i := 0; i < numWorkers; i++ {
		workers.Add(1)

		go func() {
			defer workers.Done()

			for {
				select {
				case <-done:
					return

				default:
					res := client.ProduceSync(ctx, createRandomRecord())
					if err := res.FirstErr(); err == nil {
						writeSuccessCount.Add(1)
					} else {
						if !errors.Is(err, ErrMaxBuffered) {
							t.Fatalf("unexpected error: %v", err)
						}

						writeFailureCount.Add(1)
					}
				}
			}
		}()
	}

	// Keep it running for some time.
	time.Sleep(testDuration)

	// Signal workers to stop and wait until they're done.
	close(done)
	workers.Wait()

	t.Logf("writes succeeded: %d", writeSuccessCount.Load())
	t.Logf("writes failed:    %d", writeFailureCount.Load())
}
