package dialer

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// hangingDialer blocks until its context ends, like a QUIC handshake towards a
// server that has no QUIC listener.
func hangingDialer(protocol string, calls *atomic.Int32) *MockDialer {
	return &MockDialer{
		protocolStr: protocol,
		dialFunc: func(ctx context.Context, _ string) (net.Conn, error) {
			calls.Add(1)
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
}

// A relay server that restarts answers WS with an HTTP error first, while the
// QUIC attempt hangs. Once the server is back, the race returns a WS
// connection without waiting for the QUIC attempt to time out.
func TestRaceDialRedialsFailedWhileOtherHangs(t *testing.T) {
	var quicCalls, wsCalls atomic.Int32
	wsConn := &MockConn{remoteAddr: &MockAddr{network: "ws"}}
	ws := &MockDialer{
		protocolStr: "ws",
		dialFunc: func(ctx context.Context, _ string) (net.Conn, error) {
			if wsCalls.Add(1) == 1 {
				return nil, errors.New("expected handshake response status code 101 but got 404")
			}
			return wsConn, nil
		},
	}

	rd := NewRaceDial(logrus.NewEntry(logrus.New()), 30*time.Second, "rels://relay.test:443",
		hangingDialer("quic", &quicCalls), ws)

	start := time.Now()
	done := make(chan struct{})
	var conn net.Conn
	var err error
	go func() {
		defer close(done)
		conn, err = rd.Dial(context.Background())
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the race must return the WS connection without waiting for the hanging QUIC attempt")
	}
	require.NoError(t, err)
	assert.Same(t, wsConn, conn, "the second WS attempt wins")
	assert.Less(t, time.Since(start), 3*time.Second, "WS is dialed again about 1 s after its failure")
	assert.Equal(t, int32(2), wsCalls.Load(), "WS dialed twice")
	assert.Equal(t, int32(1), quicCalls.Load(), "the hanging QUIC attempt is not dialed again")
}

// When every attempt failed and none is in flight, the race ends at once and
// dials nothing more.
func TestRaceDialNoRedialWithoutAttemptInFlight(t *testing.T) {
	var calls atomic.Int32
	failing := func(protocol string) *MockDialer {
		return &MockDialer{
			protocolStr: protocol,
			dialFunc: func(ctx context.Context, _ string) (net.Conn, error) {
				calls.Add(1)
				return nil, errors.New(protocol + " refused")
			},
		}
	}

	rd := NewRaceDial(logrus.NewEntry(logrus.New()), 30*time.Second, "rels://relay.test:443",
		failing("quic"), failing("ws"))
	start := time.Now()
	conn, err := rd.Dial(context.Background())
	require.Error(t, err)
	assert.Nil(t, conn)
	assert.Less(t, time.Since(start), 900*time.Millisecond, "no redial wait once nothing is in flight")
	assert.ErrorContains(t, err, "quic refused")
	assert.ErrorContains(t, err, "ws refused")

	time.Sleep(1500 * time.Millisecond)
	assert.Equal(t, int32(2), calls.Load(), "no dial after the race ended")
}

// A dialer that keeps failing is dialed again only while the race is younger
// than its connection timeout; the race then ends with the last errors.
func TestRaceDialRedialStopsAtConnectionTimeout(t *testing.T) {
	var quicCalls, wsCalls atomic.Int32
	ws := &MockDialer{
		protocolStr: "ws",
		dialFunc: func(ctx context.Context, _ string) (net.Conn, error) {
			wsCalls.Add(1)
			return nil, errors.New("ws 502")
		},
	}

	rd := NewRaceDial(logrus.NewEntry(logrus.New()), 2200*time.Millisecond, "rels://relay.test:443",
		hangingDialer("quic", &quicCalls), ws)
	start := time.Now()
	conn, err := rd.Dial(context.Background())
	require.Error(t, err)
	assert.Nil(t, conn)
	assert.Less(t, time.Since(start), 4*time.Second, "the race ends with the QUIC connection timeout")
	assert.ErrorContains(t, err, "ws 502")
	// Dials at 0 s, 1 s and 3 s, each wait ±20 %: the second one comes by
	// 1.2 s, the third one not before 2.4 s, past the 2.2 s timeout.
	assert.Equal(t, int32(2), wsCalls.Load(), "WS dialed at 0 s and 1 s only")
	assert.Equal(t, int32(1), quicCalls.Load())
}

// Each redial wait moves by up to 20 % either way, and not every wait is the
// same.
func TestRedialJitterBounds(t *testing.T) {
	seen := map[time.Duration]bool{}
	for i := 0; i < 200; i++ {
		d := jittered(time.Second)
		require.GreaterOrEqual(t, d, 800*time.Millisecond)
		require.LessOrEqual(t, d, 1200*time.Millisecond)
		seen[d] = true
	}
	assert.Greater(t, len(seen), 1, "the waits must differ")
}

// closeTrackingConn records its Close.
type closeTrackingConn struct {
	MockConn
	closed atomic.Bool
}

func (c *closeTrackingConn) Close() error {
	c.closed.Store(true)
	return nil
}

// QUIC wins while the WS redial waits. The race returns QUIC, the pending
// redial never dials, and a WS attempt that connects after the abort is
// closed.
func TestRaceDialWinWhileRedialPending(t *testing.T) {
	var wsCalls atomic.Int32
	quicConn := &MockConn{remoteAddr: &MockAddr{network: "quic"}}
	quic := &MockDialer{
		protocolStr: "quic",
		dialFunc: func(ctx context.Context, _ string) (net.Conn, error) {
			time.Sleep(300 * time.Millisecond)
			return quicConn, nil
		},
	}
	ws := &MockDialer{
		protocolStr: "ws",
		dialFunc: func(ctx context.Context, _ string) (net.Conn, error) {
			wsCalls.Add(1)
			return nil, errors.New("ws 502")
		},
	}

	rd := NewRaceDial(logrus.NewEntry(logrus.New()), 30*time.Second, "rels://relay.test:443", quic, ws)
	conn, err := rd.Dial(context.Background())
	require.NoError(t, err)
	assert.Same(t, quicConn, conn, "QUIC wins while the WS redial waits")

	time.Sleep(firstRedialDelay*6/5 + 300*time.Millisecond)
	assert.Equal(t, int32(1), wsCalls.Load(), "the pending WS redial must not dial after the race was won")
}

// A losing attempt that connects after the race was won is closed.
func TestRaceDialLoserConnectingLateIsClosed(t *testing.T) {
	quicConn := &MockConn{remoteAddr: &MockAddr{network: "quic"}}
	late := &closeTrackingConn{MockConn: MockConn{remoteAddr: &MockAddr{network: "ws"}}}
	quic := &MockDialer{
		protocolStr: "quic",
		dialFunc: func(ctx context.Context, _ string) (net.Conn, error) {
			return quicConn, nil
		},
	}
	ws := &MockDialer{
		protocolStr: "ws",
		dialFunc: func(ctx context.Context, _ string) (net.Conn, error) {
			<-ctx.Done()
			// The handshake finished just as the race was won.
			return late, nil
		},
	}

	rd := NewRaceDial(logrus.NewEntry(logrus.New()), 30*time.Second, "rels://relay.test:443", quic, ws)
	conn, err := rd.Dial(context.Background())
	require.NoError(t, err)
	assert.Same(t, quicConn, conn)
	require.Eventually(t, late.closed.Load, time.Second, 10*time.Millisecond, "the late WS connection must be closed")
}

// A failure that is dialed again logs at Debug. Only the final failure of a
// transport logs at Error.
func TestRaceDialRedialFailuresLogDebug(t *testing.T) {
	logger, hook := logtest.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	var quicCalls atomic.Int32
	ws := &MockDialer{
		protocolStr: "ws",
		dialFunc: func(ctx context.Context, _ string) (net.Conn, error) {
			return nil, errors.New("ws 502")
		},
	}

	rd := NewRaceDial(logrus.NewEntry(logger), 2200*time.Millisecond, "rels://relay.test:443",
		hangingDialer("quic", &quicCalls), ws)
	_, err := rd.Dial(context.Background())
	require.Error(t, err)

	var debug, errs int
	for _, e := range hook.AllEntries() {
		if !strings.Contains(e.Message, "failed to dial via ws") {
			continue
		}
		switch e.Level {
		case logrus.DebugLevel:
			debug++
		case logrus.ErrorLevel:
			errs++
		}
	}
	assert.Equal(t, 1, debug, "the redialed WS failure logs at Debug")
	assert.Equal(t, 1, errs, "the final WS failure logs at Error")
}
