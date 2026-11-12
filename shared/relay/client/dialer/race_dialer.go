package dialer

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	DefaultConnectionTimeout = 30 * time.Second

	// firstRedialDelay is the wait before a failed transport is dialed again
	// within a race. Each next wait is twice as long.
	firstRedialDelay = time.Second
	// redialJitter is the share of each redial wait that is random, both
	// ways, so clients that lost a restarting relay do not dial it in step.
	redialJitter = 0.2
)

type DialeFn interface {
	// Dial connects to address. serverName, when non-empty, overrides the TLS
	// ServerName used for SNI/cert validation. Empty means derive from address.
	Dial(ctx context.Context, address, serverName string) (net.Conn, error)
	Protocol() string
}

type dialResult struct {
	Conn     net.Conn
	Protocol string
	Err      error
}

type RaceDial struct {
	log               *log.Entry
	serverURL         string
	serverName        string
	dialerFns         []DialeFn
	connectionTimeout time.Duration
	sequential        bool
}

func NewRaceDial(log *log.Entry, connectionTimeout time.Duration, serverURL string, dialerFns ...DialeFn) *RaceDial {
	return &RaceDial{
		log:               log,
		serverURL:         serverURL,
		dialerFns:         dialerFns,
		connectionTimeout: connectionTimeout,
	}
}

// WithServerName sets a TLS SNI/cert validation override. Used when serverURL
// contains an IP literal but the cert is issued for a different hostname.
//
// Mutates the receiver and is not safe for concurrent reconfiguration; a
// RaceDial is intended to be constructed per dial and discarded.
func (r *RaceDial) WithServerName(serverName string) *RaceDial {
	r.serverName = serverName
	return r
}

// WithSequential makes Dial try the dialers in order, falling back to the next
// only when one fails to connect, instead of racing them concurrently.
//
// Mutates the receiver and is not safe for concurrent reconfiguration; a
// RaceDial is intended to be constructed per dial and discarded.
func (r *RaceDial) WithSequential() *RaceDial {
	r.sequential = true
	return r
}

func (r *RaceDial) Dial(ctx context.Context) (net.Conn, error) {
	if r.sequential {
		return r.dialSequential(ctx)
	}
	return r.dialRace(ctx)
}

// dialSequential tries each dialer in order, returning the first connection and
// falling back to the next on failure.
func (r *RaceDial) dialSequential(ctx context.Context) (net.Conn, error) {
	var errs []error
	for _, dfn := range r.dialerFns {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		attemptCtx, cancel := context.WithTimeout(ctx, r.connectionTimeout)
		r.log.Infof("dialing Relay server via %s", dfn.Protocol())
		conn, err := dfn.Dial(attemptCtx, r.serverURL, r.serverName)
		cancel()
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil, err
			}
			r.log.Errorf("failed to dial via %s: %s", dfn.Protocol(), err)
			errs = append(errs, fmt.Errorf("%s: %w", dfn.Protocol(), err))
			continue
		}
		r.log.Infof("successfully dialed via: %s", dfn.Protocol())
		return conn, nil
	}
	return nil, dialErr(errs)
}

// race is the state of one concurrent dial. Only the goroutine running
// dialRace touches it.
type race struct {
	start    time.Time
	inFlight int
	// nextRedial is the wait before the next dial of each dialer, by index.
	nextRedial     []time.Duration
	errsByProtocol map[string]error
}

// dialRace dials all transports at once and returns the first connection. A
// transport that fails is dialed again while another attempt is still in
// flight: a relay server that is starting answers WS with an error at once,
// while its QUIC attempt can hang for seconds, and on a phone those seconds
// spread over several short wakes. The race ends with an error once no attempt
// is in flight, or at the connection timeout.
func (r *RaceDial) dialRace(ctx context.Context) (net.Conn, error) {
	raceCtx, abort := context.WithTimeout(ctx, r.connectionTimeout)
	defer abort()

	results := make(chan dialResult)
	redialDue := make(chan int)
	rc := &race{
		start:          time.Now(),
		nextRedial:     make([]time.Duration, len(r.dialerFns)),
		errsByProtocol: make(map[string]error),
	}
	for i, dfn := range r.dialerFns {
		rc.nextRedial[i] = firstRedialDelay
		rc.inFlight++
		go r.dial(dfn, raceCtx, results)
	}

	for rc.inFlight > 0 {
		select {
		case i := <-redialDue:
			rc.inFlight++
			go r.dial(r.dialerFns[i], raceCtx, results)
		case dr := <-results:
			rc.inFlight--
			if dr.Err == nil {
				r.log.Infof("successfully dialed via: %s", dr.Protocol)
				abort()
				go r.closeLosers(results, rc.inFlight)
				return dr.Conn, nil
			}
			i, delay, redial := r.redialIndex(rc, dr)
			r.recordFailure(rc, dr, !redial)
			if redial {
				go r.redialAfter(raceCtx, i, delay, redialDue)
				rc.nextRedial[i] *= 2
			}
		}
	}
	return nil, dialErr(r.orderedErrs(rc.errsByProtocol))
}

// recordFailure logs a failed attempt and keeps its error, the newest per
// transport. A failure that is dialed again logs at Debug, final ones at
// Error, so a race logs one error per transport as without redials.
func (r *RaceDial) recordFailure(rc *race, dr dialResult, final bool) {
	if errors.Is(dr.Err, context.Canceled) {
		r.log.Infof("connection attempt aborted via: %s", dr.Protocol)
		return
	}
	if final {
		r.log.Errorf("failed to dial via %s: %s", dr.Protocol, dr.Err)
	} else {
		r.log.Debugf("failed to dial via %s, dialing again: %s", dr.Protocol, dr.Err)
	}
	rc.errsByProtocol[dr.Protocol] = fmt.Errorf("%s: %w", dr.Protocol, dr.Err)
}

// redialIndex returns the dialer of a failed attempt and the wait before its
// next dial when it is to be dialed again: another attempt is still in
// flight, the attempt was not cancelled, and the next dial starts within the
// connection timeout of awake time.
func (r *RaceDial) redialIndex(rc *race, dr dialResult) (int, time.Duration, bool) {
	if rc.inFlight == 0 || errors.Is(dr.Err, context.Canceled) || errors.Is(dr.Err, context.DeadlineExceeded) {
		return 0, 0, false
	}
	for i, dfn := range r.dialerFns {
		if dfn.Protocol() != dr.Protocol {
			continue
		}
		delay := jittered(rc.nextRedial[i])
		if time.Since(rc.start)+delay >= r.connectionTimeout {
			return 0, 0, false
		}
		return i, delay, true
	}
	return 0, 0, false
}

// jittered returns d moved by a random share of up to redialJitter either way.
func jittered(d time.Duration) time.Duration {
	return d + time.Duration((rand.Float64()*2-1)*redialJitter*float64(d))
}

// redialAfter hands dialer i back to the race after d.
func (r *RaceDial) redialAfter(ctx context.Context, i int, d time.Duration, due chan<- int) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		return
	}
	select {
	case due <- i:
	case <-ctx.Done():
	}
}

func (r *RaceDial) dial(dfn DialeFn, ctx context.Context, results chan<- dialResult) {
	r.log.Infof("dialing Relay server via %s", dfn.Protocol())
	conn, err := dfn.Dial(ctx, r.serverURL, r.serverName)
	results <- dialResult{Conn: conn, Protocol: dfn.Protocol(), Err: err}
}

// closeLosers collects the n attempts still in flight after the race was won,
// and closes any connection one of them made before it saw the abort.
func (r *RaceDial) closeLosers(results <-chan dialResult, n int) {
	for ; n > 0; n-- {
		dr := <-results
		if dr.Err != nil {
			r.log.Infof("connection attempt aborted via: %s", dr.Protocol)
			continue
		}
		if cerr := dr.Conn.Close(); cerr != nil {
			r.log.Warnf("failed to close connection via %s: %s", dr.Protocol, cerr)
		}
	}
}

// orderedErrs returns the per-protocol errors in dialer order, so the combined
// error is stable regardless of which attempt failed first.
func (r *RaceDial) orderedErrs(byProtocol map[string]error) []error {
	errs := make([]error, 0, len(byProtocol))
	for _, dfn := range r.dialerFns {
		if err, ok := byProtocol[dfn.Protocol()]; ok {
			errs = append(errs, err)
		}
	}
	return errs
}

// dialErr combines per-dialer failures, preserving the underlying reasons
// (e.g. "connection refused") rather than a generic message.
func dialErr(errs []error) error {
	if len(errs) == 0 {
		return errors.New("no relay transport available")
	}
	return errors.Join(errs...)
}
