package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/Syntheresis/melta/extension"
)

var errStopped = errors.New("worker: stopped")

type server struct {
	ctx    context.Context
	author *authorServer
	reader *Reader
	writer *Writer

	next atomic.Uint64

	mu       sync.Mutex
	stopped  bool
	done     chan struct{}
	requests map[string]context.CancelFunc
	pending  map[string]chan hostcallOutcome
	fatal    chan error
	handlers sync.WaitGroup
}

type hostcallOutcome struct {
	result extension.Result
	err    error
}

type frameResult struct {
	frame Frame
	err   error
}

type protocolFailure struct {
	err error
}

func (e protocolFailure) Error() string { return e.err.Error() }

func badRoute(format string, args ...any) error {
	return protocolFailure{err: fmt.Errorf(format, args...)}
}

func decodeFailure(what string, err error) error {
	return protocolFailure{err: fmt.Errorf("worker: %s: %w", what, err)}
}

func (s *server) run() error {
	frames := make(chan frameResult)
	go s.readLoop(frames)
	first := true
	for {
		select {
		case <-s.ctx.Done():
			s.shutdown()
			return s.ctx.Err()
		case err := <-s.fatal:
			s.shutdown()
			return err
		case event := <-frames:
			if event.err != nil {
				s.shutdown()
				if errors.Is(event.err, io.EOF) {
					return nil
				}
				return event.err
			}
			if first {
				first = false
				if err := s.authorHello(event.frame); err != nil {
					s.shutdown()
					return err
				}
				continue
			}
			if err := s.handleFrame(event.frame); err != nil {
				s.shutdown()
				return err
			}
		}
	}
}

func (s *server) readLoop(frames chan<- frameResult) {
	for {
		frame, err := s.reader.ReadFrame()
		select {
		case frames <- frameResult{frame: frame, err: err}:
		case <-s.done:
			return
		}
		if err != nil {
			return
		}
	}
}

func (s *server) shutdown() {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return
	}
	s.stopped = true
	close(s.done)
	for _, cancel := range s.requests {
		cancel()
	}
	s.mu.Unlock()
	s.handlers.Wait()
	if s.author.close != nil {
		_ = s.author.close(context.Background())
	}
}

func (s *server) fail(err error) {
	select {
	case s.fatal <- err:
	default:
	}
}

func (s *server) rejectActivation(id string, cause error) error {
	failure := &Error{Class: ClassIncompatible, Message: cause.Error()}
	s.writeError(id, failure)
	return failure
}

func (s *server) handleFrame(frame Frame) error {
	switch frame.Kind {
	case KindRequest:
		return s.admitRequest(frame)
	case KindResponse:
		return s.routeResponse(frame)
	case KindCancel:
		s.cancelRequest(frame.ID)
		return nil
	case KindProgress:
		return nil
	default:
		return fmt.Errorf("worker: unknown frame kind %q", frame.Kind)
	}
}

// admitRequest performs request admission and instance lookup in frame order,
// then runs the callback concurrently. A decode or routing failure ends the
// activation; an author callback error is the call's ordinary error.
func (s *server) admitRequest(frame Frame) error {
	if frame.ID == "" {
		return errors.New("worker: request without id")
	}
	if frame.Method == "" {
		return fmt.Errorf("worker: request %q without a method", frame.ID)
	}
	s.mu.Lock()
	_, duplicated := s.requests[frame.ID]
	s.mu.Unlock()
	if duplicated {
		return fmt.Errorf("worker: duplicate request id %q", frame.ID)
	}
	ctx, cancel := context.WithCancel(s.ctx)
	return s.admitAuthorRequest(frame, ctx, cancel)
}

// The request's call scope ends when its handler returns: cancel its context
// before the response is observable, so a retained callback call is closed and
// outstanding nested calls are cancelled first.
func (s *server) launch(id string, ctx context.Context, cancel context.CancelFunc, run func(context.Context) (json.RawMessage, error)) {
	s.mu.Lock()
	s.requests[id] = cancel
	s.handlers.Add(1)
	s.mu.Unlock()
	go func() {
		defer s.handlers.Done()
		result, err := run(ctx)
		cancel()
		s.mu.Lock()
		delete(s.requests, id)
		s.mu.Unlock()
		var failure protocolFailure
		if errors.As(err, &failure) {
			s.fail(failure.err)
			return
		}
		s.respond(id, result, err)
	}()
}

func (s *server) cancelRequest(id string) {
	s.mu.Lock()
	cancel := s.requests[id]
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (s *server) callFunc(scope context.Context, token string) func(context.Context, string, string, json.RawMessage) (extension.Result, error) {
	return func(ctx context.Context, service, method string, arguments json.RawMessage) (extension.Result, error) {
		return s.hostcall(ctx, scope, token, service, method, arguments)
	}
}

func (s *server) hostcall(ctx context.Context, scope context.Context, token, binding, method string, arguments json.RawMessage) (extension.Result, error) {
	if binding == "" || method == "" {
		return extension.Result{}, &Error{Class: ClassInvalid, Message: "hostcall requires a binding and method"}
	}
	if err := scope.Err(); err != nil {
		return extension.Result{}, extension.ErrClosed
	}
	encoded, err := json.Marshal(AuthorHostcallParams{Token: token, Service: binding, Method: method, Arguments: JSONBytes(arguments)})
	if err != nil {
		return extension.Result{}, err
	}
	return s.requestHost(ctx, scope, "hostcall", MethodHostcall, encoded)
}

// requestHost is the one worker->host request routine: it registers the
// pending terminal, writes the bounded frame, and waits through cancellation
// or server shutdown. scope optionally bounds the call in addition to ctx.
func (s *server) requestHost(ctx, scope context.Context, label, method string, params []byte) (extension.Result, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return extension.Result{}, err
	}
	combined, cancel := context.WithCancel(ctx)
	defer cancel()
	if scope != nil {
		stop := context.AfterFunc(scope, cancel)
		defer stop()
	}
	id := s.nextRequestID()
	outcomes := make(chan hostcallOutcome, 1)
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return extension.Result{}, extension.ErrClosed
	}
	s.pending[id] = outcomes
	s.mu.Unlock()
	if err := s.write(Frame{Kind: KindRequest, ID: id, Method: method, Params: params}); err != nil {
		s.mu.Lock()
		delete(s.pending, id)
		s.mu.Unlock()
		if errors.Is(err, ErrFrameTooLarge) {
			return extension.Result{}, &Error{Class: ClassInternal, Message: label + " exceeds the frame limit"}
		}
		return extension.Result{}, err
	}
	cancelled := combined.Done()
	for {
		select {
		case outcome := <-outcomes:
			return outcome.result, serviceCallError(outcome.err)
		case <-s.done:
			s.mu.Lock()
			delete(s.pending, id)
			s.mu.Unlock()
			select {
			case outcome := <-outcomes:
				return outcome.result, serviceCallError(outcome.err)
			default:
			}
			return extension.Result{}, extension.ErrClosed
		case <-cancelled:
			cancelled = nil
			_ = s.write(Frame{Kind: KindCancel, ID: id})
		}
	}
}

// serviceCallError exposes one hostcall failure to the author: a failure with
// a produced partial becomes the same typed CallError native authors receive,
// wrapping the classed worker error so errors.Is keeps the cause.
func serviceCallError(err error) error {
	if err == nil {
		return nil
	}
	var failure *Error
	if errors.As(err, &failure) && len(failure.Partial) != 0 {
		return &extension.CallError{Err: failure, Partial: append(json.RawMessage(nil), failure.Partial...)}
	}
	return err
}

func (s *server) nextRequestID() string {
	return "w" + strconv.FormatUint(s.next.Add(1), 10)
}

func (s *server) routeResponse(frame Frame) error {
	if frame.ID == "" {
		return errors.New("worker: response without an id")
	}
	s.mu.Lock()
	pending := s.author.pending[frame.ID]
	if pending != nil {
		delete(s.author.pending, frame.ID)
	}
	s.mu.Unlock()
	if pending != nil {
		if (frame.Result == nil) == (frame.Error == nil) || (frame.Error != nil && !frame.Error.Class.valid()) {
			return fmt.Errorf("worker: invalid response %q", frame.ID)
		}
		pending <- frame
		return nil
	}
	s.mu.Lock()
	outcomes, known := s.pending[frame.ID]
	if known {
		delete(s.pending, frame.ID)
	}
	s.mu.Unlock()
	if !known {
		return fmt.Errorf("worker: response to unknown request %q", frame.ID)
	}
	hasResult := frame.Result != nil
	hasError := frame.Error != nil
	if hasResult == hasError {
		return fmt.Errorf("worker: response %q must carry exactly one of result or error", frame.ID)
	}
	if hasError {
		if !frame.Error.Class.valid() {
			return fmt.Errorf("worker: response %q carries unknown error class %q", frame.ID, frame.Error.Class)
		}
		if len(frame.Error.Partial) != 0 && !validPresentJSON(json.RawMessage(frame.Error.Partial)) {
			return fmt.Errorf("worker: response %q carries an invalid error partial", frame.ID)
		}
		outcomes <- hostcallOutcome{err: frame.Error}
		return nil
	}
	result, err := DecodeServiceResult(frame.Result)
	if err != nil {
		return fmt.Errorf("worker: response %q: %w", frame.ID, err)
	}
	if err := extension.ValidateResultEnvelope(result); err != nil {
		return fmt.Errorf("worker: response %q: %w", frame.ID, err)
	}
	if result.Failure != nil {
		if !validPresentJSON(result.Failure.Data) || !validPresentJSON(result.Failure.Partial) {
			return fmt.Errorf("worker: response %q carries invalid failure payload", frame.ID)
		}
	} else if !validPresentJSON(result.Value) {
		return fmt.Errorf("worker: response %q carries an invalid success value", frame.ID)
	}
	outcomes <- hostcallOutcome{result: result}
	return nil
}

func validPresentJSON(raw json.RawMessage) bool {
	return raw == nil || json.Valid(raw)
}

func (s *server) respond(id string, result json.RawMessage, callErr error) {
	if callErr != nil {
		s.writeError(id, classifyError(callErr))
		return
	}
	if err := s.write(Frame{Kind: KindResponse, ID: id, Result: result}); err != nil {
		if errors.Is(err, ErrFrameTooLarge) {
			s.writeError(id, &Error{Class: ClassInternal, Message: "result exceeds the frame limit"})
			return
		}
		if !errors.Is(err, errStopped) {
			s.fail(err)
		}
	}
}

func (s *server) writeError(id string, failure *Error) {
	if failure == nil {
		failure = &Error{Class: ClassInternal}
	}
	if err := s.write(Frame{Kind: KindResponse, ID: id, Error: failure}); err != nil {
		if errors.Is(err, ErrFrameTooLarge) {
			small := &Error{Class: ClassInternal, Message: "error response exceeds the frame limit"}
			if err := s.write(Frame{Kind: KindResponse, ID: id, Error: small}); err != nil && !errors.Is(err, errStopped) {
				s.fail(err)
			}
			return
		}
		if !errors.Is(err, errStopped) {
			s.fail(err)
		}
	}
}

func (s *server) write(frame Frame) error {
	s.mu.Lock()
	stopped := s.stopped
	s.mu.Unlock()
	if stopped {
		return errStopped
	}
	return s.writer.WriteFrame(frame)
}
