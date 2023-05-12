package ghtransport

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/google/go-github/v41/github"

	"golang.org/x/time/rate"
)

var (
	DefaultUserAgent = "Go-GitHub-Tranpsport/0.1.0"

	headerRateLimit     = "X-RateLimit-Limit"
	headerRateRemaining = "X-RateLimit-Remaining"
	headerRateReset     = "X-RateLimit-Reset"
)

// parseRate is copied from github.com/google/go-github/v41/github
func parseRate(r *http.Response) github.Rate {
	var rate github.Rate
	if limit := r.Header.Get(headerRateLimit); limit != "" {
		rate.Limit, _ = strconv.Atoi(limit)
	}
	if remaining := r.Header.Get(headerRateRemaining); remaining != "" {
		rate.Remaining, _ = strconv.Atoi(remaining)
	}
	if reset := r.Header.Get(headerRateReset); reset != "" {
		if v, _ := strconv.ParseInt(reset, 10, 64); v != 0 {
			rate.Reset = github.Timestamp{Time: time.Unix(v, 0)}
		}
	}
	return rate
}

type roundTripResp struct {
	resp *http.Response
	err  error
}

type roundTripReq struct {
	body    []byte
	method  string
	url     string
	attempt int
	ctx     context.Context
	respCh  chan<- roundTripResp
}

type WarnLog interface {
	WarnCtx(ctx context.Context, msg string, args ...any)
}

type RateMeasurer interface {
	RateMeasure(r github.Rate)
}

// GitHubTransport is a http.RoundTripper implementation that respects
// GitHub's rules for a well behaved client.
//   - A given transport makes all requests serially
//   - POST,PUT,PATCH,DELETE are rate limited to 1/s
//   - Requests which fail due to rate limits are retried after
//     the mandated period of time
type GitHubTransport struct {
	agent   string
	base    http.RoundTripper
	warnLog WarnLog
	rm      RateMeasurer
	ch      chan roundTripReq

	stop func()
}

// Opt is an option for setting up the transport.
type Opt func(*GitHubTransport)

// WithUserAgent lets you set the User-Agent string
// used by requests from this transport. This defaults
// to DefaultUserAgent. User-Agent is only overriden if
// the original request does not include a user-agent
// string.
func WithUserAgent(ua string) Opt {
	return func(t *GitHubTransport) {
		t.agent = ua
	}
}

// WithBaseTransport sets base round tripper to pass
// requests on to.
func WithBaseTransport(base http.RoundTripper) Opt {
	return func(t *GitHubTransport) {
		t.base = base
	}
}

// WithWarnLog sets a logger to call when rate limits and
// errors are hit.
func WithWarnLog(l WarnLog) Opt {
	return func(t *GitHubTransport) {
		t.warnLog = l
	}
}

// WithRateMeasurer can be used to pass record the rate limit settings
// returned by the github API.
func WithRateMeasurer(rm RateMeasurer) Opt {
	return func(t *GitHubTransport) {
		t.rm = rm
	}
}

func New(opts ...Opt) *GitHubTransport {
	t := &GitHubTransport{
		agent: DefaultUserAgent,
		base:  http.DefaultTransport,
	}

	for _, o := range opts {
		o(t)
	}

	ctx, cancel := context.WithCancel(context.Background())

	t.ch = make(chan roundTripReq)
	t.stop = cancel

	go t.loop(ctx)

	return t
}

func (g *GitHubTransport) Close() {
	g.stop()
}

func (g *GitHubTransport) loop(ctx context.Context) {
	postLim := rate.NewLimiter(1.0, 1)

	defer close(g.ch)
	for {
		select {
		case req := <-g.ch:
			var resp *http.Response
			var err error

			switch req.method {
			case http.MethodPatch, http.MethodPost, http.MethodPut, http.MethodDelete:
				err = postLim.Wait(ctx)
				if err != nil {
					break
				}
			default:
			}

			hreq, err := http.NewRequest(req.method, req.url, bytes.NewBuffer(req.body))
			if err == nil {
				resp, err = g.base.RoundTrip(hreq)
				if err == nil {
					err = github.CheckResponse(resp)
				}
			}

			if resp != nil && g.rm != nil {
				g.rm.RateMeasure(parseRate(resp))
			}

			var retry bool

			switch err := err.(type) {
			case *github.AbuseRateLimitError:
				wait := 5 * time.Second
				if err.RetryAfter != nil {
					wait = err.GetRetryAfter()
				}
				g.warnLog.WarnCtx(req.ctx, "abused rate limit error, sleeping", "sleep_time", wait)
				time.Sleep(wait)
				retry = true
			case *github.RateLimitError:
				g.warnLog.WarnCtx(req.ctx, "rate limit, sleeping", "sleep_time", time.Until(err.Rate.Reset.Time))
				wait := time.Until(err.Rate.Reset.Time)
				time.Sleep(wait)
				retry = true
			case *github.ErrorResponse:
				if err.Response.StatusCode == 403 {
					wait := 5 * time.Second
					g.warnLog.WarnCtx(req.ctx, "403 response from GitHub, sleeping", "sleep_time", wait)
					time.Sleep(wait)
					retry = true
				}
			default:
			}

			if retry {
				req.attempt++
				go func() {
					g.ch <- req
				}()
				continue
			}

			req.respCh <- roundTripResp{resp: resp, err: err}
		case <-ctx.Done():
			return
		}
	}
}

// RoundTrip executes a single HTTP transaction, returning
// a Response for the provided Request.
//
// Requests are dispatched per the GitHub API best practices.
func (g *GitHubTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	ch := make(chan roundTripResp, 1)
	defer close(ch)
	var body []byte

	uah := "User-Agent"
	if r.Header.Get(uah) == "" {
		r.Header.Set(uah, g.agent)
	}

	if r.Body != nil {
		buf := bytes.NewBuffer([]byte{})
		io.Copy(buf, r.Body)
		body = buf.Bytes()
	}
	g.ch <- roundTripReq{
		method: r.Method,
		url:    r.URL.String(),
		body:   body,
		respCh: ch,
		ctx:    r.Context(),
	}
	resp := <-ch
	return resp.resp, resp.err
}
