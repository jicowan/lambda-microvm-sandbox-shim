// Package microvmclient provides the client-side transport that lets the
// agent-sandbox Go SDK reach sandboxd running inside a Lambda MicroVM, over
// the MicroVM's public HTTPS endpoint.
//
// REST path: this is achievable TODAY without SDK changes. The SDK connector
// accepts a pluggable http.RoundTripper (connectorConfig.HTTPTransport) and a
// DirectStrategy{URL}. Wire ProxyRoundTripper as that transport, pointed at the
// endpoint, and it injects the Lambda proxy headers on every request:
//
//	X-aws-proxy-auth: <JWE token from CreateMicrovmAuthToken>
//	X-aws-proxy-port: 8080         (sandboxd REST, the endpoint default)
//
// gRPC path: NOT yet possible without an upstream change. The SDK connector
// hardcodes insecure (plaintext) gRPC creds and only accepts a gRPC target from
// the pod port-forward strategy. See README.md for the exact change needed.
package microvmclient

import (
	"context"
	"net/http"
	"sync"
	"time"
)

const (
	headerProxyAuth = "X-aws-proxy-auth"
	headerProxyPort = "X-aws-proxy-port"

	// SandboxdRESTPort is the endpoint's default routed port; sandboxd REST is
	// fronted there by the in-VM shim.
	SandboxdRESTPort = "8080"
	// SandboxdGRPCPort must be requested explicitly via X-aws-proxy-port.
	SandboxdGRPCPort = "9090"
)

// TokenProvider returns a valid JWE auth token for the MicroVM endpoint,
// refreshing it as needed. Implementations typically call
// lambda-microvms:CreateMicrovmAuthToken and cache until near expiry.
type TokenProvider interface {
	Token(ctx context.Context) (string, error)
}

// ProxyRoundTripper wraps a base RoundTripper (default http.DefaultTransport
// with TLS to the public endpoint) and adds the Lambda MicroVM proxy headers.
type ProxyRoundTripper struct {
	Base     http.RoundTripper
	Tokens   TokenProvider
	Port     string // target port inside the MicroVM; defaults to SandboxdRESTPort
}

func (t *ProxyRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	tok, err := t.Tokens.Token(req.Context())
	if err != nil {
		return nil, err
	}
	// Clone so we never mutate the caller's request (RoundTripper contract).
	r2 := req.Clone(req.Context())
	r2.Header.Set(headerProxyAuth, tok)
	port := t.Port
	if port == "" {
		port = SandboxdRESTPort
	}
	r2.Header.Set(headerProxyPort, port)

	base := t.Base
	if base == nil {
		base = http.DefaultTransport
	}
	return base.RoundTrip(r2)
}

// StaticToken is a trivial TokenProvider for a fixed, pre-minted token. Use a
// caching/refreshing implementation in real deployments (tokens expire, and a
// long-lived Start stream may outlive a token — see DESIGN.md #3).
type StaticToken string

func (s StaticToken) Token(context.Context) (string, error) { return string(s), nil }

// CachingToken refreshes via a mint function and caches until refreshBefore the
// stated expiry. Sketch only — wire mint to CreateMicrovmAuthToken.
type CachingToken struct {
	Mint          func(ctx context.Context) (token string, expiry time.Time, err error)
	RefreshBefore time.Duration

	mu     sync.Mutex
	token  string
	expiry time.Time
}

func (c *CachingToken) Token(ctx context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.token != "" && time.Until(c.expiry) > c.RefreshBefore {
		return c.token, nil
	}
	tok, exp, err := c.Mint(ctx)
	if err != nil {
		return "", err
	}
	c.token, c.expiry = tok, exp
	return tok, nil
}
