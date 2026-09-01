// Copyright 2026 The Kubernetes Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Kubernetes-free entry point for talking to sandboxd inside an AWS Lambda
// MicroVM over the MicroVM's public HTTPS endpoint.
//
// Transport:
//   - Filesystem: sandboxd's REST API (/v1/files), over HTTP/1.1 (the shim
//     fronts port 8080 and proxies to sandboxd).
//   - Exec: sandboxd's NATIVE gRPC ProcessService on port 9090. gRPC needs
//     HTTP/2 end-to-end; the Lambda endpoint downgrades to HTTP/1.1 by default,
//     so every RPC carries `x-aws-proxy-force-h2: true` to force h2c to the
//     backend (plus x-aws-proxy-auth and x-aws-proxy-port).
//
// Per-session env (incl. Secrets Manager values) is materialized by the in-VM
// shim at the /run hook into a file that shell commands source (sandboxd's own
// process env is fixed at boot), see sessionEnvFile.

package sandbox

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/emptypb"

	processv1 "sigs.k8s.io/agent-sandbox/packages/sandboxd/spec/process/v1"
)

const (
	headerProxyAuth = "X-aws-proxy-auth"
	headerProxyPort = "X-aws-proxy-port"

	// sessionEnvFile is written by the in-VM shim at the /run hook. Shell
	// commands source it so per-session env (from runHookPayload + Secrets
	// Manager) reaches sandboxd-executed commands; sandboxd's own env is fixed
	// at boot, so it cannot inject per-session values itself.
	sessionEnvFile = "/etc/microvm/session.env"
)

// TokenProvider returns a valid JWE auth token for the MicroVM endpoint,
// refreshing as needed (typically wraps lambda-microvms:CreateMicrovmAuthToken).
type TokenProvider interface {
	Token(ctx context.Context) (string, error)
}

// StaticToken is a fixed token, for tests/spikes. Real use needs refresh.
type StaticToken string

// Token implements TokenProvider.
func (s StaticToken) Token(context.Context) (string, error) { return string(s), nil }

// RefreshingToken mints JWE tokens via Mint and caches them until within
// RefreshBefore of expiry, then re-mints. Safe for concurrent use. Because the
// REST RoundTripper and every gRPC per-RPC credential call Token(ctx), each
// request/RPC uses a fresh token automatically.
type RefreshingToken struct {
	Mint          func(ctx context.Context) (token string, expiry time.Time, err error)
	RefreshBefore time.Duration

	mu    sync.Mutex
	tok   string
	exp   time.Time
	mints int
}

// Token implements TokenProvider.
func (r *RefreshingToken) Token(ctx context.Context) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.tok != "" && time.Until(r.exp) > r.RefreshBefore {
		return r.tok, nil
	}
	tok, exp, err := r.Mint(ctx)
	if err != nil {
		return "", err
	}
	r.tok, r.exp, r.mints = tok, exp, r.mints+1
	return tok, nil
}

// Mints reports how many times a token has been minted (observability/tests).
func (r *RefreshingToken) Mints() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mints
}

// MicrovmOptions configures a Kubernetes-free MicroVM sandbox client.
type MicrovmOptions struct {
	Endpoint string        // https://<id>.lambda-microvm.<region>.on.aws
	Tokens   TokenProvider // mints/refreshes the JWE auth token
	Name     string        // identifies the sandbox in logs/traces

	RESTPort int // in-VM REST port the shim fronts. Default 8080.
	GRPCPort int // in-VM gRPC port sandboxd serves ProcessService on. Default 9090.

	TLSConfig         *tls.Config
	RequestTimeout    time.Duration
	PerAttemptTimeout time.Duration

	Logger           logr.Logger
	TracerProvider   trace.TracerProvider
	TraceServiceName string
	Quiet            bool
}

// Microvm is a Kubernetes-free sandbox handle backed by a Lambda MicroVM.
type Microvm struct {
	connector *connector
	commands  *Commands
	files     *Files
	name      string
}

// NewMicrovm builds a MicroVM sandbox client and connects the transport. The
// MicroVM must already be running (RunMicrovm) and reachable at Endpoint.
func NewMicrovm(ctx context.Context, o MicrovmOptions) (*Microvm, error) {
	if o.Endpoint == "" {
		return nil, fmt.Errorf("sandbox: MicrovmOptions.Endpoint is required")
	}
	if o.Tokens == nil {
		return nil, fmt.Errorf("sandbox: MicrovmOptions.Tokens is required")
	}
	if o.RESTPort == 0 {
		o.RESTPort = defaultSandboxdRESTPort
	}
	if o.GRPCPort == 0 {
		o.GRPCPort = defaultSandboxdGRPCPort
	}
	if o.RequestTimeout == 0 {
		o.RequestTimeout = defaultRequestTimeout
	}
	if o.PerAttemptTimeout == 0 {
		o.PerAttemptTimeout = defaultPerAttemptTimeout
	}
	u, err := url.Parse(o.Endpoint)
	if err != nil || u.Host == "" {
		return nil, fmt.Errorf("sandbox: invalid Endpoint %q: %w", o.Endpoint, err)
	}
	if u.Scheme != "https" {
		return nil, fmt.Errorf("sandbox: Endpoint must be https, got %q", o.Endpoint)
	}

	sdkOpts := Options{Logger: o.Logger, Quiet: o.Quiet, TracerProvider: o.TracerProvider, TraceServiceName: o.TraceServiceName}
	sdkOpts.setDefaults()
	tracer, svcName := newTracer(sdkOpts)

	// REST (filesystem): DirectStrategy + a RoundTripper injecting the proxy headers.
	restRT := &proxyRoundTripper{base: tlsTransport(o.TLSConfig), tokens: o.Tokens, port: strconv.Itoa(o.RESTPort)}

	// gRPC (exec): dial the endpoint over TLS; per-RPC metadata carries auth,
	// the target port, and force-h2 so the edge speaks HTTP/2 to sandboxd.
	grpcTarget := u.Host
	if u.Port() == "" {
		grpcTarget = u.Host + ":443"
	}
	grpcOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(credentials.NewTLS(tlsForHost(o.TLSConfig, u.Hostname()))),
		grpc.WithPerRPCCredentials(proxyPerRPC{tokens: o.Tokens, port: strconv.Itoa(o.GRPCPort)}),
	}

	conn := newConnector(connectorConfig{
		Strategy:          &DirectStrategy{URL: o.Endpoint},
		Namespace:         "microvm",
		RouterHeaders:     false,
		RequestTimeout:    o.RequestTimeout,
		PerAttemptTimeout: o.PerAttemptTimeout,
		HTTPTransport:     restRT,
		GRPCDialOptions:   grpcOpts,
		Log:               sdkOpts.Logger,
		Tracer:            tracer,
		TraceServiceName:  svcName,
	})
	conn.SetIdentity(o.Name)
	conn.SetGRPCTarget(grpcTarget)
	if err := conn.Connect(ctx); err != nil {
		return nil, fmt.Errorf("sandbox: connect microvm endpoint: %w", err)
	}

	noop := func() func() { return func() {} }
	lifecycle := func() context.Context { return context.Background() }
	prefix := func() string { return "microvm[" + o.Name + "]" }

	m := &Microvm{connector: conn, name: o.Name}
	m.commands = &Commands{
		connector: conn, runtime: RuntimeSandboxd, tracer: tracer, svcName: svcName,
		log: sdkOpts.Logger, errPrefix: prefix, trackOp: noop, lifecycleCtx: lifecycle,
	}
	m.files = &Files{
		connector: conn, runtime: RuntimeSandboxd, tracer: tracer, svcName: svcName,
		log: sdkOpts.Logger, maxDownload: defaultMaxDownloadSize, maxUpload: defaultMaxUploadSize,
		errPrefix: prefix, trackOp: noop, lifecycleCtx: lifecycle,
	}
	return m, nil
}

// Files returns the file operations sub-object (sandboxd REST).
func (m *Microvm) Files() *Files { return m.files }

// Run executes a shell command via sandboxd's native gRPC ProcessService.Execute
// (unary) and returns stdout/stderr/exit code.
func (m *Microvm) Run(ctx context.Context, command string, opts ...CallOption) (*ExecutionResult, error) {
	return m.commands.Run(ctx, wrapSessionEnv(command), opts...)
}

// Write/Read/List proxy to sandboxd's REST filesystem API.
func (m *Microvm) Write(ctx context.Context, path string, b []byte, opts ...CallOption) error {
	return m.files.Write(ctx, path, b, opts...)
}
func (m *Microvm) Read(ctx context.Context, path string, opts ...CallOption) ([]byte, error) {
	return m.files.Read(ctx, path, opts...)
}
func (m *Microvm) List(ctx context.Context, path string, opts ...CallOption) ([]FileEntry, error) {
	return m.files.List(ctx, path, opts...)
}

// Close tears down the transport (does not terminate the MicroVM).
func (m *Microvm) Close() error { return m.connector.Close() }

// ---- streaming / interactive exec (native gRPC ProcessService.Start) --------

// PTYSize requests a pseudo-terminal of the given dimensions.
type PTYSize struct {
	Cols uint16
	Rows uint16
}

// ExecSpec configures a streaming command execution.
type ExecSpec struct {
	Command string    // run via /bin/sh -c (session env sourced)
	Argv    []string  // or exec argv directly (takes precedence; no env sourcing)
	Cwd     string    // working dir
	PTY     *PTYSize  // non-nil allocates a PTY; Stdout carries combined output
	Stdin   io.Reader // optional; streamed to the process via WriteStdin
	Stdout  io.Writer // required
	Stderr  io.Writer // optional; nil folds stderr into Stdout (always so under PTY)
}

// Exec runs a command with streamed I/O over sandboxd's gRPC Start RPC and
// returns the exit code.
func (m *Microvm) Exec(ctx context.Context, spec ExecSpec) (int, error) {
	if spec.Stdout == nil {
		return -1, fmt.Errorf("microvm[%s]: ExecSpec.Stdout is required", m.name)
	}
	conn, err := m.connector.GRPCConn()
	if err != nil {
		return -1, fmt.Errorf("microvm[%s]: grpc conn: %w", m.name, err)
	}
	client := processv1.NewProcessServiceClient(conn)

	cfg := &processv1.ProcessConfig{}
	if spec.Cwd != "" {
		cwd := spec.Cwd
		cfg.Cwd = &cwd
	}
	if len(spec.Argv) > 0 {
		cfg.Command = spec.Argv
	} else {
		cfg.Command = []string{"/bin/sh", "-c", wrapSessionEnv(spec.Command)}
	}
	req := &processv1.StartRequest{Config: cfg}
	if spec.PTY != nil {
		req.Pty = &processv1.PTY{Cols: uint32(spec.PTY.Cols), Rows: uint32(spec.PTY.Rows)}
	}

	stream, err := client.Start(ctx, req)
	if err != nil {
		return -1, fmt.Errorf("microvm[%s]: Start: %w", m.name, err)
	}
	stderr := spec.Stderr
	if stderr == nil {
		stderr = spec.Stdout
	}
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return 0, nil // stream ended without an explicit exit event
		}
		if err != nil {
			return -1, fmt.Errorf("microvm[%s]: stream: %w", m.name, err)
		}
		if init := msg.GetInit(); init != nil {
			if spec.Stdin != nil {
				go pumpStdin(ctx, client, init.GetProcessId(), spec.Stdin)
			}
			continue
		}
		if out := msg.GetStdout(); len(out) > 0 {
			if _, err := spec.Stdout.Write(out); err != nil {
				return -1, err
			}
		}
		if e := msg.GetStderr(); len(e) > 0 {
			if _, err := stderr.Write(e); err != nil {
				return -1, err
			}
		}
		if ex := msg.GetExit(); ex != nil {
			return int(ex.GetExitCode()), nil
		}
	}
}

// pumpStdin streams spec.Stdin to the running process via WriteStdin, then EOF.
func pumpStdin(ctx context.Context, client processv1.ProcessServiceClient, pid int32, r io.Reader) {
	buf := make([]byte, 32<<10)
	for {
		n, rerr := r.Read(buf)
		if n > 0 {
			_, err := client.WriteStdin(ctx, &processv1.WriteStdinRequest{
				ProcessId: pid,
				Payload:   &processv1.WriteStdinRequest_Input{Input: append([]byte(nil), buf[:n]...)},
			})
			if err != nil {
				return
			}
		}
		if rerr != nil {
			_, _ = client.WriteStdin(ctx, &processv1.WriteStdinRequest{
				ProcessId: pid,
				Payload:   &processv1.WriteStdinRequest_Eof{Eof: &emptypb.Empty{}},
			})
			return
		}
	}
}

// ---- helpers ----------------------------------------------------------------

// wrapSessionEnv prefixes a shell command with sourcing the per-session env file
// the shim writes at /run (so Secrets Manager / runHookPayload env apply).
func wrapSessionEnv(cmd string) string {
	return "[ -r " + sessionEnvFile + " ] && . " + sessionEnvFile + "; " + cmd
}

func tlsTransport(cfg *tls.Config) http.RoundTripper {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.TLSClientConfig = cfg
	return t
}

func tlsForHost(cfg *tls.Config, host string) *tls.Config {
	if cfg == nil {
		cfg = &tls.Config{}
	} else {
		cfg = cfg.Clone()
	}
	if cfg.ServerName == "" {
		cfg.ServerName = host
	}
	return cfg
}

// proxyRoundTripper adds the Lambda MicroVM proxy headers to every REST request.
type proxyRoundTripper struct {
	base   http.RoundTripper
	tokens TokenProvider
	port   string
}

func (t *proxyRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	tok, err := t.tokens.Token(req.Context())
	if err != nil {
		return nil, err
	}
	r2 := req.Clone(req.Context())
	r2.Header.Set(headerProxyAuth, tok)
	r2.Header.Set(headerProxyPort, t.port)
	return t.base.RoundTrip(r2)
}

// proxyPerRPC injects the proxy auth + target port + force-h2 as gRPC per-RPC
// metadata over TLS (force-h2 makes the endpoint speak HTTP/2 to sandboxd).
type proxyPerRPC struct {
	tokens TokenProvider
	port   string
}

func (c proxyPerRPC) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	tok, err := c.tokens.Token(ctx)
	if err != nil {
		return nil, err
	}
	return map[string]string{
		"x-aws-proxy-auth":     tok,
		"x-aws-proxy-port":     c.port,
		"x-aws-proxy-force-h2": "true",
	}, nil
}

func (proxyPerRPC) RequireTransportSecurity() bool { return true }
