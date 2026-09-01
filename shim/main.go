// Command microvm-shim is the in-VM entrypoint for a Lambda MicroVM that hosts
// the agent-sandbox sandboxd runtime (KEP-539.2).
//
// It:
//  1. Supervises sandboxd, which serves its REST filesystem API on a loopback
//     port and its gRPC ProcessService on the endpoint-facing gRPC port (0.0.0.0).
//  2. Serves the Lambda MicroVM lifecycle hooks sandboxd does not
//     (/aws/lambda-microvms/runtime/v1/{ready,validate,run,resume,suspend,terminate})
//     and reverse-proxies every other HTTP request to sandboxd's REST API.
//
// Exec no longer goes through the shim: clients use sandboxd's NATIVE gRPC
// ProcessService directly over the endpoint (with x-aws-proxy-force-h2). The
// shim's only exec-related job is materializing per-session env (from the /run
// hook, incl. Secrets Manager) into a file that shell commands source, since
// sandboxd's own process env is fixed at boot.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
)

const (
	hookPrefix = "/aws/lambda-microvms/runtime/v1/"
	// sessionEnvFile is sourced by shell commands run via sandboxd's gRPC exec,
	// so per-session env (from the /run hook + Secrets Manager) applies even
	// though sandboxd's own process env is fixed at boot. Must match the SDK's
	// sessionEnvFile constant.
	sessionEnvFile = "/etc/microvm/session.env"
)

type config struct {
	sandboxdBin        string
	workspace          string
	publicHTTPAddr     string // endpoint-facing REST + hooks (8080)
	sandboxdListenHost string // interface sandboxd binds (0.0.0.0)
	sandboxdRESTPort   string // sandboxd REST port (shim proxies to it on loopback)
	sandboxdGRPCPort   string // sandboxd gRPC port (edge routes here directly)
}

func (c config) restTarget() string { return "127.0.0.1:" + c.sandboxdRESTPort }

func loadConfig() config {
	_, grpcPort := splitHostPort(env("PUBLIC_GRPC_ADDR", "0.0.0.0:9090"))
	return config{
		sandboxdBin:        env("SANDBOXD_BIN", "/usr/local/bin/sandboxd"),
		workspace:          env("WORKSPACE", "/workspace"),
		publicHTTPAddr:     env("PUBLIC_HTTP_ADDR", "0.0.0.0:8080"),
		sandboxdListenHost: "0.0.0.0",
		sandboxdRESTPort:   "8081",
		sandboxdGRPCPort:   grpcPort,
	}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// running gates the REST proxy: it defaults OPEN. Lambda holds external traffic
// until /run returns 200 when a run hook is configured; /terminate flips it off.
var running atomic.Bool

func init() { running.Store(true) }

// session holds per-session state applied by /run: env + working dir, used to
// write the session env file and launch the optional startup command.
type sessionState struct {
	mu  sync.Mutex
	env map[string]string
	cwd string
}

var session = &sessionState{}

func (s *sessionState) set(env map[string]string, cwd string) {
	s.mu.Lock()
	s.env, s.cwd = env, cwd
	s.mu.Unlock()
}

func (s *sessionState) cmdEnv() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := os.Environ()
	for k, v := range s.env {
		e = append(e, k+"="+v)
	}
	return e
}

func (s *sessionState) dir(fallback string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cwd != "" {
		return s.cwd
	}
	return fallback
}

func main() {
	cfg := loadConfig()
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	sbx := startSandboxd(ctx, cfg)

	httpSrv := &http.Server{
		Addr:              cfg.publicHTTPAddr,
		Handler:           httpHandler(cfg),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		log.Printf("shim: HTTP (hooks + REST proxy) on %s; sandboxd gRPC on 0.0.0.0:%s", cfg.publicHTTPAddr, cfg.sandboxdGRPCPort)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("shim: http server: %v", err)
		}
	}()

	<-ctx.Done()
	log.Print("shim: shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdownCtx)
	if sbx.Process != nil {
		_ = sbx.Process.Signal(syscall.SIGTERM)
	}
	_ = sbx.Wait()
}

// startSandboxd launches sandboxd: REST on loopback restPort, gRPC on 0.0.0.0
// grpcPort (the edge routes native gRPC there directly, via x-aws-proxy-force-h2).
func startSandboxd(ctx context.Context, cfg config) *exec.Cmd {
	cmd := exec.CommandContext(ctx, cfg.sandboxdBin,
		"--listen-host="+cfg.sandboxdListenHost,
		"--rest-port="+cfg.sandboxdRESTPort,
		"--grpc-port="+cfg.sandboxdGRPCPort,
		"--root-dir="+cfg.workspace,
	)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Start(); err != nil {
		log.Fatalf("shim: start sandboxd: %v", err)
	}
	log.Printf("shim: started sandboxd pid=%d (listen=%s rest=%s grpc=%s root=%s)",
		cmd.Process.Pid, cfg.sandboxdListenHost, cfg.sandboxdRESTPort, cfg.sandboxdGRPCPort, cfg.workspace)
	return cmd
}

func httpHandler(cfg config) http.Handler {
	restURL, _ := url.Parse("http://" + cfg.restTarget())
	proxy := httputil.NewSingleHostReverseProxy(restURL)

	mux := http.NewServeMux()
	mux.HandleFunc(hookPrefix+"ready", hookReady(cfg))
	mux.HandleFunc(hookPrefix+"validate", hookValidate(cfg))
	mux.HandleFunc(hookPrefix+"run", hookRun(cfg))
	mux.HandleFunc(hookPrefix+"resume", hookResume(cfg))
	mux.HandleFunc(hookPrefix+"suspend", hookSuspend(cfg))
	mux.HandleFunc(hookPrefix+"terminate", hookTerminate(cfg))

	// Everything else -> sandboxd REST (/v1/files, /v1/health, /v1/metadata).
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, hookPrefix) {
			http.NotFound(w, r)
			return
		}
		if !running.Load() {
			http.Error(w, "sandbox not yet running (awaiting /run)", http.StatusServiceUnavailable)
			return
		}
		proxy.ServeHTTP(w, r)
	})
	return mux
}

// ---- lifecycle hooks -------------------------------------------------------

type runRequest struct {
	MicrovmID      string `json:"microvmId"`
	RunHookPayload string `json:"runHookPayload"`
}

func hookReady(cfg config) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		if sandboxdHealthy(cfg) {
			w.WriteHeader(http.StatusOK)
			return
		}
		http.Error(w, "sandboxd not ready", http.StatusServiceUnavailable)
	}
}

func hookValidate(cfg config) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		if sandboxdHealthy(cfg) {
			w.WriteHeader(http.StatusOK)
			return
		}
		http.Error(w, "validation pending", http.StatusServiceUnavailable)
	}
}

// runPayload is the JSON contract for RunMicrovm's runHookPayload (max 16 KB).
type runPayload struct {
	ResetWorkspace *bool             `json:"reset_workspace,omitempty"`
	Env            map[string]string `json:"env,omitempty"`
	SecretEnv      []string          `json:"secret_env,omitempty"` // AWS Secrets Manager IDs (fetched under exec role)
	RunCommand     []string          `json:"run_command,omitempty"`
	Cwd            string            `json:"cwd,omitempty"`
}

// hookRun applies per-session state at session start: reset the workspace,
// resolve env (payload + Secrets Manager), write it to the session env file
// (sourced by exec'd shell commands), and optionally launch a startup command.
func hookRun(cfg config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req runRequest
		_ = json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req)

		var p runPayload
		if req.RunHookPayload != "" {
			if err := json.Unmarshal([]byte(req.RunHookPayload), &p); err != nil {
				log.Printf("shim: /run payload not JSON (%v); treating as empty", err)
			}
		}
		reset := true
		if p.ResetWorkspace != nil {
			reset = *p.ResetWorkspace
		}
		removed := 0
		if reset {
			removed = resetWorkspace(cfg.workspace)
		}

		env := map[string]string{}
		for k, v := range p.Env {
			env[k] = v
		}
		secretKeys := 0
		if len(p.SecretEnv) > 0 {
			secEnv, err := fetchSecretEnv(r.Context(), p.SecretEnv)
			if err != nil {
				log.Printf("shim: /run secrets fetch error: %v", err)
			}
			for k, v := range secEnv {
				env[k] = v
				secretKeys++
			}
		}
		session.set(env, p.Cwd)
		if err := writeSessionEnvFile(env); err != nil {
			log.Printf("shim: /run write session env: %v", err)
		}
		if len(p.RunCommand) > 0 {
			startStartupCommand(p.RunCommand, session.dir(cfg.workspace), session.cmdEnv())
		}
		log.Printf("shim: /run microvmId=%s reset=%v removed=%d envKeys=%d secretKeys=%d cwd=%q startup=%v",
			req.MicrovmID, reset, removed, len(p.Env), secretKeys, p.Cwd, len(p.RunCommand) > 0)
		running.Store(true)
		w.WriteHeader(http.StatusOK)
	}
}

func hookResume(_ config) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		log.Print("shim: /resume")
		running.Store(true)
		w.WriteHeader(http.StatusOK)
	}
}

func hookSuspend(_ config) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		log.Print("shim: /suspend")
		w.WriteHeader(http.StatusOK)
	}
}

func hookTerminate(_ config) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		log.Print("shim: /terminate")
		running.Store(false)
		w.WriteHeader(http.StatusOK)
	}
}

// writeSessionEnvFile writes the session env as shell `export K='V'` lines to
// sessionEnvFile (0600). Values are single-quote escaped.
func writeSessionEnvFile(envs map[string]string) error {
	if err := os.MkdirAll(filepath.Dir(sessionEnvFile), 0o755); err != nil {
		return err
	}
	var b strings.Builder
	for k, v := range envs {
		b.WriteString("export ")
		b.WriteString(k)
		b.WriteString("=")
		b.WriteString("'" + strings.ReplaceAll(v, "'", `'\''`) + "'")
		b.WriteString("\n")
	}
	return os.WriteFile(sessionEnvFile, []byte(b.String()), 0o600)
}

// startStartupCommand runs the PodSpec command/args once at session start, in
// the background, logging to a file.
func startStartupCommand(argv []string, dir string, env []string) {
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Dir, cmd.Env = dir, env
	if f, err := os.Create(filepath.Join(dir, ".microvm-startup.log")); err == nil {
		cmd.Stdout, cmd.Stderr = f, f
		go func() { _ = cmd.Wait(); _ = f.Close() }()
	} else {
		go func() { _ = cmd.Wait() }()
	}
	if err := cmd.Start(); err != nil {
		log.Printf("shim: /run startup command failed to start: %v", err)
		return
	}
	log.Printf("shim: /run startup command started pid=%d argv=%v cwd=%s", cmd.Process.Pid, argv, dir)
}

// fetchSecretEnv resolves each Secrets Manager secret ID to its key/value pairs
// (SecretString must be a JSON object) under the MicroVM execution role.
func fetchSecretEnv(ctx context.Context, secretIDs []string) (map[string]string, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	sm := secretsmanager.NewFromConfig(cfg)
	out := map[string]string{}
	for _, id := range secretIDs {
		id := id
		r, err := sm.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: &id})
		if err != nil {
			return out, fmt.Errorf("get secret %q: %w", id, err)
		}
		if r.SecretString == nil {
			continue
		}
		var kv map[string]string
		if err := json.Unmarshal([]byte(*r.SecretString), &kv); err != nil {
			return out, fmt.Errorf("secret %q is not a JSON string map: %w", id, err)
		}
		for k, v := range kv {
			out[k] = v
		}
	}
	return out, nil
}

func resetWorkspace(dir string) int {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			_ = os.MkdirAll(dir, 0o755)
		}
		return 0
	}
	n := 0
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(dir, e.Name())); err == nil {
			n++
		}
	}
	return n
}

func sandboxdHealthy(cfg config) bool {
	c := &http.Client{Timeout: 2 * time.Second}
	resp, err := c.Get("http://" + cfg.restTarget() + "/v1/health")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode == http.StatusOK
}

func splitHostPort(hostport string) (host, port string) {
	h, p, err := net.SplitHostPort(hostport)
	if err != nil {
		return hostport, ""
	}
	return h, p
}
