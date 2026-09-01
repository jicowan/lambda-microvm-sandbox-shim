// Command provider is a proof-of-concept "provider seam" that schedules AWS
// Lambda MicroVMs through the agent-sandbox Sandbox API.
//
// It is an event-driven controller (dynamic informer + workqueue) over
// agents.x-k8s.io/v1beta1 Sandbox resources annotated
// microvm.agents.x-k8s.io/backend=lambda-microvm. It maps the supported subset
// of the PodSpec to RunMicrovm, provisions/suspends/resumes/terminates MicroVMs
// via the `aws lambda-microvms` CLI, publishes the endpoint for routing in
// status.serviceFQDN, and optionally serves claims from a warm pool.
//
// The upstream agent-sandbox controller (Sandbox->Pod) is deliberately NOT
// installed; this provider owns the marked Sandboxes and ignores the rest.
// Runs out-of-cluster (kubeconfig + local AWS creds) for the PoC.
package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/util/workqueue"
)

const (
	annBackend    = "microvm.agents.x-k8s.io/backend"
	annImage      = "microvm.agents.x-k8s.io/image"
	annRole       = "microvm.agents.x-k8s.io/execution-role-arn"
	annSecretEnv  = "microvm.agents.x-k8s.io/secret-env" // comma-separated ASM secret IDs
	annMicrovmID  = "microvm.agents.x-k8s.io/microvm-id"
	annEndpoint   = "microvm.agents.x-k8s.io/endpoint"
	annState      = "microvm.agents.x-k8s.io/state"
	annPorts      = "microvm.agents.x-k8s.io/ports"
	annIngress    = "microvm.agents.x-k8s.io/ingress-connector"    // ARN or short name (ALL_INGRESS/NO_INGRESS/SHELL_INGRESS)
	annEgress     = "microvm.agents.x-k8s.io/egress-connector"     // ARN (VPC connector) or short name (INTERNET_EGRESS)
	annIdlePolicy = "microvm.agents.x-k8s.io/idle-policy"          // raw idle-policy JSON override
	annMaxDur     = "microvm.agents.x-k8s.io/max-duration-seconds" // overrides spec.lifecycle.shutdownTime
	annLaunchType = "microvm.agents.x-k8s.io/launch-type"          // cold | warm
	annSessSecret = "microvm.agents.x-k8s.io/session-secret" // ASM secret mirrored from k8s, cleaned up on delete
	finalizer     = "microvm.agents.x-k8s.io/finalizer"
	backendValue  = "lambda-microvm"

	// warmPoolLabel is set by the agent-sandbox SandboxWarmPool controller on the
	// Sandboxes it creates; the provider treats those as opt-in too (so claims
	// work without the SandboxTemplate carrying our backend annotation).
	warmPoolLabel = "agents.x-k8s.io/warm-pool-sandbox"

	warmPoolConfigMap = "microvm-provider-warmpool" // persists the free pool across restarts
)

var (
	sandboxGVR   = schema.GroupVersionResource{Group: "agents.x-k8s.io", Version: "v1beta1", Resource: "sandboxes"}
	secretGVR    = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "secrets"}
	configMapGVR = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}
	saGVR        = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "serviceaccounts"}
)

type poolVM struct {
	ID       string `json:"id"`
	Endpoint string `json:"endpoint"`
}

type provider struct {
	dyn         dynamic.Interface
	region      string
	defaultImg  string
	ingressConn string
	egressConn  string
	queue       workqueue.TypedRateLimitingInterface[string]
	indexer     cache.Indexer
	resync      time.Duration
	workers     int
	cluster     string // EKS cluster name, for resolving Pod Identity associations

	// backAll makes the provider own every Sandbox in the cluster, not just
	// those opted in via annotation/label. Safe when the upstream core
	// Sandbox->Pod controller is NOT installed (as here) — it lets
	// SandboxClaim/WarmPool-created Sandboxes (which carry no backend
	// annotation) be backed by MicroVMs.
	backAll bool

	// warm pool
	warmSize  int
	warmImage string
	warmRole  string
	warmMu    sync.Mutex
	warmFree  []poolVM
}

func main() {
	var kubeconfig, region, defImg, warmImage, warmRole string
	var warmSize, workers int
	flag.StringVar(&kubeconfig, "kubeconfig", defaultKubeconfig(), "path to kubeconfig (empty = in-cluster)")
	flag.StringVar(&region, "region", env("AWS_REGION", "us-east-2"), "AWS region")
	flag.StringVar(&defImg, "default-image", "microvm-sandbox-shim", "default MicroVM image name")
	flag.StringVar(&warmImage, "warm-image", "microvm-sandbox-shim", "MicroVM image for the warm pool")
	flag.StringVar(&warmRole, "warm-role", "", "execution role ARN for warm pool VMs")
	flag.IntVar(&warmSize, "warm-size", 0, "warm pool size (0 = disabled)")
	flag.IntVar(&workers, "workers", 2, "reconcile workers")
	backAll := flag.Bool("back-all-sandboxes", false, "back EVERY Sandbox (not just annotated/warm-labeled); safe only when the core Sandbox->Pod controller is absent")
	resync := flag.Duration("resync", 30*time.Second, "informer resync period (catches external MicroVM state drift)")
	leaderElect := flag.Bool("leader-elect", false, "enable leader election (single active replica via a Lease)")
	leaseNS := flag.String("leader-election-namespace", "default", "namespace for the leader-election Lease")
	cluster := flag.String("cluster", "", "EKS cluster name, for resolving a ServiceAccount's Pod Identity association to an execution role")
	flag.Parse()

	cfg, err := loadKubeConfig(kubeconfig)
	if err != nil {
		log.Fatalf("kubeconfig: %v", err)
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		log.Fatalf("dynamic client: %v", err)
	}
	p := &provider{
		dyn:         dyn,
		region:      region,
		defaultImg:  defImg,
		ingressConn: fmt.Sprintf("arn:aws:lambda:%s:aws:network-connector:aws-network-connector:ALL_INGRESS", region),
		egressConn:  fmt.Sprintf("arn:aws:lambda:%s:aws:network-connector:aws-network-connector:INTERNET_EGRESS", region),
		queue:       workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]()),
		warmSize:    warmSize,
		warmImage:   warmImage,
		warmRole:    warmRole,
		backAll:     *backAll,
		cluster:     *cluster,
	}

	p.resync, p.workers = *resync, workers

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if !*leaderElect {
		p.run(ctx)
		return
	}

	// Leader election: only the holder of the Lease runs the controllers.
	id, _ := os.Hostname()
	k8s, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		log.Fatalf("kubernetes client: %v", err)
	}
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{Name: "microvm-provider", Namespace: *leaseNS},
		Client:    k8s.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{Identity: id},
	}
	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   15 * time.Second,
		RenewDeadline:   10 * time.Second,
		RetryPeriod:     2 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(c context.Context) { log.Printf("provider: became leader (%s)", id); p.run(c) },
			OnStoppedLeading: func() { log.Printf("provider: lost leadership (%s)", id) },
		},
	})
}

// run wires the informer + workers and blocks until ctx is cancelled.
func (p *provider) run(ctx context.Context) {
	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(p.dyn, p.resync, metav1.NamespaceAll, nil)
	inf := factory.ForResource(sandboxGVR).Informer()
	p.indexer = inf.GetIndexer()
	_, _ = inf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(o interface{}) { p.enqueue(o) },
		UpdateFunc: func(_, o interface{}) { p.enqueue(o) },
		DeleteFunc: func(o interface{}) { p.enqueue(o) },
	})
	factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), inf.HasSynced) {
		log.Print("cache sync failed")
		return
	}
	log.Printf("provider: event-driven; watching Sandboxes region=%s workers=%d warmSize=%d backAll=%v",
		p.region, p.workers, p.warmSize, p.backAll)

	if p.warmSize > 0 {
		p.loadWarmPool(ctx)
		go p.ensureWarmPool(ctx)
	}
	for i := 0; i < p.workers; i++ {
		go p.runWorker(ctx)
	}

	<-ctx.Done()
	log.Print("provider: shutting down")
	p.queue.ShutDown()
	p.drainWarmPool()
}

// owns reports whether this provider should back the Sandbox: either it opted
// in via the backend annotation, or it was created by a SandboxWarmPool (so
// agent-sandbox claims/warmpools route to MicroVMs without template changes).
func (p *provider) owns(u *unstructured.Unstructured) bool {
	if p.backAll {
		return true
	}
	if u.GetAnnotations()[annBackend] == backendValue {
		return true
	}
	_, warm := u.GetLabels()[warmPoolLabel]
	return warm
}

func (p *provider) enqueue(o interface{}) {
	if key, err := cache.MetaNamespaceKeyFunc(o); err == nil {
		p.queue.Add(key)
	}
}

func (p *provider) runWorker(ctx context.Context) {
	for {
		key, shutdown := p.queue.Get()
		if shutdown {
			return
		}
		requeueAfter, err := p.reconcileKey(ctx, key)
		if err != nil {
			log.Printf("provider: reconcile %s: %v", key, err)
			p.queue.AddRateLimited(key)
		} else {
			p.queue.Forget(key)
			if requeueAfter > 0 {
				p.queue.AddAfter(key, requeueAfter)
			}
		}
		p.queue.Done(key)
	}
}

func (p *provider) reconcileKey(ctx context.Context, key string) (time.Duration, error) {
	obj, exists, err := p.indexer.GetByKey(key)
	if err != nil {
		return 0, err
	}
	if !exists {
		return 0, nil // deleted and finalizer already released
	}
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return 0, nil
	}
	if !p.owns(u) {
		return 0, nil
	}
	return p.reconcile(ctx, u.DeepCopy())
}

func (p *provider) reconcile(ctx context.Context, u *unstructured.Unstructured) (time.Duration, error) {
	ns, name := u.GetNamespace(), u.GetName()
	ann := u.GetAnnotations()
	if ann == nil {
		ann = map[string]string{}
	}

	// Deletion: terminate the MicroVM, then drop the finalizer.
	if u.GetDeletionTimestamp() != nil {
		if hasFinalizer(u) {
			if id := ann[annMicrovmID]; id != "" {
				log.Printf("provider: terminating microvm %s for deleted sandbox %s/%s", id, ns, name)
				if err := p.awsTerminate(ctx, id); err != nil {
					return 0, fmt.Errorf("terminate: %w", err)
				}
			}
			if s := ann[annSessSecret]; s != "" {
				if err := p.awsDeleteSecret(ctx, s); err != nil {
					log.Printf("provider: delete session secret %s: %v", s, err)
				}
			}
			return 0, p.removeFinalizer(ctx, u)
		}
		return 0, nil
	}

	if !hasFinalizer(u) {
		if err := p.addFinalizer(ctx, u); err != nil {
			return 0, err
		}
	}

	desired := operatingMode(u) // "Running" | "Suspended"

	// Already provisioned: reconcile live state against desired operating mode.
	if id := ann[annMicrovmID]; id != "" {
		state, ep, err := p.awsGet(ctx, id)
		if err != nil {
			return 0, fmt.Errorf("get microvm: %w", err)
		}
		switch {
		case desired == "Suspended" && state == "RUNNING":
			log.Printf("provider: suspending microvm %s (sandbox %s/%s operatingMode=Suspended)", id, ns, name)
			if err := p.awsSuspend(ctx, id); err != nil {
				return 0, fmt.Errorf("suspend: %w", err)
			}
			state = "SUSPENDING"
		case desired == "Running" && state == "SUSPENDED":
			log.Printf("provider: resuming microvm %s (sandbox %s/%s operatingMode=Running)", id, ns, name)
			if err := p.awsResume(ctx, id); err != nil {
				return 0, fmt.Errorf("resume: %w", err)
			}
			state = "RESUMING"
		}
		if err := p.writeStatus(ctx, u, id, ep, state); err != nil {
			return 0, err
		}
		return requeueFor(state), nil
	}

	// Not yet provisioned: map the PodSpec and provision (warm adopt or cold run).
	spec, err := mapSandbox(ctx, p, u, p.defaultImg)
	if err != nil {
		return 0, p.writeCondition(ctx, u, "False", "MappingError", err.Error())
	}
	if spec.ports != "" {
		_ = p.patchAnnotations(ctx, u, map[string]string{annPorts: spec.ports})
	}

	// Mirror k8s-Secret-derived env into a per-Sandbox ASM secret so the values
	// never travel in the (plaintext, CloudTrail-logged) runHookPayload. The
	// MicroVM fetches them from ASM under its execution role via secret_env.
	if len(spec.secretResolved) > 0 {
		secretName := sessionSecretName(ns, name)
		if err := p.awsPutSecret(ctx, secretName, spec.secretResolved); err != nil {
			return 0, p.writeCondition(ctx, u, "False", "SecretMirrorError", err.Error())
		}
		spec.secretEnv = append(spec.secretEnv, secretName)
		if err := p.patchAnnotations(ctx, u, map[string]string{annSessSecret: secretName}); err != nil {
			return 0, err
		}
		log.Printf("provider: mirrored %d k8s-secret env var(s) for %s/%s -> ASM %s", len(spec.secretResolved), ns, name, secretName)
	}

	roleArn := p.execRole(ctx, u)
	// Secrets Manager env is fetched inside the MicroVM under its execution role.
	// With no resolvable role the VM has no AWS identity, so the fetch silently
	// yields nothing — warn loudly rather than let secrets vanish quietly.
	if roleArn == "" && len(spec.secretEnv) > 0 {
		log.Printf("provider: WARNING %s/%s requests secret-env (%v) but no execution role resolved "+
			"(set spec.podTemplate.spec.serviceAccountName or the %s annotation); Secrets Manager env will be empty",
			ns, name, spec.secretEnv, annRole)
	}
	id, ep, launch, err := p.provision(ctx, u, spec, roleArn)
	if err != nil {
		return 0, p.writeCondition(ctx, u, "False", "RunMicrovmError", err.Error())
	}
	if err := p.patchAnnotations(ctx, u, map[string]string{
		annMicrovmID: id, annEndpoint: ep, annState: "PENDING", annLaunchType: launch,
	}); err != nil {
		return 0, err
	}
	log.Printf("provider: sandbox %s/%s -> microvm %s (%s) image=%s memMiB=%d", ns, name, id, launch, spec.image, spec.memMiB)
	if err := p.writeCondition(ctx, u, "False", "Provisioning", "MicroVM launching: "+id); err != nil {
		return 0, err
	}
	return 3 * time.Second, nil
}

// provision returns a MicroVM for the sandbox, adopting from the warm pool when
// eligible (template-identical: no per-claim env/secret/command), else cold.
func (p *provider) provision(ctx context.Context, u *unstructured.Unstructured, s microvmSpec, role string) (id, ep, launch string, err error) {
	if p.warmSize > 0 && s.warmEligible(p.warmImage) {
		if vm, ok := p.popWarm(); ok {
			go p.ensureWarmPool(context.Background()) // replenish
			return vm.ID, vm.Endpoint, "warm", nil
		}
	}
	id, ep, err = p.awsRun(ctx, s, role)
	return id, ep, "cold", err
}

// ---- PodSpec -> MicroVM mapping --------------------------------------------

type microvmSpec struct {
	image      string
	memMiB     int
	env        map[string]string // plaintext env (from PodSpec values/configmaps)
	secretEnv  []string          // explicit ASM secret IDs (secret-env annotation)
	secretResolved map[string]string // env resolved from k8s Secrets -> mirrored to ASM (never in payload)
	runCommand []string
	cwd        string
	ports      string

	// run-time options resolved from the Sandbox spec/annotations
	ingressConn    string // ingress connector ARN
	egressConn     string // egress connector ARN (managed INTERNET_EGRESS or a VPC connector)
	idlePolicy     string // idle-policy JSON
	maxDurationSec int    // from spec.lifecycle.shutdownTime; 0 = unset
}

// warmEligible reports whether this spec matches the warm-pool template
// (default image, no per-claim config) so a pre-run VM can be adopted as-is.
func (s microvmSpec) warmEligible(warmImage string) bool {
	return s.image == warmImage && len(s.env) == 0 && len(s.secretEnv) == 0 &&
		len(s.secretResolved) == 0 && len(s.runCommand) == 0 && s.cwd == ""
}

var validBaselinesMiB = []int{512, 1024, 2048, 4096, 8192}

func mapSandbox(ctx context.Context, p *provider, u *unstructured.Unstructured, defaultImage string) (microvmSpec, error) {
	ms := microvmSpec{image: defaultImage, memMiB: 2048, env: map[string]string{}, secretResolved: map[string]string{}}
	ns := u.GetNamespace()
	ann := u.GetAnnotations()
	if v := ann[annImage]; v != "" {
		ms.image = v
	}
	for _, s := range splitCSV(ann[annSecretEnv]) {
		ms.secretEnv = append(ms.secretEnv, s)
	}

	// Networking + lifecycle run options.
	ms.ingressConn = p.connectorARN(ann[annIngress], p.ingressConn)
	ms.egressConn = p.connectorARN(ann[annEgress], p.egressConn)
	ms.idlePolicy = ann[annIdlePolicy] // empty -> awsRun default
	if v := ann[annMaxDur]; v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			ms.maxDurationSec = n
		}
	} else if st, ok, _ := unstructured.NestedString(u.Object, "spec", "lifecycle", "shutdownTime"); ok && st != "" {
		// Map the Sandbox's absolute shutdownTime to a relative MicroVM max duration.
		if t, err := time.Parse(time.RFC3339, st); err == nil {
			secs := int(time.Until(t).Seconds())
			if secs > 0 {
				if secs < 60 {
					secs = 60
				}
				if secs > 28800 {
					secs = 28800
				}
				ms.maxDurationSec = secs
			}
		}
	}

	// Arch guard: Lambda MicroVMs are Graviton.
	if nodeSel, ok, _ := unstructured.NestedStringMap(u.Object, "spec", "podTemplate", "spec", "nodeSelector"); ok {
		if a := nodeSel["kubernetes.io/arch"]; a != "" && a != "arm64" {
			return ms, fmt.Errorf("nodeSelector kubernetes.io/arch=%q unsupported; Lambda MicroVMs are arm64/Graviton", a)
		}
	}

	containers, ok, _ := unstructured.NestedSlice(u.Object, "spec", "podTemplate", "spec", "containers")
	if !ok || len(containers) == 0 {
		return ms, fmt.Errorf("spec.podTemplate.spec.containers is empty")
	}
	if len(containers) > 1 {
		return ms, fmt.Errorf("multiple containers not supported by Lambda MicroVMs (got %d)", len(containers))
	}
	c, _ := containers[0].(map[string]interface{})

	// image: a MicroVM image ARN in the container image field takes precedence.
	if img, _, _ := unstructured.NestedString(c, "image"); strings.Contains(img, ":microvm-image:") {
		ms.image = img
	}

	// command + args -> startup command (argv).
	cmd, _, _ := unstructured.NestedStringSlice(c, "command")
	args, _, _ := unstructured.NestedStringSlice(c, "args")
	ms.runCommand = append(append([]string{}, cmd...), args...)

	// workingDir -> cwd.
	if wd, _, _ := unstructured.NestedString(c, "workingDir"); wd != "" {
		ms.cwd = wd
	}

	// ports -> informational annotation (edge routes any port via X-aws-proxy-port).
	if ports, ok, _ := unstructured.NestedSlice(c, "ports"); ok {
		var ps []string
		for _, pt := range ports {
			pm, _ := pt.(map[string]interface{})
			if cp, ok, _ := unstructured.NestedInt64(pm, "containerPort"); ok {
				ps = append(ps, strconv.FormatInt(cp, 10))
			}
		}
		ms.ports = strings.Join(ps, ",")
	}

	// envFrom: whole Secret/ConfigMap -> env (the "k8s secret" path).
	if envFrom, ok, _ := unstructured.NestedSlice(c, "envFrom"); ok {
		for _, ef := range envFrom {
			efm, _ := ef.(map[string]interface{})
			prefix, _, _ := unstructured.NestedString(efm, "prefix")
			if n, ok, _ := unstructured.NestedString(efm, "secretRef", "name"); ok {
				kv, err := p.readK8s(ctx, secretGVR, ns, n)
				if err != nil {
					return ms, fmt.Errorf("envFrom secret %q: %w", n, err)
				}
				for k, v := range kv {
					ms.secretResolved[prefix+k] = v // -> mirrored to ASM, not payload
				}
			}
			if n, ok, _ := unstructured.NestedString(efm, "configMapRef", "name"); ok {
				kv, err := p.readK8s(ctx, configMapGVR, ns, n)
				if err != nil {
					return ms, fmt.Errorf("envFrom configmap %q: %w", n, err)
				}
				for k, v := range kv {
					ms.env[prefix+k] = v
				}
			}
		}
	}

	// env: name/value and valueFrom secretKeyRef/configMapKeyRef.
	if envs, ok, _ := unstructured.NestedSlice(c, "env"); ok {
		for _, e := range envs {
			em, _ := e.(map[string]interface{})
			n, _, _ := unstructured.NestedString(em, "name")
			if n == "" {
				continue
			}
			if v, hasV, _ := unstructured.NestedString(em, "value"); hasV {
				ms.env[n] = v
				continue
			}
			if sn, ok, _ := unstructured.NestedString(em, "valueFrom", "secretKeyRef", "name"); ok {
				key, _, _ := unstructured.NestedString(em, "valueFrom", "secretKeyRef", "key")
				kv, err := p.readK8s(ctx, secretGVR, ns, sn)
				if err != nil {
					return ms, fmt.Errorf("env %q secretKeyRef %q: %w", n, sn, err)
				}
				ms.secretResolved[n] = kv[key] // -> mirrored to ASM, not payload
				continue
			}
			if cn, ok, _ := unstructured.NestedString(em, "valueFrom", "configMapKeyRef", "name"); ok {
				key, _, _ := unstructured.NestedString(em, "valueFrom", "configMapKeyRef", "key")
				kv, err := p.readK8s(ctx, configMapGVR, ns, cn)
				if err != nil {
					return ms, fmt.Errorf("env %q configMapKeyRef %q: %w", n, cn, err)
				}
				ms.env[n] = kv[key]
				continue
			}
			return ms, fmt.Errorf("env %q uses an unsupported valueFrom (only secretKeyRef/configMapKeyRef)", n)
		}
	}

	// memory request -> nearest valid MicroVM baseline.
	if memStr, ok, _ := unstructured.NestedString(c, "resources", "requests", "memory"); ok && memStr != "" {
		q, err := resource.ParseQuantity(memStr)
		if err != nil {
			return ms, fmt.Errorf("parse memory %q: %w", memStr, err)
		}
		ms.memMiB = snapBaseline(int(q.Value() / (1024 * 1024)))
	}
	return ms, nil
}

// readK8s reads a Secret (base64-decoded) or ConfigMap data map.
func (p *provider) readK8s(ctx context.Context, gvr schema.GroupVersionResource, ns, name string) (map[string]string, error) {
	u, err := p.dyn.Resource(gvr).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	data, _, _ := unstructured.NestedMap(u.Object, "data")
	out := map[string]string{}
	for k, v := range data {
		s, _ := v.(string)
		if gvr == secretGVR {
			if dec, err := base64.StdEncoding.DecodeString(s); err == nil {
				out[k] = string(dec)
				continue
			}
		}
		out[k] = s
	}
	return out, nil
}

func snapBaseline(mib int) int {
	for _, b := range validBaselinesMiB {
		if mib <= b {
			return b
		}
	}
	return validBaselinesMiB[len(validBaselinesMiB)-1]
}

func operatingMode(u *unstructured.Unstructured) string {
	m, _, _ := unstructured.NestedString(u.Object, "spec", "operatingMode")
	if m == "" {
		return "Running"
	}
	return m
}

// requeueFor returns a short requeue delay while a MicroVM is mid-transition.
func requeueFor(state string) time.Duration {
	switch state {
	case "RUNNING", "SUSPENDED", "TERMINATED", "FAILED":
		return 0
	default: // PENDING, CREATING, SUSPENDING, RESUMING, ...
		return 3 * time.Second
	}
}

// ---- warm pool -------------------------------------------------------------

func (p *provider) ensureWarmPool(ctx context.Context) {
	for {
		p.warmMu.Lock()
		n := len(p.warmFree)
		p.warmMu.Unlock()
		if n >= p.warmSize {
			return
		}
		spec := microvmSpec{image: p.warmImage, memMiB: 2048, env: map[string]string{}}
		id, ep, err := p.awsRun(ctx, spec, p.warmRole)
		if err != nil {
			log.Printf("provider: warm pool run failed: %v", err)
			return
		}
		// Wait until RUNNING so the pool holds ready VMs (instant adoption).
		for i := 0; i < 30; i++ {
			st, _, err := p.awsGet(ctx, id)
			if err == nil && st == "RUNNING" {
				break
			}
			time.Sleep(3 * time.Second)
		}
		p.warmMu.Lock()
		p.warmFree = append(p.warmFree, poolVM{ID: id, Endpoint: ep})
		free := len(p.warmFree)
		p.warmMu.Unlock()
		p.saveWarmPool(ctx)
		log.Printf("provider: warm pool +1 (%s), free=%d/%d", id, free, p.warmSize)
	}
}

func (p *provider) popWarm() (poolVM, bool) {
	p.warmMu.Lock()
	if len(p.warmFree) == 0 {
		p.warmMu.Unlock()
		return poolVM{}, false
	}
	vm := p.warmFree[len(p.warmFree)-1]
	p.warmFree = p.warmFree[:len(p.warmFree)-1]
	p.warmMu.Unlock()
	p.saveWarmPool(context.Background())
	return vm, true
}

// warm pool state persists in a ConfigMap so a provider restart doesn't leak
// (or forget) pre-run VMs.
var warmPoolCMGVR = configMapGVR

func (p *provider) saveWarmPool(ctx context.Context) {
	p.warmMu.Lock()
	data, _ := json.Marshal(p.warmFree)
	p.warmMu.Unlock()
	cm := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": map[string]interface{}{"name": warmPoolConfigMap, "namespace": "default"},
		"data":     map[string]interface{}{"free": string(data)},
	}}
	patch, _ := json.Marshal(cm.Object)
	_, err := p.dyn.Resource(warmPoolCMGVR).Namespace("default").
		Patch(ctx, warmPoolConfigMap, types.ApplyPatchType, patch, metav1.PatchOptions{FieldManager: "microvm-provider", Force: boolPtr(true)})
	if err != nil {
		log.Printf("provider: save warm pool: %v", err)
	}
}

func (p *provider) loadWarmPool(ctx context.Context) {
	cm, err := p.dyn.Resource(warmPoolCMGVR).Namespace("default").Get(ctx, warmPoolConfigMap, metav1.GetOptions{})
	if err != nil {
		return // none yet
	}
	raw, _, _ := unstructured.NestedString(cm.Object, "data", "free")
	var saved []poolVM
	if json.Unmarshal([]byte(raw), &saved) != nil {
		return
	}
	var live []poolVM
	for _, vm := range saved {
		if st, _, err := p.awsGet(ctx, vm.ID); err == nil && st == "RUNNING" {
			live = append(live, vm)
		}
	}
	p.warmMu.Lock()
	p.warmFree = live
	p.warmMu.Unlock()
	log.Printf("provider: recovered %d warm VM(s) from ConfigMap (of %d saved)", len(live), len(saved))
}

func boolPtr(b bool) *bool { return &b }

func (p *provider) drainWarmPool() {
	p.warmMu.Lock()
	free := p.warmFree
	p.warmFree = nil
	p.warmMu.Unlock()
	for _, vm := range free {
		log.Printf("provider: terminating warm pool vm %s", vm.ID)
		_ = p.awsTerminate(context.Background(), vm.ID)
	}
	p.saveWarmPool(context.Background()) // clear persisted pool
}

// ---- aws lambda-microvms CLI shell-outs ------------------------------------

// connectorARN resolves a connector annotation value: empty -> default; a full
// "arn:..." -> used as-is (e.g. a customer VPC egress connector); a short name
// (ALL_INGRESS / NO_INGRESS / SHELL_INGRESS / INTERNET_EGRESS) -> the managed ARN.
func (p *provider) connectorARN(val, def string) string {
	if val == "" {
		return def
	}
	if strings.HasPrefix(val, "arn:") {
		return val
	}
	return fmt.Sprintf("arn:aws:lambda:%s:aws:network-connector:aws-network-connector:%s", p.region, val)
}

func (p *provider) awsRun(ctx context.Context, s microvmSpec, roleArn string) (id, endpoint string, err error) {
	payload := map[string]interface{}{"reset_workspace": true}
	if len(s.env) > 0 {
		payload["env"] = s.env
	}
	if len(s.secretEnv) > 0 {
		payload["secret_env"] = s.secretEnv
	}
	if len(s.runCommand) > 0 {
		payload["run_command"] = s.runCommand
	}
	if s.cwd != "" {
		payload["cwd"] = s.cwd
	}
	pj, _ := json.Marshal(payload)
	idle := s.idlePolicy
	if idle == "" {
		idle = `{"autoResumeEnabled":true,"maxIdleDurationSeconds":900,"suspendedDurationSeconds":600}`
	}
	ingress := s.ingressConn
	if ingress == "" {
		ingress = p.ingressConn
	}
	egress := s.egressConn
	if egress == "" {
		egress = p.egressConn
	}
	args := []string{"lambda-microvms", "run-microvm",
		"--image-identifier", p.imageARN(s.image),
		"--ingress-network-connectors", ingress,
		"--egress-network-connectors", egress,
		"--idle-policy", idle,
		"--run-hook-payload", string(pj),
	}
	if s.maxDurationSec > 0 {
		args = append(args, "--maximum-duration-in-seconds", strconv.Itoa(s.maxDurationSec))
	}
	if roleArn != "" {
		args = append(args, "--execution-role-arn", roleArn)
	}
	out, err := p.aws(ctx, args...)
	if err != nil {
		return "", "", err
	}
	var r struct {
		MicrovmID string `json:"microvmId"`
		Endpoint  string `json:"endpoint"`
	}
	if err := json.Unmarshal(out, &r); err != nil {
		return "", "", fmt.Errorf("parse run-microvm: %w", err)
	}
	return r.MicrovmID, r.Endpoint, nil
}

func (p *provider) imageARN(image string) string {
	if strings.HasPrefix(image, "arn:") {
		return image
	}
	return fmt.Sprintf("arn:aws:lambda:%s:%s:microvm-image:%s", p.region, accountFromEnv(), image)
}

func (p *provider) awsGet(ctx context.Context, id string) (state, endpoint string, err error) {
	out, err := p.aws(ctx, "lambda-microvms", "get-microvm", "--microvm-identifier", id)
	if err != nil {
		return "", "", err
	}
	var r struct {
		State    string `json:"state"`
		Endpoint string `json:"endpoint"`
	}
	if err := json.Unmarshal(out, &r); err != nil {
		return "", "", err
	}
	return r.State, r.Endpoint, nil
}

func (p *provider) awsSuspend(ctx context.Context, id string) error {
	_, err := p.aws(ctx, "lambda-microvms", "suspend-microvm", "--microvm-identifier", id)
	return err
}

func (p *provider) awsResume(ctx context.Context, id string) error {
	_, err := p.aws(ctx, "lambda-microvms", "resume-microvm", "--microvm-identifier", id)
	return err
}

func (p *provider) awsTerminate(ctx context.Context, id string) error {
	_, err := p.aws(ctx, "lambda-microvms", "terminate-microvm", "--microvm-identifier", id)
	return err
}

// execRole resolves the MicroVM execution role from the Sandbox, mirroring how
// a Pod gets its AWS identity in EKS:
//  1. explicit override annotation microvm.agents.x-k8s.io/execution-role-arn;
//  2. the role bound to spec.podTemplate.spec.serviceAccountName — via the SA's
//     IRSA annotation eks.amazonaws.com/role-arn, or its EKS Pod Identity
//     association (when --cluster is set).
func (p *provider) execRole(ctx context.Context, u *unstructured.Unstructured) string {
	if r := u.GetAnnotations()[annRole]; r != "" {
		return r
	}
	sa, _, _ := unstructured.NestedString(u.Object, "spec", "podTemplate", "spec", "serviceAccountName")
	if sa == "" {
		return ""
	}
	ns := u.GetNamespace()
	if saObj, err := p.dyn.Resource(saGVR).Namespace(ns).Get(ctx, sa, metav1.GetOptions{}); err == nil {
		if r := saObj.GetAnnotations()["eks.amazonaws.com/role-arn"]; r != "" {
			return r // IRSA convention
		}
	}
	if p.cluster != "" {
		if r := p.podIdentityRole(ctx, ns, sa); r != "" {
			return r
		}
	}
	log.Printf("provider: %s/%s serviceAccountName=%q has no resolvable IAM role (no IRSA annotation or Pod Identity association)", ns, u.GetName(), sa)
	return ""
}

// podIdentityRole resolves a ServiceAccount's EKS Pod Identity association to its
// IAM role ARN (list to find the association, describe to read the role).
func (p *provider) podIdentityRole(ctx context.Context, ns, sa string) string {
	out, err := p.aws(ctx, "eks", "list-pod-identity-associations", "--cluster-name", p.cluster, "--namespace", ns, "--service-account", sa)
	if err != nil {
		return ""
	}
	var list struct {
		Associations []struct {
			AssociationID string `json:"associationId"`
		} `json:"associations"`
	}
	if json.Unmarshal(out, &list) != nil || len(list.Associations) == 0 {
		return ""
	}
	out, err = p.aws(ctx, "eks", "describe-pod-identity-association", "--cluster-name", p.cluster, "--association-id", list.Associations[0].AssociationID)
	if err != nil {
		return ""
	}
	var desc struct {
		Association struct {
			RoleArn string `json:"roleArn"`
		} `json:"association"`
	}
	if json.Unmarshal(out, &desc) != nil {
		return ""
	}
	return desc.Association.RoleArn
}

func sessionSecretName(ns, name string) string {
	return fmt.Sprintf("microvm-shim/session/%s/%s", ns, name)
}

// awsPutSecret creates or updates an ASM secret holding the given env map.
func (p *provider) awsPutSecret(ctx context.Context, name string, kv map[string]string) error {
	j, _ := json.Marshal(kv)
	if _, err := p.aws(ctx, "secretsmanager", "create-secret", "--name", name, "--secret-string", string(j)); err != nil {
		if _, e2 := p.aws(ctx, "secretsmanager", "put-secret-value", "--secret-id", name, "--secret-string", string(j)); e2 != nil {
			return fmt.Errorf("create(%v) and update(%v) failed", err, e2)
		}
	}
	return nil
}

func (p *provider) awsDeleteSecret(ctx context.Context, name string) error {
	_, err := p.aws(ctx, "secretsmanager", "delete-secret", "--secret-id", name, "--force-delete-without-recovery")
	return err
}

func (p *provider) aws(ctx context.Context, args ...string) ([]byte, error) {
	c := exec.CommandContext(ctx, "aws", append(args, "--region", p.region, "--output", "json")...)
	var stdout, stderr bytes.Buffer
	c.Stdout, c.Stderr = &stdout, &stderr
	if err := c.Run(); err != nil {
		return nil, fmt.Errorf("aws %s: %v: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), nil
}

// ---- Sandbox writes --------------------------------------------------------

func (p *provider) writeStatus(ctx context.Context, u *unstructured.Unstructured, id, ep, state string) error {
	if err := p.patchAnnotations(ctx, u, map[string]string{annState: state, annEndpoint: ep}); err != nil {
		return err
	}
	switch state {
	case "RUNNING":
		return p.writeStatusFields(ctx, u, ep, "True", "DependenciesReady", "MicroVM "+id+" running at "+ep)
	case "SUSPENDED":
		return p.writeStatusFields(ctx, u, ep, "False", "SandboxSuspended", "MicroVM "+id+" suspended")
	case "PENDING", "RESUMING":
		return p.writeStatusFields(ctx, u, ep, "False", "Provisioning", "MicroVM "+id+" state="+state)
	default:
		return p.writeStatusFields(ctx, u, ep, "False", "MicrovmState"+state, "MicroVM "+id+" state="+state)
	}
}

// writeStatusFields sets status.serviceFQDN (the routing handle) + Ready condition.
func (p *provider) writeStatusFields(ctx context.Context, u *unstructured.Unstructured, fqdn, status, reason, msg string) error {
	cond := map[string]interface{}{
		"type":               "Ready",
		"status":             status,
		"reason":             reason,
		"message":            msg,
		"lastTransitionTime": time.Now().UTC().Format(time.RFC3339),
		"observedGeneration": u.GetGeneration(),
	}
	st := map[string]interface{}{"conditions": []interface{}{cond}}
	if fqdn != "" {
		// serviceFQDN is the MicroVM's routable address AND the signal the
		// (forked) SandboxClaim controller uses to treat a non-Pod-backed
		// sandbox as network-ready for adoption (no podIPs placeholder needed).
		st["serviceFQDN"] = fqdn
	}
	patch, _ := json.Marshal(map[string]interface{}{"status": st})
	_, err := p.dyn.Resource(sandboxGVR).Namespace(u.GetNamespace()).
		Patch(ctx, u.GetName(), types.MergePatchType, patch, metav1.PatchOptions{FieldManager: "microvm-provider"}, "status")
	return err
}

func (p *provider) writeCondition(ctx context.Context, u *unstructured.Unstructured, status, reason, msg string) error {
	return p.writeStatusFields(ctx, u, "", status, reason, msg)
}

func (p *provider) patchAnnotations(ctx context.Context, u *unstructured.Unstructured, add map[string]string) error {
	patch, _ := json.Marshal(map[string]interface{}{"metadata": map[string]interface{}{"annotations": toIface(add)}})
	_, err := p.dyn.Resource(sandboxGVR).Namespace(u.GetNamespace()).
		Patch(ctx, u.GetName(), types.MergePatchType, patch, metav1.PatchOptions{FieldManager: "microvm-provider"})
	return err
}

// ---- finalizer helpers -----------------------------------------------------

func hasFinalizer(u *unstructured.Unstructured) bool {
	for _, f := range u.GetFinalizers() {
		if f == finalizer {
			return true
		}
	}
	return false
}

func (p *provider) addFinalizer(ctx context.Context, u *unstructured.Unstructured) error {
	return p.patchFinalizers(ctx, u, append(u.GetFinalizers(), finalizer))
}

func (p *provider) removeFinalizer(ctx context.Context, u *unstructured.Unstructured) error {
	var fins []string
	for _, f := range u.GetFinalizers() {
		if f != finalizer {
			fins = append(fins, f)
		}
	}
	return p.patchFinalizers(ctx, u, fins)
}

func (p *provider) patchFinalizers(ctx context.Context, u *unstructured.Unstructured, fins []string) error {
	patch, _ := json.Marshal(map[string]interface{}{"metadata": map[string]interface{}{"finalizers": fins}})
	_, err := p.dyn.Resource(sandboxGVR).Namespace(u.GetNamespace()).
		Patch(ctx, u.GetName(), types.MergePatchType, patch, metav1.PatchOptions{FieldManager: "microvm-provider"})
	return err
}

// ---- misc ------------------------------------------------------------------

func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func toIface(m map[string]string) map[string]interface{} {
	out := map[string]interface{}{}
	for k, v := range m {
		out[k] = v
	}
	return out
}

func loadKubeConfig(path string) (*rest.Config, error) {
	if path == "" {
		return rest.InClusterConfig()
	}
	return clientcmd.BuildConfigFromFlags("", path)
}

func defaultKubeconfig() string {
	if v := os.Getenv("KUBECONFIG"); v != "" {
		return v
	}
	if h, err := os.UserHomeDir(); err == nil {
		return filepath.Join(h, ".kube", "config")
	}
	return ""
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func accountFromEnv() string { return env("AWS_ACCOUNT_ID", "") }
