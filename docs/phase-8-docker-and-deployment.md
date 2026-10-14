# Phase 8 — Docker and Deployment

## Goal

Package the cluster so it can run somewhere other than a developer's own machine: a multi-stage Docker image, a local 3-node Docker Compose cluster to prove the image is correct, and — since this is a personal showcase project — an actual deployment to a real host. **Both parts are complete and verified**: Docker Compose locally, and a real 3-pod Kubernetes (k3s) StatefulSet on a Google Cloud VM, reachable and fully functional from outside the cluster.

---

## Part 1: Docker Compose (local 3-node cluster) — Complete

### Image design

`deployments/docker/Dockerfile` is a two-stage build: `golang:1.23-alpine` compiles static binaries (`CGO_ENABLED=0`, `-trimpath -ldflags="-s -w"`) for both `cmd/server` and `cmd/client`, then only the binaries are copied into `gcr.io/distroless/static-debian12:nonroot` — no shell, no package manager, nothing beyond what `kvstored` needs to run. `docker-compose.yml` defines `node1`/`node2`/`node3`, each building from that Dockerfile, on one bridge network (`kvstore`), with its own named volume mounted at `/data` and its own host port (`8001`/`8002`/`8003`) mapped to the container's `8000`.

### Bugs found and fixed

**Volume permission denied.** `distroless:nonroot` runs as a fixed non-root UID (65532), and a Docker named volume is seeded from whatever's already at the mountpoint in the image *the first time it's created* — it isn't chowned at container-start, and there's no shell in the final image to do that fixup even if we wanted to. The fix: `RUN mkdir -p /data && chown -R 65532:65532 /data` in the builder stage, then `COPY --from=builder --chown=65532:65532 /data /data` into the final stage, so the volume is seeded with correct ownership on first mount.

**Doubled URL scheme in leader redirects.** `cmd/server/main.go` builds each peer's redirect URL as `"http://" + addr` from `--client-peers`. The compose file's `--client-peers` values (copied too literally from `build-guide.md`'s illustrative snippet) already had `http://` embedded, producing a redirect header like `http://http//node3:8000/kv/hello` and a client-side DNS failure trying to resolve the literal host `http`. Fixed by using bare `host:port` for every `--client-peers` entry, matching `--peers`' format and the existing convention in `scripts/cluster.ps1`.

**Host machine can't resolve compose service hostnames.** Even after the above fix, running `kvctl` from the Windows host against a `node3`-style hostname fails — Docker's embedded DNS only resolves service names *between containers on the same user-defined network*, not from the host OS. Not a bug; verification was done from inside a container instead (`docker compose exec node1 kvctl -addr http://node1:8000 ...`), which is also a more realistic test of the redirect path than testing from the host would have been.

### Verification

Full round-trip confirmed with independent evidence: a `put` issued against `node1`, a `get` for the same key issued against the *different* node `node2`, and an independent `curl` against `/cluster/status` on a host-mapped port — confirming replication and leader redirection both work correctly across real Docker networking, not just in-process.

---

## Part 2: Kubernetes (k3s) deployment — Complete

### Why Kubernetes, why k3s

A single VM running 3 pods co-located by a scheduler is architecturally weaker as a Raft showcase than 3 genuinely separate hosts. The original plan considered running 3 local VirtualBox VMs for a "real" multi-node cluster, but landed on a single cloud VM running k3s instead, once the actual next step (a k3s **StatefulSet** with 3 replicas) turned out to give each Raft node its own pod, its own PersistentVolumeClaim, and stable per-pod network identity via a headless Service — enough separation to demonstrate real Kubernetes-orchestrated failover without the overhead of managing 3 separate operating systems by hand.

### Infrastructure: Oracle Cloud, the hard way

The original plan was Oracle Cloud's Always Free `VM.Standard.A1.Flex` (Ampere/ARM, up to 4 OCPU/24GB, genuinely free indefinitely). In practice:

- **A1.Flex capacity was exhausted** in the chosen region/availability domain (`Out of capacity for shape VM.Standard.A1.Flex...`) — a widely-reported, chronic problem with OCI's free ARM tier, not something specific to this account.
- After burning time on this and on OCI's console UX (public-IP assignment is non-obvious when a subnet is private vs. public; a from-scratch VCN wizard doesn't always make this clear), the decision was made to **accept using a paid/trial-credit x86 shape** (draws down the 30-day/$300 free trial rather than the indefinite Always Free tier) rather than keep fighting ARM capacity. Estimated cost if it ran a full month unattended: roughly $18-55/month depending on OCPU count (list pricing: $0.0255/OCPU-hour + $0.0015/GB-hour) — acceptable for the trial-credit window, not something to leave running long-term without switching to Always Free or a flat-rate VPS.
- **OCI has two independent firewall layers**, not one: the cloud-side Security List/NSG, *and* OS-level `iptables` inside the VM itself, which Oracle's Ubuntu images ship with a default-deny policy (`REJECT` after allowing only established connections, ICMP, loopback, and SSH). Both layers need a rule added for every port that must be reachable — a rule in one without the other looks identical to "nothing works" with no obvious cause.

Final instance: `kv-store-k3`, Ubuntu 20.04.6 LTS, x86_64, ~15GB RAM, single node acting as both k3s server and the only place workloads run.

### Installing k3s on Oracle: the cgroup v1/v2 trap

`curl -sfL https://get.k3s.io | sh -` installed cleanly but the service crash-looped (`systemctl status` stuck in `activating (auto-restart)`, `kubectl get nodes` couldn't even reach `127.0.0.1:6443`). `journalctl -u k3s` showed the real cause once the API server had already started successfully:

```
Error: failed to validate kubelet configuration, error: kubelet is configured to not run on a host using cgroup v1. cgroup v1 support is unsupported and will be removed in a future release
```

Ubuntu 20.04 defaults to the legacy cgroup v1 hierarchy; the k3s release that installed (`v1.36.4+k3s1`) ships a kubelet that hard-refuses to start on it. The fix is switching the host to the unified cgroup v2 hierarchy via a kernel boot parameter (`systemd.unified_cgroup_hierarchy=1` in GRUB) and rebooting — but the first attempt silently didn't take effect, because **Ubuntu's cloud images ship an extra file, `/etc/default/grub.d/50-cloudimg-settings.cfg`, sourced *after* `/etc/default/grub`**, which re-sets `GRUB_CMDLINE_LINUX_DEFAULT` and clobbers an edit made only to the main file. Fixed by patching both files, then verifying the parameter actually landed in the generated `/boot/grub/grub.cfg` *before* rebooting a second time, to avoid a blind repeat of the same failure.

### Platform migration: Oracle → Google Cloud

The Oracle x86 trial-credit instance was later **terminated after running out of trial credit** — confirming the earlier estimate that an x86 Flex shape isn't viable to run unattended without a real billing plan. Rather than keep fighting Oracle (capacity, console UX, two-layer firewall), the deployment moved to **Google Cloud**, with the whole VM + firewall + k3s setup re-run from scratch end-to-end via the `gcloud` CLI directly, rather than clicking through a console:

- A **new, dedicated GCP project** (`kvstore-k3s-88166`) was created specifically for this, rather than reusing the account's existing coursework-related projects — keeps billing and quota fully isolated from unrelated work.
- VM: `kvstore-k3s`, zone `us-central1-a`, machine type `e2-medium` (2 vCPU/4GB — comfortable headroom for k3s + 3 pods, learned from how tight 1GB free-tier shapes are on both OCI and GCP), image **Ubuntu 22.04 LTS** — chosen specifically to default to **cgroup v2**, sidestepping the entire GRUB saga from the Oracle attempt.
- A single `gcloud compute firewall-rules create` opened TCP `8081-8083` (the client ports — see the fix below) for instances tagged `kvstore`; GCP's default network already ships a `default-allow-ssh` rule, so no OS-level second firewall layer to fight this time (GCP, unlike Oracle, doesn't put a default-deny `iptables` policy inside its stock Ubuntu images).
- `curl -sfL https://get.k3s.io | sh -` came up `active (running)` and the node reached `Ready` in under 20 seconds — a clean first-try install, validating that the Oracle issues were platform/image-specific, not something wrong with the general approach.
- Two small gotchas hit along the way: `gcloud compute scp`'s Windows backend (`pscp`) doesn't expand `~` in a remote destination path (`~/file` fails; a bare relative `file` or an absolute path both work), and `gcloud compute ssh ... --command=` runs a **non-interactive** shell, which does not source `~/.bashrc` — so `KUBECONFIG` has to be exported explicitly in every such command rather than relying on an earlier interactive-session export.

### Publishing the image

Built and pushed from the local Windows machine (its Docker Desktop is also x86_64, matching the VM — no cross-arch build needed):

```powershell
docker login ghcr.io -u DhruvGandhi31
docker build -t ghcr.io/dhruvgandhi31/distributed-kv-store:latest -f deployments/docker/Dockerfile .
docker push ghcr.io/dhruvgandhi31/distributed-kv-store:latest
```

The GHCR package was switched to **public** visibility (nothing sensitive is baked into the image) specifically to avoid needing `imagePullSecrets` in the Kubernetes manifests — a reasonable simplification for a showcase project, not something to default to for anything with real secrets in the image. This image needed no rebuild for the later GCP migration — same `linux/amd64` architecture on both Oracle's and GCP's x86 VMs, so the existing GHCR tag was pulled as-is.

### Kubernetes manifest design (`deployments/k8s/kvstore.yaml`)

- **`Namespace kvstore`** — keeps everything grouped.
- **Headless Service `kvstore-headless`** (`clusterIP: None`) — gives each pod a stable DNS name (`kvstore-0.kvstore-headless`, etc.) that survives pod restarts. This is what Raft's `--peers` list points at internally.
- **StatefulSet `kvstore`**, 3 replicas — guarantees stable pod names/ordinals and gives each pod its own `PersistentVolumeClaim` via `volumeClaimTemplates` (the Kubernetes equivalent of Compose's 3 separate named volumes).
  - All 3 replicas share **one pod template** — a StatefulSet can't vary flags per replica — so `--id` is injected via the Downward API (`$(POD_NAME)`, from `fieldRef: metadata.name`). Kubernetes expands `$(VAR)` references in `args` itself, without a shell, which matters here since the runtime image is shell-less distroless.
  - `securityContext.fsGroup: 65532` — the direct k8s analogue of the Docker named-volume permission bug from Part 1: a freshly-provisioned PVC doesn't inherit ownership from the image, so without this the non-root container can't write to its own mounted `/data`.
- **Client-facing Service** — originally a single `type: LoadBalancer` in front of all 3 pods (k3s bundles its own lightweight ServiceLB/Klipper, which binds a `LoadBalancer` service directly to the node's IP — no cloud load balancer needed for a single-node k3s "cluster").

First deploy succeeded cleanly: all 3 pods reached `Running`, and `kvstore-0`'s log showed a real leader election (`became Leader term=12`) purely from the StatefulSet + headless Service DNS wiring — no manual intervention needed beyond `kubectl apply`.

### Fixed bug — external client redirects

**The original single combined LoadBalancer broke external client redirects.** `--client-peers` (used to build the `X-Leader-URL` redirect header) was initially set to the same internal headless-service DNS names as `--peers` (e.g. `kvstore-1.kvstore-headless:8000`). Those names only resolve *inside* the cluster's own DNS. A client outside the cluster (e.g. `kvctl` running on a laptop) that hit the LoadBalancer and happened to land on a follower pod was handed a redirect URL it could never resolve — the same *class* of bug as the Docker Compose scheme-doubling issue (Part 1), just relocated: a redirect URL is only useful if it's reachable by whoever receives it, and "reachable from inside the cluster" and "reachable from the public internet" are different addresses here.

**Fix:** replaced the single combined LoadBalancer with **3 separate per-pod LoadBalancer Services** (`kvstore-0-client`, `kvstore-1-client`, `kvstore-2-client`), each selecting on the `statefulset.kubernetes.io/pod-name` label Kubernetes sets automatically on every StatefulSet pod, each on a distinct external port (`8081`/`8082`/`8083`). `--client-peers` now advertises the VM's **public IP** with those distinct ports (externally reachable) instead of internal DNS names, while `--peers` (Raft-to-Raft traffic, never leaves the cluster) stays on internal headless DNS. This mirrors exactly how Compose exposed 3 distinct host ports in Part 1 — the same fix, in Kubernetes' vocabulary.

**Verification (from a Windows laptop, outside the cluster entirely):**
- `kvctl -addr http://<PUBLIC_IP>:8081,http://<PUBLIC_IP>:8082,http://<PUBLIC_IP>:8083 put hello gcloud-world` then `get hello` from the same multi-candidate address list — round trip succeeded.
- A stricter test: `put` against **only** `:8082` (`kvstore-1`, a follower) with no fallback candidates, then `get` against **only** `:8083` (`kvstore-2`, a different follower) — both succeeded, proving the leader redirect itself resolves correctly now (not just that round-robin eventually found the leader by luck).

**Resolved:** the VM's public IP was originally a GCP **ephemeral** external IP, which would have changed if the instance was ever stopped and started (not on a plain reboot). Fixed by promoting it to a **reserved static external IP** (`gcloud compute addresses create kvstore-static-ip --addresses=34.60.149.77 --region=us-central1`) — since this reserves the *same* address the VM already had, it required no redeploy and no manifest changes, just re-verification that the cluster was still reachable. One billing note: GCP charges a small idle fee (~$0.01/hr) for a reserved static IP only while it's *not* attached to a running instance — stopping the VM would start that small charge until either it restarts or the reservation is released.

## How to reproduce

**Docker Compose (local 3-node cluster):**
```powershell
docker compose build
docker compose up -d
docker compose exec node1 kvctl -addr http://node1:8000 put hello docker-world
docker compose exec node2 kvctl -addr http://node2:8000 get hello
docker compose down -v   # tears down containers and volumes
```

**k3s on a cloud VM (GCP, current):**
```bash
# on the VM
curl -sfL https://get.k3s.io | sh -
mkdir -p ~/.kube && sudo cp /etc/rancher/k3s/k3s.yaml ~/.kube/config
sudo chown $(id -u):$(id -g) ~/.kube/config
export KUBECONFIG=~/.kube/config
kubectl apply -f ~/kvstore.yaml
kubectl get pods -n kvstore -w
kubectl logs -n kvstore kvstore-0 | tail -30
```
```powershell
# from a local machine, against the VM's public IP and the 3 per-pod ports
./bin/kvctl.exe -addr "http://<PUBLIC_IP>:8081,http://<PUBLIC_IP>:8082,http://<PUBLIC_IP>:8083" put hello world
./bin/kvctl.exe -addr "http://<PUBLIC_IP>:8081,http://<PUBLIC_IP>:8082,http://<PUBLIC_IP>:8083" get hello
```
