// Command launcher is the always-available front door for the live demo.
// The demo cluster runs on a VM that powers itself off when idle; this
// service (deployed to Cloud Run, which scales to zero) starts the VM when
// someone visits, shows a "waking up" page while the cluster boots and
// elects a leader, then redirects to the dashboard.
//
// It talks to the Compute Engine REST API directly with a token from the
// Cloud Run metadata server, so it needs no client library. Its service
// account should be allowed only compute.instances.get and
// compute.instances.start on the one demo VM.
//
// Configuration (environment variables):
//
//	PROJECT, ZONE, INSTANCE  the demo VM
//	DEMO_URL                 the dashboard's base URL, e.g. http://34.60.149.77:8081
//	PORT                     set by Cloud Run (default 8080)
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

func main() {
	cfg := map[string]string{}
	for _, k := range []string{"PROJECT", "ZONE", "INSTANCE", "DEMO_URL"} {
		if cfg[k] = os.Getenv(k); cfg[k] == "" {
			log.Fatalf("launcher: %s must be set", k)
		}
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	l := &launcher{
		vm:      &gceVM{project: cfg["PROJECT"], zone: cfg["ZONE"], instance: cfg["INSTANCE"], http: &http.Client{Timeout: 10 * time.Second}},
		demoURL: strings.TrimRight(cfg["DEMO_URL"], "/"),
		probe:   &http.Client{Timeout: 3 * time.Second},
	}
	log.Printf("launcher listening on :%s for %s", port, l.demoURL)
	log.Fatal(http.ListenAndServe(":"+port, l.routes()))
}

// vmAPI is the slice of Compute Engine the launcher uses; faked in tests.
type vmAPI interface {
	Status(ctx context.Context) (string, error)
	Start(ctx context.Context) error
}

type launcher struct {
	vm      vmAPI
	demoURL string
	probe   *http.Client
	mu      sync.Mutex // serializes start attempts from concurrent visitors
}

func (l *launcher) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", l.handleIndex)
	mux.HandleFunc("/status", l.handleStatus)
	return mux
}

// state is what both endpoints report: the VM's lifecycle status and
// whether the cluster behind it is answering with an elected leader.
type state struct {
	VM    string `json:"vm"`
	Ready bool   `json:"ready"`
}

// wake starts the VM if it is stopped and reports where things stand.
// Starting is only attempted from TERMINATED (Compute Engine's "stopped");
// in every other state the VM is already on its way up or down, and a later
// poll will try again.
func (l *launcher) wake(ctx context.Context) (state, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	st, err := l.vm.Status(ctx)
	if err != nil {
		return state{}, err
	}
	if st == "TERMINATED" {
		if err := l.vm.Start(ctx); err != nil {
			return state{}, err
		}
		log.Printf("launcher: started the demo VM")
		return state{VM: "STARTING"}, nil
	}
	return state{VM: st, Ready: st == "RUNNING" && l.clusterReady(ctx)}, nil
}

// clusterReady reports whether the dashboard's node answers and knows a
// leader, i.e. a majority of the cluster is up.
func (l *launcher) clusterReady(ctx context.Context) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, l.demoURL+"/cluster/status", nil)
	if err != nil {
		return false
	}
	resp, err := l.probe.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	var s struct {
		Leader string `json:"leader"`
	}
	return resp.StatusCode == http.StatusOK && json.NewDecoder(resp.Body).Decode(&s) == nil && s.Leader != ""
}

func (l *launcher) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	st, err := l.wake(ctx)
	if err != nil {
		log.Printf("launcher: %v", err)
		w.WriteHeader(http.StatusBadGateway)
	} else if st.Ready {
		http.Redirect(w, r, l.demoURL+"/ui", http.StatusFound)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	wakingPage.Execute(w, struct {
		DashboardURL string
		Failed       bool
	}{l.demoURL + "/ui", err != nil})
}

func (l *launcher) handleStatus(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	st, err := l.wake(ctx)
	if err != nil {
		log.Printf("launcher: %v", err)
		http.Error(w, "could not reach the demo VM", http.StatusBadGateway)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	json.NewEncoder(w).Encode(st)
}

// gceVM implements vmAPI against the Compute Engine REST API.
type gceVM struct {
	project, zone, instance string
	http                    *http.Client
}

func (g *gceVM) url(suffix string) string {
	return fmt.Sprintf("https://compute.googleapis.com/compute/v1/projects/%s/zones/%s/instances/%s%s",
		g.project, g.zone, g.instance, suffix)
}

func (g *gceVM) Status(ctx context.Context) (string, error) {
	var out struct {
		Status string `json:"status"`
	}
	if err := g.call(ctx, http.MethodGet, g.url("?fields=status"), &out); err != nil {
		return "", err
	}
	return out.Status, nil
}

func (g *gceVM) Start(ctx context.Context) error {
	return g.call(ctx, http.MethodPost, g.url("/start"), nil)
}

func (g *gceVM) call(ctx context.Context, method, url string, out any) error {
	token, err := g.token(ctx)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := g.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("compute API %s %s: status %d", method, url, resp.StatusCode)
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// token fetches an access token for the service's own service account from
// the metadata server (available on Cloud Run, never off-platform).
func (g *gceVM) token(ctx context.Context) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Metadata-Flavor", "Google")
	resp, err := g.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("metadata token: %w", err)
	}
	defer resp.Body.Close()
	var tok struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tok); err != nil || tok.AccessToken == "" {
		return "", errors.New("metadata token: empty or unreadable response")
	}
	return tok.AccessToken, nil
}

var wakingPage = template.Must(template.New("waking").Parse(`<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Waking the cluster</title>
<style>
:root{--bg:#f8f9fa;--fg:#1e1e1e;--muted:#5c636a;--accent:#1971c2}
@media (prefers-color-scheme:dark){:root{--bg:#16181c;--fg:#e9ecef;--muted:#adb5bd;--accent:#74c0fc}}
body{margin:0;min-height:100vh;display:grid;place-items:center;background:var(--bg);color:var(--fg);
font:16px/1.5 system-ui,-apple-system,"Segoe UI",sans-serif;padding:0 16px}
main{max-width:34rem}
h1{font-size:1.4rem;margin:0 0 .5rem}
p{color:var(--muted);margin:.5rem 0}
a{color:var(--accent)}
.dot{display:inline-block;width:.6rem;height:.6rem;border-radius:50%;background:var(--accent);margin-right:.5rem;animation:p 1.2s infinite}
@keyframes p{50%{opacity:.25}}
</style></head><body><main>
{{if .Failed}}
<h1>The demo couldn't be started right now</h1>
<p>Please try again in a minute. A recording of the demo is in the
<a href="https://github.com/DhruvGandhi31/Distributed-Key-Value-Store">project README</a>.</p>
{{else}}
<h1><span class="dot"></span>Waking the cluster up</h1>
<p>To save resources, the demo servers sleep when nobody is using them. They're booting now,
then they'll elect a leader. This usually takes 1–2 minutes, and you'll be taken to the dashboard automatically.</p>
<p id="t">Elapsed: 0 s</p>
<p>Meanwhile, the <a href="https://github.com/DhruvGandhi31/Distributed-Key-Value-Store">project README</a> has a recording of the demo.</p>
<script>
const dashboard = {{.DashboardURL}};
const started = Date.now();
setInterval(() => { document.getElementById("t").textContent = "Elapsed: " + Math.round((Date.now() - started) / 1000) + " s"; }, 1000);
async function poll() {
  try {
    const r = await fetch("/status", {cache: "no-store"});
    if (r.ok && (await r.json()).ready) { location.href = dashboard; return; }
  } catch (e) {}
  setTimeout(poll, 3000);
}
poll();
</script>
{{end}}
</main></body></html>
`))
