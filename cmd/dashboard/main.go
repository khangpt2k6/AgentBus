// cmd/dashboard is both:
//   - the WebAssembly binary (GOOS=js GOARCH=wasm go build -o web/app.wasm .)
//   - the dashboard HTTP server that serves the WASM app + proxies /api/ to the broker
//
// Same source, two targets. No HTML/CSS/JS written by hand.
package main

import (
	"flag"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"

	_ "github.com/2006t/goqueue/web" // registers all routes via init()
	"github.com/maxence-charriere/go-app/v10/pkg/app"
)

func main() {
	// ── client side (runs when compiled to WASM in the browser) ──────────────
	app.RunWhenOnBrowser()

	// ── server side (runs as a regular Go binary) ─────────────────────────────
	brokerAddr := flag.String("broker", "http://localhost:2112", "GoQueue broker metrics/API address")
	listenAddr := flag.String("addr", ":8080", "dashboard listen address")
	wasmDir := flag.String("wasm-dir", "web", "directory containing app.wasm")
	flag.Parse()

	brokerURL, err := url.Parse(*brokerAddr)
	if err != nil {
		log.Fatalf("invalid broker address: %v", err)
	}

	proxy := httputil.NewSingleHostReverseProxy(brokerURL)

	mux := http.NewServeMux()

	// /api/* → proxied to broker (same-origin for the WASM, no CORS needed)
	mux.HandleFunc("/api/", func(w http.ResponseWriter, r *http.Request) {
		proxy.ServeHTTP(w, r)
	})

	// go-app serves: /, /app.wasm, /wasm_exec.js, /app-worker.js, /manifest.webmanifest
	mux.Handle("/", &app.Handler{
		Name:            "GoQueue Dashboard",
		Description:     "Real-time GoQueue message broker dashboard — built in Go → WASM",
		BackgroundColor: "#0d1117",
		ThemeColor:      "#0d1117",
		Icon: app.Icon{
			Default: "https://go.dev/images/favicon-gopher.png",
		},
		Resources: app.LocalDir(*wasmDir),
		RawHeaders: []string{
			`<link rel="preconnect" href="https://fonts.googleapis.com">`,
			`<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&display=swap" rel="stylesheet">`,
		},
	})

	log.Printf("dashboard  → http://localhost%s", *listenAddr)
	log.Printf("broker api → %s/api/stats", *brokerAddr)
	log.Fatal(http.ListenAndServe(*listenAddr, mux))
}
