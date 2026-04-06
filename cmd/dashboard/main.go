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
	"path/filepath"

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

	// /api/* → proxied to broker
	mux.HandleFunc("/api/", func(w http.ResponseWriter, r *http.Request) {
		proxy.ServeHTTP(w, r)
	})

	// Serve app.wasm with correct MIME type.
	// go-app's LocalDir("") generates URL /app.wasm — we handle it here explicitly
	// so the MIME type is always application/wasm (Go's FileServer may omit it).
	wasmPath := filepath.Join(*wasmDir, "app.wasm")
	mux.HandleFunc("/app.wasm", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/wasm")
		http.ServeFile(w, r, wasmPath)
	})

	// go-app handler — LocalDir("") generates /app.wasm, /app-worker.js, etc.
	// with no directory prefix, matching our routes above.
	mux.Handle("/", &app.Handler{
		Name:            "GoQueue Dashboard",
		Description:     "Real-time GoQueue message broker dashboard — built in Go → WASM",
		BackgroundColor: "#0d1117",
		ThemeColor:      "#0d1117",
		Icon: app.Icon{
			Default: "https://go.dev/images/favicon-gopher.png",
		},
		Resources: app.LocalDir(""),
		RawHeaders: []string{
			`<link rel="preconnect" href="https://fonts.googleapis.com">`,
			`<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&display=swap" rel="stylesheet">`,
			`<style>
				*{box-sizing:border-box;margin:0;padding:0}
				html,body{background:#0d1117;height:100%}
				@keyframes blink{0%,100%{opacity:1}50%{opacity:.25}}
				@keyframes fadeIn{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:none}}
				.card{animation:fadeIn .25s ease}
				.live-dot{animation:blink 1.4s ease-in-out infinite}
				::-webkit-scrollbar{width:6px}
				::-webkit-scrollbar-track{background:#161b22}
				::-webkit-scrollbar-thumb{background:#30363d;border-radius:3px}
			</style>`,
		},
	})

	log.Printf("dashboard  → http://localhost%s", *listenAddr)
	log.Printf("broker api → %s/api/stats", *brokerAddr)
	log.Fatal(http.ListenAndServe(*listenAddr, mux))
}
