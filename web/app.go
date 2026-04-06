// Package web contains the GoQueue dashboard — 100% Go, compiled to WebAssembly.
// No HTML, no CSS files, no JavaScript written by hand.
// The same internal/api types used by the broker are imported directly here.
package web

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/2006t/goqueue/internal/api"
	"github.com/maxence-charriere/go-app/v10/pkg/app"
)

func init() {
	app.Route("/", func() app.Composer { return &Dashboard{} })
	app.Route("/*", func() app.Composer { return &Dashboard{} })
}

// ── colours ──────────────────────────────────────────────────────────────────

const (
	colBg       = "#0d1117"
	colSurface  = "#161b22"
	colBorder   = "#30363d"
	colText     = "#e6edf3"
	colMuted    = "#8b949e"
	colGreen    = "#3fb950"
	colBlue     = "#58a6ff"
	colPurple   = "#d2a8ff"
	colOrange   = "#f0883e"
	colRed      = "#f85149"
	colCyan     = "#39c5cf"
	fontMono    = "'JetBrains Mono', 'Fira Code', 'Cascadia Code', monospace"
)

// ── Dashboard (root component) ────────────────────────────────────────────────

// Dashboard is the root component. It polls /api/stats every second and
// propagates the data down to child components via struct fields.
type Dashboard struct {
	app.Compo

	stats   api.BrokerStats
	pubRate int64
	conRate int64
	loading bool
	errMsg  string
	ticks   int // increments each second, used to drive the live dot blink
}

func (d *Dashboard) OnMount(ctx app.Context) {
	d.loading = true
	ctx.Async(func() {
		d.poll(ctx)
	})
}

func (d *Dashboard) poll(ctx app.Context) {
	for {
		stats, err := fetchStats()
		ctx.Dispatch(func(ctx app.Context) {
			if err != nil {
				d.errMsg = err.Error()
				d.loading = false
				return
			}
			d.pubRate = stats.TotalPublished - d.stats.TotalPublished
			d.conRate = stats.TotalConsumed - d.stats.TotalConsumed
			d.stats = stats
			d.loading = false
			d.errMsg = ""
			d.ticks++
		})
		time.Sleep(time.Second)
	}
}

func fetchStats() (api.BrokerStats, error) {
	resp, err := http.Get("/api/stats")
	if err != nil {
		return api.BrokerStats{}, err
	}
	defer resp.Body.Close()
	var s api.BrokerStats
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return api.BrokerStats{}, err
	}
	return s, nil
}

func (d *Dashboard) Render() app.UI {
	return app.Div().
		Style("background", colBg).
		Style("min-height", "100vh").
		Style("font-family", fontMono).
		Style("color", colText).
		Style("padding", "28px 32px").
		Style("box-sizing", "border-box").
		Body(
			d.renderGlobalStyle(),
			d.renderHeader(),
			d.renderErrorBanner(),
			d.renderStatCards(),
			d.renderTopics(),
			d.renderFooter(),
		)
}

// inject @keyframes and base reset via a raw <style> tag
func (d *Dashboard) renderGlobalStyle() app.UI {
	return app.Raw(`<style>
		*{box-sizing:border-box;margin:0;padding:0}
		body{background:#0d1117}
		@keyframes blink{0%,100%{opacity:1}50%{opacity:0.25}}
		@keyframes fadeIn{from{opacity:0;transform:translateY(4px)}to{opacity:1;transform:none}}
		.card{animation:fadeIn .3s ease}
		.live-dot{animation:blink 1.4s infinite}
		::-webkit-scrollbar{width:6px}
		::-webkit-scrollbar-track{background:#161b22}
		::-webkit-scrollbar-thumb{background:#30363d;border-radius:3px}
	</style>`)
}

// ── Header ────────────────────────────────────────────────────────────────────

func (d *Dashboard) renderHeader() app.UI {
	roleColor := map[string]string{
		"leader":     colGreen,
		"follower":   colBlue,
		"candidate":  colOrange,
		"standalone": colPurple,
	}
	rc := colMuted
	if c, ok := roleColor[d.stats.Role]; ok {
		rc = c
	}

	dotColor := colGreen
	if d.errMsg != "" {
		dotColor = colRed
	} else if d.loading {
		dotColor = colOrange
	}

	statusText := "LIVE"
	if d.loading {
		statusText = "CONNECTING"
	} else if d.errMsg != "" {
		statusText = "ERROR"
	}

	return app.Div().
		Style("display", "flex").
		Style("justify-content", "space-between").
		Style("align-items", "center").
		Style("border-bottom", "1px solid "+colBorder).
		Style("padding-bottom", "20px").
		Style("margin-bottom", "28px").
		Body(
			// left: logo + node info
			app.Div().
				Style("display", "flex").
				Style("align-items", "center").
				Style("gap", "14px").
				Body(
					app.Span().
						Style("color", colCyan).
						Style("font-size", "22px").
						Style("font-weight", "700").
						Style("letter-spacing", "-0.5px").
						Text("GoQueue"),
					d.separator(),
					app.Span().
						Style("color", colText).
						Style("font-size", "14px").
						Text(d.stats.NodeID),
					d.separator(),
					app.Span().
						Style("color", rc).
						Style("font-size", "12px").
						Style("background", rc+"22").
						Style("padding", "2px 8px").
						Style("border-radius", "4px").
						Style("border", "1px solid "+rc+"44").
						Text(d.stats.Role),
				),
			// right: live indicator + uptime
			app.Div().
				Style("display", "flex").
				Style("align-items", "center").
				Style("gap", "10px").
				Body(
					app.Span().
						Class("live-dot").
						Style("color", dotColor).
						Style("font-size", "9px").
						Text("●"),
					app.Span().
						Style("color", colMuted).
						Style("font-size", "12px").
						Style("letter-spacing", "1px").
						Text(statusText),
					d.separator(),
					app.Span().
						Style("color", colMuted).
						Style("font-size", "12px").
						Text("up "+d.stats.Uptime),
				),
		)
}

func (d *Dashboard) separator() app.UI {
	return app.Span().
		Style("color", colBorder).
		Style("font-size", "16px").
		Text("│")
}

// ── Error banner ──────────────────────────────────────────────────────────────

func (d *Dashboard) renderErrorBanner() app.UI {
	if d.errMsg == "" {
		return app.Text("")
	}
	return app.Div().
		Style("background", colRed+"18").
		Style("border", "1px solid "+colRed+"55").
		Style("border-radius", "6px").
		Style("padding", "10px 16px").
		Style("margin-bottom", "20px").
		Style("color", colRed).
		Style("font-size", "13px").
		Text("⚠ " + d.errMsg)
}

// ── Stat cards ────────────────────────────────────────────────────────────────

func (d *Dashboard) renderStatCards() app.UI {
	return app.Div().
		Style("display", "grid").
		Style("grid-template-columns", "repeat(auto-fit, minmax(180px, 1fr))").
		Style("gap", "16px").
		Style("margin-bottom", "28px").
		Body(
			d.statCard("PUBLISHED", fmtCount(d.stats.TotalPublished), colGreen, "total messages written"),
			d.statCard("CONSUMED", fmtCount(d.stats.TotalConsumed), colBlue, "total messages read"),
			d.statCard("PUB RATE", fmtCount(d.pubRate)+"/s", colCyan, "messages per second"),
			d.statCard("CON RATE", fmtCount(d.conRate)+"/s", colPurple, "consumed per second"),
			d.statCard("TOPICS", fmt.Sprintf("%d", len(d.stats.Topics)), colOrange, "active topics"),
			d.statCard("TCP CONNS", fmt.Sprintf("%d", d.stats.TCPConnections), colMuted, "open tcp connections"),
		)
}

func (d *Dashboard) statCard(label, value, accent, hint string) app.UI {
	return app.Div().
		Class("card").
		Style("background", colSurface).
		Style("border", "1px solid "+colBorder).
		Style("border-top", "2px solid "+accent).
		Style("border-radius", "6px").
		Style("padding", "18px 20px").
		Body(
			app.P().
				Style("color", colMuted).
				Style("font-size", "10px").
				Style("letter-spacing", "1.5px").
				Style("margin-bottom", "10px").
				Text(label),
			app.P().
				Style("color", accent).
				Style("font-size", "30px").
				Style("font-weight", "700").
				Style("line-height", "1").
				Style("margin-bottom", "6px").
				Text(value),
			app.P().
				Style("color", colBorder).
				Style("font-size", "10px").
				Text(hint),
		)
}

// ── Topics ────────────────────────────────────────────────────────────────────

func (d *Dashboard) renderTopics() app.UI {
	sectionLabel := app.P().
		Style("color", colMuted).
		Style("font-size", "10px").
		Style("letter-spacing", "1.5px").
		Style("margin-bottom", "14px").
		Text("TOPICS")

	if len(d.stats.Topics) == 0 {
		return app.Div().Body(
			sectionLabel,
			app.Div().
				Style("background", colSurface).
				Style("border", "1px dashed "+colBorder).
				Style("border-radius", "6px").
				Style("padding", "32px").
				Style("text-align", "center").
				Style("color", colMuted).
				Style("font-size", "13px").
				Text("no topics yet — publish a message to get started"),
		)
	}

	rows := make([]app.UI, 0, len(d.stats.Topics))
	for _, t := range d.stats.Topics {
		rows = append(rows, d.topicRow(t))
	}

	return app.Div().Body(
		append([]app.UI{sectionLabel},
			app.Div().
				Style("display", "flex").
				Style("flex-direction", "column").
				Style("gap", "8px").
				Body(rows...),
		)...,
	)
}

func (d *Dashboard) topicRow(t api.TopicStat) app.UI {
	// build partition badges
	badges := make([]app.UI, 0, len(t.Partitions))
	for _, p := range t.Partitions {
		fill := colGreen
		if p.Size == 0 {
			fill = colMuted
		}
		badges = append(badges,
			app.Div().
				Style("display", "flex").
				Style("flex-direction", "column").
				Style("align-items", "center").
				Style("gap", "4px").
				Body(
					app.Span().
						Style("background", colBg).
						Style("border", "1px solid "+colBorder).
						Style("border-radius", "4px").
						Style("padding", "3px 10px").
						Style("font-size", "11px").
						Style("color", fill).
						Text(fmt.Sprintf("p%d  %s", p.Index, fmtCount(p.Size))),
				),
		)
	}

	// mini bar: how full is this topic (relative to total messages)
	totalAll := d.stats.TotalPublished
	pct := 0.0
	if totalAll > 0 {
		pct = float64(t.Total) / float64(totalAll) * 100
	}
	if pct > 100 {
		pct = 100
	}

	return app.Div().
		Class("card").
		Style("background", colSurface).
		Style("border", "1px solid "+colBorder).
		Style("border-radius", "6px").
		Style("padding", "14px 18px").
		Body(
			// top row: name + total
			app.Div().
				Style("display", "flex").
				Style("justify-content", "space-between").
				Style("align-items", "center").
				Style("margin-bottom", "10px").
				Body(
					app.Span().
						Style("color", colText).
						Style("font-size", "14px").
						Style("font-weight", "600").
						Text(t.Name),
					app.Span().
						Style("color", colGreen).
						Style("font-size", "13px").
						Text(fmtCount(t.Total)+" msgs"),
				),
			// partition badges
			app.Div().
				Style("display", "flex").
				Style("gap", "8px").
				Style("flex-wrap", "wrap").
				Style("margin-bottom", "10px").
				Body(badges...),
			// fill bar
			app.Div().
				Style("height", "3px").
				Style("background", colBorder).
				Style("border-radius", "2px").
				Body(
					app.Div().
						Style("height", "100%").
						Style("width", fmt.Sprintf("%.1f%%", pct)).
						Style("background", colCyan).
						Style("border-radius", "2px").
						Style("transition", "width .4s ease"),
				),
		)
}

// ── Footer ────────────────────────────────────────────────────────────────────

func (d *Dashboard) renderFooter() app.UI {
	walBadge := colGreen
	switch d.stats.WAL.SyncMode {
	case "none":
		walBadge = colOrange
	case "always":
		walBadge = colGreen
	case "interval":
		walBadge = colBlue
	}

	return app.Div().
		Style("border-top", "1px solid "+colBorder).
		Style("margin-top", "28px").
		Style("padding-top", "16px").
		Style("display", "flex").
		Style("justify-content", "space-between").
		Style("align-items", "center").
		Body(
			app.Div().
				Style("display", "flex").
				Style("gap", "18px").
				Style("align-items", "center").
				Body(
					app.Span().Style("color", colMuted).Style("font-size", "11px").
						Text("WAL: "+d.stats.WAL.Path),
					app.Span().
						Style("color", walBadge).
						Style("font-size", "10px").
						Style("background", walBadge+"22").
						Style("padding", "1px 7px").
						Style("border-radius", "3px").
						Text("sync="+d.stats.WAL.SyncMode),
				),
			app.Span().
				Style("color", colBorder).
				Style("font-size", "11px").
				Text("goqueue • built in Go → WASM"),
		)
}

// ── helpers ───────────────────────────────────────────────────────────────────

func fmtCount(n int64) string {
	switch {
	case n < 0:
		return "0"
	case n >= 1_000_000_000:
		return fmt.Sprintf("%.2fB", float64(n)/1e9)
	case n >= 1_000_000:
		return fmt.Sprintf("%.2fM", float64(n)/1e6)
	case n >= 1_000:
		return fmt.Sprintf("%.1fK", float64(n)/1e3)
	default:
		return fmt.Sprintf("%d", n)
	}
}
