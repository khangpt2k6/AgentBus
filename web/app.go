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

// ── colours ───────────────────────────────────────────────────────────────────

const (
	colBg      = "#0d1117"
	colSurface = "#161b22"
	colBorder  = "#30363d"
	colText    = "#e6edf3"
	colMuted   = "#8b949e"
	colGreen   = "#3fb950"
	colBlue    = "#58a6ff"
	colPurple  = "#d2a8ff"
	colOrange  = "#f0883e"
	colRed     = "#f85149"
	colCyan    = "#39c5cf"
	fontMono   = "'JetBrains Mono', 'Fira Code', 'Cascadia Code', monospace"
)

// ── Dashboard (root component) ────────────────────────────────────────────────

type Dashboard struct {
	app.Compo

	stats   api.BrokerStats
	pubRate int64
	conRate int64
	errMsg  string
}

func (d *Dashboard) OnMount(ctx app.Context) {
	ctx.Async(func() {
		for {
			s, err := fetchStats()
			ctx.Dispatch(func(ctx app.Context) {
				if err != nil {
					d.errMsg = err.Error()
					return
				}
				d.pubRate = s.TotalPublished - d.stats.TotalPublished
				d.conRate = s.TotalConsumed - d.stats.TotalConsumed
				d.stats = s
				d.errMsg = ""
			})
			time.Sleep(time.Second)
		}
	})
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
		Body(
			d.renderHeader(),
			d.renderErrorBanner(),
			d.renderStatCards(),
			d.renderTopics(),
			d.renderFooter(),
		)
}

// ── Header ────────────────────────────────────────────────────────────────────

func (d *Dashboard) renderHeader() app.UI {
	roleColors := map[string]string{
		"leader":     colGreen,
		"follower":   colBlue,
		"candidate":  colOrange,
		"standalone": colPurple,
	}
	rc := colMuted
	if c, ok := roleColors[d.stats.Role]; ok {
		rc = c
	}

	dotColor, statusText := colGreen, "LIVE"
	if d.errMsg != "" {
		dotColor, statusText = colRed, "ERROR"
	} else if d.stats.NodeID == "" {
		dotColor, statusText = colOrange, "CONNECTING"
	}

	nodeID := d.stats.NodeID
	if nodeID == "" {
		nodeID = "—"
	}
	role := d.stats.Role
	if role == "" {
		role = "—"
	}
	uptime := d.stats.Uptime
	if uptime == "" {
		uptime = "—"
	}

	return app.Div().
		Style("display", "flex").
		Style("justify-content", "space-between").
		Style("align-items", "center").
		Style("border-bottom", "1px solid "+colBorder).
		Style("padding-bottom", "20px").
		Style("margin-bottom", "28px").
		Body(
			app.Div().
				Style("display", "flex").
				Style("align-items", "center").
				Style("gap", "14px").
				Body(
					app.Span().
						Style("color", colCyan).
						Style("font-size", "22px").
						Style("font-weight", "700").
						Text("GoQueue"),
					app.Span().Style("color", colBorder).Text("│"),
					app.Span().
						Style("color", colText).
						Style("font-size", "14px").
						Text(nodeID),
					app.Span().Style("color", colBorder).Text("│"),
					app.Span().
						Style("color", rc).
						Style("font-size", "12px").
						Style("background", rc+"22").
						Style("padding", "2px 8px").
						Style("border-radius", "4px").
						Style("border", "1px solid "+rc+"44").
						Text(role),
				),
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
					app.Span().Style("color", colBorder).Text("│"),
					app.Span().
						Style("color", colMuted).
						Style("font-size", "12px").
						Text("up "+uptime),
				),
		)
}

// ── Error banner ──────────────────────────────────────────────────────────────

func (d *Dashboard) renderErrorBanner() app.UI {
	if d.errMsg == "" {
		return app.Span()
	}
	return app.Div().
		Style("background", "#f8514922").
		Style("border", "1px solid #f8514955").
		Style("border-radius", "6px").
		Style("padding", "10px 16px").
		Style("margin-bottom", "20px").
		Style("color", colRed).
		Style("font-size", "13px").
		Text("⚠  broker unreachable — " + d.errMsg)
}

// ── Stat cards ────────────────────────────────────────────────────────────────

func (d *Dashboard) renderStatCards() app.UI {
	return app.Div().
		Style("display", "grid").
		Style("grid-template-columns", "repeat(auto-fit, minmax(160px, 1fr))").
		Style("gap", "16px").
		Style("margin-bottom", "28px").
		Body(
			d.statCard("PUBLISHED", fmtCount(d.stats.TotalPublished), colGreen, "total written"),
			d.statCard("CONSUMED", fmtCount(d.stats.TotalConsumed), colBlue, "total read"),
			d.statCard("PUB/s", fmtCount(d.pubRate), colCyan, "messages/sec"),
			d.statCard("CON/s", fmtCount(d.conRate), colPurple, "consumed/sec"),
			d.statCard("TOPICS", fmt.Sprintf("%d", len(d.stats.Topics)), colOrange, "active topics"),
			d.statCard("TCP", fmt.Sprintf("%d", d.stats.TCPConnections), colMuted, "open connections"),
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
	label := app.P().
		Style("color", colMuted).
		Style("font-size", "10px").
		Style("letter-spacing", "1.5px").
		Style("margin-bottom", "14px").
		Text("TOPICS")

	if len(d.stats.Topics) == 0 {
		return app.Div().Body(
			label,
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

	rows := make([]app.UI, 0, len(d.stats.Topics)+1)
	rows = append(rows, label)
	for _, t := range d.stats.Topics {
		rows = append(rows, d.topicRow(t))
	}

	return app.Div().
		Style("display", "flex").
		Style("flex-direction", "column").
		Style("gap", "8px").
		Body(rows...)
}

func (d *Dashboard) topicRow(t api.TopicStat) app.UI {
	badges := make([]app.UI, 0, len(t.Partitions))
	for _, p := range t.Partitions {
		col := colGreen
		if p.Size == 0 {
			col = colMuted
		}
		badges = append(badges,
			app.Span().
				Style("background", colBg).
				Style("border", "1px solid "+colBorder).
				Style("border-radius", "4px").
				Style("padding", "3px 10px").
				Style("font-size", "11px").
				Style("color", col).
				Text(fmt.Sprintf("p%d  %s", p.Index, fmtCount(p.Size))),
		)
	}

	pct := 0.0
	if d.stats.TotalPublished > 0 {
		pct = float64(t.Total) / float64(d.stats.TotalPublished) * 100
		if pct > 100 {
			pct = 100
		}
	}

	return app.Div().
		Class("card").
		Style("background", colSurface).
		Style("border", "1px solid "+colBorder).
		Style("border-radius", "6px").
		Style("padding", "14px 18px").
		Body(
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
			app.Div().
				Style("display", "flex").
				Style("gap", "8px").
				Style("flex-wrap", "wrap").
				Style("margin-bottom", "10px").
				Body(badges...),
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
	walColor := colOrange
	switch d.stats.WAL.SyncMode {
	case "always":
		walColor = colGreen
	case "interval":
		walColor = colBlue
	}
	syncMode := d.stats.WAL.SyncMode
	if syncMode == "" {
		syncMode = "—"
	}
	walPath := d.stats.WAL.Path
	if walPath == "" {
		walPath = "—"
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
				Style("gap", "14px").
				Style("align-items", "center").
				Body(
					app.Span().
						Style("color", colMuted).
						Style("font-size", "11px").
						Text("wal: "+walPath),
					app.Span().
						Style("color", walColor).
						Style("font-size", "10px").
						Style("background", walColor+"22").
						Style("padding", "1px 7px").
						Style("border-radius", "3px").
						Text("sync="+syncMode),
				),
			app.Span().
				Style("color", colBorder).
				Style("font-size", "11px").
				Text("goqueue · built in Go → WASM"),
		)
}

// ── helpers ───────────────────────────────────────────────────────────────────

func fmtCount(n int64) string {
	if n <= 0 {
		return "0"
	}
	switch {
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
