// Package web — GoQueue dashboard, 100% Go compiled to WebAssembly.
// No HTML / CSS / JS files written by hand.
package web

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/2006t/goqueue/internal/api"
	"github.com/maxence-charriere/go-app/v10/pkg/app"
)

func init() {
	app.Route("/", func() app.Composer { return &Dashboard{} })
	app.Route("/*", func() app.Composer { return &Dashboard{} })
}

// ── palette (GitHub light-mode inspired) ─────────────────────────────────────

const (
	bg          = "#f6f8fa"
	glass       = "rgba(255,255,255,0.85)"
	glassBorder = "rgba(0,0,0,0.10)"
	glassHover  = "rgba(0,0,0,0.06)"
	txt         = "#0d1117"
	muted       = "rgba(87,96,106,0.85)"
	accent      = "#0969da"
	accentDim   = "rgba(9,105,218,0.10)"
	accentBdr   = "rgba(9,105,218,0.30)"
	warn    = "#9a6700"
	warnDim = "rgba(154,103,0,0.12)"
	danger  = "#cf222e"
	dangerDim   = "rgba(207,34,46,0.10)"
	ok          = "rgba(26,127,55,0.90)"
	fontMono    = "'JetBrains Mono','Fira Code','Cascadia Code',monospace"
)

// ── Dashboard ─────────────────────────────────────────────────────────────────

type Dashboard struct {
	app.Compo

	// live stats
	stats       api.BrokerStats
	pubRate     int64
	conRate     int64
	prevPubRate int64
	prevConRate int64
	connErr     string

	// left panel — selected topic
	selectedTopic string

	// right panel — active tab
	activeTab string // "publish" | "fetch" | "guide"

	// publish form
	pubTopic    string
	pubKey      string
	pubPartStr  string
	pubPayload  string
	pubFeedback string
	pubIsErr    bool
	publishing  bool

	// fetch form
	fetchTopic   string
	fetchPartStr string
	fetchOffStr  string
	fetchLimStr  string
	fetchResults []api.FetchedMessage
	fetchErr     string
	fetching     bool
}

func (d *Dashboard) OnMount(ctx app.Context) {
	d.activeTab = "publish"
	ctx.Async(func() {
		for {
			s, err := fetchStats()
			ctx.Dispatch(func(ctx app.Context) {
				if err != nil {
					d.connErr = err.Error()
					return
				}
				d.prevPubRate = d.pubRate
				d.prevConRate = d.conRate
				d.pubRate = s.TotalPublished - d.stats.TotalPublished
				d.conRate = s.TotalConsumed - d.stats.TotalConsumed
				d.stats = s
				d.connErr = ""
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
	return s, json.NewDecoder(resp.Body).Decode(&s)
}

// ── root render ───────────────────────────────────────────────────────────────

func (d *Dashboard) Render() app.UI {
	return app.Div().
		Style("min-height", "100vh").
		Style("font-family", fontMono).
		Style("color", txt).
		Style("padding", "24px 28px").
		Body(
			d.renderHeader(),
			d.renderConnErr(),
			d.renderStatRow(),
			d.renderMain(),
			d.renderFooter(),
		)
}

// ── header ────────────────────────────────────────────────────────────────────

func (d *Dashboard) renderHeader() app.UI {
	nodeID := or(d.stats.NodeID, "connecting…")
	role := or(d.stats.Role, "—")
	uptime := or(d.stats.Uptime, "—")

	dotCol := accent
	if d.connErr != "" {
		dotCol = danger
	}

	return app.Div().
		Style("display", "flex").
		Style("justify-content", "space-between").
		Style("align-items", "center").
		Style("padding-bottom", "18px").
		Style("margin-bottom", "20px").
		Style("border-bottom", "1px solid "+glassBorder).
		Body(
			// logo + node
			app.Div().Style("display", "flex").Style("align-items", "center").Style("gap", "12px").Body(
				app.Span().Style("font-size", "18px").Style("font-weight", "700").Style("color", accent).Text("GoQueue"),
				app.Span().Style("color", glassBorder).Text("│"),
				app.Span().Style("font-size", "13px").Style("color", txt).Text(nodeID),
				app.Span().Style("color", glassBorder).Text("│"),
				app.Span().
					Style("font-size", "11px").Style("color", accent).
					Style("background", accentDim).Style("border", "1px solid "+accentBdr).
					Style("padding", "1px 8px").Style("border-radius", "4px").
					Text(role),
			),
			// live dot + uptime
			app.Div().Style("display", "flex").Style("align-items", "center").Style("gap", "8px").Body(
				app.Span().Class("pulse").Style("color", dotCol).Style("font-size", "8px").Text("●"),
				app.Span().Style("color", muted).Style("font-size", "11px").Text("up "+uptime),
			),
		)
}

// ── connection error banner ───────────────────────────────────────────────────

func (d *Dashboard) renderConnErr() app.UI {
	if d.connErr == "" {
		return app.Span()
	}
	return app.Div().
		Style("background", dangerDim).Style("border", "1px solid rgba(248,81,73,0.3)").
		Style("border-radius", "8px").Style("padding", "9px 14px").
		Style("margin-bottom", "16px").Style("font-size", "12px").Style("color", danger).
		Text("⚠  " + d.connErr)
}

// ── stat row ─────────────────────────────────────────────────────────────────

func (d *Dashboard) renderStatRow() app.UI {
	backlog := max(d.stats.TotalPublished-d.stats.TotalConsumed, 0)
	backlogCol := ok
	switch {
	case backlog > 100_000:
		backlogCol = danger
	case backlog > 1_000:
		backlogCol = warn
	}
	return app.Div().
		Style("display", "grid").
		Style("grid-template-columns", "repeat(auto-fit,minmax(130px,1fr))").
		Style("gap", "10px").Style("margin-bottom", "20px").
		Body(
			d.stat("PUBLISHED", fmtN(d.stats.TotalPublished), txt),
			d.stat("CONSUMED", fmtN(d.stats.TotalConsumed), txt),
			d.stat("PUB/s", fmtN(d.pubRate)+trend(d.pubRate, d.prevPubRate), accent),
			d.stat("CON/s", fmtN(d.conRate)+trend(d.conRate, d.prevConRate), accent),
			d.stat("BACKLOG", fmtN(backlog), backlogCol),
			d.stat("TOPICS", strconv.Itoa(len(d.stats.Topics)), txt),
			d.stat("TCP CONN", strconv.FormatInt(d.stats.TCPConnections, 10), txt),
			d.stat("WAL SYNC", or(d.stats.WAL.SyncMode, "—"), txt),
		)
}

func (d *Dashboard) stat(label, value, valueColor string) app.UI {
	return app.Div().Class("glass fade-in").
		Style("padding", "14px 16px").
		Style("border-left", "3px solid "+accentBdr).
		Body(
			app.P().Style("color", muted).Style("font-size", "9px").Style("letter-spacing", "1.2px").
				Style("margin-bottom", "7px").Text(label),
			app.P().Style("color", valueColor).Style("font-size", "22px").Style("font-weight", "700").
				Style("line-height", "1").Text(value),
		)
}

func trend(curr, prev int64) string {
	if curr > prev {
		return " ↑"
	}
	if curr < prev {
		return " ↓"
	}
	return ""
}

// ── main 2-column layout ──────────────────────────────────────────────────────

func (d *Dashboard) renderMain() app.UI {
	return app.Div().
		Style("display", "grid").
		Style("grid-template-columns", "260px 1fr").
		Style("gap", "14px").
		Style("margin-bottom", "20px").
		Body(
			d.renderTopicList(),
			d.renderToolPanel(),
		)
}

// ── left: topic list ─────────────────────────────────────────────────────────

func (d *Dashboard) renderTopicList() app.UI {
	header := app.P().
		Style("color", muted).Style("font-size", "9px").Style("letter-spacing", "1.2px").
		Style("margin-bottom", "12px").Text("TOPICS")

	if len(d.stats.Topics) == 0 {
		return app.Div().Class("glass").Style("padding", "16px").Body(
			header,
			app.Div().
				Style("border", "1px dashed "+glassBorder).Style("border-radius", "8px").
				Style("padding", "24px 12px").Style("text-align", "center").
				Style("color", muted).Style("font-size", "12px").
				Text("no topics yet"),
		)
	}

	rows := make([]app.UI, 0, len(d.stats.Topics)+1)
	rows = append(rows, header)
	for _, t := range d.stats.Topics {
		t := t
		active := d.selectedTopic == t.Name
		leftBorder := "3px solid transparent"
		bg2 := "transparent"
		if active {
			leftBorder = "3px solid " + accent
			bg2 = accentDim
		}
		rows = append(rows,
			app.Div().
				Style("border-left", leftBorder).
				Style("background", bg2).
				Style("border-radius", "0 6px 6px 0").
				Style("padding", "9px 12px").
				Style("cursor", "pointer").
				Style("transition", "all .15s").
				OnClick(func(ctx app.Context, e app.Event) {
					ctx.Dispatch(func(ctx app.Context) {
						d.selectedTopic = t.Name
						d.pubTopic = t.Name
						d.fetchTopic = t.Name
					})
				}).
				Body(
					app.Div().Style("display", "flex").Style("justify-content", "space-between").
						Style("align-items", "center").
						Body(
							app.Span().Style("font-size", "13px").Style("color", txt).Text(t.Name),
							app.Span().Style("font-size", "10px").Style("color", muted).
								Text(fmtN(t.Total)),
						),
					app.Div().Style("display", "flex").Style("gap", "4px").Style("margin-top", "5px").
						Body(d.partBadges(t)...),
					d.fillBar(t),
				),
		)
	}
	return app.Div().Class("glass").Style("padding", "16px").Body(rows...)
}

func (d *Dashboard) partBadges(t api.TopicStat) []app.UI {
	out := make([]app.UI, 0, len(t.Partitions))
	for _, p := range t.Partitions {
		col, bg2 := muted, "rgba(0,0,0,0.04)"
		switch {
		case p.FillPct > 80:
			col, bg2 = danger, "rgba(207,34,46,0.10)"
		case p.FillPct > 50:
			col, bg2 = warn, warnDim
		case p.Size > 0:
			col, bg2 = accent, accentDim
		}
		label := fmt.Sprintf("p%d", p.Index)
		if p.Size > 0 {
			label = fmt.Sprintf("p%d·%s", p.Index, fmtN(p.Size))
		}
		out = append(out,
			app.Span().
				Style("font-size", "9px").Style("color", col).
				Style("background", bg2).
				Style("border", "1px solid "+glassBorder).
				Style("border-radius", "3px").Style("padding", "1px 6px").
				Text(label),
		)
	}
	return out
}

// fillBar renders a thin animated progress bar showing the max partition fill %.
// Returns an empty span when all partitions are empty.
func (d *Dashboard) fillBar(t api.TopicStat) app.UI {
	maxFill := 0.0
	for _, p := range t.Partitions {
		if p.FillPct > maxFill {
			maxFill = p.FillPct
		}
	}
	if maxFill < 0.01 {
		return app.Span()
	}
	barCol := accent
	switch {
	case maxFill > 80:
		barCol = danger
	case maxFill > 50:
		barCol = warn
	}
	pct := fmt.Sprintf("%.2f%%", maxFill)
	return app.Div().
		Style("display", "flex").Style("align-items", "center").Style("gap", "6px").
		Style("margin-top", "6px").
		Body(
			app.Div().
				Style("flex", "1").Style("height", "3px").
				Style("background", glassBorder).Style("border-radius", "2px").
				Body(
					app.Div().
						Style("height", "100%").Style("width", pct).
						Style("background", barCol).Style("border-radius", "2px").
						Style("transition", "width 1s ease"),
				),
			app.Span().Style("font-size", "9px").Style("color", muted).Text(pct+" full"),
		)
}

// ── right: tool panel ────────────────────────────────────────────────────────

func (d *Dashboard) renderToolPanel() app.UI {
	tabs := []struct{ id, label string }{
		{"publish", "PUBLISH"},
		{"fetch", "FETCH"},
		{"guide", "GUIDE"},
	}
	tabBtns := make([]app.UI, 0, len(tabs))
	for _, t := range tabs {
		t := t
		active := d.activeTab == t.id
		col, bg2, bdr := muted, "transparent", "transparent"
		if active {
			col, bg2, bdr = accent, accentDim, accentBdr
		}
		tabBtns = append(tabBtns,
			app.Button().
				Style("background", bg2).Style("border", "1px solid "+bdr).
				Style("border-radius", "5px").Style("color", col).
				Style("font-family", fontMono).Style("font-size", "10px").
				Style("letter-spacing", "1px").Style("padding", "5px 12px").
				Style("cursor", "pointer").Style("transition", "all .15s").
				OnClick(func(ctx app.Context, e app.Event) {
					ctx.Dispatch(func(ctx app.Context) { d.activeTab = t.id })
				}).
				Text(t.label),
		)
	}

	var content app.UI
	switch d.activeTab {
	case "fetch":
		content = d.renderFetchForm()
	case "guide":
		content = d.renderGuide()
	default:
		content = d.renderPublishForm()
	}

	return app.Div().Class("glass").Style("padding", "16px").Body(
		// tab bar
		app.Div().Style("display", "flex").Style("gap", "6px").Style("margin-bottom", "16px").
			Body(tabBtns...),
		// divider
		app.Div().Style("height", "1px").Style("background", glassBorder).Style("margin-bottom", "16px"),
		// tab content
		content,
	)
}

// ── publish form ──────────────────────────────────────────────────────────────

func (d *Dashboard) renderPublishForm() app.UI {
	selLabel := or(d.pubTopic, "— pick a topic on the left or type below —")
	selCol := muted
	if d.pubTopic != "" {
		selCol = accent
	}

	btnLabel := "SEND →"
	if d.publishing {
		btnLabel = "sending…"
	}

	var feedback app.UI = app.Span()
	if d.pubFeedback != "" {
		col := ok
		if d.pubIsErr {
			col = danger
		}
		feedback = app.P().
			Style("font-size", "11px").Style("color", col).
			Style("margin-top", "10px").Style("word-break", "break-all").
			Text(d.pubFeedback)
	}

	return app.Div().Body(
		// topic indicator
		app.P().Style("font-size", "11px").Style("color", selCol).
			Style("margin-bottom", "14px").Text(selLabel),

		d.formRow("TOPIC",
			app.Input().Type("text").Placeholder("orders").
				Value(d.pubTopic).
				OnInput(func(ctx app.Context, e app.Event) {
					d.pubTopic = ctx.JSSrc().Get("value").String()
				}),
		),
		d.formRow("KEY  (optional — for partition routing)",
			app.Input().Type("text").Placeholder("user-123").
				Value(d.pubKey).
				OnInput(func(ctx app.Context, e app.Event) {
					d.pubKey = ctx.JSSrc().Get("value").String()
				}),
		),
		d.formRow("PARTITION  (optional — leave blank for auto)",
			app.Input().Type("text").Placeholder("-1").
				Value(d.pubPartStr).
				OnInput(func(ctx app.Context, e app.Event) {
					d.pubPartStr = ctx.JSSrc().Get("value").String()
				}),
		),
		d.formRow("PAYLOAD",
			app.Textarea().
				Style("resize", "vertical").Style("min-height", "80px").
				Placeholder(`{"hello":"world"}`).
				OnInput(func(ctx app.Context, e app.Event) {
					d.pubPayload = ctx.JSSrc().Get("value").String()
				}).
				Text(d.pubPayload),
		),

		// send button
		app.Button().
			Style("margin-top", "12px").
			Style("background", accentDim).Style("border", "1px solid "+accentBdr).
			Style("color", accent).Style("font-family", fontMono).
			Style("font-size", "11px").Style("letter-spacing", "1px").
			Style("padding", "7px 18px").Style("border-radius", "6px").
			Style("cursor", "pointer").Style("transition", "all .15s").
			Disabled(d.publishing).
			OnClick(d.doPublish).
			Text(btnLabel),

		feedback,
	)
}

func (d *Dashboard) doPublish(ctx app.Context, e app.Event) {
	if d.pubTopic == "" || d.pubPayload == "" {
		ctx.Dispatch(func(ctx app.Context) {
			d.pubFeedback = "topic and payload are required"
			d.pubIsErr = true
		})
		return
	}
	ctx.Dispatch(func(ctx app.Context) { d.publishing = true; d.pubFeedback = "" })

	topic, key, payload, partStr := d.pubTopic, d.pubKey, d.pubPayload, d.pubPartStr
	partition := -1
	if partStr != "" {
		fmt.Sscanf(partStr, "%d", &partition)
	}

	ctx.Async(func() {
		body, _ := json.Marshal(api.PublishRequest{
			Topic:     topic,
			Key:       key,
			Partition: partition,
			Payload:   payload,
		})
		resp, err := http.Post("/api/publish", "application/json", bytes.NewReader(body))
		ctx.Dispatch(func(ctx app.Context) {
			d.publishing = false
			if err != nil {
				d.pubFeedback = "error: " + err.Error()
				d.pubIsErr = true
				return
			}
			defer resp.Body.Close()
			var pr api.PublishResponse
			if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
				d.pubFeedback = "error: " + err.Error()
				d.pubIsErr = true
				return
			}
			d.pubFeedback = fmt.Sprintf("✓  %s  partition=%d  offset=%d", pr.Topic, pr.Partition, pr.Offset)
			d.pubIsErr = false
		})
	})
}

// ── fetch form ────────────────────────────────────────────────────────────────

func (d *Dashboard) renderFetchForm() app.UI {
	btnLabel := "FETCH →"
	if d.fetching {
		btnLabel = "fetching…"
	}

	var results app.UI = app.Span()
	if d.fetchErr != "" {
		results = app.P().Style("color", danger).Style("font-size", "11px").
			Style("margin-top", "10px").Text("error: " + d.fetchErr)
	} else if len(d.fetchResults) > 0 {
		rows := make([]app.UI, 0, len(d.fetchResults))
		for _, m := range d.fetchResults {
			rows = append(rows,
				app.Div().
					Style("border-bottom", "1px solid "+glassBorder).
					Style("padding", "7px 10px").
					Body(
						app.Div().Style("display", "flex").Style("gap", "10px").
							Style("margin-bottom", "3px").
							Body(
								app.Span().Style("color", accent).Style("font-size", "10px").
									Text(fmt.Sprintf("#%d", m.Offset)),
								app.Span().Style("color", muted).Style("font-size", "10px").
									Text(m.Timestamp),
							),
						app.P().Style("font-size", "12px").Style("color", txt).
							Style("word-break", "break-all").Text(m.Payload),
					),
			)
		}
		results = app.Div().
			Style("margin-top", "12px").
			Style("border", "1px solid "+glassBorder).Style("border-radius", "8px").
			Style("max-height", "300px").Style("overflow-y", "auto").
			Body(rows...)
	}

	return app.Div().Body(
		d.formRow("TOPIC",
			app.Input().Type("text").Placeholder("orders").
				Value(d.fetchTopic).
				OnInput(func(ctx app.Context, e app.Event) {
					d.fetchTopic = ctx.JSSrc().Get("value").String()
				}),
		),
		app.Div().Style("display", "grid").Style("grid-template-columns", "1fr 1fr 1fr").
			Style("gap", "10px").
			Body(
				d.formRow("PARTITION",
					app.Input().Type("text").Placeholder("0").Value(d.fetchPartStr).
						OnInput(func(ctx app.Context, e app.Event) {
							d.fetchPartStr = ctx.JSSrc().Get("value").String()
						}),
				),
				d.formRow("OFFSET",
					app.Input().Type("text").Placeholder("0").Value(d.fetchOffStr).
						OnInput(func(ctx app.Context, e app.Event) {
							d.fetchOffStr = ctx.JSSrc().Get("value").String()
						}),
				),
				d.formRow("LIMIT",
					app.Input().Type("text").Placeholder("20").Value(d.fetchLimStr).
						OnInput(func(ctx app.Context, e app.Event) {
							d.fetchLimStr = ctx.JSSrc().Get("value").String()
						}),
				),
			),
		app.Button().
			Style("margin-top", "12px").
			Style("background", accentDim).Style("border", "1px solid "+accentBdr).
			Style("color", accent).Style("font-family", fontMono).
			Style("font-size", "11px").Style("letter-spacing", "1px").
			Style("padding", "7px 18px").Style("border-radius", "6px").
			Style("cursor", "pointer").Style("transition", "all .15s").
			Disabled(d.fetching).
			OnClick(d.doFetch).
			Text(btnLabel),
		results,
	)
}

func (d *Dashboard) doFetch(ctx app.Context, e app.Event) {
	if d.fetchTopic == "" {
		ctx.Dispatch(func(ctx app.Context) { d.fetchErr = "topic is required" })
		return
	}
	ctx.Dispatch(func(ctx app.Context) { d.fetching = true; d.fetchErr = "" })

	topic, partStr, offStr, limStr := d.fetchTopic, d.fetchPartStr, d.fetchOffStr, d.fetchLimStr
	partition, offset, limit := 0, int64(0), 20
	if partStr != "" {
		fmt.Sscanf(partStr, "%d", &partition)
	}
	if offStr != "" {
		fmt.Sscanf(offStr, "%d", &offset)
	}
	if limStr != "" {
		fmt.Sscanf(limStr, "%d", &limit)
	}

	ctx.Async(func() {
		url := fmt.Sprintf("/api/fetch?topic=%s&partition=%d&offset=%d&limit=%d",
			topic, partition, offset, limit)
		resp, err := http.Get(url)
		ctx.Dispatch(func(ctx app.Context) {
			d.fetching = false
			if err != nil {
				d.fetchErr = err.Error()
				return
			}
			defer resp.Body.Close()
			var msgs []api.FetchedMessage
			if err := json.NewDecoder(resp.Body).Decode(&msgs); err != nil {
				d.fetchErr = err.Error()
				return
			}
			d.fetchResults = msgs
		})
	})
}

// ── guide / settings tab ──────────────────────────────────────────────────────

func (d *Dashboard) renderGuide() app.UI {
	walMode := or(d.stats.WAL.SyncMode, "—")
	walPath := or(d.stats.WAL.Path, "—")

	return app.Div().Body(
		d.guideSection("QUICK START", []string{
			"1.  go run ./cmd/broker/          # start broker  :9090 TCP  :9095 gRPC",
			"2.  go run ./cmd/dashboard/       # open dashboard at :8080",
			"3.  open browser → localhost:8080",
		}),
		d.guideSection("PUBLISH A MESSAGE", []string{
			"Via dashboard  →  click PUBLISH tab, fill topic + payload, hit SEND",
			"Via CLI (TCP)  →  goqueue publish --topic orders \"hello\"",
			"Via CLI (gRPC) →  goqueue publish --grpc --topic orders \"hello\"",
			"With key       →  goqueue publish --topic orders --key user-42 \"hi\"",
			"Batch (TCP)    →  goqueue publish-batch --topic orders --count 1000",
		}),
		d.guideSection("READ MESSAGES", []string{
			"Via dashboard  →  click FETCH tab, set topic + offset + limit",
			"Via CLI        →  goqueue fetch --topic orders --offset 0",
			"Subscribe      →  goqueue consume --topic orders --group myapp",
		}),
		d.guideSection("PARTITION ROUTING", []string{
			"No key         →  round-robin across partitions (3 by default)",
			"With key       →  FNV-32a hash → deterministic partition",
			"Explicit part  →  goqueue publish --grpc --partition 1 --topic orders \"x\"",
		}),
		d.guideSection("CURRENT CONFIG", []string{
			"broker tcp   :9090",
			"broker grpc  :9095",
			"metrics      :2112",
			"wal path     " + walPath,
			"wal sync     " + walMode,
		}),
	)
}

func (d *Dashboard) guideSection(title string, lines []string) app.UI {
	items := make([]app.UI, 0, len(lines))
	for _, l := range lines {
		items = append(items,
			app.P().
				Style("font-size", "11px").Style("color", txt).
				Style("opacity", "0.75").Style("padding", "2px 0").
				Style("white-space", "pre").Text(l),
		)
	}
	return app.Div().Style("margin-bottom", "18px").Body(
		app.P().Style("font-size", "9px").Style("color", accent).
			Style("letter-spacing", "1.2px").Style("margin-bottom", "7px").Text(title),
		app.Div().
			Style("background", "rgba(255,255,255,0.03)").
			Style("border", "1px solid "+glassBorder).
			Style("border-radius", "6px").Style("padding", "10px 14px").
			Body(items...),
	)
}

// ── footer ────────────────────────────────────────────────────────────────────

func (d *Dashboard) renderFooter() app.UI {
	return app.Div().
		Style("display", "flex").Style("justify-content", "space-between").
		Style("align-items", "center").
		Style("border-top", "1px solid "+glassBorder).Style("padding-top", "14px").
		Body(
			app.Span().Style("color", muted).Style("font-size", "10px").
				Text("wal: "+or(d.stats.WAL.Path, "—")+"  sync="+or(d.stats.WAL.SyncMode, "—")),
			app.Span().Style("color", "rgba(255,255,255,0.15)").Style("font-size", "10px").
				Text("goqueue · built in Go → WASM"),
		)
}

// ── form helpers ──────────────────────────────────────────────────────────────

func (d *Dashboard) formRow(label string, input app.UI) app.UI {
	return app.Div().Style("margin-bottom", "10px").Body(
		app.P().
			Style("font-size", "9px").Style("color", muted).
			Style("letter-spacing", "1px").Style("margin-bottom", "4px").
			Text(label),
		app.Div().Style("width", "100%").Body(input),
	)
}

// ── number formatter ──────────────────────────────────────────────────────────

func fmtN(n int64) string {
	if n <= 0 {
		return "0"
	}
	switch {
	case n >= 1_000_000_000:
		return fmt.Sprintf("%.1fB", float64(n)/1e9)
	case n >= 1_000_000:
		return fmt.Sprintf("%.1fM", float64(n)/1e6)
	case n >= 1_000:
		return fmt.Sprintf("%.1fK", float64(n)/1e3)
	default:
		return strconv.FormatInt(n, 10)
	}
}

func or(s, fallback string) string {
	if s == "" {
		return fallback
	}
	return s
}
