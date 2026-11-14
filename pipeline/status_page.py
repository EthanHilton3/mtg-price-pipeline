"""Build the public status page:  python -m pipeline.status_page  ->  site/index.html

Runs at the end of every workflow run (even failed ones). Reads only small
tables (ingest_runs, daily_counts, top_movers), so it's cheap to run hourly.
The page is static HTML: no database connection or token ever reaches the browser.

"Last run N minutes ago" is computed by the viewer's browser, not here. If the
schedule silently dies, the page stops being rebuilt and visibly goes stale.
"""
import html
import json
from datetime import datetime, timezone
from pathlib import Path

from pipeline import store
from pipeline.healthcheck import check

OUT = Path(__file__).resolve().parent.parent / "site"
REPO = "https://github.com/EthanHilton3/mtg-price-pipeline"


def esc(v) -> str:
    return html.escape("" if v is None else str(v))


def iso(ts) -> str:
    return ts.replace(tzinfo=timezone.utc).isoformat() if ts else ""


def bar_chart(days) -> str:
    """Inline SVG: rows per price_date. Seed days muted, pipeline-loaded days in accent."""
    if not days:
        return "<p class='muted'>No data yet.</p>"
    w, h, pad = 900, 220, 28
    peak = max(n for _, n, _ in days)
    bw = (w - pad) / len(days)
    bars = []
    for i, (d, n, from_seed) in enumerate(days):
        bh = (h - pad) * n / peak
        cls = "seed" if from_seed > n / 2 else "live"
        bars.append(
            f"<rect class='{cls}' x='{pad + i * bw + 0.5:.1f}' y='{h - pad - bh:.1f}' "
            f"width='{max(bw - 1, 1):.1f}' height='{bh:.1f}'>"
            f"<title>{d}: {n:,} rows ({'seed' if cls == 'seed' else 'loaded by pipeline'})</title></rect>")
    first, last = days[0][0], days[-1][0]
    return (f"<svg viewBox='0 0 {w} {h}' role='img' aria-label='Rows per day'>"
            f"<text class='axis' x='{pad}' y='12'>{peak:,} rows</text>"
            f"<line class='grid' x1='{pad}' x2='{w}' y1='{h - pad}' y2='{h - pad}'/>"
            + "".join(bars) +
            f"<text class='axis' x='{pad}' y='{h - 8}'>{first}</text>"
            f"<text class='axis' x='{w}' y='{h - 8}' text-anchor='end'>{last}</text></svg>")


def main() -> None:
    con = store.connect()
    if con.execute("SELECT count(*) FROM daily_counts").fetchall()[0][0] == 0:
        store.refresh_gold(con)   # self-heal: first run after adding the gold tables

    health = check(con)
    days = con.execute(
        "SELECT price_date, n_rows, n_rows_from_seed FROM daily_counts ORDER BY price_date").fetchall()
    total_rows = sum(n for _, n, _ in days)
    n_cards = con.execute("SELECT count(*) FROM cards").fetchall()[0][0]
    runs = con.execute("""
        SELECT started_at, finished_at, trigger, mode, status, rows_inserted, rows_updated, message
        FROM ingest_runs ORDER BY started_at DESC LIMIT 24
    """).fetchall()
    last_run = con.execute("""
        SELECT started_at, status, mode FROM ingest_runs
        WHERE mode <> 'seed' ORDER BY started_at DESC LIMIT 1
    """).fetchall()
    day_stats = con.execute("""
        SELECT count(*), count_if(status = 'success'), count_if(status = 'failed'),
               count_if(mode IN ('daily', 'backfill'))
        FROM ingest_runs WHERE started_at > current_timestamp::TIMESTAMP - INTERVAL 24 HOUR
    """).fetchall()[0]
    movers = con.execute("""
        SELECT rank, name, set_name, finish, avg_prior, today, pct_change, as_of
        FROM top_movers ORDER BY rank
    """).fetchall()

    last_ts, last_status, last_mode = last_run[0] if last_run else (None, None, None)
    status_json = json.dumps({
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "last_run_at": iso(last_ts),
        "last_run_status": last_status,
        "problems": health["problems"],
    })

    run_rows = "".join(
        f"<tr><td><time data-ts='{iso(r[0])}'>{esc(r[0])} UTC</time></td><td>{esc(r[2])}</td>"
        f"<td><span class='pill {esc(r[3])}'>{esc(r[3])}</span></td>"
        f"<td><span class='pill {esc(r[4])}'>{esc(r[4])}</span></td>"
        f"<td class='num'>{'' if r[5] is None else f'{r[5]:,}'}</td>"
        f"<td class='num'>{'' if r[6] is None else f'{r[6]:,}'}</td>"
        f"<td class='msg'>{esc(r[7])}</td></tr>" for r in runs)
    mover_rows = "".join(
        f"<tr><td class='num'>{m[0]}</td><td>{esc(m[1])}</td><td class='muted'>{esc(m[2])}</td>"
        f"<td>{esc(m[3])}</td><td class='num'>${m[4]:,.2f}</td><td class='num'>${m[5]:,.2f}</td>"
        f"<td class='num up'>+{m[6]:,.1f}%</td></tr>" for m in movers)
    movers_as_of = movers[0][7] if movers else ""

    page = f"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>MTG Price Pipeline Status</title>
<style>
:root {{ --bg:#f7f7f5; --card:#fff; --ink:#1c1c1a; --muted:#6b6b66; --line:#e3e2dd;
  --accent:#2f6fde; --seed:#c9c8c1; --ok:#1f8a4c; --warn:#b7791f; --bad:#c53030; }}
@media (prefers-color-scheme: dark) {{ :root {{ --bg:#141413; --card:#1e1e1c; --ink:#ecebe6;
  --muted:#9b9a93; --line:#33332f; --accent:#6b9cf0; --seed:#4a4a45; --ok:#48bb78; --warn:#ecc94b; --bad:#fc8181; }} }}
* {{ box-sizing:border-box }}
body {{ margin:0; background:var(--bg); color:var(--ink);
  font:15px/1.5 system-ui,-apple-system,"Segoe UI",sans-serif; }}
main {{ max-width:1000px; margin:0 auto; padding:24px 16px 48px }}
h1 {{ font-size:22px; margin:0 0 4px }} h2 {{ font-size:16px; margin:0 0 12px }}
a {{ color:var(--accent) }} .muted {{ color:var(--muted) }}
.card {{ background:var(--card); border:1px solid var(--line); border-radius:10px; padding:16px; margin-top:16px }}
.banner {{ display:flex; gap:12px; align-items:center; font-size:17px; font-weight:600 }}
.dot {{ width:14px; height:14px; border-radius:50%; background:var(--muted); flex:none }}
.banner.ok .dot {{ background:var(--ok) }} .banner.warn .dot {{ background:var(--warn) }}
.banner.bad .dot {{ background:var(--bad) }}
.tiles {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(170px,1fr)); gap:12px; margin-top:16px }}
.tile {{ background:var(--card); border:1px solid var(--line); border-radius:10px; padding:12px 14px }}
.tile b {{ display:block; font-size:22px; font-variant-numeric:tabular-nums }}
.tile span {{ color:var(--muted); font-size:13px }}
svg {{ width:100%; height:auto; display:block }}
rect.seed {{ fill:var(--seed) }} rect.live {{ fill:var(--accent) }}
.axis {{ fill:var(--muted); font-size:12px }} .grid {{ stroke:var(--line) }}
.legend {{ font-size:13px; color:var(--muted); margin-top:8px }}
.legend i {{ display:inline-block; width:10px; height:10px; border-radius:2px; margin:0 4px 0 12px }}
.scroll {{ overflow-x:auto }}
table {{ width:100%; border-collapse:collapse; font-size:13px }}
th,td {{ text-align:left; padding:6px 8px; border-bottom:1px solid var(--line); white-space:nowrap }}
th {{ color:var(--muted); font-weight:500 }}
td.num {{ text-align:right; font-variant-numeric:tabular-nums }} td.msg {{ white-space:normal; color:var(--muted) }}
.up {{ color:var(--ok) }}
.pill {{ padding:1px 8px; border-radius:99px; font-size:12px; background:var(--line) }}
.pill.success {{ background:color-mix(in srgb,var(--ok) 20%,transparent) }}
.pill.failed {{ background:color-mix(in srgb,var(--bad) 25%,transparent) }}
.pill.running {{ background:color-mix(in srgb,var(--warn) 25%,transparent) }}
.pill.daily,.pill.backfill {{ background:color-mix(in srgb,var(--accent) 22%,transparent) }}
footer {{ margin-top:24px; font-size:13px; color:var(--muted) }}
</style></head>
<body><main>
<h1>MTG Card Price Pipeline</h1>
<div class="muted">MTGJSON &rarr; GitHub Actions (hourly) &rarr; MotherDuck &middot; <a href="{REPO}">source</a></div>

<div class="card"><div id="banner" class="banner"><span class="dot"></span><span id="banner-text">Checking&hellip;</span></div>
<div id="banner-sub" class="muted" style="margin-top:6px"></div></div>

<div class="tiles">
  <div class="tile"><span>Price rows</span><b>{total_rows:,}</b></div>
  <div class="tile"><span>Newest prices</span><b>{esc(health['newest'])}</b></div>
  <div class="tile"><span>Days of history</span><b>{len(days)}</b></div>
  <div class="tile"><span>Cards + tokens</span><b>{n_cards:,}</b></div>
  <div class="tile"><span>Runs in last 24h</span><b>{day_stats[0]}</b></div>
  <div class="tile"><span>Failed in last 24h</span><b>{day_stats[2]}</b></div>
</div>

<div class="card"><h2>Rows per day</h2>{bar_chart(days)}
<div class="legend"><i style="background:var(--seed)"></i>one-time seed<i style="background:var(--accent)"></i>loaded by the hourly pipeline</div></div>

<div class="card"><h2>Recent runs</h2>
<p class="muted" style="margin-top:-6px">MTGJSON publishes once a day, so most hourly runs are <em>skip</em>: it checked, and there was nothing new.</p>
<div class="scroll"><table><thead><tr><th>Started</th><th>Trigger</th><th>Mode</th><th>Status</th>
<th class="num">Inserted</th><th class="num">Updated</th><th>Message</th></tr></thead>
<tbody>{run_rows}</tbody></table></div></div>

<div class="card"><h2>Biggest movers ({esc(movers_as_of)})</h2>
<p class="muted" style="margin-top:-6px">TCGplayer retail, newest day vs. that printing's average over the prior days. Cards &ge; $5.</p>
<div class="scroll"><table><thead><tr><th>#</th><th>Card</th><th>Set</th><th>Finish</th>
<th class="num">Prior avg</th><th class="num">Today</th><th class="num">Change</th></tr></thead>
<tbody>{mover_rows}</tbody></table></div></div>

<footer>Page built <time data-ts="{datetime.now(timezone.utc).isoformat()}"></time>. If this page stops updating, the schedule has stopped.</footer>
</main>
<script>
const S = {status_json};
const fmtAgo = ms => {{ const m = Math.round(ms / 60000);
  return m < 60 ? m + " min ago" : m < 2880 ? (m / 60).toFixed(1) + " hours ago" : Math.round(m / 1440) + " days ago"; }};
function render() {{
  const b = document.getElementById("banner"), t = document.getElementById("banner-text"),
        sub = document.getElementById("banner-sub");
  if (!S.last_run_at) {{ t.textContent = "No pipeline runs yet"; return; }}
  const age = Date.now() - new Date(S.last_run_at);
  let cls = "ok", msg = "Pipeline healthy";
  if (S.last_run_status === "failed") {{ cls = "bad"; msg = "Last run failed"; }}
  else if (age > 3 * 3600e3) {{ cls = "bad"; msg = "Pipeline appears stopped"; }}
  else if (age > 1.5 * 3600e3 || S.problems.length) {{ cls = "warn"; msg = "Pipeline delayed"; }}
  b.className = "banner " + cls;
  t.textContent = msg + " \\u00b7 last run " + fmtAgo(age);
  sub.textContent = S.problems.length ? "Healthcheck: " + S.problems.join("; ")
    : "Runs hourly at :17 UTC. Last run: " + new Date(S.last_run_at).toLocaleString();
  document.querySelectorAll("time[data-ts]").forEach(el => {{
    const d = new Date(el.dataset.ts); if (isNaN(d)) return;
    el.textContent = d.toLocaleString([], {{month:"short", day:"numeric", hour:"numeric", minute:"2-digit"}});
    el.title = fmtAgo(Date.now() - d); }});
}}
render(); setInterval(render, 30000);
</script>
</body></html>"""

    OUT.mkdir(exist_ok=True)
    (OUT / "index.html").write_text(page, encoding="utf-8")
    (OUT / "status.json").write_text(status_json, encoding="utf-8")
    print(f"wrote {OUT / 'index.html'} ({len(page):,} bytes, {len(runs)} runs, {len(days)} days)")


if __name__ == "__main__":
    main()
