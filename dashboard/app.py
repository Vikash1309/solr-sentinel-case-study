"""Solr Sentinel dashboard (doc §9.3) - Streamlit UI over the FastAPI backend.

Run:  streamlit run dashboard/app.py      (API at SENTINEL_API_URL, default http://127.0.0.1:8000)

Shows: cluster summary and node health with an explicit Unknown state; incidents with severity, node, age
and RCA status; filters (time, node, severity, status, event type); incident timeline with
observed/inferred/temporal relationships; evidence with raw-record links; RCA, alternatives, impact and
remediation; model/prompt version and analysis time; and the empty, parse-error, stale-data and
LLM-unavailable states."""
from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime

import altair as alt
import pandas as pd
import streamlit as st

API = os.getenv("SENTINEL_API_URL", "http://127.0.0.1:8000").rstrip("/")
KEY = os.getenv("SENTINEL_API_KEY", "")

STATE_COLOR = {"healthy": "#2e8b57", "degraded": "#c98a12", "critical": "#c8372d", "unknown": "#7b8794"}
SEV_COLOR = {"critical": "#c8372d", "warning": "#c98a12", "info": "#2c6db5"}
REL_COLOR = {"observed": "#2e8b57", "inferred": "#2c6db5", "temporal": "#7b8794"}

st.set_page_config(page_title="Solr Sentinel", page_icon="🛰️", layout="wide")
st.markdown("""<style>
.badge{display:inline-block;padding:2px 9px;border-radius:999px;font-size:12px;font-weight:600;color:#fff;margin-right:4px}
.muted{color:#7b8794;font-size:13px}
.card{border:1px solid rgba(128,128,128,.25);border-radius:10px;padding:12px 14px;margin-bottom:8px}
.kv{font-size:13px;line-height:1.7}
.big{font-size:28px;font-weight:700;line-height:1}
</style>""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------------------------- API client
def _req(method: str, path: str, data: bytes | None = None, headers: dict | None = None, timeout: int = 60):
    h = {"X-API-Key": KEY} if KEY else {}
    h.update(headers or {})
    req = urllib.request.Request(API + path, data=data, headers=h, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        body = r.read()
        return json.loads(body) if r.headers.get_content_type() == "application/json" else body.decode()


@st.cache_data(ttl=5, show_spinner=False)
def get(path: str):
    return _req("GET", path)


def post_json(path: str, payload: dict | None = None):
    return _req("POST", path, json.dumps(payload or {}).encode(), {"content-type": "application/json"}, timeout=300)


def post_upload(node_id: str, source_type: str, name: str, content: bytes):
    boundary = "----sentinel" + str(int(time.time() * 1000))
    parts = []
    for k, v in (("node_id", node_id), ("source_type", source_type)):
        parts.append(f'--{boundary}\r\nContent-Disposition: form-data; name="{k}"\r\n\r\n{v}\r\n'.encode())
    parts.append(f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="{name}"\r\n'
                 f"Content-Type: text/plain\r\n\r\n".encode() + content + b"\r\n")
    parts.append(f"--{boundary}--\r\n".encode())
    return _req("POST", "/api/v1/logs/upload", b"".join(parts), {"content-type": f"multipart/form-data; boundary={boundary}"}, 300)


def badge(text: str, color: str) -> str:
    return f'<span class="badge" style="background:{color}">{text}</span>'


def fmt(v, unit=""):
    return "—" if v is None else f"{v:,.1f}{unit}" if isinstance(v, float) else f"{v}{unit}"


def dt(iso: str) -> datetime:
    return datetime.fromisoformat(iso.replace("Z", "+00:00"))


def hm(iso: str | None) -> str:
    return iso[11:19] if iso else "—"


# ---------------------------------------------------------------------------------------------- load
try:
    ov = get("/api/v1/overview")
except urllib.error.HTTPError as e:
    st.error(f"API error {e.code}: {e.read().decode()[:200]}. Is SENTINEL_API_KEY set the same for the API and the dashboard?")
    st.stop()
except Exception as e:  # API not running
    st.error(f"Cannot reach the Sentinel API at {API} ({e}). Start it with `python run.py api`.")
    st.stop()

# ---------------------------------------------------------------------------------------------- sidebar
with st.sidebar:
    st.markdown("### 🛰️ Solr Sentinel")
    st.caption(f"API {API}")
    c1, c2 = st.columns(2)
    if c1.button("Run analysis", width="stretch"):
        with st.spinner("Analysing..."):
            post_json("/api/v1/analysis/run")
        st.cache_data.clear()
        st.rerun()
    auto = c2.toggle("Auto 15s", value=False)
    with st.expander("Upload logs"):
        up = st.file_uploader("Solr or GC log", type=["log", "txt"])
        node_in = st.text_input("Node ID", "solr-node1")
        src_in = st.selectbox("Source type", ["solr", "gc"])
        if st.button("Upload & analyse", disabled=up is None):
            res = post_upload(node_in, src_in, up.name, up.getvalue())
            st.success(f"Stored as {res['stored_as']}; {res['analysis'].get('incidents', 0)} incidents")
            st.cache_data.clear()

if ov.get("empty"):
    st.title("Solr Sentinel")
    st.info("**No data yet.** Put logs under `sample_data/logs/<node>/{solr,gc}.log` (or run `python run.py generate`), "
            "or upload a file from the sidebar, then click **Run analysis**.")
    if ov.get("parse_errors"):
        st.warning(f"{ov['parse_errors']} record(s) could not be parsed - see the Parse errors tab after data arrives.")
    st.stop()

health = get("/api/v1/health/cluster")
timeline = get("/api/v1/health/timeline")
incidents = get("/api/v1/incidents")
nodes_all = sorted(health.get("nodes", {}))
t_from = dt(ov["data_from"]).replace(second=0, microsecond=0)
t_to = dt(ov["data_to"])

with st.sidebar:
    st.markdown("#### Filters")
    tr = st.slider("Time range (UTC)", min_value=t_from, max_value=t_to, value=(t_from, t_to), format="HH:mm")
    f_nodes = st.multiselect("Nodes", nodes_all, default=nodes_all)
    f_sev = st.multiselect("Severity", ["critical", "warning", "info"], default=["critical", "warning", "info"])
    f_status = st.multiselect("Status", ["open", "resolved"], default=["open", "resolved"])

# ---------------------------------------------------------------------------------------------- header
state = health.get("state", "unknown")
h1, h2 = st.columns([3, 2])
with h1:
    st.markdown(f"## Solr Sentinel &nbsp; {badge('CLUSTER ' + state.upper(), STATE_COLOR[state])}"
                + (badge("PARTIAL ASSESSMENT", "#5b6770") if health.get("partial") else ""), unsafe_allow_html=True)
    st.markdown(f"<span class='muted'>Cluster <b>{health.get('cluster_id', '')}</b> · assessed at "
                f"<b>{hm(ov.get('assessed_at'))} UTC</b> ({ov.get('clock')} clock) · data {hm(ov['data_from'])}–{hm(ov['data_to'])} · "
                f"{ov['events_total']:,} events · {ov['incidents']} incidents ({ov['open_incidents']} open)</span>",
                unsafe_allow_html=True)
with h2:
    st.markdown(f"<div class='muted' style='text-align:right'>LLM <b>{ov['llm']}</b> · RCA queue {ov['rca_queue_depth']}"
                f" · analysis {ov.get('analysis_ms', '—')} ms · auth {'on' if ov.get('auth_enabled') else 'OFF (dev)'}</div>",
                unsafe_allow_html=True)

# ---------------------------------------------------------------------------------------------- state banners
if not ov.get("llm_enabled"):
    st.info("**AI RCA unavailable** (no LLM provider configured). Incidents, evidence and the deterministic RCA are shown; "
            "set a provider (see README) to enable AI analysis.", icon="🤖")
if (ov.get("poller") or {}).get("last_error"):
    st.error(f"Last analysis cycle failed: {ov['poller']['last_error']}")
stale = [(n["node_id"], t["source_type"], t["age_s"]) for n in health["nodes"].values() for t in n["telemetry"] if t["stale"]]
if stale:
    st.warning("**Stale telemetry:** " + "; ".join(f"{n} {s} (no records for {int((a or 0) // 60)} min)" for n, s, a in stale)
               + ". Affected nodes are reported as *unknown*, not healthy and not down.", icon="📡")
if ov.get("parse_errors"):
    st.warning(f"**{ov['parse_errors']} record(s) could not be parsed** and were kept in the parse-error store "
               "(see the *Parse errors* tab).", icon="🧩")
if health.get("partial"):
    with st.expander("Why is the cluster assessment partial?"):
        for r in health["partial_reasons"]:
            st.markdown(f"- {r}")
if health.get("reasons"):
    st.markdown(" · ".join(f"<span class='muted'>{r}</span>" for r in health["reasons"][:4]), unsafe_allow_html=True)

# ---------------------------------------------------------------------------------------------- node cards
cols = st.columns(max(1, len(nodes_all)))
for col, nid in zip(cols, nodes_all, strict=True):
    n = health["nodes"][nid]
    tel = " ".join(badge(f"{t['source_type']} {'stale' if t['stale'] else 'ok'}", "#7b8794" if t["stale"] else "#2e8b57")
                   for t in n["telemetry"])
    reps = ", ".join(f"{c.replace('products_', '')}: {s}" for c, s in sorted(n["replicas"].items()))
    col.markdown(f"""<div class='card' style='border-left:5px solid {STATE_COLOR[n['state']]}'>
<div style='display:flex;justify-content:space-between'><div><b style='font-family:monospace'>{nid}</b><br>{badge(n['state'].upper(), STATE_COLOR[n['state']])}</div>
<div class='big'>{'?' if n['score'] is None or n['state'] == 'unknown' else n['score']}<span class='muted'>/100</span></div></div>
<div class='kv'>Heap after GC <b>{fmt(n['heap_after_gc_pct'], '%')}</b> · Max GC pause <b>{fmt(n['max_gc_pause_ms_5m'], ' ms')}</b><br>
GC overhead <b>{fmt(n['gc_overhead_pct_5m'], '%')}</b> · p95 <b>{fmt(n['p95_qtime_ms_5m'], ' ms')}</b> · errors <b>{fmt(n['errors_5m'])}</b><br>
Leader of <b>{', '.join(n['leader_of']) or '—'}</b><br><span class='muted'>{reps}</span><br>{tel}</div></div>""",
                 unsafe_allow_html=True)

# ---------------------------------------------------------------------------------------------- health timeline
rows = [{"row": "cluster", "t": c["t"], "state": c["state"]} for c in timeline.get("cluster", [])]
for nid, tl in timeline.get("nodes", {}).items():
    rows += [{"row": nid, "t": x["t"], "state": x["state"], "signals": ", ".join(x["active_signals"])} for x in tl]
if rows:
    df = pd.DataFrame(rows)
    df["t"] = pd.to_datetime(df["t"], format="ISO8601", utc=True)
    df = df[(df["t"] >= pd.Timestamp(tr[0])) & (df["t"] <= pd.Timestamp(tr[1]))]
    df["minute"] = df["t"].dt.strftime("%H:%M")
    minutes = sorted(df["minute"].unique())
    order = ["cluster"] + nodes_all
    heat = alt.Chart(df).mark_rect(stroke="white", strokeWidth=0.5).encode(
        x=alt.X("minute:O", title="UTC", sort=minutes,
                axis=alt.Axis(values=[m for m in minutes if m.endswith(("0", "5"))], labelAngle=0)),
        y=alt.Y("row:N", title=None, sort=order),
        color=alt.Color("state:N", scale=alt.Scale(domain=list(STATE_COLOR), range=["#8fcca6", "#e0a526", "#c8372d", "#9aa5b1"]),
                        legend=alt.Legend(orient="top", title=None)),
        tooltip=["row", "minute", "state", "signals"]).properties(height=alt.Step(28), width="container")
    st.altair_chart(heat, width="stretch")

# ---------------------------------------------------------------------------------------------- tabs
t_inc, t_met, t_sig, t_raw, t_perr = st.tabs(["Incidents", "Node metrics", "Signals", "Raw events", "Parse errors"])

with t_inc:
    flt = [i for i in incidents if i["severity"] in f_sev and i["status"] in f_status and set(i["nodes"]) & set(f_nodes)
           and dt(i["last_seen"]) >= tr[0] and dt(i["first_seen"]) <= tr[1]]
    assessed = dt(ov["assessed_at"])
    if not flt:
        st.success("No incidents match the filters.")
    else:
        table = pd.DataFrame([{
            "Incident": i["incident_id"], "Severity": i["severity"], "Status": i["status"], "Title": i["title"],
            "Nodes": ", ".join(i["nodes"]), "First seen": hm(i["first_seen"]),
            "Age (min)": round((assessed - dt(i["first_seen"])).total_seconds() / 60),
            "RCA": f"{i['rca']['status']}" + (f" · {i['rca']['source']}" if i['rca']['source'] else "") +
                   (f" · v{i['rca']['version']}" if i['rca']['version'] else "")} for i in flt])
        st.dataframe(table, hide_index=True, width="stretch")
        ids_f = [i["incident_id"] for i in flt]
        want = st.query_params.get("incident")          # deep link: ?incident=INC-...
        sel = st.selectbox("Incident", ids_f, index=ids_f.index(want) if want in ids_f else 0,
                           format_func=lambda x: f"{x} — {next(i['title'] for i in flt if i['incident_id'] == x)}")
        d = get(f"/api/v1/incidents/{sel}")
        inc, rca, sigs = d["incident"], d["rca"], {s["signal_id"]: s for s in d["signals"]}
        st.markdown(f"### {inc['title']}")
        meta = (badge(inc["severity"].upper(), SEV_COLOR[inc["severity"]]) + badge(inc["status"], "#5b6770") +
                (badge("recurrence of " + inc["recurrence_of"], "#8a5cc2") if inc.get("recurrence_of") else ""))
        st.markdown(meta + f"<span class='muted'>Severity by policy: {inc['severity_reason']} · "
                    f"{hm(inc['first_seen'])}–{hm(inc['last_seen'])} UTC · nodes {', '.join(inc['nodes'])}</span>",
                    unsafe_allow_html=True)
        if not rca:
            st.info(f"RCA status: **{inc['rca_status']}** - the incident and its evidence are available below while the RCA runs.")
        else:
            src = "AI RCA" if rca["source"] == "llm" else "Deterministic RCA"
            st.markdown(f"<span class='muted'>{src} · version {rca['version']} ({rca['trigger']}) · model "
                        f"<b>{rca['model'] or '—'}</b> · prompt <b>{rca['prompt_version']}</b> · "
                        f"{rca['created_at'][:19].replace('T', ' ')} UTC"
                        + (f" · AI RCA not used: <b>{rca['reason'].replace('_', ' ')}</b>" if rca.get("reason") else "")
                        + "</span>", unsafe_allow_html=True)
        rc1, rc2, rc3 = st.columns([1, 1, 2])
        model_override = rc2.text_input("Model override (optional)", "", label_visibility="collapsed",
                                        placeholder="model override (optional)")
        if rc1.button("Re-analyse (new RCA version)"):
            try:
                post_json(f"/api/v1/incidents/{sel}/reanalyze?sync=true", {"model": model_override or None})
                st.cache_data.clear()
                st.rerun()
            except urllib.error.HTTPError as e:
                st.error(f"{e.code}: {e.read().decode()[:200]}")

        s_rca, s_tl, s_ev, s_ver = st.tabs(["Root cause & remediation", "Timeline", "Evidence", "RCA versions"])
        with s_rca:
            if rca and rca.get("report"):
                r = rca["report"]
                rc = r["probable_root_cause"]
                conf_c = {"high": "#2e8b57", "medium": "#c98a12", "low": "#7b8794"}[rc["confidence"]]
                st.markdown(f"**Summary.** {r['incident_summary']}")
                st.markdown(f"#### Probable root cause {badge('confidence ' + rc['confidence'], conf_c)}", unsafe_allow_html=True)
                st.markdown(rc["hypothesis"])
                a, b, c = st.columns(3)
                a.markdown("**Supporting evidence**\n\n" + ("\n".join(f"- `{x}` {sigs[x]['title'] if x in sigs else ''}"
                                                                      for x in rc["supporting_evidence_ids"]) or "—"))
                b.markdown("**Contradicting evidence**\n\n" + ("\n".join(f"- `{x}`" for x in rc["contradicting_evidence_ids"]) or "none"))
                c.markdown("**Missing evidence**\n\n" + ("\n".join(f"- {x}" for x in rc["missing_evidence"]) or "—"))
                oi = r["operational_impact"]
                st.markdown(f"#### Impact {badge(oi['severity'], SEV_COLOR[oi['severity']])}", unsafe_allow_html=True)
                a, b = st.columns(2)
                a.markdown("**Observed symptoms**\n\n" + "\n".join(f"- {x}" for x in oi["observed_symptoms"]))
                b.markdown("**Potential consequences**\n\n" + "\n".join(f"- {x}" for x in oi["potential_consequences"]))
                if oi.get("business_impact"):
                    st.markdown(f"**Business impact.** {oi['business_impact']}")
                st.markdown("#### Recommended remediation")
                st.dataframe(pd.DataFrame([{"Priority": x["priority"], "Type": x["type"], "Action": x["action"],
                                            "Human approval": "required" if x["requires_human_approval"] else "not required",
                                            "Evidence": ", ".join(x["evidence_ids"])} for x in r["recommended_remediation"]]),
                             hide_index=True, width="stretch")
                if r["alternative_hypotheses"]:
                    st.markdown("#### Alternative hypotheses")
                    for x in r["alternative_hypotheses"]:
                        st.markdown(f"- **{x['hypothesis']}** — {x['assessment']}")
                if r["limitations"]:
                    st.markdown("#### Limitations\n" + "\n".join(f"- {x}" for x in r["limitations"]))
                v = rca.get("validation", {})
                st.markdown(f"<span class='muted'>Validation: {'✅ passed' if v.get('valid') else '❌ failed'} · "
                            f"{len(v.get('cited_ids', []))} cited IDs resolved · unverified numbers: "
                            f"{', '.join(v.get('unverified_numbers', [])) or 'none'}"
                            + "".join(f" · ⚠ {w}" for w in v.get("warnings", [])) + "</span>", unsafe_allow_html=True)
                if rca.get("attempts"):
                    with st.expander(f"LLM attempts ({len(rca['attempts'])})"):
                        st.json(rca["attempts"])
            else:
                st.info("RCA pending.")
        with s_tl:
            sdf = pd.DataFrame([{"start": s["start"], "end": s["end"], "node": s["node_id"], "signal": f"{s['title']} · {s['node_id']}",
                                 "type": s["signal_type"], "severity": s["severity"], "id": s["signal_id"],
                                 "root": s["signal_id"] == inc["root_signal_id"], "detail": s["detail"]} for s in sigs.values()])
            if not sdf.empty:
                sdf["start"] = pd.to_datetime(sdf["start"], format="ISO8601", utc=True)
                sdf["end"] = pd.to_datetime(sdf["end"], format="ISO8601", utc=True) + pd.Timedelta(seconds=20)
                g = alt.Chart(sdf).mark_bar(cornerRadius=3).encode(
                    x=alt.X("start:T", title="UTC", scale=alt.Scale(type="utc"), axis=alt.Axis(format="%H:%M")), x2="end:T",
                    y=alt.Y("signal:N", sort=alt.EncodingSortField("start"), title=None, axis=alt.Axis(labelLimit=360)),
                    color=alt.Color("severity:N", scale=alt.Scale(domain=list(SEV_COLOR), range=list(SEV_COLOR.values()))),
                    tooltip=["id", "node", "type", "severity", "detail"]).properties(height=max(160, 22 * len(sdf)))
                st.altair_chart(g, width="stretch")
            st.markdown("**Relationships** (observed = the records reference it · inferred = known mechanism + timing · "
                        "temporal = time proximity only)")
            st.dataframe(pd.DataFrame([{"cause": f"{l['cause']} {sigs.get(l['cause'], {}).get('title', '')} ({sigs.get(l['cause'], {}).get('node_id', '')})",
                                        "relation": l["relation"],
                                        "effect": f"{l['effect']} {sigs.get(l['effect'], {}).get('title', '')} ({sigs.get(l['effect'], {}).get('node_id', '')})",
                                        "reason": l["reason"]} for l in inc["links"]]),
                         hide_index=True, width="stretch")
        with s_ev:
            if rca:
                ev = rca["evidence"]
                st.markdown("**Representative records sent to the model** (search terms redacted)")
                st.dataframe(pd.DataFrame([{"id": x["id"], "node": x["node"], "level": x["level"], "count": x["count"],
                                            "first": hm(x["first_seen"]), "last": hm(x["last_seen"]),
                                            "signals": ", ".join(x.get("signals", [])), "sample": x["sample"]}
                                           for x in ev["records"]]), hide_index=True, width="stretch")
                st.markdown("**Metric summaries**")
                st.dataframe(pd.DataFrame([{"id": m["id"], **{f"before.{k}": v for k, v in (m["baseline_15min_before"] or {}).items()},
                                            **{f"during.{k}": v for k, v in (m["during_incident"] or {}).items()}}
                                           for m in ev["metrics"]]), hide_index=True, width="stretch")
            pick = st.selectbox("Raw records for signal", list(sigs), format_func=lambda x: f"{x} · {sigs[x]['title']} ({sigs[x]['node_id']})")
            raw = get(f"/api/v1/signals/{pick}/events?limit=50")
            st.code("\n".join(e["raw"] for e in raw) or "metric-based signal: no individual records", language="log")
        with s_ver:
            vers = d["rca_versions"]
            if vers:
                st.dataframe(pd.DataFrame(vers), hide_index=True, width="stretch")
                vsel = st.selectbox("Show version", [v["version"] for v in vers][::-1])
                old = get(f"/api/v1/incidents/{sel}/rca?version={vsel}")
                st.markdown(f"**v{vsel}** ({old['source']}, {old.get('model') or 'rules'}): "
                            f"{old['report']['probable_root_cause']['hypothesis'] if old.get('report') else '—'}")

with t_met:
    series = get("/api/v1/node-metrics")
    node_m = st.selectbox("Node", nodes_all, key="metrics_node")
    mdf = pd.DataFrame(series.get(node_m, []))
    if not mdf.empty:
        mdf["t"] = pd.to_datetime(mdf["t"], format="ISO8601", utc=True)
        mdf = mdf[(mdf["t"] >= pd.Timestamp(tr[0])) & (mdf["t"] <= pd.Timestamp(tr[1]))]
        mdf["t"] = mdf["t"].dt.tz_convert(None)  # display UTC wall time
        a, b = st.columns(2)
        a.markdown("**Heap after GC (%)**")
        a.line_chart(mdf.set_index("t")[["heap_after_max_pct"]], height=220)
        b.markdown("**Longest GC pause per minute (ms)**")
        b.bar_chart(mdf.set_index("t")[["gc_pause_max_ms"]], height=220)
        a.markdown("**p95 query time (ms)**")
        a.line_chart(mdf.set_index("t")[["p95_qtime"]], height=220)
        b.markdown("**Requests, ERROR records and HTTP 5xx per minute**")
        b.line_chart(mdf.set_index("t")[["requests", "errors", "http_5xx"]], height=220)

with t_sig:
    ids = [i["incident_id"] for i in incidents]
    st.caption("Signals are rule, threshold or baseline violations; incidents group related signals.")
    sig_rows = []
    for iid in ids:
        dd = get(f"/api/v1/incidents/{iid}")
        for s in dd["signals"]:
            if s["node_id"] in f_nodes and s["severity"] in f_sev:
                sig_rows.append({"start": hm(s["start"]), "end": hm(s["end"]), "node": s["node_id"], "type": s["signal_type"],
                                 "severity": s["severity"], "method": s["method"], "count": s["count"], "incident": iid,
                                 "detail": s["detail"]})
    st.dataframe(pd.DataFrame(sorted(sig_rows, key=lambda x: x["start"])), hide_index=True, width="stretch")

with t_raw:
    c1, c2, c3 = st.columns(3)
    rn = c1.selectbox("Node", nodes_all, key="raw_node")
    et = c2.selectbox("Event type", ["", "solr_log", "solr_request", "solr_slow_request", "solr_update", "gc_pause",
                                     "gc_concurrent", "gc_evacuation_failure"], format_func=lambda x: x or "any")
    lim = c3.number_input("Limit", 10, 2000, 200)
    q = {"node": rn, "since": tr[0].timestamp(), "until": tr[1].timestamp(), "limit": int(lim)}
    if et:
        q["event_type"] = et
    evs = get("/api/v1/events?" + urllib.parse.urlencode(q))
    st.dataframe(pd.DataFrame([{"time": e["timestamp"][11:23], "source": e["source_type"], "level": e["severity"],
                                "type": e["event_type"], "message": e["message"][:300], "file": e["source_file"],
                                "offset": e["source_offset"], "event_id": e["event_id"]} for e in evs]),
                 hide_index=True, width="stretch")

with t_perr:
    pe = get("/api/v1/parse-errors?limit=200")
    st.markdown(f"**{pe['total']}** record(s) could not be parsed. They are kept (never silently dropped) with the reason "
                "and source position, so parsers can be fixed and the records replayed.")
    if pe["items"]:
        st.dataframe(pd.DataFrame(pe["items"])[["node_id", "source_type", "source_file", "source_offset", "reason", "raw"]],
                     hide_index=True, width="stretch")

if auto:
    time.sleep(15)
    st.cache_data.clear()
    st.rerun()
