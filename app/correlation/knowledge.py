"""Curated, versioned Solr/JVM operations knowledge ("approved runbook material", doc §7.3, §9.1).

Used for: ranking candidate hypotheses (deterministic), the runbook section of the LLM evidence package,
the deterministic fallback RCA, and the remediation allowlist (types + approval requirements, doc §8.3)."""
from __future__ import annotations

from typing import Any

KNOWLEDGE_VERSION = "kb-2026.10.1"

# Alternative explanations that must be considered before accepting a GC/latency correlation (doc §6.4)
GENERIC_ALTERNATIVES = [
    "slow disk I/O (segment merges, page cache pressure)",
    "indexing contention (bulk updates, frequent commits)",
    "workload spike unrelated to memory",
    "Jetty thread-pool saturation",
    "network delay or packet loss between nodes",
    "a downstream shard problem on another node",
    "shared infrastructure issue (host, hypervisor, storage)",
]

# Missing telemetry that limits any RCA built only from logs (doc §7.3 "explicit missing telemetry")
LOG_ONLY_LIMITATIONS = [
    "no OS/host metrics (CPU, disk I/O, network)",
    "no Solr Metrics API or CLUSTERSTATUS snapshot (topology inferred from logs)",
    "no per-client request attribution (Jetty request log)",
    "no thread dumps or heap histogram",
]

R = lambda action, typ, prio, approval=None: {  # noqa: E731
    "action": action, "type": typ, "priority": prio,
    "requires_human_approval": approval if approval is not None else typ != "diagnostic"}

PATTERNS: list[dict[str, Any]] = [
    {
        "id": "HEAP_EXHAUSTION_GC_STALL_ZK_EXPIRY",
        "name": "Heap exhaustion → long GC pause → ZooKeeper session expiry",
        "short": "GC stall caused ZooKeeper session expiry",
        "required": [{"GC_FULL", "GC_LONG_PAUSE", "GC_PAUSE_EXCEEDS_ZK_TIMEOUT"}, {"ZK_SESSION_EXPIRED", "ZK_DISCONNECTED"}],
        "optional": ["GC_HEAP_PRESSURE", "GC_EVAC_FAILURE", "GC_PAUSE_EXCEEDS_ZK_TIMEOUT", "NODE_LOST", "LEADER_ELECTION",
                     "REPLICA_RECOVERY", "NO_LEADER", "REMOTE_TIMEOUT", "EXPENSIVE_QUERIES", "GC_HUMONGOUS"],
        "excludes": [],
        "mechanism": ("When the old generation fills, G1 falls back to evacuation failures and full compactions. All application "
                      "threads, including the ZooKeeper client heartbeat, stop. A pause longer than zkClientTimeout expires the "
                      "session, removes the node's live_node entry, and leaders hosted there are re-elected elsewhere. When the "
                      "JVM resumes, its replicas must recover (PeerSync or full replication)."),
        "alternatives": ["ZooKeeper ensemble or network problem (would show expiry WITHOUT a matching long pause)",
                         "host-level stall (VM pause, swap) - check OS metrics"],
        "remediation": [
            R("Capture a heap histogram / heap dump of the affected node to confirm what retains memory", "diagnostic", "high"),
            R("Identify the client issuing expensive requests (Jetty request log) and confirm the timing", "diagnostic", "high"),
            R("Throttle or block the offending request pattern at the gateway/proxy", "mitigation", "high"),
            R("Controlled restart of the affected node if heap after GC stays above 90% after load is reduced", "mitigation", "medium"),
            R("Right-size the heap from the measured live set and review zkClientTimeout against worst-case pauses", "corrective", "medium"),
            R("Isolate reporting workloads (dedicated replicas or collection) and enable memory circuit breakers", "corrective", "medium"),
        ],
        "checks": ["heap histogram/dump", "Jetty request log for the client", "ZooKeeper server log for the expiry time", "OS metrics for host stalls"],
    },
    {
        "id": "EXPENSIVE_QUERY_MEMORY_PRESSURE",
        "name": "Expensive queries driving heap pressure",
        "short": "Expensive queries exhausting the heap",
        "required": [{"EXPENSIVE_QUERIES"}, {"GC_HUMONGOUS", "GC_HEAP_PRESSURE", "GC_OVERHEAD", "GC_FULL"}],
        "optional": ["CIRCUIT_BREAKER", "LATENCY_DEGRADATION", "GC_EVAC_FAILURE", "GC_LONG_PAUSE", "GC_HUMONGOUS", "GC_HEAP_PRESSURE"],
        "excludes": [],
        "mechanism": ("Deep paging (large start/rows), tens of thousands of rows and facet.limit=-1 / pivot facets allocate large "
                      "per-request structures. These are humongous objects for G1, allocated directly in the old generation, so "
                      "concurrent heavy requests raise the live set quickly."),
        "alternatives": ["undersized heap for the normal workload", "cache sizing (filterCache, fieldValueCache) inflating the live set"],
        "remediation": [
            R("Identify the client and the exact request parameters (rows, start, facet.limit)", "diagnostic", "high"),
            R("Rate-limit or pause the client issuing the expensive requests", "mitigation", "high"),
            R("Replace deep paging with cursorMark, cap rows, set explicit facet.limit; use /export for bulk reads", "corrective", "medium"),
            R("Add a request guard rejecting rows > N or facet.limit=-1 for interactive collections", "corrective", "low"),
        ],
        "checks": ["distribution of rows/start/facet params per client", "humongous region counts over time"],
    },
    {
        "id": "UNDERSIZED_HEAP_OR_LEAK",
        "name": "Undersized heap or memory retention",
        "short": "Sustained heap pressure (sizing or retention)",
        "required": [{"GC_HEAP_PRESSURE"}, {"GC_FULL", "GC_OVERHEAD", "GC_LONG_PAUSE"}],
        "optional": ["GC_EVAC_FAILURE", "SOLR_OOM", "CIRCUIT_BREAKER"],
        "excludes": ["EXPENSIVE_QUERIES"],
        "mechanism": ("The live set after GC is close to the maximum heap without a workload change: cache sizing, field cache on "
                      "fields without docValues, or a retention bug. High heap alone does not prove the heap should be increased."),
        "alternatives": ["workload change not visible in logs", "indexing pressure"],
        "remediation": [
            R("Capture a heap dump and inspect the dominator tree", "diagnostic", "high"),
            R("Review cache sizes/hit ratios and docValues on sort/facet fields", "diagnostic", "medium"),
            R("Controlled restart to restore service after capturing diagnostics", "mitigation", "medium"),
            R("Fix the confirmed retention or resize the heap based on measured live set", "corrective", "medium"),
        ],
        "checks": ["heap dump", "cache metrics"],
    },
    {
        "id": "COMMIT_STORM",
        "name": "Commit storm (too-frequent explicit commits)",
        "short": "Commit storm from an indexing client",
        "required": [{"EXCESSIVE_COMMITS", "SEARCHER_LIMIT", "OVERLAPPING_SEARCHERS"}],
        "optional": ["SEARCHER_LIMIT", "OVERLAPPING_SEARCHERS", "LATENCY_DEGRADATION", "EXCESSIVE_COMMITS", "SLOW_QUERIES", "GC_OVERHEAD"],
        "excludes": [],
        "mechanism": ("Every commit with openSearcher=true opens and warms a new searcher. Commits arriving faster than warming "
                      "completes overlap; Solr rejects further commits at maxWarmingSearchers and discards caches each time, so "
                      "query latency rises."),
        "alternatives": ["slow warming queries (autowarm too large)", "disk I/O slowing searcher opening"],
        "remediation": [
            R("Identify the indexing client sending commit=true (Jetty request log)", "diagnostic", "high"),
            R("Ask the client to stop per-batch commits; rely on autoCommit/autoSoftCommit or commitWithin", "mitigation", "high"),
            R("Reduce autowarmCount; enforce commit policy with an update processor that ignores client commits", "corrective", "medium"),
        ],
        "checks": ["which client sends commit=true", "searcher warmup time"],
    },
    {
        "id": "ZK_INSTABILITY",
        "name": "ZooKeeper connectivity problem",
        "short": "ZooKeeper connectivity problem",
        "required": [{"ZK_SESSION_EXPIRED", "ZK_DISCONNECTED"}],
        "optional": ["NODE_LOST", "LEADER_ELECTION", "REPLICA_RECOVERY", "NO_LEADER"],
        "excludes": ["GC_FULL", "GC_LONG_PAUSE", "GC_PAUSE_EXCEEDS_ZK_TIMEOUT"],
        "mechanism": "Session loss without a matching JVM pause points to network issues or an unhealthy ZooKeeper ensemble.",
        "alternatives": ["host stall not visible in GC logs"],
        "remediation": [
            R("Check ZooKeeper ensemble health (mntr/ruok) and network between Solr and ZooKeeper", "diagnostic", "high"),
            R("Move ZooKeeper transaction logs to dedicated disks if latency is high", "corrective", "medium"),
        ],
        "checks": ["ZooKeeper server logs", "network loss between hosts"],
    },
    {
        "id": "RESOURCE_EXHAUSTION",
        "name": "OS / JVM resource exhaustion",
        "short": "Resource exhaustion",
        "required": [{"DISK_FULL", "TOO_MANY_OPEN_FILES", "SOLR_OOM"}],
        "optional": ["ERROR_RATE_SPIKE", "NODE_LOST"],
        "excludes": [],
        "mechanism": "Disk, file descriptors or heap are exhausted; Solr can no longer write segments or serve requests.",
        "alternatives": [],
        "remediation": [
            R("Confirm the exhausted resource (df -h, ulimit -n, heap)", "diagnostic", "high"),
            R("Free the resource following the approved runbook", "mitigation", "high"),
            R("Capacity alerts at 75% utilisation and correct limits", "corrective", "medium"),
        ],
        "checks": ["df -h", "ulimit -n", "lsof counts"],
    },
    {
        "id": "TELEMETRY_GAP",
        "name": "Telemetry gap (collector or shipping problem)",
        "short": "Telemetry gap",
        "required": [{"TELEMETRY_STALE"}],
        "optional": [],
        "excludes": [],
        "mechanism": ("A log source stopped arriving. This is a telemetry incident: it reduces confidence in health assessment "
                      "but is not evidence that the node is down while other sources from the node keep reporting."),
        "alternatives": ["the JVM/node actually stopped (check the other source and Solr health API)", "log rotation misconfiguration"],
        "remediation": [
            R("Check the collector/agent status and the log file on the node", "diagnostic", "high"),
            R("Verify node liveness independently (Solr health API / ping)", "diagnostic", "high"),
            R("Restart the collector following the runbook", "mitigation", "medium"),
        ],
        "checks": ["collector status", "Solr ping / CLUSTERSTATUS"],
    },
]
PATTERN_BY_ID: dict[str, dict[str, Any]] = {p["id"]: p for p in PATTERNS}

# Supported impact statements: evidence -> wording (doc §8.2). Never invent percentages or customer counts.
IMPACT_STATEMENTS = {
    "REMOTE_TIMEOUT": "Search operations fanned out to the affected node were delayed or failed (HTTP 500).",
    "NO_LEADER": "Updates to the affected shard were rejected until a new leader was elected (HTTP 503).",
    "SEARCHER_LIMIT": "Indexing commits were rejected (HTTP 503); data freshness may be delayed if the client does not retry.",
    "LATENCY_DEGRADATION": "Search latency increased on the affected node(s).",
    "REPLICA_RECOVERY": "Redundancy was reduced while replicas recovered.",
    "LEADER_ELECTION": "Shard leadership moved; the remaining nodes carry more load.",
    "GC_LONG_PAUSE": "Processing capacity fell and latency rose during stop-the-world pauses.",
    "GC_FULL": "Processing capacity fell and latency rose during stop-the-world pauses.",
    "TELEMETRY_STALE": "Confidence in the health assessment of the node is reduced.",
    "EXPENSIVE_QUERIES": "Expensive requests consumed memory and CPU shared with interactive search traffic.",
}


def match_patterns(types: set[str]) -> list[dict]:
    out = []
    for p in PATTERNS:
        if not all(types & grp for grp in p["required"]):
            continue
        opt = [o for o in p["optional"] if o in types]
        score = (len(p["required"]) + len(opt)) / (len(p["required"]) + len(p["optional"]))
        contra = [x for x in p["excludes"] if x in types]
        score -= 0.3 * len(contra)
        if score <= 0:
            continue
        out.append({"pattern_id": p["id"], "name": p["name"], "score": round(score, 2),
                    "matched": sorted({t for g in p["required"] for t in g if t in types} | set(opt)),
                    "contradicting": contra})
    return sorted(out, key=lambda x: -x["score"])
