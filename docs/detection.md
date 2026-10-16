# Detection logic

Detection is plain code. No LLM reads log lines. It runs in three layers and produces **signals**. Correlation
then groups signals into **incidents**, and health is judged from both.

* A **signal** is one problem on one node, such as "long GC pause on node2, 10:31–10:33".
* An **incident** is a group of related signals with one likely root cause.
* **Health** is the current state of each node and of the cluster.

Repeated log lines are grouped into bursts (same type, less than 2 minutes apart), so 300 timeout lines become
one signal, not 300.

## Layer 1: known failure patterns

21 rules match well-known Solr, ZooKeeper and JVM messages (`app/detection/rules.py`).

| Area | Signal | Severity |
|---|---|---|
| GC | `GC_LONG_PAUSE` (pause ≥ 1 s), `GC_FULL`, `GC_EVAC_FAILURE`, `GC_HUMONGOUS` (≥ 5 large allocations) | warning |
| | `GC_PAUSE_EXCEEDS_ZK_TIMEOUT`: pause longer than the ZooKeeper timeout, so the node will be dropped | critical |
| ZooKeeper | `ZK_DISCONNECTED` | warning |
| | `ZK_SESSION_EXPIRED` | critical |
| Replication | `LEADER_ELECTION`, `REPLICA_RECOVERY` | warning |
| | `NODE_LOST`, `NO_LEADER`, `RECOVERY_FAILED` | critical |
| Queries | `SLOW_QUERIES` (≥ 5 queries over 1 s) | warning |
| | `EXPENSIVE_QUERIES`: ≥ 3 queries with `rows` ≥ 5000, `start` ≥ 10000, `facet.limit=-1`, `group.limit=-1` or facet pivots | warning |
| | `REMOTE_TIMEOUT`: one node timing out on another; the target node is read from the URL | warning |
| Indexing | `SEARCHER_LIMIT` (commits rejected), `OVERLAPPING_SEARCHERS` | warning |
| Resources | `CIRCUIT_BREAKER` | warning |
| | `SOLR_OOM`, `DISK_FULL`, `TOO_MANY_OPEN_FILES` | critical |

## Layer 2: thresholds

Per node, per minute:

| Signal | Fires when |
|---|---|
| `GC_HEAP_PRESSURE` | Heap still ≥ 85 % full *after* GC (critical at 92 %) |
| `GC_OVERHEAD` | GC takes ≥ 10 % of the minute (critical at 25 %) |
| `EXCESSIVE_COMMITS` | More than 6 explicit commits a minute |
| `TELEMETRY_STALE` | A log source has been silent for 5 minutes |

A silent source makes the node **Unknown**, not "down". Missing logs prove only that the logs are missing.

## Layer 3: baselines

Some metrics have no fixed safe value, such as query latency (p95) or the rate of uncategorised errors. Each
one is compared with its own recent history, the last 30 minutes, using a robust z-score:

```
robust_z = (value − median) / (1.4826 × MAD)        flag when robust_z > 3.5
MAD      = median(|xᵢ − median|)
```

Median and MAD are used instead of mean and standard deviation because one spike barely moves them. The
baseline needs at least 5 samples, has a floor for when MAD is 0, and leaves out minutes already flagged, so an
incident cannot raise its own baseline. Signals: `LATENCY_DEGRADATION`, `ERROR_RATE_SPIKE`.

All thresholds are settings in `app/config.py`.

## Correlation

`app/correlation/engine.py` links signals by cause and effect. Every link says how strong its support is:

| Link | Meaning | Example |
|---|---|---|
| **observed** | The logs state it directly | A timeout names the node it timed out on |
| **inferred** | A known mechanism, with the right order and timing | Heap stays full → full GC |
| **temporal** | They happened at the same time, nothing more | Latency rose while something else happened |

Then:

1. Linked signals form one incident, and overlapping groups merge. The incident ID is built from the root signal,
   the node and a 10-minute bucket, so re-running the analysis updates the incident instead of duplicating it.
2. The likely root cause is the earliest signal that nothing else explains. Known patterns rank the
   explanations, for example `HEAP_EXHAUSTION_GC_STALL_ZK_EXPIRY`, `EXPENSIVE_QUERY_MEMORY_PRESSURE`,
   `COMMIT_STORM` and `TELEMETRY_GAP`.
3. Other explanations are kept, not thrown away. A GC pause just before a timeout is a strong lead, not proof:
   slow disks, network trouble or thread-pool exhaustion could look the same. These go to the RCA as
   alternatives.
4. If the same root signal reappears on the same node within 6 hours of an incident ending, the new incident is
   marked **Recurring** and linked to the old one.

## Severity

Code sets incident severity (`severity_policy`), and the LLM cannot change it:

* **critical**: something that hurts availability (no leader, node lost, out of memory, disk full, failed
  recovery, ZooKeeper session expired), or a critical signal backed by other correlated signals.
* **warning**: everything else that matters. A single critical-looking signal with nothing else behind it is
  capped here. One long GC pause is not an outage.
* **info**: minor. A single info signal never becomes an incident.

## Health

| State | Meaning |
|---|---|
| Healthy | No significant problems, and logs are current |
| Degraded | Real problems, but the node is probably still serving |
| Critical | Strong evidence of serious impact |
| Unknown | Logs are missing or stale, so we can't tell |

Cluster health is **not** an average of the nodes. It depends on whether every shard still has a leader and
enough replicas, and how many nodes are Unknown. When the topology is only partly known, the result is marked
*partial*.

## How the sample incidents are caught

| Scenario | Signals, in causal order | Result |
|---|---|---|
| S1 commit storm, node3 | excessive commits → overlapping searchers → searcher limit, plus slower queries | warning |
| S2 heavy queries, node2 | expensive queries → humongous allocations → heap pressure → evacuation failure → full GC → pause longer than ZooKeeper timeout → session expired → node lost, leader election, no leader and timeouts on node1 and node3 | critical, 3 nodes, root cause = the queries |
| S3 queries return, node2 | expensive queries → humongous allocations → heap pressure → long GC pause | warning, open, linked to S2 |
| S4 GC log stops, node3 | telemetry stale | warning, node3 Unknown |

Across 5 random seeds, every planted problem is found with no false alarms. See [EVALUATION.md](EVALUATION.md).
