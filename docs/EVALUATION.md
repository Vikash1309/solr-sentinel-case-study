# Evaluation

Generated 2026-10-16 05:45 UTC by `python run.py eval`. The expected answers are in [eval/ground_truth.json](../eval/ground_truth.json): 4 planted problems and 4 malformed records. Raw results: `eval/results/latest.json`.

## 1. Detection

Each seed regenerates the logs with different background noise (traffic, latency, GC timing). The planted problems stay the same.

| Seed | Events | Signals | Recall | Precision | Incidents | Scenario checks | Unexpected incidents | Parse errors captured | Replay idempotent | Cluster |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 19,078 | 38 | 100% | 100% | 4 | 19/19 | 0 | 4 ✓ | ✓ | degraded |
| 2 | 19,432 | 38 | 100% | 100% | 4 | 19/19 | 0 | 4 ✓ | ✓ | degraded |
| 3 | 19,077 | 38 | 100% | 100% | 4 | 19/19 | 0 | 4 ✓ | ✓ | degraded |
| 4 | 19,300 | 39 | 100% | 100% | 4 | 19/19 | 0 | 4 ✓ | ✓ | degraded |
| 5 | 19,349 | 38 | 100% | 100% | 4 | 19/19 | 0 | 4 ✓ | ✓ | degraded |

*Scenario checks* cover, for each planted problem: the top explanation, the root signal and node, the affected nodes and the severity, plus the repeat link and open status for S3 and the Unknown node for S4. *Replay idempotent* means checkpoints were deleted and every log re-read, with no new events or incidents.

Missed signals: none.  
False alarms: none.  
Failed checks: none.

## 2. Root-cause answers

Each provider explains the same 4 incidents (seed 42).

| Provider | Incidents | LLM answers | Valid 1st try | Fallbacks | Root-cause accuracy | Root signal cited | Root node named | Severity kept | Unverified numbers | Avg latency | Tokens in/out |
|---|---|---|---|---|---|---|---|---|---|---|---|
| rules (deterministic) | 4 | 0 | 0 | n/a | 100% | 100% | 100% | 100% | 0 | 0.0 s | 0/0 |
| gemini:gemini-3.8-flash | 4 | 3 | 3 | 1 | 100% | 100% | 100% | 100% | 0 | 22.9 s | 26,971/4,071 |

The rules row is the baseline. An LLM is worth using only if it matches the baseline on accuracy and evidence, and adds what rules can't: the mechanism, alternatives, and fixes specific to the incident. *Root-cause accuracy* means the explanation names the expected concepts (for example expensive queries, GC and heap, ZooKeeper). A fallback is an LLM call that failed and was replaced by the rules-based report.

### The explanations

- **rules (deterministic) / S1** (rules, confidence medium): Commit storm (too-frequent explicit commits). Earliest triggering condition: 'Excessive explicit commits' on solr-node3 at 10:10:00 UTC (59 explicit hard commits/min opening new searchers (threshold 6)).
- **rules (deterministic) / S2** (rules, confidence medium): Heap exhaustion → long GC pause → ZooKeeper session expiry. Earliest triggering condition: 'Expensive queries (deep paging / unbounded facets)' on solr-node2 at 10:18:02 UTC (348 record(s) between 10:18:02 and 10:41:59 UTC, QTime median 4654 ms / max 7799 ms). Contributing pattern(s): Expensive queries driving heap pressure.
- **rules (deterministic) / S3** (rules, confidence low): Expensive queries driving heap pressure. Earliest triggering condition: 'Expensive queries (deep paging / unbounded facets)' on solr-node2 at 10:55:02 UTC (71 record(s) between 10:55:02 and 10:59:58 UTC, QTime median 4445 ms / max 7721 ms).
- **rules (deterministic) / S4** (rules, confidence low): Telemetry gap (collector or shipping problem). Earliest triggering condition: 'No gc telemetry' on solr-node3 at 10:46:56 UTC (No gc log records from solr-node3 for 13 min (last at 10:46:56 UTC; threshold 5 min)).
- **gemini:gemini-3.8-flash / S1** (llm, confidence high): An indexing client issued rapid explicit commits (59/min) opening new searchers on products_shard2_replica_n10, exceeding the maxWarmingSearchers limit (=2), rejecting commits, and invalidating query caches.
- **gemini:gemini-3.8-flash / S2** (llm, confidence high): Expensive queries (deep paging with large rows and unbounded facet limits) caused excessive humongous allocations directly into the G1 old generation, driving heap occupancy to 99.1% and inducing a stop-the-world Full GC pause of 18234.6 ms that exceeded the 15000 ms ZooKeeper client timeout, resulting in session expiry and replica degradation on solr-node2.
- **gemini:gemini-3.8-flash / S3** (rules, confidence low): Expensive queries driving heap pressure. Earliest triggering condition: 'Expensive queries (deep paging / unbounded facets)' on solr-node2 at 10:55:02 UTC (71 record(s) between 10:55:02 and 10:59:58 UTC, QTime median 4445 ms / max 7721 ms).
- **gemini:gemini-3.8-flash / S4** (llm, confidence high): GC log telemetry pipeline failure or collector/shipping process issue on solr-node3 causing stale GC metrics while the Solr process remained operational.
