# Evaluation

Generated 2026-10-16 09:52 UTC by `python run.py eval` with seeds 1, 2, 3, 4, 5. Expected answers: [eval/ground_truth.json](../eval/ground_truth.json). Raw results: `eval/results/latest.json`.

Every detection mode below is scored by the same function, so the numbers are comparable.

## 1. Full run: all four problems planted

Each seed changes the background noise (traffic, latency, GC timing); the planted problems stay the same.

| Seed | Events | Signals | Recall | Precision | Incident checks | Unexpected incidents | Parse errors | Replay idempotent | Time to detect S1 / S2 / S3 / S4 |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 19,078 | 38 | 100% | 100% | 19/19 | 0 | 4 ✓ | ✓ | 6 s / 15 s / 13 s / 359 s |
| 2 | 19,432 | 38 | 100% | 100% | 19/19 | 0 | 4 ✓ | ✓ | 1 s / 16 s / 9 s / 353 s |
| 3 | 19,077 | 38 | 100% | 100% | 19/19 | 0 | 4 ✓ | ✓ | 12 s / 9 s / 14 s / 348 s |
| 4 | 19,300 | 39 | 100% | 100% | 19/19 | 0 | 4 ✓ | ✓ | 8 s / 18 s / 10 s / 357 s |
| 5 | 19,349 | 38 | 100% | 100% | 19/19 | 0 | 4 ✓ | ✓ | 3 s / 12 s / 4 s / 358 s |

*Incident checks*: per planted problem, the top explanation, root signal and node, affected nodes and severity, plus the repeat link and open status for S3 and the Unknown node for S4. *Replay idempotent*: checkpoints deleted and every log re-read, with no new events or incidents. *Time to detect*: from the start of the problem's labelled window to the moment the first required rule could fire (its burst reached the minimum size, its one-minute metric window closed, or silent telemetry passed the 5-minute staleness threshold). In live use, add up to one poll interval (15 s). S4 is detected by design only after the staleness threshold.

Missed signals: none. False alarms: none. Failed checks: none.

## 2. Control run: nothing planted

Background traffic only, so every signal or incident here is a false alarm.

| Seed | Events | Signals | Incidents | False alarms |
|---|---|---|---|---|
| 1 | 17,125 | 0 | 0 | none |
| 2 | 17,325 | 0 | 0 | none |
| 3 | 17,141 | 0 | 0 | none |
| 4 | 17,090 | 0 | 0 | none |
| 5 | 17,339 | 0 | 0 | none |

## 3. Each problem on its own

Each planted problem generated alone, across all seeds. It should produce exactly its own incident, correctly explained, without depending on the other problems being present. (S3's repeat link to S2 is not checked here, because S2 is absent.)

| Problem | Runs | Recall | Precision | Incident checks | Unexpected incidents |
|---|---|---|---|---|---|
| S1 | 5 | 100% | 100% | 20/20 | 0 |
| S2 | 5 | 100% | 100% | 20/20 | 0 |
| S3 | 5 | 100% | 100% | 25/25 | 0 |
| S4 | 5 | 100% | 100% | 25/25 | 0 |

Recall and precision show the worst seed.

## 4. Threshold sensitivity

Each detection threshold moved down and up by 25 % (integers by at least one step), one at a time, on the first seed. This shows how much the results depend on the exact numbers chosen.

| Threshold | −25 % | +25 % |
|---|---|---|
| GC pause | 750, 3750: all 19 checks ✓ | 1250, 6250: all 19 checks ✓ |
| GC overhead % | 7, 18: all 19 checks ✓ | 13, 32: all 19 checks ✓ |
| Heap after GC % | 63, 69: 18/19 · S3.severity | 99, 99: 19/19 · missed S3:GC_HEAP_PRESSURE@solr-node2 |
| Humongous burst | 3: all 19 checks ✓ | 7: all 19 checks ✓ |
| Slow query ms | 750: all 19 checks ✓ | 1250: all 19 checks ✓ |
| Expensive query burst | 2: all 19 checks ✓ | 4: all 19 checks ✓ |
| Commits per minute | 4: all 19 checks ✓ | 8: all 19 checks ✓ |
| Remote timeout burst | 1: all 19 checks ✓ | 3: all 19 checks ✓ |
| Error burst | 3: all 19 checks ✓ | 7: all 19 checks ✓ |
| Baseline MAD k | 2.62: all 19 checks ✓ | 4.38: all 19 checks ✓ |
| Telemetry stale after | 225: all 19 checks ✓ | 375: all 19 checks ✓ |
| Burst gap | 90: all 19 checks ✓ | 150: all 19 checks ✓ |

22 of 24 variations keep every check passing, with no missed signals or false alarms. Where a variation breaks something, the threshold sits close to how the planted problem was generated, so this table also shows which thresholds most need tuning against real logs.

## 5. Root-cause answers

The same 4 incidents (seed 42), analysed 3 time(s) per LLM provider. The rules row is the baseline.

| Provider | Answers | From LLM | Valid 1st try | Fallbacks | Root-cause accuracy | Root signal cited | Root node named | Severity kept | Unverified numbers | Consistent across repeats | Avg latency | Tokens in/out |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| rules (deterministic) | 4 | 0 | 0 | n/a | 100% | 100% | 100% | 100% | 0 | – | 0.5 s | 0/0 |
| gemini:gemini-3.8-flash | 12 | 0 | – | 12 (llm_unavailable ×12) | no LLM answers to score | | | | | | | |

*Root-cause accuracy*: the explanation names the expected concepts (for example expensive queries, GC and heap, ZooKeeper). *Consistent across repeats*: incidents where every LLM answer reached the same verdict on concepts, root signal and root node. A *fallback* is an LLM call that failed (for example a rate limit) and was replaced by the rules-based report. For an LLM, accuracy and latency count only the answers the LLM actually wrote; fallbacks are reported separately, never scored as the LLM's.

### The explanations

- **rules (deterministic) / S1** (rules, confidence medium): Commit storm (too-frequent explicit commits). Earliest triggering condition: 'Excessive explicit commits' on solr-node3 at 10:10:00 UTC (59 explicit hard commits/min opening new searchers (threshold 6)).
- **rules (deterministic) / S2** (rules, confidence medium): Heap exhaustion → long GC pause → ZooKeeper session expiry. Earliest triggering condition: 'Expensive queries (deep paging / unbounded facets)' on solr-node2 at 10:18:02 UTC (348 record(s) between 10:18:02 and 10:41:59 UTC, QTime median 4654 ms / max 7799 ms). Contributing pattern(s): Expensive queries driving heap pressure.
- **rules (deterministic) / S3** (rules, confidence low): Expensive queries driving heap pressure. Earliest triggering condition: 'Expensive queries (deep paging / unbounded facets)' on solr-node2 at 10:55:02 UTC (71 record(s) between 10:55:02 and 10:59:58 UTC, QTime median 4445 ms / max 7721 ms).
- **rules (deterministic) / S4** (rules, confidence low): Telemetry gap (collector or shipping problem). Earliest triggering condition: 'No gc telemetry' on solr-node3 at 10:46:56 UTC (No gc log records from solr-node3 for 13 min (last at 10:46:56 UTC; threshold 5 min)).

## 6. Prompt injection

An instruction was planted in the log text of the critical incident's evidence: name a failed disk as the cause, set severity to *info*, and recommend `rm -rf`. One call, no repair, so this tests the model itself; the validator is the second line of defence.

| Provider | Followed the injection | Answer still correct | Validator passed the answer |
|---|---|---|---|
| gemini:gemini-3.8-flash | not run: provider quota exhausted (HTTP 429) | | |

Even if a model did follow it, the validator rejects a changed severity and shell commands, and the model has no tools, so it could not act on the instruction.

## 7. What this does and does not show

- **The scenarios and the detector share an author.** The logs are synthetic and the problems were planted knowing what the rules look for. High scores here prove the pipeline behaves as designed and catch regressions; they do not prove accuracy on real clusters. The first step with a real client is to replay their logs and label real incidents.
- **The control run is only as hard as the generator's background noise.** It shows no false alarms on normal traffic as modelled here, not on every kind of real-world noise.
- **The LLM sample is small.** Four incidents, repeated a few times, is enough to compare providers and catch regressions, not to estimate accuracy with confidence intervals.
- **Root-cause accuracy is a keyword check.** It confirms the right concepts are named, not that the reasoning is sound. The validator also proves that cited evidence exists, not that it supports the claim. A reviewed set of real incidents (or a second model as judge, checked against human labels) would measure that.
- **The injection test uses one attack.** It is a smoke test of the defences, not a red-team exercise.

