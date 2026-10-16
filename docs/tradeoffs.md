# Trade-offs and next steps

The prototype keeps things simple where that doesn't hurt the result. With more time, the order of work would be:

1. Check the parsers against real client logs.
2. Improve detection and correlation using what those logs show.
3. Build a labelled set of real incidents.
4. Measure the AI answers against it.
5. Only then consider a multi-step agent.

What is already hardened, and the remaining gaps before production traffic, are listed in
[production-readiness.md](production-readiness.md).

## What was traded, and what comes next

| Choice in the prototype | Cost | With more time |
|---|---|---|
| Regex parsers for specific Solr and JDK versions | May break when log formats change | Test fixtures from real logs for each version; alert when the parse-error rate rises |
| Fixed thresholds plus 30-minute baselines | Doesn't know about daily or weekly patterns | Baselines by time of day and week, per collection |
| Everything in one process, re-analysing 24 h each cycle (≈ 1.5 s for 19k events) | Limited throughput; state lost if the process dies | Kafka plus partitioned workers with incremental state |
| SQLite | One machine only | PostgreSQL for incidents, OpenSearch for events, S3 for raw logs |
| Hand-written cause-and-effect rules | Only covers known patterns | Learn from confirmed incidents; use the real cluster layout from Solr's API |
| One LLM call with one retry | May struggle with genuinely ambiguous incidents | A staged agent, but only if the evaluation shows it helps |
| Confidence is low / medium / high | Not calibrated | Calibrate against real incidents and engineer feedback |
| Every fix needs human approval | Slower than automation | Automate low-risk fixes behind policy, approval and rollback |
| Logs only | Can't see CPU, disk, network or who sent a bad query | Add host metrics, the Solr Metrics API, ZooKeeper stats and request logs |
| Streamlit dashboard | Basic; no alert routing | Plug into the existing monitoring, ticketing and on-call tools |
| Synthetic test scenarios, written by the same author as the rules | Not real incidents: high scores prove the pipeline works as designed, not accuracy in the field. Partly offset by a control run, isolated scenarios and a threshold sensitivity analysis | Replay real client logs and label real incidents; add disk-full, OOM, ZooKeeper outage, network split and genuinely ambiguous cases |
| Root-cause accuracy checked by keywords, on 4 incidents | Confirms the right concepts are named, not the quality of the reasoning; small sample | A reviewed set of real incidents, and a second model as judge, calibrated against human labels |

## Where the prototype goes beyond the design document

* **Three severity levels** (info, warning, critical), set by code. One critical-looking signal with nothing
  behind it is capped at warning.
* **Business impact** is part of every report, because the case study asks for it. It only states impact the
  evidence supports.
* **Every cause-and-effect link is labelled** observed, inferred or temporal, so readers can see how strong it is.
* **The rules-based report has the same format as the AI one**, so the dashboard always shows a full answer,
  clearly labelled when the AI wasn't used.
* **An evaluation harness** runs the labelled scenarios over 5 random seeds. It also checks that re-reading logs
  creates no duplicates and that broken lines are captured.
