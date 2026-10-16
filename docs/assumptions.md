# Assumptions

Each assumption is marked **Handled** (built into the prototype), **Partly handled**, or **Open** (needs
checking with the client).

| # | Assumption | Status and how it is handled |
|---|---|---|
| 1 | The sample logs match the formats we will see in production | **Open.** No logs came with the case study, so the sample logs follow the public Solr 9 and JDK 17 formats. The first step with real logs is to run them through the parsers and look at the parse-error store. |
| 2 | Each log file can be tied to a node | **Handled.** The folder name is the node ID (`<log_dir>/<node_id>/`). Uploads give it in a form field. In production the log shipper adds it. |
| 3 | Timestamps can be compared across nodes | **Handled.** Solr times are read as UTC (Solr's default). GC times carry their own offset and are converted. Correlation windows of 5–120 s absorb small clock differences. |
| 4 | GC logs do not always include heap figures | **Handled.** Heap values are stored only when present. A missing value stays missing; it is never treated as 0. |
| 5 | Logs alone don't show the full cluster layout | **Partly handled.** Replicas come from log context and leader changes from election messages. Leaders never seen changing are unknown, so cluster health is marked *partial*. Production would call Solr's CLUSTERSTATUS API. |
| 6 | True error rates need request counts | **Handled.** Request counts come from Solr's request log lines. Impact is described in words, not percentages we can't support. |
| 7 | New logs arrive periodically | **Handled.** The API checks for new data every 15 seconds; `python run.py simulate` demonstrates it. |
| 8 | Baselines need history before they can be trusted | **Handled.** Fixed thresholds always apply. Baselines start only after 5 samples and ignore minutes already flagged. |
| 9 | An LLM may not be available | **Handled.** The provider is a setting. With none, the rules-based report is shown and labelled as such. |
| 10 | Fixes are recommended, never run | **Handled.** The model has no tools. Anything that changes the system needs human approval, and shell commands are rejected. |
| 11 | "Now" means the time of the newest log | **Handled.** Replayed logs are judged at their own time (`SENTINEL_CLOCK=data`). Use `wall` for live collection. |
| 12 | The ZooKeeper timeout is 15 s | **Handled.** Set for the sample cluster (Solr 9 defaults to 30 s) and configurable. |
| 13 | Search terms may contain personal data | **Handled.** Query text is redacted before any LLM call. Credentials and tokens are redacted before storage. |
| 14 | Two identical lines in the same millisecond are the same event | **Accepted.** Event IDs come from the line's content, which makes re-reading safe. Two byte-identical lines in the same millisecond would count once. |
| 15 | One machine is enough for the prototype | **Handled.** Runs with `docker compose up` or `python run.py demo`. Production scaling is described in [architecture.md](architecture.md). |
