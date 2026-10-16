"""
Synthetic SolrCloud + JVM G1 GC log generator.

Produces one hour of realistic logs for a 3-node SolrCloud cluster
(collection `products`, 2 shards x 3 replicas) with four embedded scenarios (any subset can be
generated with --scenarios; --scenarios none gives a control run with background traffic only):

  1. 10:10-10:14  solr-node3  Commit storm: a bulk indexer sends commit=true on every
                              batch -> overlapping searchers -> maxWarmingSearchers errors.
  2. 10:18-10:43  solr-node2  Heap exhaustion: an expensive reporting workload (deep paging,
                              unbounded facets) drives humongous allocations and old-gen growth
                              -> evacuation failures -> Full GCs -> an 18 s stop-the-world
                              pause that exceeds zkClientTimeout (15 s) -> ZK session expiry
                              -> shard1 leader moves to node1 -> node2 replicas recover;
                              node1/node3 see timeouts and "No registered leader" errors.
  3. 10:55-11:00  solr-node2  Recurrence: the reporting workload starts again and heap after
                              GC climbs again (still active at the end of the logs).
  4. 10:47-11:00  solr-node3  Telemetry gap: the GC log collector on node3 stops shipping (Solr log
                              continues). Expected: stale-telemetry signal, not "node down".
  +  malformed records: a partially written Solr line and JVM warnings inside gc.log, which must
     land in the parse-error store instead of being silently dropped.

Layout written:   <out>/<node>/solr.log   and   <out>/<node>/gc.log

Usage:
    python scripts/generate_synthetic_logs.py --out sample_data/logs
    python scripts/generate_synthetic_logs.py --out sample_data/logs --simulate --batch-minutes 5 --interval 10
"""
from __future__ import annotations

import argparse
import math
import os
import random
import time
from datetime import datetime, timedelta, timezone

START = datetime(2026, 10, 15, 10, 0, 0, tzinfo=timezone.utc)
MINUTES = 60
HEAP_MAX = 8192  # MB  (-Xmx8g)
JVM_START = START - timedelta(hours=6)
NODES = ["solr-node1", "solr-node2", "solr-node3"]
# Planted problems. Any subset can be generated; the empty set is a control run (background traffic only).
ALL_SCENARIOS = frozenset({"S1", "S2", "S3", "S4"})

# collection layout: (shard, core_node, core_name, node)
REPLICAS = [
    ("shard1", "core_node1", "products_shard1_replica_n1", "solr-node1"),
    ("shard1", "core_node3", "products_shard1_replica_n2", "solr-node2"),
    ("shard1", "core_node5", "products_shard1_replica_n4", "solr-node3"),
    ("shard2", "core_node7", "products_shard2_replica_n6", "solr-node1"),
    ("shard2", "core_node9", "products_shard2_replica_n8", "solr-node2"),
    ("shard2", "core_node11", "products_shard2_replica_n10", "solr-node3"),
]


def m(minutes: float) -> datetime:
    return START + timedelta(seconds=minutes * 60)


def cores_on(node):
    return [r for r in REPLICAS if r[3] == node]


def ctx(rep, rng):
    shard, core_node, core, _ = rep
    return f"c:products s:{shard} r:{core_node} x:{core} t:null-{rng.randint(1000, 99999)}"


def fmt_solr(ts, level, thread, context, logger, msg):
    return f"{ts:%Y-%m-%d %H:%M:%S}.{ts.microsecond // 1000:03d} {level:<5} ({thread}) [{context}] {logger} {msg}"


def fmt_gc(ts, level, tags, msg):
    up = (ts - JVM_START).total_seconds()
    return f"[{ts:%Y-%m-%dT%H:%M:%S}.{ts.microsecond // 1000:03d}+0000][{up:.3f}s][{level}][{tags:<12}] {msg}"


class Sim:
    def __init__(self, seed=42, scenarios=ALL_SCENARIOS):
        unknown = set(scenarios) - ALL_SCENARIOS
        if unknown:
            raise ValueError(f"unknown scenarios: {sorted(unknown)}")
        self.scenarios = frozenset(scenarios)
        self.rng = random.Random(seed)
        self.out = {n: {"solr": [], "gc": []} for n in NODES}
        self.gcid = {n: 4100 + i * 37 for i, n in enumerate(NODES)}

    # ------------------------------------------------------------------ helpers
    def solr(self, node, ts, level, thread, context, logger, msg, extra=None):
        self.out[node]["solr"].append((ts, fmt_solr(ts, level, thread, context, logger, msg)))
        for i, line in enumerate(extra or []):
            self.out[node]["solr"].append((ts + timedelta(microseconds=i + 1), line))

    def gc(self, node, ts, tags, msg, level="info"):
        self.out[node]["gc"].append((ts, fmt_gc(ts, level, tags, msg)))

    def qtp(self):
        return f"qtp1489193907-{self.rng.randint(20, 400)}"

    # ------------------------------------------------------------------ traffic
    def select_line(self, node, ts, qtime, heavy=False, status=0):
        rng = self.rng
        rep = rng.choice(cores_on(node))
        if heavy:
            params = rng.choice([
                "q=*:*&start=%d&rows=10000&fl=*&wt=javabin&version=2" % (rng.randint(5, 40) * 10000),
                "q=*:*&rows=50000&facet=true&facet.field=sku&facet.field=supplier_id&facet.limit=-1&wt=javabin",
                "q=category:*&rows=20000&sort=price+desc&facet=true&facet.pivot=brand,sku&facet.limit=-1",
            ])
            hits = rng.randint(4_000_000, 4_600_000)
        else:
            params = rng.choice([
                "q=category:shoes&rows=20&fl=id,name,price&wt=json",
                "q=name:(running+shoe)&rows=10&defType=edismax&qf=name^2+description&wt=json",
                "q=*:*&fq=brand:acme&rows=24&sort=popularity+desc&wt=json",
                "q=id:SKU-%d&wt=json" % rng.randint(10000, 99999),
                "q=description:waterproof&rows=20&facet=true&facet.field=brand&facet.limit=20",
            ])
            hits = rng.randint(0, 25000)
        thread = self.qtp()
        c = ctx(rep, rng)
        msg = f"webapp=/solr path=/select params={{{params}}} hits={hits} status={status} QTime={qtime}"
        self.solr(node, ts, "INFO", thread, c, "o.a.s.c.S.Request", msg)
        if qtime >= 1000:
            self.solr(node, ts + timedelta(microseconds=50), "WARN", thread, c, "o.a.s.c.S.SlowRequest", "slow: " + msg)

    def update_line(self, node, ts, qtime, commit=False, status=0):
        rng = self.rng
        rep = rng.choice(cores_on(node))
        n = rng.randint(50, 200)
        params = "commit=true&wt=javabin&version=2" if commit else "wt=javabin&version=2"
        msg = (f"webapp=/solr path=/update params={{{params}}}{{add=[SKU-{rng.randint(10000, 99999)} "
               f"(1781{rng.randint(100000000, 999999999)}), ... ({n} adds)]}} {status} {qtime}")
        self.solr(node, ts, "INFO", self.qtp(), ctx(rep, rng), "o.a.s.u.p.LogUpdateProcessorFactory", msg)

    def soft_commit(self, node, ts):
        rng = self.rng
        for rep in cores_on(node):
            c = ctx(rep, rng)
            th = f"commitScheduler-{rng.randint(10, 30)}-thread-1"
            self.solr(node, ts, "INFO", th, c, "o.a.s.u.DirectUpdateHandler2",
                      "start commit{,optimize=false,openSearcher=true,waitSearcher=true,expungeDeletes=false,softCommit=true,prepareCommit=false}")
            self.solr(node, ts + timedelta(milliseconds=rng.randint(5, 40)), "INFO", th, c, "o.a.s.s.SolrIndexSearcher",
                      f"Opening [Searcher@{rng.randint(0x1000000, 0xfffffff):x}[{rep[2]}] main]")
            self.solr(node, ts + timedelta(milliseconds=rng.randint(40, 90)), "INFO", th, c, "o.a.s.u.DirectUpdateHandler2",
                      "end_commit_flush")

    # ------------------------------------------------------------------ scenario predicates
    # Each check of a scenario comes before any random draw it guards, so generating all scenarios
    # consumes the random stream exactly as before and the output is byte-identical per seed.
    def on(self, scenario: str) -> bool:
        return scenario in self.scenarios

    def heavy_reporting(self, node, t):  # t in minutes
        return node == "solr-node2" and ((self.on("S2") and 18 <= t < 42) or (self.on("S3") and 55 <= t < 60))

    def commit_storm(self, node, t):
        return self.on("S1") and node == "solr-node3" and 10 <= t < 14.3

    def node2_stalled(self, t):  # inside the 18.2 s full GC at 38:05 or ZK-disconnected window
        return self.on("S2") and 38 + 5 / 60 <= t < 38 + 24 / 60

    # ------------------------------------------------------------------ main generation
    def traffic(self):
        # Per node, in this order: user selects, heavy reporting queries, updates, soft commits. The order matters:
        # it fixes how the random stream is consumed, so a seed always produces the same logs.
        for node in NODES:
            self._selects(node)
            self._heavy_queries(node)
            self._updates(node)
            self._soft_commits(node)

    def _select_qtime(self, node, t):
        rng = self.rng
        q = int(rng.lognormvariate(math.log(22), 0.6))
        if self.commit_storm(node, t):
            q = int(q * rng.uniform(4, 14))  # caches thrown away on every new searcher
        if self.on("S2") and node == "solr-node2" and 18 <= t < 43:
            q = int(q * (1 + (min(t, 38) - 18) / 6))  # GC pauses inflate latency
        if self.on("S2") and node == "solr-node1" and 38.4 <= t < 45:
            q = int(q * 2.2)
        if self.on("S3") and node == "solr-node2" and 55 <= t:
            q = int(q * 1.8)
        return q

    def _selects(self, node):
        rng = self.rng
        t = 0.0
        while t < MINUTES:
            rate = 1.1  # selects per second
            if self.on("S2") and node == "solr-node1" and 38.4 <= t < 45:
                rate = 1.6  # takes over traffic + shard1 leadership
            t += rng.expovariate(rate) / 60
            if t >= MINUTES:
                break
            ts = m(t)
            if node == "solr-node2" and self.node2_stalled(t):
                continue  # JVM is paused/disconnected: no request lines are logged
            q = self._select_qtime(node, t)
            # node1/node3 distributed queries fan out to node2 and time out while it is stalled
            if self.on("S2") and node != "solr-node2" and (34.5 <= t < 38.5) and rng.random() < (0.55 if t >= 38 + 5 / 60 else 0.12):
                self.remote_timeout(node, ts, "select")
                continue
            self.select_line(node, ts, max(q, 1), status=0)

    def _heavy_queries(self, node):
        # heavy reporting queries pinned to node2 (the random draws happen even when no scenario uses them)
        rng = self.rng
        t = 0.0
        while node == "solr-node2" and t < MINUTES:
            t += rng.expovariate(0.25) / 60
            if t >= MINUTES:
                break
            if self.heavy_reporting(node, t) and not self.node2_stalled(t):
                self.select_line(node, m(t), rng.randint(1400, 7800), heavy=True)

    def _updates(self, node):
        rng = self.rng
        t = 0.0
        while t < MINUTES:
            storm = self.commit_storm(node, t)
            t += rng.expovariate(1.2 if storm else 0.3) / 60
            if t >= MINUTES:
                break
            ts = m(t)
            if node == "solr-node2" and self.node2_stalled(t):
                continue
            if storm and rng.random() < 0.85:
                self.commit_storm_update(node, ts)
                continue
            # updates for shard1 arriving at node3 while there is no shard1 leader
            if self.on("S2") and node == "solr-node3" and 38 + 6 / 60 <= t < 38 + 27 / 60 and rng.random() < 0.8:
                self.no_leader(node, ts)
                continue
            if self.on("S2") and node != "solr-node2" and 35 <= t < 38.4 and rng.random() < 0.15:
                self.distrib_update_timeout(node, ts)
                continue
            self.update_line(node, ts, rng.randint(15, 70))

    def _soft_commits(self, node):
        # soft commits every 60s (staggered)
        off = {"solr-node1": 7, "solr-node2": 23, "solr-node3": 41}[node]
        for minute in range(MINUTES):
            if self.on("S2") and node == "solr-node2" and minute == 38:
                continue
            self.soft_commit(node, START + timedelta(minutes=minute, seconds=off))

    def remote_timeout(self, node, ts, kind):
        rng = self.rng
        rep = rng.choice(cores_on(node))
        shard = rep[0]
        target_core = "products_shard1_replica_n2" if shard == "shard1" else "products_shard2_replica_n8"
        thread = self.qtp()
        c = ctx(rep, rng)
        self.solr(node, ts, "ERROR", thread, c, "o.a.s.h.RequestHandlerBase",
                  f"org.apache.solr.common.SolrException: org.apache.solr.client.solrj.SolrServerException: "
                  f"Timeout occurred while waiting response from server at: http://solr-node2:8983/solr/{target_core}/select",
                  extra=["\tat org.apache.solr.handler.component.HttpShardHandler.take(HttpShardHandler.java:218)",
                         "\tat org.apache.solr.handler.component.SearchHandler.handleRequestBody(SearchHandler.java:476)",
                         "Caused by: java.util.concurrent.TimeoutException: Total timeout 5000 ms elapsed"])
        self.solr(node, ts + timedelta(milliseconds=1), "INFO", thread, c, "o.a.s.c.S.Request",
                  f"webapp=/solr path=/select params={{q=category:shoes&rows=20&wt=json}} status=500 QTime={rng.randint(5000, 5100)}")

    def distrib_update_timeout(self, node, ts):
        rng = self.rng
        rep = rng.choice(cores_on(node))
        target = "products_shard1_replica_n2" if rep[0] == "shard1" else "products_shard2_replica_n8"
        self.solr(node, ts, "ERROR", f"updateExecutor-8-thread-{rng.randint(1, 40)}", ctx(rep, rng),
                  "o.a.s.u.SolrCmdDistributor",
                  f"java.net.SocketTimeoutException: Read timed out while forwarding update to http://solr-node2:8983/solr/{target}/",
                  extra=["\tat java.base/sun.nio.ch.NioSocketImpl.timedRead(NioSocketImpl.java:288)"])

    def no_leader(self, node, ts):
        rng = self.rng
        rep = [r for r in cores_on(node) if r[0] == "shard1"][0]
        thread = self.qtp()
        c = ctx(rep, rng)
        self.solr(node, ts, "ERROR", thread, c, "o.a.s.h.RequestHandlerBase",
                  "org.apache.solr.common.SolrException: No registered leader was found after waiting for 4000ms , collection: products slice: shard1",
                  extra=["\tat org.apache.solr.common.cloud.ZkStateReader.getLeaderRetry(ZkStateReader.java:1016)"])
        self.solr(node, ts + timedelta(milliseconds=1), "INFO", thread, c, "o.a.s.u.p.LogUpdateProcessorFactory",
                  "webapp=/solr path=/update params={wt=javabin&version=2}{} 503 4004")

    def commit_storm_update(self, node, ts):
        rng = self.rng
        rep = [r for r in cores_on(node) if r[0] == "shard2"][0]  # node3 leads shard2
        c = ctx(rep, rng)
        thread = self.qtp()
        self.solr(node, ts, "INFO", thread, c, "o.a.s.u.DirectUpdateHandler2",
                  "start commit{,optimize=false,openSearcher=true,waitSearcher=true,expungeDeletes=false,softCommit=false,prepareCommit=false}")
        r = rng.random()
        if r < 0.45:
            self.solr(node, ts + timedelta(milliseconds=30), "WARN", thread, c, "o.a.s.c.SolrCore",
                      f"[{rep[2]}] PERFORMANCE WARNING: Overlapping onDeckSearchers=2")
            self.update_line(node, ts + timedelta(milliseconds=900), rng.randint(800, 2400), commit=True)
        elif r < 0.75:
            self.solr(node, ts + timedelta(milliseconds=20), "ERROR", thread, c, "o.a.s.h.RequestHandlerBase",
                      "org.apache.solr.common.SolrException: Error opening new searcher. exceeded limit of maxWarmingSearchers=2, try again later.",
                      extra=["\tat org.apache.solr.core.SolrCore.getSearcher(SolrCore.java:2366)",
                             "\tat org.apache.solr.update.DirectUpdateHandler2.commit(DirectUpdateHandler2.java:701)"])
            self.update_line(node, ts + timedelta(milliseconds=25), rng.randint(200, 600), commit=True, status=503)
        else:
            self.update_line(node, ts + timedelta(milliseconds=600), rng.randint(500, 1500), commit=True)

    # ------------------------------------------------------------------ cluster events (scenario 2)
    def cluster_events(self):
        if not self.on("S2"):
            return
        rng = self.rng
        n1, n2, n3 = NODES
        # warnings before the stall: node2 heap pressure visible to Solr circuit breaker
        for t in (33.2, 35.1, 36.6, 37.4):
            rep = rng.choice(cores_on(n2))
            self.solr(n2, m(t), "WARN", self.qtp(), ctx(rep, rng), "o.a.s.u.CircuitBreakerManager",
                      "Circuit Breakers tripped: JVMHeapCircuitBreaker: memory threshold 90% exceeded, current used heap 93%")
        # ZK session expiry on node2 right after the 18.2 s pause ends at 38:23.3
        t0 = m(38 + 23.4 / 60)
        self.solr(n2, t0, "WARN", "main-EventThread", "   ", "o.a.s.c.c.ConnectionManager",
                  "Watcher org.apache.solr.common.cloud.ConnectionManager@5a2e4553 name: ZooKeeperConnection Watcher:zk1:2181,zk2:2181,zk3:2181 "
                  "got event WatchedEvent state:Expired type:None path:null path: null type: None")
        self.solr(n2, t0 + timedelta(milliseconds=2), "WARN", "main-EventThread", "   ", "o.a.s.c.c.ConnectionManager",
                  "Our previous ZooKeeper session was expired. Attempting to reconnect to recover relationship with ZooKeeper...")
        self.solr(n2, t0 + timedelta(milliseconds=40), "INFO", "main-EventThread", "   ", "o.a.s.c.ZkController",
                  "ZooKeeper session expired, publishing all cores on solr-node2:8983_solr as DOWN")
        # node1 notices first (zk callback) and wins shard1 election. ZK actually expired the session at ~38:20
        te = m(38 + 20.5 / 60)
        self.solr(n1, te, "INFO", "zkCallback-12-thread-3", "   ", "o.a.s.c.c.ZkStateReader",
                  "Updated live nodes from ZooKeeper... (3) -> (2)")
        rep1 = [r for r in cores_on(n1) if r[0] == "shard1"][0]
        c1 = ctx(rep1, rng)
        self.solr(n1, te + timedelta(milliseconds=300), "INFO", "zkCallback-12-thread-4", c1, "o.a.s.c.ShardLeaderElectionContext",
                  "Running the leader process for shard=shard1 and weAreReplacement=true and leaderVoteWait=180000")
        self.solr(n1, te + timedelta(seconds=2), "INFO", "zkCallback-12-thread-4", c1, "o.a.s.c.SyncStrategy",
                  "Sync replicas to http://solr-node1:8983/solr/products_shard1_replica_n1/")
        self.solr(n1, te + timedelta(seconds=5), "INFO", "zkCallback-12-thread-4", c1, "o.a.s.c.ShardLeaderElectionContext",
                  "I am the new leader: http://solr-node1:8983/solr/products_shard1_replica_n1/ shard1")
        rep3 = [r for r in cores_on(n3) if r[0] == "shard1"][0]
        self.solr(n3, te + timedelta(milliseconds=500), "INFO", "zkCallback-14-thread-1", "   ", "o.a.s.c.c.ZkStateReader",
                  "Updated live nodes from ZooKeeper... (3) -> (2)")
        self.solr(n3, te + timedelta(seconds=6), "INFO", "zkCallback-14-thread-2", ctx(rep3, rng), "o.a.s.c.ZkController",
                  "Leader for products/shard1 changed to http://solr-node1:8983/solr/products_shard1_replica_n1/")
        # node2 reconnects and recovers both replicas
        tr = m(38 + 31 / 60)
        self.solr(n2, tr, "INFO", "main-EventThread", "   ", "o.a.s.c.c.ConnectionManager",
                  "Connection with ZooKeeper reestablished.")
        self.solr(n2, tr + timedelta(milliseconds=100), "INFO", "main-EventThread", "   ", "o.a.s.c.ZkController",
                  "ZooKeeper session re-connected ... refreshing core states after session expiration.")
        for i, rep in enumerate(cores_on(n2)):
            c = ctx(rep, rng)
            leader = "solr-node1:8983/solr/products_shard1_replica_n1" if rep[0] == "shard1" else "solr-node3:8983/solr/products_shard2_replica_n10"
            th = f"recoveryExecutor-7-thread-{i + 1}-processing-{rep[2]}"
            b = tr + timedelta(seconds=1 + i)
            self.solr(n2, b, "INFO", th, c, "o.a.s.c.RecoveryStrategy", "Starting recovery process. recoveringAfterStartup=false")
            self.solr(n2, b + timedelta(seconds=1), "INFO", th, c, "o.a.s.c.RecoveryStrategy",
                      f"Attempting to PeerSync from [http://{leader}/] - recoveringAfterStartup=[false]")
            self.solr(n2, b + timedelta(seconds=3), "WARN", th, c, "o.a.s.u.PeerSyncWithLeader",
                      "PeerSync: core=%s url=http://solr-node2:8983/solr too many updates received since start - startingUpdates no longer overlaps with our currentUpdates" % rep[2])
            self.solr(n2, b + timedelta(seconds=3, milliseconds=5), "INFO", th, c, "o.a.s.c.RecoveryStrategy",
                      "PeerSync Recovery was not successful - trying replication.")
            self.solr(n2, b + timedelta(seconds=4), "INFO", th, c, "o.a.s.c.RecoveryStrategy", "Starting Replication Recovery.")
            self.solr(n2, b + timedelta(seconds=6), "INFO", th, c, "o.a.s.h.IndexFetcher",
                      f"Starting download (fullCopy=true) to NRTCachingDirectory ... bytesDownloaded=0 of {rng.randint(9, 14)}.2 GB")
            done = b + timedelta(seconds=170 + 20 * i)
            self.solr(n2, done, "INFO", th, c, "o.a.s.h.IndexFetcher", "Total time taken for download (fullCopy=true): %d secs" % (164 + 20 * i))
            self.solr(n2, done + timedelta(seconds=1), "INFO", th, c, "o.a.s.c.RecoveryStrategy", "Replication Recovery was successful.")
            self.solr(n2, done + timedelta(seconds=2), "INFO", th, c, "o.a.s.c.RecoveryStrategy",
                      "Registering as Active after recovery.")
            self.solr(n2, done + timedelta(seconds=2, milliseconds=50), "INFO", th, c, "o.a.s.c.RecoveryStrategy",
                      "Finished recovery process, successful=[true]")

    # ------------------------------------------------------------------ GC
    def heap_target_pct(self, node, t):
        """Heap occupancy after young GC (% of max) as a function of time."""
        base = 33 + 11 * ((t + NODES.index(node) * 2.3) % 6.5) / 6.5  # mixed-GC sawtooth 33-44 %
        if node != "solr-node2":
            if self.on("S1") and node == "solr-node3" and 10 <= t < 14.5:
                return base + 6
            return base
        s2, s3 = self.on("S2"), self.on("S3")
        if t < 18 or (not s2 and t < 55):
            return base
        if s2 and t < 32:
            return 44 + (88 - 44) * (t - 18) / 14
        if s2 and t < 38.1:
            return 88 + (95 - 88) * (t - 32) / 6.1
        if s2 and t < 42.5:
            return 92 + self.rng.uniform(-1, 1.5)
        if t < 55 or not s3:
            return 41 + 6 * (t - 42.5) / 12.5 if s2 else base
        return 47 + (87 - 47) * (t - 55) / 4.7

    # scripted stop-the-world events on node2: (minute, kind, before, after, pause_ms)
    SCRIPTED_GC = {
        "solr-node2": [
            (31 + 10 / 60, "evac", 7930, 8040, 912.4),
            (32.0, "full", 8150, 7220, 3842.7),
            (33 + 40 / 60, "evac", 8020, 8110, 1204.9),
            (34.5, "full", 8170, 7390, 6218.3),
            (36 + 10 / 60, "full", 8180, 7480, 9531.6),
            (38 + 5 / 60, "full", 8188, 7560, 18234.6),
            (40 + 2 / 60, "evac", 8050, 8120, 1488.0),
            (42.5, "full", 7980, 3390, 7104.2),
            (59.0, "young_long", 7300, 7090, 1312.5),
        ]
    }

    def gc_logs(self):
        for node in NODES:
            self._gc_header(node)
            last_after = None
            for (t, kind, before, after, pause) in self._gc_schedule(node):
                if kind == "concurrent":
                    self._gc_concurrent(node, t)
                else:
                    last_after = self._gc_pause(node, t, kind, before, after, pause, last_after)

    def _gc_header(self, node):
        hdr_ts = START - timedelta(seconds=1)
        for line in ["Using G1", "Version: 17.0.12+7 (release)", "CPUs: 16 total, 16 available",
                     "Memory: 31G", "Heap Region Size: 4M", "Heap Min Capacity: 8G",
                     "Heap Initial Capacity: 8G", "Heap Max Capacity: 8G", "Pre-touch: Disabled"]:
            self.gc(node, hdr_ts, "gc,init", line)

    def _gc_schedule(self, node):
        """Young GCs at a workload-dependent rate, concurrent cycles, and the scripted long pauses, in time order.
        Young GCs that would fall inside a scripted long pause are dropped."""
        rng = self.rng
        events = []
        t = 0.0
        while t < MINUTES:
            heavy = self.heavy_reporting(node, t)
            gap = rng.uniform(3, 5.5) if heavy else rng.uniform(9, 14)
            if self.commit_storm(node, t):
                gap = rng.uniform(6, 8)
            t += gap / 60
            if t < MINUTES:
                events.append((t, "young", None, None, None))
        # concurrent cycles roughly every 6.5 minutes
        for k in range(10):
            events.append((((k * 6.5) + NODES.index(node) * 2.3 + 6.3) % MINUTES, "concurrent", None, None, None))
        for ev in self.SCRIPTED_GC.get(node, []):
            if self.on("S3" if ev[0] >= 55 else "S2"):   # the 10:59 pause belongs to the recurrence
                events.append(ev)
        events.sort(key=lambda e: e[0])
        blocked = [(e[0], e[0] + e[4] / 60000) for e in events if e[1] in ("full", "evac", "young_long")]
        return [e for e in events if not (e[1] == "young" and any(a - 0.05 <= e[0] <= b + 0.05 for a, b in blocked))]

    def _next_gc_id(self, node):
        gid = self.gcid[node]
        self.gcid[node] += 1
        return gid

    def _gc_concurrent(self, node, t):
        rng = self.rng
        gid = self._next_gc_id(node)
        ts = m(t)
        self.gc(node, ts, "gc", f"GC({gid}) Concurrent Mark Cycle")
        dur = rng.uniform(300, 900) * (2.5 if self.on("S2") and node == "solr-node2" and 25 <= t < 43 else 1)
        end = ts + timedelta(milliseconds=dur)
        remark_at = ts + timedelta(milliseconds=dur * 0.8)
        h = int(self.heap_target_pct(node, t) * HEAP_MAX / 100)
        self.gc(node, remark_at, "gc", f"GC({gid}) Pause Remark {h}M->{h - rng.randint(5, 60)}M({HEAP_MAX}M) {rng.uniform(3, 18):.3f}ms")
        self.gc(node, end, "gc", f"GC({gid}) Concurrent Mark Cycle {dur:.3f}ms")

    def _young_gc(self, node, t):
        """Sizes, pause and label of a regular young GC at minute t."""
        rng = self.rng
        pct = self.heap_target_pct(node, t)
        after = int(pct * HEAP_MAX / 100 + rng.uniform(-60, 60))
        eden = max(250, min(3000, HEAP_MAX - after - 300))
        before = min(HEAP_MAX - 10, after + int(eden * rng.uniform(0.9, 1.0)))
        pressure = max(0.0, (pct - 50) / 45)
        pause = rng.uniform(18, 55) + pressure * rng.uniform(150, 420)
        if self.commit_storm(node, t):
            pause *= 1.6
        humongous = self.heavy_reporting(node, t) and rng.random() < 0.35
        mixed = (not humongous) and rng.random() < 0.08
        label = ("Pause Young (Concurrent Start) (G1 Humongous Allocation)" if humongous
                 else "Pause Young (Mixed) (G1 Evacuation Pause)" if mixed
                 else "Pause Young (Normal) (G1 Evacuation Pause)")
        return before, after, pause, label

    def _gc_pause(self, node, t, kind, before, after, pause, last_after):
        rng = self.rng
        gid = self._next_gc_id(node)
        ts = m(t)
        if kind == "young":
            before, after, pause, label = self._young_gc(node, t)
        else:
            label = {"evac": "Pause Young (Normal) (G1 Evacuation Pause)",
                     "young_long": "Pause Young (Concurrent Start) (G1 Humongous Allocation)"}.get(kind, "Pause Full (G1 Compaction Pause)")
        end = ts + timedelta(milliseconds=pause)
        self.gc(node, ts, "gc,start", f"GC({gid}) {label}")
        if kind != "full":
            self.gc(node, ts + timedelta(milliseconds=1), "gc,task", f"GC({gid}) Using 16 workers of 16 for evacuation")
        if kind == "evac":
            self.gc(node, end - timedelta(milliseconds=2), "gc", f"GC({gid}) To-space exhausted")
        if kind == "full":
            for phase, frac in (("Phase 1: Mark live objects", 0.45), ("Phase 2: Prepare for compaction", 0.15),
                                ("Phase 3: Adjust pointers", 0.2), ("Phase 4: Compact heap", 0.2)):
                self.gc(node, ts + timedelta(milliseconds=pause * frac), "gc,phases", f"GC({gid}) {phase} {pause * frac:.3f}ms")
        self.gc(node, end, "gc,heap", f"GC({gid}) Eden regions: {max(1, (before - (last_after or after)) // 4)}->0")
        self.gc(node, end, "gc,heap", f"GC({gid}) Humongous regions: {rng.randint(2, 9) if not self.heavy_reporting(node, t) else rng.randint(60, 240)}->{rng.randint(0, 8)}")
        self.gc(node, end, "gc", f"GC({gid}) {label} {before}M->{after}M({HEAP_MAX}M) {pause:.3f}ms")
        self.gc(node, end, "gc,cpu", f"GC({gid}) User={pause * 12 / 1000:.2f}s Sys={pause * 0.4 / 1000:.2f}s Real={pause / 1000:.2f}s")
        return after

    def telemetry_and_malformed(self):
        # scenario 4: node3's GC log shipping stops at 10:47
        if self.on("S4"):
            cut = m(47)
            self.out["solr-node3"]["gc"] = [(ts, l) for ts, l in self.out["solr-node3"]["gc"] if ts < cut]
        # malformed records: a partial write in node1's solr.log and JVM warnings inside node2's gc.log
        self.out["solr-node1"]["solr"].append((m(25 + 13.4 / 60), "2026-10-15 10:25:13.4"))
        self.out["solr-node1"]["solr"].append((m(25 + 13.41 / 60), "2026-10-15 10:25:13.412 INFO  (qtp1489193907-77) [c:products s:shard1"))
        self.out["solr-node2"]["gc"].append((m(31 + 0.5 / 60),
            "OpenJDK 64-Bit Server VM warning: Attempt to protect stack guard pages failed."))
        self.out["solr-node2"]["gc"].append((m(38 + 25 / 60),
            "OpenJDK 64-Bit Server VM warning: CodeCache is full. Compiler has been disabled."))

    def generate(self):
        self.traffic()
        self.cluster_events()
        self.gc_logs()
        self.telemetry_and_malformed()
        for node in NODES:
            for k in ("solr", "gc"):
                self.out[node][k].sort(key=lambda x: x[0])
        return self.out


def write_all(out_dir, data, until=None, append_from=None):
    """Write lines with ts in [append_from, until) (append mode when append_from is set)."""
    for node, kinds in data.items():
        d = os.path.join(out_dir, node)
        os.makedirs(d, exist_ok=True)
        for kind, lines in kinds.items():
            path = os.path.join(d, f"{kind}.log")
            sel = [l for ts, l in lines if (append_from is None or ts >= append_from) and (until is None or ts < until)]
            with open(path, "a" if append_from else "w", encoding="utf-8", newline="\n") as f:
                if sel:
                    f.write("\n".join(sel) + "\n")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default="sample_data/logs")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--simulate", action="store_true", help="write logs progressively to mimic periodic arrival")
    ap.add_argument("--batch-minutes", type=float, default=5, help="log minutes written per batch (simulate)")
    ap.add_argument("--interval", type=float, default=10, help="seconds between batches (simulate)")
    ap.add_argument("--scenarios", default="S1,S2,S3,S4",
                    help="planted problems to include, comma-separated; 'none' writes a control run with background traffic only")
    a = ap.parse_args()
    chosen = set() if a.scenarios.strip().lower() == "none" else {x.strip() for x in a.scenarios.split(",") if x.strip()}
    data = Sim(a.seed, chosen).generate()
    if not a.simulate:
        write_all(a.out, data)
        total = sum(len(v) for k in data.values() for v in k.values())
        print(f"Wrote {total} log lines for {len(NODES)} nodes to {a.out}")
        return
    # truncate first, then append batches
    write_all(a.out, data, until=START)
    cur = START
    end = START + timedelta(minutes=MINUTES)
    while cur < end:
        nxt = min(end, cur + timedelta(minutes=a.batch_minutes))
        write_all(a.out, data, until=nxt, append_from=cur)
        print(f"[simulate] appended logs {cur:%H:%M} - {nxt:%H:%M}")
        cur = nxt
        if cur < end:
            time.sleep(a.interval)


if __name__ == "__main__":
    main()
