from app.parsers import solr_parser

P = dict(cluster_id="c1", node_id="n1", file="n1/solr.log")


def parse(lines):
    return solr_parser.parse_lines(list(enumerate(lines)), **P)


def test_request_line_fields():
    evs, errs = parse(["2026-10-15 10:18:02.421 WARN  (qtp1-213) [c:products s:shard1 r:core_node3 x:products_shard1_replica_n2 t:null-1] "
                       "o.a.s.c.S.SlowRequest slow: webapp=/solr path=/select params={q=*:*&start=60000&rows=10000} hits=4057189 status=0 QTime=4022"])
    e = evs[0]
    assert not errs and e.event_type == "solr_slow_request" and e.severity == "WARN"
    assert e.attributes["core"] == "products_shard1_replica_n2" and e.attributes["qtime_ms"] == 4022
    assert e.attributes["params"].startswith("q=*:*") and e.timestamp.tzinfo is not None


def test_update_tail_and_unknown_values_stay_unknown():
    evs, _ = parse(["2026-10-15 10:38:06.000 INFO  (qtp1-2) [c:products s:shard1 r:core_node5 x:c t:x] "
                    "o.a.s.u.p.LogUpdateProcessorFactory webapp=/solr path=/update params={wt=javabin}{} 503 4004",
                    "2026-10-15 10:38:07.000 INFO  (main) [   ] o.a.s.c.ZkController hello"])
    assert evs[0].attributes["status"] == 503 and evs[0].attributes["qtime_ms"] == 4004
    assert "qtime_ms" not in evs[1].attributes and "collection" not in evs[1].attributes   # not set to 0


def test_multiline_stack_trace_is_folded():
    evs, errs = parse(["2026-10-15 10:38:06.001 ERROR (qtp1-2) [   ] o.a.s.h.RequestHandlerBase org.apache.solr.common.SolrException: boom",
                       "\tat org.apache.solr.Foo.bar(Foo.java:1)", "Caused by: java.net.SocketTimeoutException: Read timed out"])
    assert len(evs) == 1 and not errs
    assert "Caused by" in evs[0].attributes["stack"] and evs[0].attributes["exception_class"] == "org.apache.solr.common.SolrException"


def test_malformed_and_orphan_lines_go_to_parse_errors():
    evs, errs = parse(["\tat org.apache.Orphan(Orphan.java:1)",
                       "2026-10-15 10:25:13.4",
                       "2026-10-15 10:25:14.000 INFO  (qtp1-1) [   ] o.a.s.c.X ok"])
    assert len(evs) == 1 and len(errs) == 2
    assert {e.reason.split()[0] for e in errs} == {"continuation", "record"}


def test_secrets_are_redacted_before_storage():
    evs, _ = parse(["2026-10-15 10:00:00.000 INFO  (main) [   ] o.a.s.c.X connecting http://admin:s3cret@zk1:2181 password=hunter2"])
    assert "s3cret" not in evs[0].raw and "hunter2" not in evs[0].message
