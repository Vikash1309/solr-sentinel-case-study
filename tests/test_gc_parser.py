from app.parsers import gc_parser

P = dict(cluster_id="c1", node_id="n2", file="n2/gc.log")


def parse(lines):
    return gc_parser.parse_lines(list(enumerate(lines)), **P)


def test_unified_full_gc_and_units():
    evs, errs, _ = parse(["[2026-10-15T10:38:23.234+0000][23903.235s][info][gc          ] GC(4510) Pause Full (G1 Compaction Pause) 8188M->7560M(8192M) 18234.600ms"])
    a = evs[0].attributes
    assert not errs and a["gc_type"] == "full" and a["pause_ms"] == 18234.6
    assert (a["heap_after_mb"], a["heap_capacity_mb"]) == (7560, 8192) and evs[0].severity == "WARN"


def test_humongous_marker_and_skipped_lines():
    evs, errs, skipped = parse([
        "[2026-10-15T10:18:10.000+0000][1.0s][info][gc          ] GC(1) Pause Young (Concurrent Start) (G1 Humongous Allocation) 6456M->3602M(8192M) 37.581ms",
        "[2026-10-15T10:31:10.910+0000][1s][info][gc          ] GC(4419) To-space exhausted",
        "[2026-10-15T10:31:10.910+0000][1s][info][gc,heap     ] GC(1) Eden regions: 3->0"])
    assert evs[0].attributes["cause"] == "G1 Humongous Allocation"
    assert evs[1].event_type == "gc_evacuation_failure" and skipped == 1 and not errs


def test_legacy_jdk8():
    evs, _, _ = parse(["2026-10-15T10:38:23.234+0000: 23903.235: [Full GC (Allocation Failure)  8188M->7560M(8192M), 18.2346000 secs]",
                       "2026-10-15T10:00:05.123+0000: 3605.1: [GC pause (G1 Evacuation Pause) (young) 4G->1650M(8G), 0.0401230 secs]"])
    assert evs[0].attributes["gc_type"] == "full" and round(evs[0].attributes["pause_ms"], 1) == 18234.6
    assert evs[1].attributes["heap_before_mb"] == 4096 and evs[1].attributes["heap_capacity_mb"] == 8192


def test_non_gc_lines_and_missing_time_decorator_are_parse_errors():
    _, errs, _ = parse(["OpenJDK 64-Bit Server VM warning: CodeCache is full.",
                        "[23903.235s][info][gc] GC(1) Pause Young (Normal) (G1 Evacuation Pause) 10M->5M(100M) 1.0ms"])
    assert len(errs) == 2 and "time decorator" in errs[1].reason
