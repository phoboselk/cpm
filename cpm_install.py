#!/usr/bin/env python3
"""CPM Installer - ML jobs, datafeeds, and Watchers for multi-cluster routing.

Usage:
    python3 cpm_install.py                         # uses cpm_settings.json
    python3 cpm_install.py -url https://... -key API_KEY_BASE64  # override
    python3 cpm_install.py --clean                 # delete and recreate indices
"""

import json, re, sys, os, time, argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urlparse
import requests

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_CA = SCRIPT_DIR / "docker-ca.crt"

# ── Load settings ─────────────────────────────────────────
p = argparse.ArgumentParser(description="CPM Installer")
p.add_argument("-url", help="Elasticsearch URL (overrides cpm_settings.json)")
p.add_argument("-key", help="API key, Base64-encoded (overrides cpm_settings.json)")
p.add_argument("-monitoring-index", help="Monitoring index pattern (overrides cpm_settings.json)")
p.add_argument("-ca", "--ca", help="Path to CA certificate for TLS (Docker: docker-ca.crt)")
p.add_argument("--insecure", action="store_true",
               help="Disable TLS certificate verification (not recommended)")
p.add_argument("-configs", default="cpm_configs.json", help="Path to cpm_configs.json")
p.add_argument("-settings", default="cpm_settings.json", help="Path to settings file")
p.add_argument("-start", default=None, help="Datafeed start ISO8601 (default: now-2d)")
p.add_argument("--clean", action="store_true", help="Delete and recreate config indices")
p.add_argument("--bootstrap", action="store_true",
               help="After install: patch registry, seed templates, run routing/state/pipeline watchers")
args = p.parse_args()

SETTINGS_PATH = args.settings
settings = {}
if os.path.exists(SETTINGS_PATH):
    with open(SETTINGS_PATH) as f:
        settings = json.load(f)

ES  = args.url or settings.get("es_host")
KEY = args.key or settings.get("es_api_key")
MON = args.monitoring_index or settings.get("monitoring_index", ".monitoring-es-8-*")
START = args.start or (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%SZ")

if not ES or not KEY:
    print(f"Error: es_host and es_api_key must be provided via -url/-key flags or in {SETTINGS_PATH}.")
    sys.exit(1)

if not ES.startswith(("http://", "https://")):
    ES = f"https://{ES}"
ES = ES.rstrip("/")
_es = urlparse(ES)
ES_HOSTNAME = _es.hostname or "localhost"
ES_PORT = _es.port or (443 if _es.scheme == "https" else 80)
H   = {"Authorization": f"ApiKey {KEY}", "Content-Type": "application/json"}

ca_path = args.ca or settings.get("es_ca_cert")
if not ca_path and ES_HOSTNAME in ("localhost", "127.0.0.1") and DEFAULT_CA.is_file():
    ca_path = str(DEFAULT_CA)
if args.insecure:
    VERIFY_SSL = False
elif ca_path and os.path.isfile(ca_path):
    VERIFY_SSL = ca_path
else:
    VERIFY_SSL = True

with open(args.configs) as f:
    bundle = json.load(f)

failures = []


def req(method, path, body=None, ok404=False, ok_exists=False):
    r = requests.request(method, f"{ES}{path}", headers=H, json=body, timeout=30, verify=VERIFY_SSL)
    if ok404 and r.status_code == 404:
        return None
    if ok_exists and r.status_code == 400 and "resource_already_exists_exception" in r.text:
        ok("already exists (kept)")
        return None
    if not r.ok:
        print(f"  ✗ {r.status_code}: {r.text[:400]}")
        failures.append(f"{method} {path}")
        return None
    return r.json()


def hdr(s): print(f"\n{'─'*60}\n  {s}\n{'─'*60}")
def ok(s):  print(f"  ✓ {s}")
def inf(s): print(f"    {s}")


def delete_job(name):
    req("POST", f"/_ml/datafeeds/datafeed-{name}/_stop?force=true", ok404=True)
    req("POST", f"/_ml/anomaly_detectors/{name}/_close?force=true",  ok404=True)
    req("DELETE", f"/_ml/datafeeds/datafeed-{name}",                 ok404=True)
    req("DELETE", f"/_ml/anomaly_detectors/{name}",                  ok404=True)
    ok(f"Cleared {name}")


# ──────────────────────────────────────────────────────────
# STEP 0: Verify field paths
# ──────────────────────────────────────────────────────────
hdr("STEP 0: Field Path Verification")

if args.insecure:
    inf("TLS: certificate verification disabled (--insecure)")
elif isinstance(VERIFY_SSL, str):
    inf(f"TLS: using CA certificate {VERIFY_SSL}")

print("\n  [0a] Cluster ID field...")
CID = None
r = req("POST", f"/{MON}/_search", {
    "size": 1,
    "query": {"bool": {"filter": [{"match_phrase": {"event.dataset": "elasticsearch.cluster.stats"}}]}},
    "_source": ["cluster_uuid", "elasticsearch.cluster.id"]
})
if r and r["hits"]["hits"]:
    src = r["hits"]["hits"][0]["_source"]
    CID = "cluster_uuid" if "cluster_uuid" in src else "elasticsearch.cluster.id"
    ok(f"cluster ID field: {CID}")

print("\n  [0b] JVM heap field...")
HEAP = None
HEAP_CANDIDATES = [
    "elasticsearch.node.stats.jvm.mem.heap.used.pct",
    "node_stats.jvm.mem.heap.used.pct",
    "elasticsearch.node.stats.jvm.mem.heap_used_percent",
    "node_stats.jvm.mem.heap_used_percent",
]
for field in HEAP_CANDIDATES:
    r = req("POST", f"/{MON}/_search", {
        "size": 0,
        "query": {"bool": {"filter": [{"match_phrase": {"event.dataset": "elasticsearch.node.stats"}}]}},
        "aggs": {"probe": {"max": {"field": field}}},
    })
    if r and r.get("aggregations", {}).get("probe", {}).get("value") is not None:
        HEAP = field
        break
if HEAP:
    ok(f"heap field: {HEAP}")

print("\n  [0c] Shard count field...")
SHARDS = None
SHARD_CANDIDATES = [
    "elasticsearch.cluster.stats.indices.shards.count",
    "cluster_stats.indices.shards.count",
    "elasticsearch.cluster.stats.indices.shards.total",
    "cluster_stats.indices.shards.total",
]
for field in SHARD_CANDIDATES:
    r = req("POST", f"/{MON}/_search", {
        "size": 0,
        "query": {"bool": {"filter": [{"match_phrase": {"event.dataset": "elasticsearch.cluster.stats"}}]}},
        "aggs": {"probe": {"max": {"field": field}}},
    })
    if r and r.get("aggregations", {}).get("probe", {}).get("value") is not None:
        SHARDS = field
        break
if SHARDS:
    ok(f"shard field: {SHARDS}")

print("\n  [0d] Write queue field...")
WQUEUE = None
r = req("POST", f"/{MON}/_search", {
    "size": 1,
    "query": {"bool": {"filter": [{"match_phrase": {"event.dataset": "elasticsearch.node.stats"}}]}},
    "_source": ["node_stats.thread_pool.write.queue",
                "elasticsearch.node.stats.thread_pool.write.queue.count"]
})
if r and r["hits"]["hits"]:
    src = r["hits"]["hits"][0]["_source"]
    WQUEUE = ("node_stats.thread_pool.write.queue"
              if "node_stats" in src
              else "elasticsearch.node.stats.thread_pool.write.queue.count")
    ok(f"write queue field: {WQUEUE}")

if not HEAP or not SHARDS or not CID:
    print("\nERROR: Could not resolve required field paths (cluster_id, heap, shards). Aborting.")
    sys.exit(1)

# Build field substitution map: source paths in cpm_configs.json → detected paths
FIELD_SUBS = {}
if CID != "elasticsearch.cluster.id":
    FIELD_SUBS["elasticsearch.cluster.id"] = CID
if HEAP != "elasticsearch.node.stats.jvm.mem.heap.used.pct":
    FIELD_SUBS["elasticsearch.node.stats.jvm.mem.heap.used.pct"] = HEAP
if SHARDS != "elasticsearch.cluster.stats.indices.shards.count":
    FIELD_SUBS["elasticsearch.cluster.stats.indices.shards.count"] = SHARDS
if WQUEUE and WQUEUE != "elasticsearch.node.stats.thread_pool.write.queue.count":
    FIELD_SUBS["elasticsearch.node.stats.thread_pool.write.queue.count"] = WQUEUE

print(f"""
  Resolved field paths:
    cluster_id   : {CID}
    heap_used    : {HEAP}
    shards_total : {SHARDS}
    write_queue  : {WQUEUE or "(not detected - cpm-cluster-event-rate may not work)"}

  Field substitutions: {len(FIELD_SUBS)} replacement(s) will be applied to datafeeds.
""")


def adapt_fields(cfg):
    s = json.dumps(cfg)
    for old, new in sorted(FIELD_SUBS.items(), key=lambda x: -len(x[0])):
        s = s.replace(old, new)
    return json.loads(s)


def patch_registry(registry_cfg: dict) -> None:
    if not registry_cfg:
        return
    r = req("POST", "/cpm-cluster-registry/_search", {"size": 50, "query": {"match_all": {}}})
    if not r:
        return
    for hit in r["hits"]["hits"]:
        src = hit["_source"]
        name = src.get("cluster_name", "")
        defaults = registry_cfg.get(name) or registry_cfg.get(src.get("cluster_id", ""))
        if not defaults:
            continue
        doc = {**src, **{k: v for k, v in defaults.items() if v is not None}}
        if req("PUT", f"/cpm-cluster-registry/_doc/{hit['_id']}?refresh=wait_for", doc):
            ok(f"Registry {name or hit['_id']}: {', '.join(defaults.keys())}")


def ensure_template_mapping() -> None:
    mapping = bundle.get("mappings", {}).get("cpm-pipeline-templates", {})
    props = mapping.get("properties", {})
    extra = {k: v for k, v in props.items() if k in ("consumer_threads",)}
    if extra:
        req("PUT", "/cpm-pipeline-templates/_mapping", {"properties": extra}, ok404=True)


def ensure_index_mappings() -> None:
    """Add new mapping fields to indices that already exist (upgrade path)."""
    for idx, mapping in bundle.get("mappings", {}).items():
        props = mapping.get("properties", {})
        if not props:
            continue
        r = req("PUT", f"/{idx}/_mapping", {"properties": props}, ok404=True)
        if r:
            ok(f"Mapping verified for {idx}")


def execute_watcher(name):
    r = req("POST", f"/_watcher/watch/{name}/_execute", {})
    if not r:
        return False
    state = r.get("watch_record", {}).get("status", {}).get("execution_state", "?")
    ok(f"{name} executed ({state})")
    return True


# ──────────────────────────────────────────────────────────
# STEP 1: ML jobs (from cpm_configs.json)
# ──────────────────────────────────────────────────────────
hdr("STEP 1: ML jobs (delete + reinstall)")

JOB_ORDER = [
    "cpm-event-rate", "cpm-store-size", "cpm-jvm-heap",
    "cpm-shard-count", "cpm-cluster-event-rate",
]

for name in JOB_ORDER:
    if name not in bundle["jobs"]:
        print(f"  ⚠ {name} not in cpm_configs.json, skipping")
        continue
    delete_job(name)
    r = req("PUT", f"/_ml/anomaly_detectors/{name}", bundle["jobs"][name])
    if r: ok(f"Job {name} created")


# ──────────────────────────────────────────────────────────
# STEP 2: Datafeeds (from cpm_configs.json)
# ──────────────────────────────────────────────────────────
hdr("STEP 2: Datafeeds (from cpm_configs.json)")

for name, cfg in bundle["feeds"].items():
    cfg = adapt_fields(cfg)
    cfg.pop("authorization", None)
    cfg["job_id"] = name.replace("datafeed-", "", 1)
    s = json.dumps(cfg).replace(".monitoring-es-8-*", MON)
    cfg = json.loads(s)
    req("DELETE", f"/_ml/datafeeds/{name}", ok404=True)
    r = req("PUT", f"/_ml/datafeeds/{name}", cfg)
    if r: ok(f"Datafeed {name} created")


# ──────────────────────────────────────────────────────────
# STEP 3: Open jobs & start datafeeds
# ──────────────────────────────────────────────────────────
hdr("STEP 3: Open jobs & start datafeeds")

for name in JOB_ORDER:
    if name not in bundle["jobs"]:
        continue
    r = req("POST", f"/_ml/anomaly_detectors/{name}/_open")
    if r: ok(f"Opened {name}")

time.sleep(3)

for name in JOB_ORDER:
    if name not in bundle["jobs"]:
        continue
    feed = f"datafeed-{name}"
    r = req("POST", f"/_ml/datafeeds/{feed}/_start", {"start": START})
    if r: ok(f"Started {feed} from {START}")


# ──────────────────────────────────────────────────────────
# STEP 4: Config indices
# ──────────────────────────────────────────────────────────
hdr("STEP 4: Config indices (from cpm_configs.json)")

for idx, mapping in bundle.get("mappings", {}).items():
    if args.clean:
        req("DELETE", f"/{idx}", ok404=True)
    r = req("PUT", f"/{idx}", {
        "settings": {"number_of_shards": 1, "number_of_replicas": 1},
        "mappings": mapping,
    }, ok_exists=not args.clean)
    if r: ok(f"Index {idx} created")

ensure_index_mappings()

if args.clean:
    print("  ⚠ --clean: indices were deleted and recreated")


# ──────────────────────────────────────────────────────────
# STEP 5: Watchers
# ──────────────────────────────────────────────────────────
hdr("STEP 5: Watchers (from cpm_configs.json)")

SRC_HOST = bundle["src_host"]
watch_str = json.dumps(bundle["watches"])
watch_str = watch_str.replace(SRC_HOST, ES_HOSTNAME)
watch_str = watch_str.replace('"port": 443', f'"port": {ES_PORT}')
watch_str = watch_str.replace(".monitoring-es-8-*", MON)
watch_str = re.sub(
    r'"Authorization": "ApiKey [A-Za-z0-9+/=]+"',
    f'"Authorization": "ApiKey {KEY}"',
    watch_str,
)
watch_str = watch_str.replace('"Authorization": "ApiKey YOUR_API_KEY"', f'"Authorization": "ApiKey {KEY}"')

for name, cfg in json.loads(watch_str).items():
    r = req("PUT", f"/_watcher/watch/{name}", cfg)
    if r: ok(f"Watch {name} installed")


# ──────────────────────────────────────────────────────────
# STEP 6: Pipeline templates
# ──────────────────────────────────────────────────────────
hdr("STEP 6: Pipeline templates (from cpm_configs.json)")

ensure_template_mapping()
for tpl_id, tpl in bundle.get("templates", {}).items():
    r = req("PUT", f"/cpm-pipeline-templates/_doc/{tpl_id}", tpl)
    if r: ok(f"Template {tpl_id} seeded")


# ──────────────────────────────────────────────────────────
# STEP 7: Cluster registry ingest_hosts / dc
# ──────────────────────────────────────────────────────────
hdr("STEP 7: Cluster registry defaults")

REGISTRY_CFG = settings.get("cluster_registry", {})
if REGISTRY_CFG:
    patch_registry(REGISTRY_CFG)
else:
    inf("No cluster_registry in settings — set ingest_hosts manually or via bootstrap script")


# ──────────────────────────────────────────────────────────
# STEP 8: Bootstrap routing → state → pipelines (optional)
# ──────────────────────────────────────────────────────────
if args.bootstrap:
    hdr("STEP 8: Bootstrap CPM pipeline chain")
    execute_watcher("cpm-registry-sync")
    patch_registry(REGISTRY_CFG)
    time.sleep(1)
    execute_watcher("cpm-scoring")
    execute_watcher("cpm-routing-advisor")
    # Preserve logs-beats-raw on central before state-manager merges monitoring datasets
    central = None
    r = req("POST", "/cpm-cluster-registry/_search", {
        "size": 1,
        "query": {"term": {"cluster_name": "central-cluster"}},
    })
    if r and r["hits"]["hits"]:
        central = r["hits"]["hits"][0]
    if central:
        src = central["_source"]
        dc = src.get("dc") or "dc-central"
        cid = src["cluster_id"]
        req("PUT", "/cpm-pipeline-state/_doc/logs-beats-raw", {
            "data_stream_type": "logs",
            "dataset": "beats",
            "namespace": "raw",
            "pipeline_type": "catchall",
            "pipeline_id": f"{dc}_cpm-catchall-{cid}",
            "cluster_id": cid,
            "dc": dc,
            "topic": "logs-beats-raw",
            "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        })
        ok("Seeded logs-beats-raw on central catchall")
    execute_watcher("cpm-state-manager")
    patch_registry(REGISTRY_CFG)
    execute_watcher("cpm-pipeline-manager")

    r = req("GET", "/_logstash/pipeline")
    if r:
        ids = list(r.keys()) if isinstance(r, dict) else []
        ok(f"Logstash pipelines: {', '.join(ids) if ids else '(none)'}")

    r = req("POST", "/cpm-pipeline-state/_search", {"size": 20})
    if r:
        ok(f"Pipeline state entries: {r['hits']['total']['value']}")


# ──────────────────────────────────────────────────────────
hdr("ALL DONE")
if failures:
    print(f"\n  ⚠ {len(failures)} error(s) occurred:")
    for f in failures:
        print(f"    - {f}")
print("""
  Next steps:
  1. Wait ~30 min for jobs to build initial models
  2. Check job health:
       GET _ml/datafeeds/_stats
       GET _ml/anomaly_detectors/_stats
  3. Trigger initial forecasts manually:
       POST _ml/anomaly_detectors/cpm-store-size/_forecast  {"duration":"24h"}
       POST _ml/anomaly_detectors/cpm-jvm-heap/_forecast    {"duration":"24h"}
       POST _ml/anomaly_detectors/cpm-shard-count/_forecast {"duration":"24h"}
  4. Seed pipeline templates and set cluster ingest_hosts in cpm_settings.json:
       "cluster_registry": {
         "central-cluster": {"ingest_hosts": "https://es-central", "dc": "dc-central"},
         "remote-a":          {"ingest_hosts": "https://es-remote-a", "dc": "dc-a"},
         "remote-b":          {"ingest_hosts": "https://es-remote-b", "dc": "dc-b"}
       }
  5. Bootstrap routing and push Logstash pipelines:
       python3 cpm_install.py --bootstrap
     Or manually:
       POST _watcher/watch/cpm-routing-advisor/_execute
       POST _watcher/watch/cpm-state-manager/_execute
       POST _watcher/watch/cpm-pipeline-manager/_execute
  6. Test scoring watcher:
       POST _watcher/watch/cpm-scoring/_execute
  7. Verify output:
       GET cpm-scores/_search?sort=scored_at:desc&size=1
       GET _logstash/pipeline/
       GET cpm-pipeline-state/_search?size=20
""")