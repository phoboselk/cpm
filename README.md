# CPM - Cluster Pipeline Manager

Autonomous multi-cluster routing for Elasticsearch. CPM monitors cluster health via ML anomaly detection, scores clusters on disk/JVM/shard pressure, and dynamically routes data streams to the healthiest cluster.

## Components

- **5 ML jobs** - Anomaly detection on event rate, store size, JVM heap, shard count, and write queue depth
- **5 datafeeds** - Aggregated monitoring data fed into each ML job
- **6 watchers** - Scheduled automation: scoring, routing advisor, state manager, pipeline manager, registry sync, forecast trigger
- **7 config indices** - Cluster registry, routing config, scores, bytes-per-event, routing suggestions, pipeline templates, pipeline state

## Quick Start

1. Copy the settings template and fill in your credentials:
   ```
   cp cpm_settings.json.example cpm_settings.json
   ```
   Edit `cpm_settings.json` with your Elasticsearch URL and API key.

2. Run the installer:
   ```
   python3 cpm_install.py
   ```

3. Wait ~30 min for ML models to build, then trigger initial forecasts:
   ```
   POST _ml/anomaly_detectors/cpm-store-size/_forecast  {"duration":"24h"}
   POST _ml/anomaly_detectors/cpm-jvm-heap/_forecast    {"duration":"24h"}
   POST _ml/anomaly_detectors/cpm-shard-count/_forecast {"duration":"24h"}
   ```

5. Execute the scoring watcher and verify output:
   ```
   POST _watcher/watch/cpm-scoring/_execute
   GET cpm-scores/_search?sort=scored_at:desc&size=1
   ```

6. Set `cluster_registry` ingest_hosts in `cpm_settings.json` (see `cpm_settings.json.example`).

7. Bootstrap routing and push Logstash pipelines:
   ```
   python3 cpm_install.py --bootstrap
   ```
   Or for the Docker stack only:
   ```
   python3 docker/scripts/bootstrap_cpm_pipelines.py
   ```

## CLI Options

| Flag | Description |
|------|-------------|
| `-url` | Elasticsearch URL (overrides cpm_settings.json) |
| `-key` | API key, Base64-encoded (overrides cpm_settings.json) |
| `-monitoring-index` | Monitoring index pattern (default: `.monitoring-es-8-*`) |
| `-settings` | Path to settings file (default: `cpm_settings.json`) |
| `-ca` | Path to CA certificate for TLS (auto-uses `docker-ca.crt` for localhost) |
| `--insecure` | Disable TLS certificate verification |
| `-configs` | Path to cpm_configs.json (default: `cpm_configs.json`) |
| `-start` | Datafeed start ISO8601 timestamp (default: now-2d) |
| `--clean` | Delete and recreate config indices (destroys existing data) |

## Watcher Schedule (UTC)

| Time | Watcher | Purpose |
|------|---------|---------|
| 00:00 | cpm-registry-sync | Sync cluster registry from monitoring data |
| 00:05 | cpm-scoring | Score clusters on disk/JVM/shard pressure |
| 00:10 | cpm-routing-advisor | Compute optimal dataset-to-cluster routing |
| 00:15 | cpm-state-manager | Build pipeline state from routing + monitoring |
| Manual | cpm-pipeline-manager | Render and push Logstash pipelines from state + templates |
| Hourly | cpm-forecast-trigger | Trigger 24h ML forecasts |

## Documentation

Full architecture documentation with diagrams: [docs/CPM_Architecture.md](docs/CPM_Architecture.md)