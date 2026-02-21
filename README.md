# Distributed Feature Store & Model Serving Platform
End-to-end **AI Platform / ML Systems** project that ingests user events, computes features (batch + streaming), trains a simple model, and serves **low-latency online predictions** with observability and tests.

This repo is designed so you can:
- run the full stack **locally** with Docker Compose
- deploy the same stack to a **cloud VM** (simple and cheap)
- optionally split CPU + GPU for cost control (GPU only when needed)

---

## What you’re building

**Goal:** Build a system that ingests raw user data, turns it into ML features, trains a model, and serves predictions in real time at scale.

**Example use case:** a click/search stream that powers a basic recommendation or CTR model.

### High-level pipeline

1. **Ingest** events (clicks/searches/impressions) into Kafka
2. **Stream features** into an **online feature store** (Redis)
3. **Batch features** into an **offline store** (Postgres) for training
4. **Train** a model (PyTorch)
5. **Serve** predictions via FastAPI with low latency
6. **Observe** everything (metrics + logs), and run tests in CI

---

## Architecture

```
                         +------------------+
                         |  Event Producer  |
                         | (simulator / SDK)|
                         +--------+---------+
                                  |
                                  v
+------------------+      +------------------+       +------------------+
|     Postgres     |<-----|  Spark Batch Job |<------+     Kafka        |
|  Offline Store   |      |  (daily/hourly)  |       |  (event stream)  |
+--------+---------+      +------------------+       +---------+--------+
         ^                                                     |
         |                                                     v
         |                                           +------------------+
         |                                           | Spark Streaming  |
         |                                           |  Feature Pipe    |
         |                                           +--------+---------+
         |                                                    |
         |                                                    v
         |                                           +------------------+
         |                                           |      Redis       |
         |                                           | Online Features  |
         |                                           +--------+---------+
         |                                                    |
         v                                                    v
+------------------+                                  +------------------+
|   Training Job   |                                  |  Model Serving   |
|  (PyTorch)       |                                  |   (FastAPI)      |
+--------+---------+                                  +--------+---------+
         |                                                     |
         v                                                     v
+------------------+                                  +------------------+
| Model Registry   |                                  |  Prometheus +    |
| (local/MLflow)   |                                  |   Grafana        |
+------------------+                                  +------------------+
```

---

## Tech stack

- **Python 3.11**
- **Kafka** (event ingestion)
- **Spark** (feature pipelines: batch + streaming)
- **Redis** (online feature store)
- **Postgres** (offline feature store + metadata)
- **FastAPI** (model serving API)
- **PyTorch** (training)
- **Airflow** (optional but included for realistic orchestration)
- **Prometheus + Grafana** (metrics dashboards)
- **pytest** (tests), **ruff** (lint), **mypy** (types)
- **Docker + Docker Compose** (local + VM deployment)
- **GitHub Actions** (CI)

> Kubernetes is intentionally *not required* for this project. It’s excellent at scale, but for a portfolio project (and many real workloads), Compose-on-VM is simpler, cheaper, and easier to operate.

---

## Repository layout (code tree)

```
.
├── README.md
├── docker-compose.yml
├── docker-compose.override.yml
├── .env.example
├── Makefile
├── pyproject.toml
├── scripts/
│   ├── bootstrap_local.sh
│   ├── bootstrap_cloud_vm.sh
│   ├── create_topics.sh
│   ├── seed_events.py
│   ├── run_e2e_smoke_test.sh
│   └── load_test_locust.py
├── infra/
│   ├── aws/                    # optional IaC examples
│   │   ├── terraform/
│   │   └── docs.md
│   └── hetzner/                # optional VM notes
│       └── docs.md
├── services/
│   ├── ingestion/
│   │   ├── app.py              # event producer API or simulator
│   │   └── schemas.py
│   ├── feature_pipeline/
│   │   ├── streaming_job.py    # Spark streaming -> Redis
│   │   ├── batch_job.py        # Spark batch -> Postgres
│   │   └── features.py         # feature definitions + transformations
│   ├── training/
│   │   ├── train.py            # trains a simple model
│   │   ├── dataset.py
│   │   └── model.py
│   ├── serving/
│   │   ├── api.py              # FastAPI prediction endpoint
│   │   ├── model_loader.py
│   │   └── ab_test.py          # simple bucketing logic
│   └── common/
│       ├── config.py
│       ├── logging.py
│       └── utils.py
├── dags/
│   └── feature_and_train_dag.py # Airflow DAG example
└── tests/
    ├── unit/
    │   ├── test_features.py
    │   ├── test_model.py
    │   └── test_serving_contract.py
    ├── integration/
    │   ├── test_kafka_to_redis.py
    │   ├── test_batch_to_postgres.py
    │   └── test_end_to_end.py
    └── load/
        └── locustfile.py
```

---

## Quickstart (local, Docker Compose)

### 1) Prerequisites

Install:
- Docker + Docker Compose
- Python 3.11 (for local tooling/tests)
- `make` (recommended)

Optional (only if you want to run Spark locally outside Docker):
- Java 17+
- Apache Spark 3.x

### 2) Clone + configure

```bash
git clone <YOUR_GITHUB_REPO_URL>
cd distributed-feature-store-serving
cp .env.example .env
```

Edit `.env` as needed. Defaults are safe for local dev.

### 3) Start the stack

```bash
make up
```

This should bring up:
- Kafka + ZooKeeper (or KRaft depending on compose)
- Postgres
- Redis
- FastAPI serving service
- Prometheus + Grafana
- (Optional) Airflow

### 4) Create Kafka topics + seed events

```bash
make kafka-topics
make seed
```

### 5) Run the feature pipeline

Streaming (writes online features to Redis):
```bash
make stream
```

Batch (writes offline features to Postgres):
```bash
make batch
```

### 6) Train + register a model

```bash
make train
```

### 7) Query the prediction API

```bash
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{"user_id":"u123","item_id":"i999"}'
```

Expected: a JSON response like:
```json
{"user_id":"u123","item_id":"i999","score":0.37,"model_version":"2026-02-21T00:00:00Z"}
```

---

## Makefile commands

Common targets (recommended workflow):

```bash
make up              # start all services
make down            # stop
make logs            # tail logs
make kafka-topics    # create required topics
make seed            # publish sample events
make stream          # run streaming feature job
make batch           # run batch feature job
make train           # train a model and publish artifact
make serve           # run serving API locally (outside docker)
make test            # all tests
make test-unit       # unit tests only
make test-int        # integration tests only
make load-test       # locust load test
make lint            # ruff
make typecheck       # mypy
```

---

## Event schema (data contract)

All events are JSON with a stable schema. Example:

```json
{
  "event_id": "evt_01H...",
  "event_type": "click",
  "ts": "2026-02-21T12:34:56Z",
  "user_id": "u123",
  "item_id": "i999",
  "query": "wireless headphones",
  "device": "mobile",
  "country": "US",
  "session_id": "s456",
  "metadata": {"referrer": "feed"}
}
```

### Topics

- `events.raw` (input)
- `features.online` (optional debug topic)
- `metrics.pipeline` (optional)

---

## Feature definitions (example)

Typical real-time features:
- `user_7d_clicks` (rolling count)
- `user_1h_clicks`
- `item_7d_clicks`
- `user_item_last_seen_seconds`
- `ctr_proxy = clicks / impressions` (where available)

Offline features mirror online definitions to avoid training/serving skew.

---

## Model training

The training job:
1. Reads labeled examples from Postgres (offline store)
2. Builds a dataset (user_id, item_id, features…)
3. Trains a simple model (logistic regression / small MLP)
4. Saves artifacts to `./artifacts/` (or optional MLflow)
5. Serving loads the latest “blessed” model at startup (or via reload endpoint)

> Keep it simple. A clean end-to-end system beats a fancy model every time for portfolio value.

---

## Model serving (FastAPI)

### Endpoints

- `POST /predict`  
  Takes identifiers, fetches features from Redis, returns prediction.

- `GET /healthz`  
  Liveness/readiness probe.

- `GET /metrics`  
  Prometheus metrics endpoint.

### Latency goals

Locally you should see typical p50 in single-digit ms for feature reads + inference (depends on hardware).

---

## A/B experiments (simple but real)

A/B split is done by consistent bucketing on `user_id`:
- Variant A: baseline model
- Variant B: new model/version (or different feature set)

The `services/serving/ab_test.py` module demonstrates:
- stable hashing
- rollout percentage
- per-variant metrics

---

## Observability

### Metrics

Prometheus scrapes:
- API request count/latency
- Redis feature lookup latency
- model inference latency
- pipeline lag (if you export it)

Grafana dashboards:
- API p50/p95
- error rates
- Kafka lag
- model score distribution (optional)

Access:
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (default user/pass in `.env.example`)

---

## Testing

This project includes **unit**, **integration**, and **end-to-end** tests.

### 1) Unit tests

Fast and local. No external services required.

```bash
make test-unit
```

Examples:
- feature transformation correctness
- model forward pass + serialization
- API request/response contract tests

### 2) Integration tests

Run against the Docker Compose stack.

```bash
make up
make test-int
```

Examples:
- publish event -> streaming job -> Redis feature present
- batch job -> Postgres tables updated
- serving reads Redis and returns response

### 3) End-to-end smoke test

Runs the full path:
seed events → compute features → train → serve → predict

```bash
make e2e
```

### 4) Load tests

Locust-based load test for the serving API:

```bash
make load-test
```

Then open the Locust UI (configured by Makefile, typically on port 8089).

#### Suggested targets
- **1000 queries/day** is small, but test bursts (e.g., 10–50 RPS) to validate headroom.
- Monitor p95 latency and error rate.

---

## CI (GitHub Actions)

Recommended workflow:
- On PR: lint + unit tests
- On main: lint + unit + integration tests (optionally), build/push Docker images

A typical `.github/workflows/ci.yml` should run:
- `ruff check`
- `pytest -m "not integration"`
- optional: `docker compose up` + integration test job

---

## Local development tips

### Python env

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e ".[dev]"
```

### Pre-commit (optional)

```bash
pip install pre-commit
pre-commit install
```

---

## Cloud deployment (simple VM + Docker Compose)

This is the recommended “cloud” setup for this repo: **one VM**, Docker, and Compose.

### Why this approach

- fast to deploy and iterate
- easy to debug
- cheap (great for portfolio projects)
- good enough for many real workloads

### Option A: Single VM (all services)

**Good for:** demos, small traffic, single-tenant projects

1. Provision a VM (Ubuntu 22.04 recommended)
   - 8 vCPU / 16–32 GB RAM is comfortable for the full stack
2. Install Docker + Compose
3. Clone the repo
4. Set `.env` (production values)
5. `make up`

#### Hardening checklist
- enable firewall (allow only 22, 80/443, and any ports you truly need)
- put the API behind a reverse proxy (Caddy or Nginx) with TLS
- store secrets in env vars (or a secrets manager if you prefer)
- enable backups for Postgres volumes

### Option B: Split CPU + GPU servers (cost control)

**CPU server (always-on):** Kafka, Postgres, Redis, Airflow, FastAPI  
**GPU server (scheduled/on-demand):** training only (if needed)

How it works:
- training can run on CPU for a simple model
- if you want GPU training, run `services/training/train.py` on the GPU box and push artifacts to object storage (S3/minio) or back to the CPU box

This keeps GPU costs low while the serving path stays stable.

---

## AWS deployment (optional reference)

If you want a “cloud-native” version:
- Kafka → MSK
- Postgres → RDS
- Redis → ElastiCache
- Serving → ECS/Fargate (or EKS)
- Spark → EMR (batch) or Glue/Spark jobs

This repo includes `infra/aws/docs.md` with an opinionated path and Terraform examples scaffold.

---

## Security notes

Minimum recommended security practices:
- never commit `.env`
- rotate credentials
- enable network policies/firewall rules
- validate input schema at ingestion and serving layers
- add rate limiting on `/predict` if exposed publicly

---

## Troubleshooting

### Kafka doesn’t start
- check available RAM
- confirm ports 9092/29092 aren’t in use
- `docker compose logs kafka`

### Spark job can’t connect to Kafka
- confirm you’re using the correct broker address inside the network (`kafka:9092`)
- verify topics exist: `make kafka-topics`

### Serving returns “features missing”
- streaming job may not have warmed Redis yet
- run `make seed` and keep `make stream` running
- check Redis keys: `docker exec -it redis redis-cli keys "*"`

---

## License

MIT (recommended for portfolio projects). Add a `LICENSE` file if you want it explicit.

---

## Next upgrades (if you want to go further)

- add a proper feature registry (Feast-compatible interface)
- model registry with MLflow + promotion stages
- drift monitoring: compare online feature stats vs training
- canary deployments for model versions
- chaos testing (kill Kafka/Redis containers and validate retries)
