# CLAUDE.md — Build Guide for AML-Sentinel SUT

> **Read this first.** This repository is a **System Under Test (SUT)**: a runnable, intentionally-realistic mock of an AML (Anti–Money Laundering) screening service. Its purpose is to let an SDET practice **data-quality testing** across an event-driven, multi-source pipeline.

## What you are building

A service called **AML-Sentinel** that ingests client KYC profiles, normalizes them, screens them against mocked external watchlist providers (sanctions / PEP / adverse-media), fuzzy-matches names, scores risk, makes a decision, persists an audit trail, and supports list-reconciliation. The whole thing runs locally via Docker Compose. A pytest + Allure harness tests it end-to-end.

## How to consume these docs

Build **strictly in the order** given by `docs/03_implementation_roadmap.md`. Each phase has a **Definition of Done (DoD)** and a **verification step**. Do not start a phase until the previous phase's verification passes. This is deliberate: the value of the SUT is that every layer is independently verifiable.

| Doc | Purpose |
|-----|---------|
| `docs/01_system_architecture.md` | Components, tech stack, architecture diagram, key/dependency map |
| `docs/02_data_flows.md` | End-to-end data flows + sequence diagrams, topics, schemas, idempotency keys |
| `docs/03_implementation_roadmap.md` | **Ordered**, iterative, individually-verifiable build phases (start here after reading 01/02) |
| `docs/04_data_generation.md` | Deterministic data + watchlist + golden-dataset generation instructions |

## Hard rules for the build

1. **Determinism.** All generated data uses a fixed seed (`SEED=42`). Re-running generators must produce byte-identical output.
2. **Golden datasets are the source of truth.** Matching/decision logic is validated against `data/golden/*.jsonl`. Never tune thresholds to fit a single test; tune to the golden set.
3. **Every record and event carries a `trace_id`.** It must be propagated unchanged from ingestion through to the audit row. This is the backbone of data-quality verification.
4. **No real third parties.** All external providers are mocks (see `docs/01` §7). Never call a real sanctions API.
5. **Structured JSON logs only.** One event per stage, machine-parseable (see `docs/02` §Logs).
6. **Idempotency everywhere.** Re-delivering the same Kafka message must not create duplicate DB rows.

## Primary stack (build this first) and stretch track

- **Primary:** Python 3.14.6 + FastAPI service, Redpanda (Kafka API), PostgreSQL, Redis, Docker Compose, pytest + Allure.
- **Stretch (optional, after Phase 9):** reimplement the screening worker in **Go** to mirror the real Exness service and exercise polyglot test tooling. Marked `[STRETCH]` in the roadmap.

## Suggested repo layout

```
payments_qa_demo/
├── CLAUDE.md                  # this file
├── docs/                      # the four design docs
├── docker-compose.yml
├── .gitlab-ci.yml
├── pyproject.toml
├── src/aml_sentinel/
│   ├── api/                   # FastAPI ingestion
│   ├── workers/               # Kafka consumers (normalize, screen, decide, reconcile)
│   ├── providers/             # provider-gateway client + caching
│   ├── matching/              # fuzzy matching + normalization
│   ├── decisioning/           # rules engine
│   ├── db/                    # models, migrations (Alembic)
│   ├── observability/         # structured logging, metrics
│   └── config.py
├── mocks/                     # mock provider HTTP services (World-Check, Dow Jones, ComplyAdvantage)
├── tools/datagen/             # data generators (see docs/04)
├── data/                      # generated profiles, watchlists, golden sets
└── tests/                     # pytest suites + Allure config + conftest fixtures
```

Start with `docs/03_implementation_roadmap.md`, Phase 0.
