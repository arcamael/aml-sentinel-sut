# 03 — Implementation Roadmap: AML-Sentinel

Build **in order**. Each phase ends with a **Definition of Done (DoD)** and a concrete **Verification** that must pass before moving on. The point is that the SUT is correct-by-construction at every layer, so the SDET can trust each seam while testing the next.

Legend: ✅ DoD · 🔎 Verification · `[STRETCH]` optional.

---

## Phase 0 — Scaffolding & infrastructure

**Goal:** one-command local environment.

**Build:**
- Repo layout per `CLAUDE.md`; `pyproject.toml` (FastAPI, confluent-kafka or aiokafka, SQLAlchemy, Alembic, redis, structlog, prometheus-client, rapidfuzz, unidecode, faker; dev: pytest, allure-pytest, testcontainers, httpx, freezegun).
- `docker-compose.yml`: `postgres`, `redpanda`, `redis`, plus placeholders for `api`, `mocks`, workers. Healthchecks on all.
- A `make up` / `make down` / `make logs` convenience.
- Topic bootstrap script that creates the 5 topics from doc 02 §1.

✅ All containers report healthy; topics exist.
🔎 `docker compose up -d && docker compose ps` shows all healthy; `rpk topic list` lists the 5 topics; `psql`, `redis-cli ping` succeed.

---

## Phase 1 — Data model & persistence

**Goal:** the system of record exists and is migratable.

**Build:**
- SQLAlchemy models + Alembic migrations for all tables in doc 02 §4.
- Append-only enforcement on `audit` (trigger or DB rule rejecting UPDATE/DELETE).
- A tiny `db/seed_smoke.py` that inserts one row per table respecting FKs.

✅ Migrations apply cleanly from empty DB; FKs + audit immutability enforced.
🔎 `alembic upgrade head` succeeds; smoke seed runs; attempting `UPDATE audit ...` raises; `\d+` shows constraints.

---

## Phase 2 — Ingestion API

**Goal:** accept a KYC profile, persist it, emit an event.

**Build:**
- `POST /clients` with Pydantic validation (required: name, dob, nationality; optional: residence, document_ids). Generate `trace_id` (UUIDv7) if absent.
- Write `raw_profile`; produce `client.submitted` (envelope from doc 02 §2).
- `GET /clients/{client_id}` for inspection; `GET /health`; `/metrics`.
- Structured `ingest` log line.

✅ Valid payload → 201 + persisted row + Kafka message; invalid → 422.
🔎 `curl POST` returns 201; `SELECT * FROM raw_profile` shows the row; `rpk topic consume client.submitted` shows the envelope with matching `trace_id`; bad payload → 422.

---

## Phase 3 — Normalizer worker

**Goal:** deterministic canonicalization — the first real data-quality surface.

**Build:**
- Consumer on `client.submitted` with idempotency (doc 02 §4.8).
- Normalization: `unidecode` transliteration, name-part parsing, DOB→ISO, country→ISO-2, casing/whitespace canonicalization, deterministic `profile_hash`.
- Persist `normalized_profile`; produce `profile.normalized`; `normalize` log line.

✅ Dirty inputs map to expected canonical outputs; same input → same `profile_hash`; redelivery creates no duplicate.
🔎 Run against `data/golden/normalization.jsonl` (doc 04): each input's produced `normalized_profile` equals `expected`. Re-consume the same offset → row count unchanged.

---

## Phase 4 — Provider gateway + mock providers

**Goal:** reliable, observable access to (mocked) external lists.

**Build:**
- Three mock providers (`mocks/world_check`, `mocks/dow_jones`, `mocks/comply_advantage`): query API, `list_version`, `/health`, `/_state`, `/_control/fault` (timeout|500|slow|malformed|empty), seedable from `data/watchlists/*`.
- Provider gateway: per-provider timeout, bounded retries with backoff, circuit breaker, Redis cache keyed `(provider_id, name_hash, list_version)`.

✅ Gateway returns planted matches; honors timeouts/faults gracefully; cache hit on repeat.
🔎 Query a known sanctioned name → candidate returned with correct `list_version`. Trigger `POST /_control/fault {timeout}` → gateway returns degraded result (not a crash) and logs `WARNING`. Repeat identical query → `cache_hits` increments in logs/metrics.

---

## Phase 5 — Screening worker + fuzzy matching

**Goal:** turn a normalized profile into scored, persisted matches.

**Build:**
- Consumer on `profile.normalized`; call gateway across all three list types.
- Fuzzy matching (`rapidfuzz`): name similarity + alias handling + name-order tolerance + DOB corroboration; emit candidate `score ∈ [0,1]`.
- Persist `screening` + `match`; capture `list_versions`; produce `screening.completed`; `screen` log line.

✅ Matching meets precision/recall targets on the golden matching set; persisted matches agree with the emitted event and the log.
🔎 Run `data/golden/matching.jsonl` (planted true/false pairs): precision ≥ 0.95, recall ≥ 0.90 at the configured threshold (tune to golden, not to single cases). Assert `screen.detail.matches == COUNT(match)` for each `screening_id`.

---

## Phase 6 — Decision engine

**Goal:** explainable CLEAR / FLAG / ESCALATE with an immutable audit trail.

**Build:**
- Rules engine (config-driven table): e.g. any `sanctions` match ≥ τ → `ESCALATE`; `pep` tier 1–2 → `ESCALATE`, tier 3–4 → `FLAG`; `adverse_media` ≥ τ → `FLAG`; nothing → `CLEAR`. Emit `reason_codes[]`.
- Persist `decision` + append-only `audit` snapshot (inputs + matches + rule trace). Produce `decision.made`; `decide` log line.

✅ Every completed screening yields exactly one decision; decisions match the golden decision set; audit captures the full rule trace.
🔎 Run `data/golden/decisions.jsonl`: produced `outcome` + `reason_codes` equal expected. `SELECT` proves 1:1 `screening`→`decision`. Audit snapshot contains the matches that drove the outcome.

---

## Phase 7 — Reconciliation

**Goal:** keep screenings fresh when lists change.

**Build:**
- Consumer on `watchlist.updated`; injectable `Clock`.
- Upsert local `watchlist_entry`, bump `list_version`, select clients last screened against an older version, re-emit `profile.normalized` with `rescreen_reason`, record `reconciliation_run`.

✅ Adding a name that matches a previously-cleared client flips them to FLAG/ESCALATE with a fresh screening referencing the new version.
🔎 Use `data/updates/scenario_add_match.jsonl`: a CLEAR client matching the new entry becomes FLAG/ESCALATE; new `screening.list_versions` references the new version; `reconciliation_run.newly_flagged == 1`; no stale-version active screenings remain.

---

## Phase 8 — Observability & data-quality monitors

**Goal:** make every quality invariant measurable.

**Build:**
- Prometheus metrics: throughput per stage, match-rate, decision mix, cache hit-rate, dead-letter count, reconciliation lag.
- Scheduled data-quality monitors implementing doc 02 §7 (completeness, orphans, lineage, coverage, freshness, idempotency, audit immutability, determinism); breaches emit to `mocks/alert_sink`.
- Dead-letter handling for failed messages.

✅ Metrics endpoint live; each monitor passes on healthy data and fires an alert on injected corruption.
🔎 `GET /metrics` returns the series. Inject an orphan `match` → orphan monitor fires → `alert_sink` records it. Remove a `normalized_profile` → completeness monitor fires.

---

## Phase 9 — Test harness (the SDET deliverable)

**Goal:** the artifact that proves the SUT — pytest + Allure + CI.

**Build:**
- `conftest.py` session fixtures: spin up stack (compose or testcontainers), wait for readiness (doc 01 §6), seed data, create Kafka consumers.
- Test layers:
  - **Unit:** normalization rules, fuzzy scorer, decision rules (golden-driven, parametrized).
  - **Integration:** gateway↔mocks incl. fault injection; consumer idempotency.
  - **E2E:** generator → API → … → `decision.made`, asserting DB + events + logs agree.
  - **SQL data-quality:** the full doc 02 §7 catalog as tests.
  - **Reconciliation:** the Phase 7 scenarios.
- Allure annotations (epics/features/steps, attachments of SQL + payloads).
- `.gitlab-ci.yml`: stages `lint → unit → integration → e2e → allure-report`; `[STRETCH]` GitHub Actions mirror.

✅ `pytest` green across layers; Allure report generated; CI pipeline passes.
🔎 `pytest -q` passes; `allure serve` shows the structured report; pushing to GitLab runs all stages green.

---

## Phase 10 `[STRETCH]` — Go re-implementation of the screening worker

**Goal:** mirror the real Exness service and exercise polyglot tooling.

**Build:** reimplement the Phase 5 worker in Go (same topics, same DB, same golden contracts). Keep the Python harness — it must pass unchanged against the Go worker (contract parity).

✅ Go worker is a drop-in: all Phase 5/9 verifications pass against it.
🔎 Swap the worker in compose; rerun the Phase 9 harness with zero test changes → green.

---

## Build-order rationale (why this sequence)

You always have a verifiable substrate before adding the next layer: infra → storage → entry point → each transform, each gated by golden data. By Phase 5 you can already test matching in isolation; by Phase 7 the hardest real-world behavior (reconciliation/freshness) is exercisable; Phase 9 ties it together. Nothing later silently invalidates something earlier, which is exactly the property an SDET wants in a system under test.
