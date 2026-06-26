# 04 — Data Generation Instructions: AML-Sentinel

Deterministic, layered data so every phase has the inputs it needs and a ground truth to assert against. **All generation is seeded (`SEED=42`) and reproducible.**

## 0. Principles

- **Determinism:** seed Faker and every RNG; re-running produces byte-identical files. Sort output before writing.
- **Ground truth first:** generate watchlists, then derive profiles *from* them so planted matches are known by construction.
- **Layered categories:** clean / dirty / edge / adversarial — each exercises a different data-quality failure mode.
- **Golden = expected output, not just input:** golden files carry the expected normalized form, expected matches, and expected decision so tests assert exact behavior.
- **Formats:** `JSONL` for records and goldens (one object per line); `CSV` only where a provider mock expects tabular import. UTF-8, `\n` line endings.

## 1. Generator CLI (`tools/datagen`)

A single entry point Claude Code should implement:

```
python -m tools.datagen all --seed 42 --out data/
# or granular:
python -m tools.datagen watchlists --seed 42 --out data/watchlists/
python -m tools.datagen profiles   --seed 42 --out data/profiles/ --n 2000
python -m tools.datagen golden      --seed 42 --out data/golden/
python -m tools.datagen updates     --seed 42 --out data/updates/
```

Output tree:

```
data/
├── watchlists/   sanctions.jsonl  pep.jsonl  adverse_media.jsonl  manifest.json
├── profiles/     clean.jsonl  dirty.jsonl  edge.jsonl  adversarial.jsonl
├── golden/       normalization.jsonl  matching.jsonl  decisions.jsonl
└── updates/      scenario_add_match.jsonl  scenario_remove.jsonl  scenario_version_bump.jsonl
```

## 2. Watchlist generation (build first)

Each provider serves one list type. Plant a controlled number of "true" entities that profiles will later match.

**`watchlist_entry` record:**
```json
{
  "entry_id": "wl_sanctions_0001",
  "provider_id": "world_check",
  "list_type": "sanctions",
  "list_version": "v1",
  "entity_name": "Ivan Petrov",
  "aliases": ["Ivan Petroff", "Иван Петров"],
  "dob_iso": "1972-03-14",
  "country_iso2": "RU",
  "risk_payload": {"program": "OFAC-SDN", "pep_tier": null, "media_confidence": null}
}
```

Generation rules:
- **Sanctions (`world_check`)**: ~200 entries. Include OFAC-style names, a handful of non-Latin originals with Latin aliases, some with partial DOB (`null` day) to test DOB corroboration.
- **PEP (`dow_jones`)**: ~300 entries across tiers 1–4 (tier drives decision severity). Include family-member style aliases.
- **Adverse media (`comply_advantage`)**: ~150 entries with `media_confidence ∈ [0.3,0.99]` and intentionally **near-duplicate** articles for the same entity (tests dedupe).
- Write `manifest.json`: per-provider counts, `list_version`, and the list of `entry_id`s flagged as "plant targets" (used to derive matching profiles).

## 3. Profile generation (derive from watchlists)

**`raw_profile` (KYC) record:**
```json
{
  "client_id": "cli_000123",
  "trace_id": "<uuidv7>",
  "full_name": "Ivan  Petroff",
  "dob": "14/03/1972",
  "nationality": "Russia",
  "residence_country": "Cyprus",
  "document_ids": [{"type":"passport","value":"RU1234567"}],
  "_meta": {"category":"dirty","expected_match_entry_id":"wl_sanctions_0001"}
}
```
`_meta` is generator-only ground truth (stripped before POST; mirrored into golden files).

### Categories (target mix for `--n 2000`)

| Category | Share | What it stresses | Examples |
|----------|-------|------------------|----------|
| **clean** | 55% | happy path; mostly non-matches | well-formed names, ISO dates, valid countries |
| **dirty** | 25% | normalization robustness | double spaces, mixed case, `DD/MM/YYYY` vs `YYYY-MM-DD`, country full-names, trailing punctuation |
| **edge** | 12% | boundary handling | non-Latin scripts (Cyrillic/Arabic/Chinese), hyphenated/compound surnames, single-name entities, missing DOB, very long names |
| **adversarial** | 8% | match accuracy | true-positives near threshold (typo of a sanctioned name), hard true-negatives (common name shared with a PEP but different DOB), alias-only matches, transposed first/last name |

Planting rule: ~15% of profiles are derived from a watchlist entry (perturbed by typo/translit/alias/name-order) and carry `expected_match_entry_id`. The rest are non-matches, **including deliberate "looks risky but isn't"** cases (same name, different DOB/country) to generate realistic false-positive pressure.

## 4. Golden datasets (expected outputs)

### 4.1 `normalization.jsonl`
```json
{"input": {"full_name":"Ivan  Petroff","dob":"14/03/1972","nationality":"Russia"},
 "expected": {"canonical_name":"ivan petroff","name_parts":{"first":"ivan","last":"petroff"},
              "dob_iso":"1972-03-14","nationality_iso2":"RU","profile_hash":"<deterministic>"}}
```
Cover every dirty/edge transformation at least once. `profile_hash` is computed by the same canonicalization the Normalizer uses (generator imports that function to stay in lock-step).

### 4.2 `matching.jsonl`
```json
{"profile_name":"Ivan Petroff","candidate_name":"Ivan Petrov","dob_profile":"1972-03-14",
 "dob_candidate":"1972-03-14","list_type":"sanctions","expected_match":true,"min_score":0.85}
```
Include balanced true/false pairs spanning: exact, typo, transliteration, alias, name-order swap, common-name-different-DOB (expected false), substring traps (expected false). These define the precision/recall target in Phase 5.

### 4.3 `decisions.jsonl`
```json
{"matches":[{"list_type":"sanctions","score":0.93}],
 "expected": {"outcome":"ESCALATE","reason_codes":["SANCTIONS_MATCH"]}}
```
One row per distinct rule path (sanctions→ESCALATE, pep tier1-2→ESCALATE, pep tier3-4→FLAG, adverse_media→FLAG, no-match→CLEAR, multi-match precedence).

## 5. Reconciliation update scenarios (`data/updates/`)

- **`scenario_add_match.jsonl`**: adds a sanctions entry matching a specific previously-CLEAR `client_id`; bumps `v1→v2`. Expected: that client → ESCALATE after reconcile.
- **`scenario_remove.jsonl`**: removes/ deactivates an entry a client matched; bump version. Expected: that client → CLEAR on re-screen.
- **`scenario_version_bump.jsonl`**: version bump with no substantive change. Expected: re-screen runs but outcomes unchanged (tests freshness without false flips).

Each line: `{ "provider_id", "list_type", "change", "entry"?, "target_client_id", "new_list_version", "expected_outcome_after" }`.

## 6. Volume & performance fixtures

- Default functional set: **2,000 profiles**, ~650 watchlist entries.
- `--n 50000` profile set (no goldens) for Phase 8 throughput/metrics and optional k6/Locust load profiles.
- Keep a tiny **`smoke` set** (`--n 20`) for fast CI unit runs.

## 7. Determinism & integrity self-checks (generator must emit `manifest.json`)

After generation, the generator validates and records:
- counts per file and per category match the requested mix (±1 rounding);
- every `expected_match_entry_id` references a real watchlist entry;
- every golden `profile_hash` recomputes identically;
- no duplicate `client_id`/`entry_id`;
- re-running with the same seed yields an identical sha256 over the sorted output (printed for CI to assert).

🔎 **Generator acceptance:** `python -m tools.datagen all --seed 42 --out data/ && python -m tools.datagen verify --out data/` exits 0 and the printed dataset sha256 matches the value committed in `data/.sha256`.

## 8. How the data wires into the phases

| Data file | Consumed by phase |
|-----------|-------------------|
| `watchlists/*` | Phase 4 (mock seeding), 5, 7 |
| `golden/normalization.jsonl` | Phase 3 |
| `golden/matching.jsonl` | Phase 5 |
| `golden/decisions.jsonl` | Phase 6 |
| `profiles/*` | Phase 2–6 E2E, Phase 9 |
| `updates/*` | Phase 7 |
| `--n 50000` set | Phase 8 |
| `smoke` set | Phase 9 CI unit stage |
