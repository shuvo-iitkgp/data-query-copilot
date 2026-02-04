# data-query-copilot

A production oriented GenAI assistant that translates natural language questions into safe, executable SQL for structured analytics.
The system combines LLM reasoning with schema awareness, constrained query execution, and automated evaluation to produce factual and consistent results over relational datasets.

## Key capabilities

- Natural language to SQL translation over structured datasets

- Schema aware prompting to reduce hallucinated queries

- Safe SQL execution with read only constraints

- Automatic retries and error correction for failed queries

- Evaluation framework using held out test queries

- Automated data summaries and reporting workflows

## Architecture Overview

- User submits a natural language analytics question

- The system retrieves schema metadata and constraints

- An LLM generates a candidate SQL query

- The query is validated and executed against the database

## Tech Stack

- Python

- SQL

- LangChain

- HuggingFace Transformers

- SQLite or Postgres

- Pandas

## Repository Structure

```
data-query-copilot/
│
├── README.md
│   └── Architecture overview, 6-phase design, usage, metrics
│
├── pyproject.toml / requirements.txt
│   └── Python dependencies (transformers, torch, sqlite, pytest)
│
├── src/
│   ├── __init__.py
│   │
│   ├── schema_loader.py
│   │   └── Phase 1
│   │       - Extract tables, columns, types, keys from SQLite
│   │       - Optional column statistics
│   │
│   ├── schema_service.py
│   │   └── Phase 1
│   │       - Schema caching
│   │       - Stable schema hash
│   │       - Prompt-ready schema blob
│   │
│   ├── sql_generator.py
│   │   └── Phase 2
│   │       - NL → SQL generation (Qwen2.5-Coder)
│   │       - Deterministic decoding
│   │       - Prompt templates + rules
│   │
│   ├── sql_policy.py
│   │   └── Phase 2 / 3
│   │       - Allowlists, blocklists
│   │       - Dialect and construct constraints
│   │
│   ├── sql_validator.py
│   │   └── Phase 3
│   │       - SQL safety validation
│   │       - Single-statement enforcement
│   │
│   ├── sql_rewriter.py
│   │   └── Phase 3
│   │       - LIMIT injection
│   │       - Defensive query rewrites
│   │
│   ├── query_executor.py
│   │   └── Phase 3
│   │       - Read-only SQLite execution
│   │       - Timeouts and row limits
│   │
│   ├── retry_logic.py
│   │   └── Phase 4
│   │       - Error-aware retries
│   │       - Retry caps
│   │       - Oscillation detection
│   │
│   ├── summarizer.py
│   │   └── Phase 6
│   │       - Table summaries
│   │       - Aggregates and trend hints
│   │
│   ├── report_generator.py
│   │   └── Phase 6
│   │       - Multi-query analytics reports
│   │       - Markdown + JSON outputs
│   │
│   ├── end_to_end.py
│   │   └── Unified pipeline
│   │       - run_and_report()
│   │       - End-to-end orchestration
│   │
│   └── cli.py
│       └── Dry-run + debugging CLI
│
├── scripts/
│   ├── csv_to_sqlite.py
│   │   └── Dataset ingestion utility
│   │
│   ├── gen_sql.py
│   │   └── Standalone SQL generation test
│   │
│   ├── run_ablations.py
│   │   └── Phase 2 prompt ablations
│   │
│   ├── eval_retries.py
│   │   └── Phase 4 retry effectiveness metrics
│   │
│   ├── run_eval.py
│   │   └── Phase 5 evaluation harness
│   │
│   ├── run_report.py
│   │   └── Phase 6 scheduled report runner
│   │
│   └── demo_end_to_end.py
│       └── One-command full system demo
│
├── eval/
│   ├── cases_nrel.jsonl
│   │   └── Phase 5 held-out NL queries
│   │
│   └── mock_team_report.json
│       └── Phase 6 recurring analytics config
│
├── tests/
│   ├── test_schema_drift.py
│   │   └── Phase 1 schema snapshot tests
│   │
│   ├── test_sql_validator.py
│   │   └── Phase 3 safety tests
│   │
│   ├── test_retry_logic.py
│   │   └── Phase 4 retry convergence tests
│   │
│   └── fixtures/
│       └── nrel_sample.sqlite
│           └── Read-only test database
│
├── reports/
│   ├── phase2_ablations/
│   ├── phase4_retries/
│   ├── phase5_eval/
│   ├── mock_team/
│   └── demo/
│       └── Generated markdown + JSON artifacts
│
└── .gitignore
    └── Model caches, local DBs, logs

```

## End to End Flow Diagram

```
┌──────────────────────────────┐
│          USER QUERY          │
│ "How many stations by state?"│
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────────────────────┐
│ Phase 1: Schema Intelligence Layer            │
│----------------------------------------------│
│ • Load DB schema (tables, columns, types)    │
│ • Optional column statistics                 │
│ • Deterministic schema hash                  │
│ • Schema snapshot tests (drift detection)    │
└──────────────┬───────────────────────────────┘
               │ schema blob + version
               ▼
┌──────────────────────────────────────────────┐
│ Phase 2: NL → SQL Generation Core             │
│----------------------------------------------│
│ • Deterministic LLM (Qwen2.5-Coder-7B)        │
│ • Schema-aware prompting                     │
│ • Hard rules (no SELECT *, LIMIT rules)      │
│ • SQL string output only                     │
└──────────────┬───────────────────────────────┘
               │ untrusted SQL
               ▼
┌──────────────────────────────────────────────┐
│ Phase 3: Query Validation & Safe Execution   │
│----------------------------------------------│
│ • SQL allowlist / blocklist                  │
│ • Single-statement enforcement               │
│ • Read-only SQLite connection                │
│ • Timeouts + row limits                      │
│ • Clear error taxonomy                       │
└──────────────┬───────────────────────────────┘
               │ execution result OR error
               ▼
┌──────────────────────────────────────────────┐
│ Phase 4: Automatic Retry & Self-Correction   │
│----------------------------------------------│
│ • Structured error feedback                  │
│ • Error-aware prompt retry                   │
│ • Retry caps                                 │
│ • Oscillation detection                      │
│ • Full retry logs                            │
└──────────────┬───────────────────────────────┘
               │ final SQL + results
               ▼
┌──────────────────────────────────────────────┐
│ Phase 5: Evaluation Harness                  │
│----------------------------------------------│
│ • Held-out NL test set                       │
│ • SQL pattern + result checks                │
│ • Execution success metrics                  │
│ • Correctness metrics                        │
│ • Consistency across runs                    │
└──────────────┬───────────────────────────────┘
               │ verified results
               ▼
┌──────────────────────────────────────────────┐
│ Phase 6: Reporting & Summarization Layer     │
│----------------------------------------------│
│ • Automatic table summaries                  │
│ • Aggregates & trend explanations            │
│ • Business-readable markdown reports         │
│ • JSON audit logs                            │
└──────────────────────────────────────────────┘
```

## Evaluation

The system includes an evaluation harness using held out natural language queries with expected SQL outputs or result checks.
Metrics include:

- Query execution success rate

- Result correctness

- Consistency across repeated runs

This allows tracking improvements as prompts or models change.

## Example Use Cases

- Ad hoc analytics without writing SQL manually

- Rapid exploration of unfamiliar datasets

- Automated generation of recurring reports

- Internal analytics support tools
  Results are returned with optional summaries or reports

Queries and responses are logged for evaluation and analysis

```

```
