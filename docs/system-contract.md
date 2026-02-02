## System Contract

**Input**: Natural-language analytics question + optional filters (date range, region, product, etc.)

**Output**:

- Generated SQL (SQLite dialect)

- Query explanation (plain English)

- Tabular results (rows + column names)

- Execution metadata (timestamp, runtime, row count, data source version)

**Hard guarantees** :

- Read only queries only

- Single statement only (no multi-statement , no semicolons)

- Only `SELECT` allowed

- No user defined functions, no extensions, no external calls

- No Python, no code execution path from model output

- Full logging: prompt, generated SQL, validation decisions and results metadata

## Trust boundary diagram

**Zone A**: Untrusted

User natural language

LLM output (SQL text is untrusted until proven safe)

**Zone B**: Trusted control layer

Schema loader (reads DB metadata)

SQL validator + policy engine (rules, allowlists)

Query rewriter (adds LIMIT, blocks risky constructs)

Audit logger (append-only)

**Zone C**: Data plane

SQLite database opened in read-only mode

Query executor with timeouts and row limits

## SQL dialect definition (SQLite scope)

Allowed:

- SELECT ... FROM ...

- WHERE, GROUP BY, HAVING, ORDER BY

- JOIN (INNER/LEFT) with explicit ON

- Aggregations: COUNT, SUM, AVG, MIN, MAX

- Simple expressions, CASE WHEN

- Window functions: optional (either “not supported” or “supported with allowlist”)
