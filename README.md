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
