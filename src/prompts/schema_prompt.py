# src/prompts/schema_prompt.py

SCHEMA_CONTEXT_TEMPLATE = """You are generating SQLite SELECT queries for analytics.

You MUST use only the tables and columns listed below.
If a field is not listed, you must not reference it.
Prefer explicit JOIN ... ON with correct keys.
Return only SQL.

{schema_blob}
"""
