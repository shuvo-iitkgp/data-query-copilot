## Audit logging spec

Log record fields:

request_id (UUID)

timestamp (UTC)

user_query (raw)

schema_version hash

generated_sql (raw)

validated_sql (post-rewrite)

validator_decisions (pass/fail + reasons)

execution_time_ms

rows_returned

error (if any)

result_fingerprint (hash of returned table)

Store logs as JSONL. Easy to diff, easy to ship.
