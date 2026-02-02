from src.schema_service import SchemaService
from src.sql_generator import SQLGenerator, GenerationConfig

svc = SchemaService("tests/fixtures/nrel_sample.sqlite")
gen = SQLGenerator(svc, GenerationConfig(max_new_tokens=200, do_sample=False))

res = gen.generate_sql("How many stations are there by state?")
print("SQL:\n", res.sql_clean)
print("Latency(ms):", res.latency_ms)
