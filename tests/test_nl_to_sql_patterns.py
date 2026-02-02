from src.schema_service import SchemaService
from src.sql_generator import SQLGenerator, GenerationConfig
from src.pipeline import generate_validate_rewrite


def test_by_state_count_pattern():
    svc = SchemaService("tests/fixtures/nrel_sample.sqlite")
    gen = SQLGenerator(svc, GenerationConfig(max_new_tokens=128, do_sample=False))

    out = generate_validate_rewrite("How many stations are there by state?", gen)

    assert out.validation.ok, out.validation.reasons
    sql = out.final_sql.upper()

    assert "FROM FUEL_STATIONS" in sql
    assert "GROUP BY STATE" in sql
    assert "COUNT" in sql
    assert "ORDER BY" in sql
    assert "LIMIT" in sql
