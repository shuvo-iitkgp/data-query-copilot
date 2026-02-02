# src/schema_service.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional

from src.schema_loader import load_schema, serialize_schema_for_prompt, Schema

@dataclass
class SchemaService:
    db_path: str
    _schema: Optional[Schema] = None
    _schema_blob: Optional[str] = None

    def refresh(self) -> None:
        self._schema = load_schema(self.db_path, include_stats=False)
        self._schema_blob = serialize_schema_for_prompt(self._schema)

    def schema(self) -> Schema:
        if self._schema is None:
            self.refresh()
        return self._schema

    def schema_blob(self) -> str:
        if self._schema_blob is None:
            self.refresh()
        return self._schema_blob

    def schema_version(self) -> str:
        return self.schema().schema_version
