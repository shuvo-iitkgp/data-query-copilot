# src/sql_generator.py
from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import Optional, Dict, Any

import torch
from transformers import AutoTokenizer, AutoModelForCausalLM

from src.schema_service import SchemaService
from src.sql_policy import SQLPolicy


CODE_FENCE_RE = re.compile(r"```(?:sql)?\s*([\s\S]*?)\s*```", re.IGNORECASE)


@dataclass(frozen=True)
class GenerationConfig:
    model_name: str = "Qwen/Qwen2.5-Coder-7B-Instruct"
    max_new_tokens: int = 256
    temperature: float = 0.0  # ignored when do_sample=False, kept for logging
    do_sample: bool = False
    top_p: float = 1.0
    repetition_penalty: float = 1.05
    device: Optional[str] = None  # "cuda", "cpu", etc.
    dtype: Optional[str] = None   # "float16", "bfloat16", "float32"


@dataclass(frozen=True)
class GenerationResult:
    sql_raw: str
    sql_clean: str
    prompt: str
    model_name: str
    latency_ms: int
    meta: Dict[str, Any]


def build_sql_prompt(
    *,
    schema_blob: str,
    question: str,
    policy: Optional[SQLPolicy] = None,
    error_context: Optional[str] = None,
) -> str:
    """
    Phase 2 prompt: schema + hard formatting rules + SQL anchor.
    Keep it minimal and stable.
    """
    policy = policy or SQLPolicy()

    # Mandatory LIMIT rule:
    # - If the query is NOT an aggregation query, enforce LIMIT.
    # This is a generation rule. Rewriter still enforces hard limits later.
    rules = f"""
You generate ONE SQLite query for analytics.

Hard rules:
- Output SQL only. No explanations. No markdown.
- Single statement. No semicolons.
- SELECT only. No PRAGMA, ATTACH, DETACH, transactions, or any write operations.
- Use ONLY the tables and columns listed in the schema.
- Do not use SELECT *.
- Use explicit column names.
- If the query does NOT use GROUP BY or an aggregate function (COUNT, SUM, AVG, MIN, MAX),
  you MUST include LIMIT {policy.default_limit}.
- Prefer simple queries that will execute on SQLite.

Schema:
{schema_blob}

User question:
{question}
""".strip()

    if error_context:
        rules += f"""

            Previous attempt feedback (JSON):
            {error_context}
            """.strip()

    rules += "\n\nSQL:\n"


    return rules


def _strip_code_fences(text: str) -> str:
    m = CODE_FENCE_RE.search(text)
    if m:
        return m.group(1).strip()
    return text.strip()


def _postprocess_to_sql(model_text: str) -> str:
    """
    Make the output safe-ish before validation:
    - strip code fences
    - take only the first statement-ish chunk
    - trim whitespace
    """
    s = _strip_code_fences(model_text)

    # If the model wrote extra junk after SQL, cut at the first blank line
    # after something that looks like SQL. Conservative.
    parts = s.splitlines()
    cleaned_lines = []
    for line in parts:
        # Stop if the model starts narrating
        if line.strip().lower().startswith(("explanation", "reason", "note")):
            break
        cleaned_lines.append(line)
    s = "\n".join(cleaned_lines).strip()

    # Also cut at first semicolon if present (validator will reject anyway)
    if ";" in s:
        s = s.split(";", 1)[0].strip()

    return s


def _select_device(user_device: Optional[str]) -> str:
    if user_device:
        return user_device
    return "cuda" if torch.cuda.is_available() else "cpu"


def _select_dtype(device: str, dtype_str: Optional[str]):
    if dtype_str is None:
        # sensible defaults
        if device == "cuda":
            return torch.float16
        return torch.float32

    d = dtype_str.lower()
    if d in ("float16", "fp16"):
        return torch.float16
    if d in ("bfloat16", "bf16"):
        return torch.bfloat16
    if d in ("float32", "fp32"):
        return torch.float32
    raise ValueError(f"Unsupported dtype: {dtype_str}")


class SQLGenerator:
    """
    Deterministic NL -> SQL generator. Does NOT validate or execute.
    That is Phase 2/3 boundary: generation is untrusted.
    """

    def __init__(self, schema_service: SchemaService, cfg: Optional[GenerationConfig] = None):
        self.schema_service = schema_service
        self.cfg = cfg or GenerationConfig()

        self.device = _select_device(self.cfg.device)
        self.dtype = _select_dtype(self.device, self.cfg.dtype)

        self.tokenizer = AutoTokenizer.from_pretrained(self.cfg.model_name, use_fast=True)
        self.model = AutoModelForCausalLM.from_pretrained(
            self.cfg.model_name,
            torch_dtype=self.dtype,
            device_map="auto" if self.device == "cuda" else None,
        )

        if self.device != "cuda":
            self.model.to(self.device)

        self.model.eval()

    @torch.inference_mode()
    def generate_sql(self, question: str, policy: Optional[SQLPolicy] = None, error_context: Optional[str] = None,
) -> GenerationResult:
        schema_blob = self.schema_service.schema_blob()
        prompt = build_sql_prompt(
            schema_blob=schema_blob,
            question=question,
            policy=policy,
            error_context=error_context,
        )

        inputs = self.tokenizer(prompt, return_tensors="pt")
        inputs = {k: v.to(self.device) for k, v in inputs.items()}

        t0 = time.time()
        gen_kwargs = dict(
            max_new_tokens=self.cfg.max_new_tokens,
            do_sample=False,  # deterministic
            pad_token_id=self.tokenizer.eos_token_id,
            eos_token_id=self.tokenizer.eos_token_id,
            repetition_penalty=self.cfg.repetition_penalty,
        )
        
        if self.cfg.do_sample:
          gen_kwargs.update(
              do_sample=True,
              temperature=self.cfg.temperature,
              top_p=self.cfg.top_p,
          )

        out = self.model.generate(
            **inputs,
            **gen_kwargs
        )


        latency_ms = int((time.time() - t0) * 1000)

        decoded = self.tokenizer.decode(out[0], skip_special_tokens=True)

        # Extract the "new" text beyond the prompt if present
        if decoded.startswith(prompt):
            completion = decoded[len(prompt):].strip()
        else:
            completion = decoded.strip()

        sql_clean = _postprocess_to_sql(completion)

        return GenerationResult(
            sql_raw=completion,
            sql_clean=sql_clean,
            prompt=prompt,
            model_name=self.cfg.model_name,
            latency_ms=latency_ms,
            meta={
                "device": self.device,
                "dtype": str(self.dtype),
                "max_new_tokens": self.cfg.max_new_tokens,
                "do_sample": self.cfg.do_sample,
                "temperature": self.cfg.temperature,
                "top_p": self.cfg.top_p,
                "repetition_penalty": self.cfg.repetition_penalty,
            },
        )
