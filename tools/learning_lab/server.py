"""Local learning workbench backed by the checkout's real rules engine.

Run from the repository root: python tools/learning_lab/server.py
No Spark session, Delta connection, external service, or package mutation occurs.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import mimetypes
import secrets
import sys
from copy import deepcopy
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

from rules_engine import (  # noqa: E402
    DeltaRowSerializer,
    FunctionRegistry,
    RulesetValidator,
    YamlRulesetCompiler,
    YamlRulesetExporter,
    __version__,
    build_authoring_manifest,
    register_standard_functions,
)
from rules_engine.runtime import SparkRowEvaluator  # noqa: E402


def json_value(value):
    """Render exact values with explicit type tags instead of rounding decimals."""
    if isinstance(value, Decimal):
        return {"$decimal": str(value)}
    if isinstance(value, int) and not isinstance(value, bool) and abs(value) > 2**53 - 1:
        return {"$integer": str(value)}
    if isinstance(value, (datetime, date)):
        return {"$" + type(value).__name__: value.isoformat()}
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value):
        return json_value(asdict(value))
    if isinstance(value, dict):
        return {str(key): json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [json_value(item) for item in value]
    if isinstance(value, (bytes, bytearray)):
        return {"$binary": value.hex()}
    return value


def registry():
    result = FunctionRegistry()
    register_standard_functions(result)
    return result


class ObservedEvaluator(SparkRowEvaluator):
    """Observe the real execution loop without reimplementing its decisions.

    This development-only adapter uses private evaluator hooks. Differential
    tests guard it against the public compact evaluator as the engine evolves.
    Unlike production full audit, it includes non-matching rules for teaching.
    """

    def __init__(self, functions):
        super().__init__(functions)
        self.steps = []
        self.committed = {}
        self.pending = {}
        self.current = None

    def _finish_commit(self):
        if self.current is not None and self.current["matched"]:
            self.committed.update(self.pending)
            self.current["after"] = deepcopy(self.committed)
            self.current["status"] = "committed"
        self.pending = {}

    def _evaluate_rule(self, rule, row, assigned_values=None):
        self._finish_commit()
        self.current = {
            "rule_id": rule.rule_id,
            "name": rule.rule_name,
            "order": rule.rule_order,
            "stop_on_match": rule.stop_on_match,
            "before": deepcopy(self.committed),
            "after": deepcopy(self.committed),
            "conditions": [],
            "assignments": [],
            "matched": False,
            "status": "evaluating",
        }
        self.steps.append(self.current)
        matched, traces = super()._evaluate_rule(rule, row, assigned_values)
        self.current.update(matched=matched, status="matched" if matched else "not matched")
        return matched, traces

    def _evaluate_condition(self, condition, group, row, assigned_values=None):
        try:
            trace = super()._evaluate_condition(condition, group, row, assigned_values)
        except Exception as exc:  # noqa: BLE001 - display the actual row error in the lab
            self.current["conditions"].append({
                "condition_id": condition.condition_id,
                "operator": condition.operator.value,
                "error": f"{type(exc).__name__}: {exc}",
            })
            raise
        self.current["conditions"].append(asdict(trace))
        return trace

    def _resolve_operand_resolution(self, operand, row, assigned_values=None):
        resolution = super()._resolve_operand_resolution(operand, row, assigned_values)
        # Preserve the concrete type in this teaching view. Production trace
        # strings alone cannot distinguish a Decimal from a string of digits.
        resolution.trace["resolved_type"] = type(resolution.value).__name__
        resolution.trace["resolved_value"] = deepcopy(resolution.value)
        return resolution

    def _observe_assignment(self, rule, assignment, old, value):
        self.pending[assignment.target_field] = deepcopy(value)
        self.current["assignments"].append({
            "id": assignment.assignment_id,
            "target": assignment.target_field,
            "old": deepcopy(old),
            "value": deepcopy(value),
        })

    def run(self, ruleset, row):
        prepared = self._prepare_ruleset(ruleset)
        try:
            execution = self._execute_prepared(
                prepared, row, full_audit=True, on_assignment=self._observe_assignment,
            )
            self._finish_commit()
        except Exception as exc:  # noqa: BLE001 - row errors are a teaching result
            if self.current:
                self.current["status"] = "error: no commit from this rule"
            return {"input": row, "steps": self.steps, "error": {
                "type": type(exc).__name__, "message": str(exc),
            }, "outcome": None, "skipped": self._skipped(prepared)}
        outcome = {
            "matched": bool(execution.matched_rule_ids),
            "matched_rule_ids": execution.matched_rule_ids,
            "assign": {target: {"applied": target in execution.assignments,
                                "value": execution.assignments.get(target)}
                       for target in prepared.assignment_targets},
        }
        return {"input": row, "steps": self.steps, "error": None,
                "outcome": outcome, "skipped": self._skipped(prepared)}

    def _skipped(self, prepared):
        visited = {step["rule_id"] for step in self.steps}
        return [rule.rule_id for rule in prepared.active_rules if rule.rule_id not in visited]


def reject_constant(value):
    raise ValueError(f"Non-finite JSON number is not allowed: {value}")


def evaluate(payload):
    """Compile, validate, serialize and evaluate bounded local teaching inputs."""
    functions = registry()
    stage = "compile"
    try:
        yaml_text = payload["yaml"]
        if not isinstance(yaml_text, str) or len(yaml_text) > 100_000:
            raise ValueError("YAML must be text of at most 100,000 characters.")
        ruleset = YamlRulesetCompiler().compile_text(yaml_text)
        if len(ruleset.rules) > 100:
            raise ValueError("The learning lab accepts at most 100 rules per experiment.")
        stage = "semantic validation"
        validation = RulesetValidator(functions).validate(ruleset)
        if not validation.passed:
            return {"ok": False, "stage": stage, "issues": json_value(validation.issues)}
        stage = "input"
        rows = json.loads(payload.get("rows", "[]"), parse_float=Decimal,
                          parse_constant=reject_constant)
        if not isinstance(rows, list) or not 1 <= len(rows) <= 100:
            raise ValueError("Rows must be a JSON array containing 1–100 objects.")
        if any(not isinstance(row, dict) for row in rows):
            raise ValueError("Every row must be a JSON object.")
        stage = "canonical round trip"
        serializer = DeltaRowSerializer()
        persisted = serializer.serialize_ruleset_version(ruleset)
        exported = YamlRulesetExporter().export_text(ruleset)
        recompiled = YamlRulesetCompiler().compile_text(exported)
        schema_result = None
        if payload.get("schema", "").strip():
            stage = "Spark schema preflight"
            try:
                from pyspark.sql.types import StructType

                from rules_engine.spark_validator import SparkRulesetCompatibilityValidator
            except ImportError:
                schema_result = {"available": False, "message": "Install PySpark for schema preflight."}
            else:
                schema = StructType.fromJson(json.loads(payload["schema"]))
                prepared = SparkRulesetCompatibilityValidator(functions).prepare(ruleset, schema)
                schema_result = {"available": True, "passed": prepared.validation.passed,
                                 "issues": json_value(prepared.validation.issues),
                                 "assignment_schema": prepared.assignment_schema.jsonValue(),
                                 "required_columns": list(prepared.required_source_columns)}
        stage = "row execution"
        result = {
            "ok": True, "stage": stage,
            "rows": [ObservedEvaluator(functions).run(ruleset, row) for row in rows],
            "canonical_yaml": exported, "model": ruleset,
            "persistence": persisted, "content_hash": persisted.content_hash,
            "round_trip_equal": serializer.content_hash(recompiled) == persisted.content_hash,
            "function_dependencies": functions.dependency_manifest(ruleset),
            "schema": schema_result,
        }
        return json_value(result)
    except Exception as exc:  # noqa: BLE001 - keep editable experiments recoverable
        return {"ok": False, "stage": stage,
                "error": {"type": type(exc).__name__, "message": str(exc)}}


def source_index():
    """Index only teaching evidence, never the entire workspace or personal files."""
    paths = [*sorted((ROOT / "src/rules_engine").glob("*.py")),
             *sorted((ROOT / "tests").glob("test_*.py")),
             *sorted((ROOT / "examples").glob("*.py")),
             ROOT / "README.md", ROOT / "docs/rules_engine_production_checklist.md",
             ROOT / "notebooks/99.rules_engine_system_tests.py"]
    sources = {}
    for path in paths:
        content = path.read_text(encoding="utf-8")
        symbols, imports = [], set()
        if path.suffix == ".py":
            tree = ast.parse(content)
            for node in ast.walk(tree):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                    symbols.append({"name": node.name, "line": node.lineno,
                                    "end": node.end_lineno, "kind": type(node).__name__})
                if isinstance(node, ast.ImportFrom) and node.module:
                    if node.module.startswith("rules_engine."):
                        imports.add(node.module.rsplit(".", 1)[-1])
        sources[path.relative_to(ROOT).as_posix()] = {
            "lines": len(content.splitlines()), "symbols": sorted(symbols, key=lambda x: x["line"]),
            "imports": sorted(imports), "sha256": hashlib.sha256(content.encode()).hexdigest(),
            "description": ast.get_docstring(tree) if path.suffix == ".py" else None,
            "text": content,
        }
    return sources


class LabHandler(BaseHTTPRequestHandler):
    """Serve a loopback-only workbench with a closed file allowlist."""

    def log_message(self, format, *args):
        pass

    def _send(self, value, status=200, content_type="application/json; charset=utf-8"):
        body = value if isinstance(value, bytes) else json.dumps(value, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Security-Policy", "default-src 'self'; style-src 'self'; "
                         "script-src 'self'; img-src 'self' data:; object-src 'none'; "
                         "frame-ancestors 'none'; base-uri 'none'")
        self.end_headers()
        self.wfile.write(body)

    def _valid_host(self):
        return self.headers.get("Host") in {
            f"127.0.0.1:{self.server.server_port}", f"localhost:{self.server.server_port}",
        }

    def do_GET(self):
        if not self._valid_host():
            return self._send({"error": "Loopback host required"}, 403)
        url = urlparse(self.path)
        if url.path == "/api/bootstrap":
            sources = source_index()
            curriculum = json.loads((HERE / "curriculum.json").read_text(encoding="utf-8"))
            digest = hashlib.sha256("".join(s["sha256"] for s in sources.values()).encode()).hexdigest()
            return self._send({
                "token": self.server.token, "version": __version__, "fingerprint": digest,
                "curriculum": curriculum, "manifest": json_value(build_authoring_manifest(registry())),
                "sources": {path: {k: v for k, v in item.items() if k != "text"}
                            for path, item in sources.items()},
            })
        if url.path == "/api/source":
            path = parse_qs(url.query).get("path", [""])[0]
            source = source_index().get(path)
            return self._send(source or {"error": "Source not in learning allowlist"},
                              200 if source else 404)
        assets = {"/": "index.html", "/app.js": "app.js", "/style.css": "style.css"}
        if url.path in assets:
            path = HERE / assets[url.path]
            content_type = mimetypes.guess_type(path.name)[0] or "text/plain"
            return self._send(path.read_bytes(), content_type=content_type + "; charset=utf-8")
        return self._send({"error": "Not found"}, 404)

    def do_POST(self):
        if not self._valid_host() or self.headers.get("X-Lab-Token") != self.server.token:
            return self._send({"error": "Reload the local workbench before running."}, 403)
        if self.path != "/api/evaluate":
            return self._send({"error": "Not found"}, 404)
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if not 0 < length <= 256_000:
                return self._send({"error": "Request must be under 256 KB."}, 413)
            payload = json.loads(self.rfile.read(length))
            if not isinstance(payload, dict):
                raise ValueError("Expected an object")
        except (ValueError, UnicodeError) as exc:
            return self._send({"error": str(exc)}, 400)
        return self._send(evaluate(payload))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    server = ThreadingHTTPServer(("127.0.0.1", args.port), LabHandler)
    server.token = secrets.token_urlsafe(32)
    print(f"Rules Engine learning lab: http://127.0.0.1:{server.server_port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
