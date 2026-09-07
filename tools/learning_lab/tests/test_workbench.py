"""Independent semantic and HTTP checks for the development-only teaching tool."""

import json
import secrets
import sys
import threading
import unittest
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from tools.learning_lab import server  # noqa: E402

CATALOG = json.loads((server.HERE / "curriculum.json").read_text(encoding="utf-8"))
LABS = {item["id"]: item for item in CATALOG["labs"]}


def run_lab(item):
    return server.evaluate({"yaml": item["yaml"], "rows": json.dumps(item["rows"]),
                            "schema": item.get("schema", "")})


class LearningSemanticsTests(unittest.TestCase):
    def test_browser_transport_preserves_large_integer_and_decimal_digits(self):
        result = server.json_value({"integer": 9007199254740993,
                                    "decimal": server.Decimal("100.00000000000000001"),
                                    "flag": True})
        self.assertEqual({"$integer": "9007199254740993"}, result["integer"])
        self.assertEqual({"$decimal": "100.00000000000000001"}, result["decimal"])
        self.assertIs(result["flag"], True)

    def test_every_scenario_matches_its_independent_teaching_expectation(self):
        for item in CATALOG["labs"]:
            with self.subTest(lab=item["id"]):
                result = run_lab(item)
                expected = item["expect"]
                if "stage" in expected:
                    self.assertFalse(result["ok"])
                    self.assertEqual(expected["stage"], result["stage"])
                    continue
                self.assertTrue(result["ok"], result)
                self.assertTrue(result["round_trip_equal"])
                for index, row in enumerate(result["rows"]):
                    if index in expected.get("errorRows", []):
                        self.assertIsNotNone(row["error"])
                        self.assertIsNone(row["outcome"])
                    else:
                        self.assertIsNone(row["error"])
                        actual = {key: value["value"] for key, value in row["outcome"]["assign"].items()
                                  if value["applied"]}
                        self.assertEqual(expected["rows"][index], actual)

    def test_observed_execution_matches_uninstrumented_evaluator(self):
        for item in CATALOG["labs"]:
            observed = run_lab(item)
            if not observed["ok"]:
                continue
            ruleset = server.YamlRulesetCompiler().compile_text(item["yaml"])
            inputs = json.loads(json.dumps(item["rows"]), parse_float=server.Decimal)
            for row, traced in zip(inputs, observed["rows"], strict=True):
                with self.subTest(lab=item["id"], row=row):
                    evaluator = server.SparkRowEvaluator(server.registry())
                    if traced["error"]:
                        with self.assertRaises(Exception) as error:
                            evaluator.evaluate_row(ruleset, row)
                        self.assertEqual(type(error.exception).__name__, traced["error"]["type"])
                        self.assertEqual(str(error.exception), traced["error"]["message"])
                    else:
                        plain = server.json_value(evaluator.evaluate_row(ruleset, row))
                        self.assertEqual(plain, traced["outcome"])

    def test_atomic_trace_distinguishes_before_and_after_without_source_mutation(self):
        result = run_lab(LABS["atomic"])["rows"][0]
        second = result["steps"][1]
        self.assertEqual({"bucket": "A"}, second["before"])
        self.assertEqual("B", second["after"]["bucket"])
        self.assertEqual("A", second["after"]["previous_bucket"])
        self.assertEqual("ORIGINAL", result["input"]["bucket"])

    def test_assignment_error_never_records_a_partial_commit(self):
        row = run_lab(LABS["atomic_error"])["rows"][0]
        self.assertIsNotNone(row["error"])
        self.assertEqual({"bucket": "initial"}, row["steps"][1]["before"])
        self.assertEqual({"bucket": "initial"}, row["steps"][1]["after"])
        self.assertEqual([], row["steps"][1]["assignments"])

    def test_stop_prevents_later_observations(self):
        item = dict(LABS["overwrite"])
        item["yaml"] = item["yaml"].replace("rule_name: Material amount",
                                            "rule_name: Material amount\n    stop_on_match: true")
        row = run_lab(item)["rows"][0]
        self.assertEqual(["manual"], row["skipped"])
        self.assertEqual("material", row["outcome"]["assign"]["bucket"]["value"])

    def test_compile_and_input_failures_remain_editable_results(self):
        for payload, stage in [
            ({"yaml": "ruleset_id: a\nruleset_id: b", "rows": "[{}]"}, "compile"),
            ({"yaml": LABS["atomic"]["yaml"], "rows": "NaN"}, "input"),
            ({"yaml": LABS["atomic"]["yaml"], "rows": "{}"}, "input"),
            ({"yaml": LABS["atomic"]["yaml"], "rows": "[1]"}, "input"),
        ]:
            with self.subTest(payload=payload):
                result = server.evaluate(payload)
                self.assertFalse(result["ok"])
                self.assertEqual(stage, result["stage"])

    def test_optional_schema_exposes_row_helper_and_spark_boundary(self):
        result = run_lab(LABS["schema"])
        self.assertTrue(result["rows"][0]["outcome"]["matched"])
        if result["schema"]["available"]:
            self.assertFalse(result["schema"]["passed"])
            self.assertIn("amount", result["schema"]["required_columns"])

    def test_every_source_reference_and_lesson_lab_resolves(self):
        sources = server.source_index()
        for item in [*CATALOG["lessons"], *CATALOG["labs"]]:
            for reference in item["refs"]:
                with self.subTest(reference=reference):
                    self.assertIn(reference["path"], sources)
                    if reference.get("symbol"):
                        names = {symbol["name"] for symbol in sources[reference["path"]]["symbols"]}
                        self.assertIn(reference["symbol"], names)
        for item in CATALOG["lessons"]:
            self.assertIn(item["lab"], LABS)


class LearningHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.httpd = server.ThreadingHTTPServer(("127.0.0.1", 0), server.LabHandler)
        cls.httpd.token = secrets.token_urlsafe(32)
        cls.thread = threading.Thread(target=cls.httpd.serve_forever, daemon=True)
        cls.thread.start()
        cls.base = f"http://127.0.0.1:{cls.httpd.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()
        cls.httpd.server_close()
        cls.thread.join()

    def test_bootstrap_and_source_are_allowlisted(self):
        with urlopen(self.base + "/api/bootstrap", timeout=10) as response:
            payload = json.load(response)
        self.assertEqual(server.__version__, payload["version"])
        self.assertNotIn("codex.md", payload["sources"])
        self.assertTrue(payload["manifest"]["functions"])
        with self.assertRaises(HTTPError) as error:
            urlopen(self.base + "/api/source?path=../../codex.md", timeout=10)
        self.assertEqual(404, error.exception.code)

    def test_post_requires_local_session_token(self):
        request = Request(self.base + "/api/evaluate", data=b"{}", method="POST")
        with self.assertRaises(HTTPError) as error:
            urlopen(request, timeout=10)
        self.assertEqual(403, error.exception.code)
        data = json.dumps({"yaml": LABS["null"]["yaml"],
                           "rows": json.dumps(LABS["null"]["rows"])}).encode()
        request = Request(self.base + "/api/evaluate", data=data, method="POST",
                          headers={"X-Lab-Token": self.httpd.token})
        with urlopen(request, timeout=10) as response:
            payload = json.load(response)
        self.assertTrue(payload["rows"][0]["outcome"]["assign"]["bucket"]["applied"])
        self.assertFalse(payload["rows"][1]["outcome"]["assign"]["bucket"]["applied"])


if __name__ == "__main__":
    unittest.main()
