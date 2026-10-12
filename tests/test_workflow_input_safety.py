"""Exercise the actual dispatch shell fragments with synthetic argument data."""

import importlib
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
MARKER = "synthetic-workflow-input-executed"
BACKFILL = {
    "start_date": "2024-01-01",
    "end_date": "2024-12-31",
    "max_activities": "20",
    "retry_failures": "false",
}
PILOT = {"hrv_days": "7", "activity_days": "180"}

SHELL_PAYLOADS = [
    f"$(printf {MARKER} >&2)",
    f"`printf {MARKER} >&2`",
    f'"; printf {MARKER} >&2; #',
    f'$(printf "%s" "$(printf {MARKER} >&2)")',
    'double " and single \' quotes',
    "first line\nsecond line",
    " spaced\twords * ? [abc] ",
    f"2024-01-01; printf {MARKER} >&2",
]


def workflow_fragment(name, module, supplied):
    """Read the repository's run literal, simple input env mappings and defaults.

    This deliberately supports only the syntax used by these two steps. Unknown
    expressions fail the test rather than being silently approximated.
    """
    text = (ROOT / f".github/workflows/{name}.yml").read_text(encoding="utf-8")
    values = {}
    for field, body in re.findall(r"(?m)^      (\w+):\n((?:        [^\n]*\n)+)", text):
        default = re.search(r"(?m)^        default: (.+)$", body)
        if default:
            values[field] = default[1].strip('"')
    values.update(supplied)
    steps = [
        step for step in re.split(r"(?m)^      - ", text)
        if f"python -m {module}" in step
    ]
    assert len(steps) == 1
    step = steps[0]
    literal = re.search(r"(?m)^        run: \|\n((?:          [^\n]*(?:\n|$)|\n)+)", step)
    assert literal is not None
    source = "\n".join(line[10:] if line else "" for line in literal[1].splitlines())
    source = re.sub(
        r"\$\{\{\s*inputs\.(\w+)\s*\}\}", lambda match: values[match[1]], source
    )
    assert "${{" not in source, "Unsupported Actions expression in tested run block"
    env = {
        key: values[field] for key, field in re.findall(
            r"(?m)^        +([A-Z_]+): \$\{\{ inputs\.(\w+) \}\}$", step
        )
    }
    return source, env


def run_fragment(name, module, supplied, tmp_path):
    source, inputs_env = workflow_fragment(name, module, supplied)
    bash = os.environ.get("SLIPSTREAM_TEST_BASH")
    if not bash and os.name == "nt":
        git_bin = Path(os.environ.get("ProgramFiles", "C:/Program Files")) / "Git/bin"
        bash = shutil.which("bash", path=str(git_bin))
    bash = bash or shutil.which("bash")
    assert bash, "Bash is required to verify dispatch argument safety"
    # Do not inherit credentials, user shell startup files or Python settings.
    env = {key: os.environ[key] for key in ("SYSTEMROOT", "WINDIR") if key in os.environ}
    env.update(HOME=tmp_path.as_posix(), PATH="/usr/bin:/bin", **inputs_env)
    # Only Bash builtins run. The real Python job is never launched here.
    capture = 'python() { printf "%s\\0" "$@"; }\n'
    result = subprocess.run(
        [bash, "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", capture + source],
        cwd=tmp_path, env=env, capture_output=True, timeout=10, check=False,
    )
    assert result.returncode == 0, result.stderr.decode("utf-8", errors="replace")
    assert result.stderr == b"", "Dispatch input executed or changed shell parsing"
    assert result.stdout.endswith(b"\0")
    return [item.decode("utf-8") for item in result.stdout[:-1].split(b"\0")]


@pytest.mark.parametrize("field", ["start_date", "end_date", "max_activities"])
@pytest.mark.parametrize("payload", SHELL_PAYLOADS)
def test_backfill_shell_like_input_is_one_literal_argument(field, payload, tmp_path):
    values = {**BACKFILL, field: payload}
    argv = run_fragment("activity-backfill", "pipeline.activity_backfill", values, tmp_path)
    assert argv == [
        "-m", "pipeline.activity_backfill",
        "--start-date", values["start_date"],
        "--end-date", values["end_date"],
        "--max-activities", values["max_activities"],
    ]

def test_backfill_defaults_reach_python_unchanged(tmp_path):
    argv = run_fragment(
        "activity-backfill", "pipeline.activity_backfill",
        {"start_date": "2024-01-01", "end_date": "2024-12-31"}, tmp_path,
    )
    assert argv == [
        "-m", "pipeline.activity_backfill",
        "--start-date", "2024-01-01", "--end-date", "2024-12-31",
        "--max-activities", "20",
    ]

@pytest.mark.parametrize("field", ["hrv_days", "activity_days"])
@pytest.mark.parametrize("payload", SHELL_PAYLOADS)
def test_pilot_shell_like_input_is_one_literal_argument(field, payload, tmp_path):
    values = {**PILOT, field: payload}
    argv = run_fragment("granular-pilot", "pipeline.granular_export", values, tmp_path)
    assert argv == [
        "-m", "pipeline.granular_export",
        "--hrv-days", values["hrv_days"], "--activity-days", values["activity_days"],
        "--cardio-count", "2", "--strength-count", "2", "--upload-r2",
    ]


def test_pilot_defaults_reach_python_unchanged(tmp_path):
    argv = run_fragment("granular-pilot", "pipeline.granular_export", {}, tmp_path)
    assert argv == [
        "-m", "pipeline.granular_export",
        "--hrv-days", "7", "--activity-days", "180",
        "--cardio-count", "2", "--strength-count", "2", "--upload-r2",
    ]

def invoke_cli(module_name, argv, monkeypatch):
    """Use the real CLI parser/validator; replace the Garmin/R2 job boundary."""
    module = importlib.import_module(module_name)
    jobs = []
    monkeypatch.setattr(module, "run", lambda **kwargs: jobs.append(kwargs))
    monkeypatch.setattr(sys, "argv", [module_name, *argv[2:]])
    try:
        module.main()
    except SystemExit as exc:
        return exc.code, jobs
    return 0, jobs


@pytest.mark.parametrize("retry,enabled", [("false", False), ("true", True)])
def test_backfill_retry_flag_and_cli_defaults_are_preserved(retry, enabled, tmp_path, monkeypatch):
    values = {**BACKFILL, "retry_failures": retry}
    argv = run_fragment("activity-backfill", "pipeline.activity_backfill", values, tmp_path)
    expected = [
        "-m", "pipeline.activity_backfill",
        "--start-date", "2024-01-01", "--end-date", "2024-12-31",
        "--max-activities", "20",
    ]
    assert argv == expected + (["--retry-failures"] if enabled else [])
    status, jobs = invoke_cli("pipeline.activity_backfill", argv, monkeypatch)
    assert status == 0
    assert jobs == [{
        "start_date": "2024-01-01", "end_date": "2024-12-31", "max_activities": 20,
        "retry_failures": enabled, "reconcile": False, "output_dir": ".granular/backfill",
    }]


@pytest.mark.parametrize("start,end,maximum,expected_maximum", [
    ("2024-02-29", "2024-02-29", "1", 1),
    ("2024-01-01", "2024-12-31", "50", 50),
    ("2024-01-01", "2024-01-01", "+20", 20),
    ("2024-01-01", "2024-01-01", " 20 ", 20),
])
def test_backfill_valid_dates_and_limits_reach_job(
    start, end, maximum, expected_maximum, tmp_path, monkeypatch,
):
    values = {**BACKFILL, "start_date": start, "end_date": end, "max_activities": maximum}
    argv = run_fragment("activity-backfill", "pipeline.activity_backfill", values, tmp_path)
    assert argv == [
        "-m", "pipeline.activity_backfill",
        "--start-date", start, "--end-date", end, "--max-activities", maximum,
    ]
    status, jobs = invoke_cli("pipeline.activity_backfill", argv, monkeypatch)
    assert status == 0
    assert jobs == [{
        "start_date": start, "end_date": end, "max_activities": expected_maximum,
        "retry_failures": False, "reconcile": False, "output_dir": ".granular/backfill",
    }]


@pytest.mark.parametrize("field,value", [
    ("start_date", "2024-02-30"), ("start_date", "2023-02-29"),
    ("end_date", "2024-02-30"), ("end_date", "2023-02-29"),
    ("start_date", "2024-1-01"), ("end_date", "2024-12-1"),
    ("start_date", ""), ("end_date", ""),
    ("start_date", "2025-01-01"), ("end_date", "2025-01-01"),
    ("max_activities", "0"), ("max_activities", "-1"), ("max_activities", "51"),
    ("max_activities", ""), ("max_activities", "1.5"), ("max_activities", "not-a-number"),
])
def test_backfill_invalid_values_stop_before_job(field, value, tmp_path, monkeypatch):
    values = {**BACKFILL, field: value}
    argv = run_fragment("activity-backfill", "pipeline.activity_backfill", values, tmp_path)
    assert argv == [
        "-m", "pipeline.activity_backfill",
        "--start-date", values["start_date"], "--end-date", values["end_date"],
        "--max-activities", values["max_activities"],
    ]
    assert invoke_cli("pipeline.activity_backfill", argv, monkeypatch) == (2, [])


@pytest.mark.parametrize("hrv,activity,expected_hrv,expected_activity", [
    ("0", "0", 0, 0), ("60", "3660", 60, 3660),
    ("7", "180", 7, 180), ("+7", " 180 ", 7, 180),
])
def test_pilot_valid_limits_and_defaults_reach_job(
    hrv, activity, expected_hrv, expected_activity, tmp_path, monkeypatch,
):
    argv = run_fragment(
        "granular-pilot", "pipeline.granular_export",
        {"hrv_days": hrv, "activity_days": activity}, tmp_path,
    )
    assert argv == [
        "-m", "pipeline.granular_export", "--hrv-days", hrv, "--activity-days", activity,
        "--cardio-count", "2", "--strength-count", "2", "--upload-r2",
    ]
    status, jobs = invoke_cli("pipeline.granular_export", argv, monkeypatch)
    assert status == 0
    assert jobs == [{
        "hrv_days": expected_hrv, "activity_days": expected_activity,
        "cardio_count": 2, "strength_count": 2, "output_dir": ".granular", "upload_r2": True,
    }]


@pytest.mark.parametrize("field,value", [
    ("hrv_days", "-1"), ("hrv_days", "61"),
    ("activity_days", "-1"), ("activity_days", "3661"),
    ("hrv_days", ""), ("activity_days", ""),
    ("hrv_days", "1.5"), ("activity_days", "not-a-number"),
])
def test_pilot_invalid_values_stop_before_job(field, value, tmp_path, monkeypatch):
    values = {**PILOT, field: value}
    argv = run_fragment("granular-pilot", "pipeline.granular_export", values, tmp_path)
    assert argv == [
        "-m", "pipeline.granular_export",
        "--hrv-days", values["hrv_days"], "--activity-days", values["activity_days"],
        "--cardio-count", "2", "--strength-count", "2", "--upload-r2",
    ]
    assert invoke_cli("pipeline.granular_export", argv, monkeypatch) == (2, [])


@pytest.mark.parametrize("name,module,field,defaults", [
    ("activity-backfill", "pipeline.activity_backfill", "start_date", BACKFILL),
    ("activity-backfill", "pipeline.activity_backfill", "end_date", BACKFILL),
    ("activity-backfill", "pipeline.activity_backfill", "max_activities", BACKFILL),
    ("granular-pilot", "pipeline.granular_export", "hrv_days", PILOT),
    ("granular-pilot", "pipeline.granular_export", "activity_days", PILOT),
])
@pytest.mark.parametrize("payload", SHELL_PAYLOADS)
def test_shell_like_values_are_rejected_by_real_cli(
    name, module, field, defaults, payload, tmp_path, monkeypatch,
):
    argv = run_fragment(name, module, {**defaults, field: payload}, tmp_path)
    assert argv[argv.index("--" + field.replace("_", "-")) + 1] == payload
    assert invoke_cli(module, argv, monkeypatch) == (2, [])
