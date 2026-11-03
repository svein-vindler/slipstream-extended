"""Closed failure projections only: no exception text, arbitrary names, IDs or paths."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path

_MODULES = frozenset(
    "pipeline.__init__ pipeline.activity_backfill pipeline.activity_backfill_scheduler pipeline.activity_pagination pipeline.activity_refresh pipeline.backup_pilot pipeline.backup_source pipeline.coach pipeline.coach_backfill pipeline.coach_context_sources pipeline.coach_segment_metrics pipeline.coach_stored_context pipeline.dispatch_receipt pipeline.fetch pipeline.freshness pipeline.garmin_sdk pipeline.gct_sides pipeline.granular pipeline.granular_export pipeline.health_detail pipeline.health_detail_backfill pipeline.health_history_index pipeline.health_index_reconcile pipeline.health_sync pipeline.health_writer pipeline.history_backfill pipeline.history_canonical pipeline.history_collect pipeline.history_publish pipeline.history_source pipeline.history_state pipeline.hrv_backfill pipeline.latest_activity pipeline.latest_night pipeline.local_bootstrap pipeline.local_import pipeline.logical_bouts pipeline.manual_activity_refresh pipeline.measurements pipeline.native_lap_workout_binding pipeline.native_night pipeline.night_baseline_refresh pipeline.night_followup pipeline.night_local_repair pipeline.night_products pipeline.night_training pipeline.output_privacy pipeline.private_backup pipeline.provider_check pipeline.provider_retention pipeline.provider_setup pipeline.r2_store pipeline.recent_body pipeline.recent_health pipeline.recorded_cadence pipeline.recorded_running_metrics pipeline.refresh pipeline.refresh_summaries pipeline.schema pipeline.signal_series pipeline.sources.__init__ pipeline.sources.activity_page pipeline.sources.garmin pipeline.sources.garmin_health pipeline.sources.garmin_personal pipeline.sources.garmin_policy pipeline.sources.garmin_readonly pipeline.sources.personal_policy pipeline.sources.provider_capabilities pipeline.sources.provider_config pipeline.sources.provider_diagnostic pipeline.sources.training_readiness pipeline.summary_export pipeline.summary_publication pipeline.summary_restore pipeline.training_day pipeline.training_effect pipeline.training_sync pipeline.weight_index pipeline.workout_targets pipeline.writer scripts.benchmark_activity_pagination scripts.benchmark_body_reuse scripts.benchmark_coach_reuse scripts.benchmark_health_indexes scripts.benchmark_health_reuse scripts.benchmark_refresh_budget scripts.benchmark_summary_recovery scripts.benchmark_summary_reuse scripts.benchmark_summary_windows scripts.benchmark_training_day scripts.build_coach_current_view_contract scripts.build_coach_recovery_contract scripts.build_coach_stored_context_contract scripts.build_coach_training_effect_contract scripts.build_coach_training_effect_matrix scripts.build_coach_versions_contract scripts.build_dispatch_contract scripts.build_functional_fix_contract scripts.build_gct_sides_contract scripts.build_health_contract scripts.build_logical_bouts_contract scripts.build_native_lap_binding_contract scripts.build_night_baseline_contract scripts.build_night_products_contract scripts.build_night_signals_contract scripts.build_night_training_contract scripts.build_positive_components_contract scripts.build_recorded_running_metrics_contract scripts.build_running_segments_contract scripts.build_scalar_leaf_contract scripts.build_signal_partial_contract scripts.build_source_status_contract scripts.build_training_context_contract scripts.build_training_day_contract scripts.build_weight_contract scripts.check_installation scripts.garmin_login scripts.night_baseline_fixtures scripts.public_release_check".split()
)
_ROOT = Path(__file__).resolve().parent.parent
_ACTIVE = ContextVar("hosted_failure_diagnostics", default=False)
_OPERATIONS = frozenset(
    {
        "execute",
        "get",
        "put",
        "list",
        "head",
        "read",
        "authenticate",
        "validate",
        "cleanup",
        "checkpoint",
    }
)
_STAGES = frozenset(
    {
        "cli",
        "provider_logger",
        "restore_summaries",
        "latest_activity",
        "latest_night",
        "activity_baseline",
        "historical_health",
        "export_summaries",
        "refresh_summaries",
        "recent_health",
        "recent_body",
        "manual_activity",
        "training_context",
        "health_indexes",
        "refresh-diagnostics-publication",
    }
) | frozenset(name.split(".")[-1] for name in _MODULES)
_BACKUP_CODES = frozenset(
    [
        "analysis_reference_mismatch",
        "archive_size_limit",
        "backup_authentication_failed",
        "bounded_client_configuration_required",
        "context_reference_mismatch",
        "context_version_mismatch",
        "decoded_object_limit",
        "decoded_size_mismatch",
        "decoded_total_limit",
        "destination_exists",
        "destination_parent_required",
        "document_size_limit",
        "duplicate_context_id",
        "duplicate_json_key",
        "duplicate_or_invalid_object_type",
        "duplicate_profile_id",
        "excluded_object_field",
        "incomplete_backup",
        "invalid_analysis_activity",
        "invalid_analysis_context",
        "invalid_analysis_limitations",
        "invalid_analysis_profile",
        "invalid_analysis_sections",
        "invalid_analysis_source_hash",
        "invalid_analysis_sources",
        "invalid_analysis_thresholds",
        "invalid_backup_header",
        "invalid_context_rpe",
        "invalid_context_source",
        "invalid_context_text",
        "invalid_format_fields",
        "invalid_gct_declaration",
        "invalid_gct_sides",
        "invalid_json",
        "invalid_json_number",
        "invalid_limits",
        "invalid_logical_bouts",
        "invalid_native_lap_binding",
        "invalid_object_date",
        "invalid_object_encoding",
        "invalid_object_metadata",
        "invalid_object_name",
        "invalid_object_timestamp",
        "invalid_object_value_type",
        "invalid_profile",
        "invalid_profile_source",
        "invalid_r2_configuration",
        "invalid_recorded_cadence",
        "invalid_recorded_running_metric",
        "invalid_recorded_running_metrics",
        "invalid_recorded_running_source",
        "invalid_recorded_running_time",
        "invalid_recovery_running_metrics",
        "invalid_retry_limit",
        "invalid_running_segment_bounds",
        "invalid_running_segment_coverage",
        "invalid_running_segment_group",
        "invalid_running_segment_metric",
        "invalid_running_segment_policy",
        "invalid_running_segment_time",
        "invalid_running_segments",
        "invalid_scope",
        "invalid_section_hr_coverage",
        "invalid_section_measurement_coverage",
        "invalid_segment_running_metrics",
        "invalid_source_inventory",
        "invalid_source_pagination",
        "invalid_stored_context",
        "invalid_stored_context_clock",
        "invalid_stored_context_metric",
        "invalid_stored_context_source",
        "invalid_stored_health_attribution",
        "invalid_stored_health_context",
        "invalid_stored_training_attribution",
        "invalid_stored_training_context",
        "invalid_training_effect",
        "invalid_training_effect_source",
        "invalid_training_effect_time",
        "invalid_training_effect_value",
        "invalid_workout_targets",
        "json_depth_limit",
        "json_node_limit",
        "json_size_limit",
        "linked_path_not_allowed",
        "listed_object_limit",
        "local_restore_checksum_failed",
        "manifest_reference_or_size_mismatch",
        "missing_profile_reference",
        "object_checksum_mismatch",
        "object_count_limit",
        "object_outside_scope",
        "object_type_not_allowed",
        "operation_limit",
        "password_confirmation_failed",
        "password_length_invalid",
        "pilot_activity_has_no_covered_objects",
        "pilot_copy_mismatch",
        "pilot_preview_wrote_target",
        "pilot_reference_mismatch",
        "pilot_requires_one_activity_and_fixed_limits",
        "pilot_round_trip_mismatch",
        "private_directory_must_be_outside_git",
        "private_parent_directory_required",
        "profile_effective_date_mismatch",
        "profile_reference_mismatch",
        "profile_version_mismatch",
        "read_only_credential_required",
        "regular_file_required",
        "source_changed",
        "source_close_failed",
        "source_directory_limit",
        "source_directory_required",
        "source_inventory_required",
        "source_not_consistent",
        "source_read_failed",
        "source_revision_required",
        "source_size_limit",
        "stored_object_limit",
        "total_size_limit",
        "unsupported_analysis_schema",
        "unsupported_backup_version",
        "unsupported_object_version",
    ]
)
_BUDGET = frozenset(
    {"write_attempt_limit", "write_byte_limit", "bucket_object_limit", "bucket_byte_limit"}
)


@contextmanager
def diagnostic_scope():
    token = _ACTIVE.set(True)
    try:
        yield
    finally:
        _ACTIVE.reset(token)


def _chain(error):
    seen = set()
    for _ in range(8):
        if not isinstance(error, BaseException) or id(error) in seen:
            return
        seen.add(id(error))
        yield error
        error = (
            error.__cause__
            if error.__cause__ is not None
            else None
            if error.__suppress_context__
            else error.__context__
        )


def _site(error):
    best = {"module": "cli", "line": 0}
    for current in _chain(error):
        tb = current.__traceback__
        for _ in range(64):
            if tb is None:
                break
            try:
                relative = Path(tb.tb_frame.f_code.co_filename).resolve().relative_to(_ROOT)
                name = ".".join(relative.with_suffix("").parts)
                if (
                    name in _MODULES
                    and name not in {"pipeline.output_privacy", "pipeline.failure_diagnostics"}
                    and type(tb.tb_lineno) is int
                    and 1 <= tb.tb_lineno <= 1000000
                ):
                    if not (
                        tb.tb_frame.f_code.co_name == "require"
                        and name in {"pipeline.native_night", "pipeline.night_baseline_refresh"}
                    ):
                        best = {"module": name, "line": tb.tb_lineno}
            except (OSError, ValueError):
                pass
            tb = tb.tb_next
    return best


def _classify(error):
    from botocore.exceptions import (
        ClientError,
        ConnectionClosedError,
        ConnectTimeoutError,
        EndpointConnectionError,
        IncompleteReadError,
        NoCredentialsError,
        PartialCredentialsError,
        ReadTimeoutError,
    )
    from garminconnect import (
        GarminConnectAuthenticationError,
        GarminConnectConnectionError,
        GarminConnectTooManyRequestsError,
    )

    from .r2_store import R2BudgetError, R2ReadBoundaryError
    from .sources.garmin_personal import PersonalSourceError
    from .sources.garmin_policy import GarminReadOnlyViolation
    from .sources.provider_diagnostic import CATEGORIES, ProviderDiagnosticError

    if isinstance(error, ProviderDiagnosticError):
        category = (
            error.category
            if isinstance(error.category, str) and error.category in CATEGORIES
            else "operation_failed"
        )
        detail = {"category": category, "component": "garmin", "operation": "authenticate"}
        if type(error.http_status) is int and 100 <= error.http_status <= 599:
            detail["http_status"] = error.http_status
        if isinstance(error.phase, str) and error.phase in {
            "preflight",
            "auth_refresh",
            "identity_profile",
            "identity_settings",
        }:
            detail["reason"] = error.phase
        if type(error.attempts) is int and 0 <= error.attempts <= 8:
            detail["physical_http_attempts"] = error.attempts
        return detail
    for kind, category in (
        (GarminReadOnlyViolation, "garmin_policy_rejected"),
        (GarminConnectAuthenticationError, "garmin_authentication_required"),
        (GarminConnectTooManyRequestsError, "garmin_rate_limited"),
        (GarminConnectConnectionError, "garmin_connection_failed"),
    ):
        if isinstance(error, kind):
            return {
                "category": category,
                "component": "garmin",
                "operation": "authenticate"
                if category == "garmin_authentication_required"
                else "read",
            }
    if isinstance(error, PersonalSourceError):
        category = (
            {
                "auth_required": "garmin_authentication_required",
                "account_mismatch": "garmin_account_mismatch",
                "rate_limited": "garmin_rate_limited",
                "schema_unverified": "garmin_response_unverified",
                "payload_limit": "garmin_response_limit",
            }.get(error.status, "operation_failed")
            if isinstance(error.status, str)
            else "operation_failed"
        )
        result = {"category": category, "component": "garmin", "operation": "read"}
        status = getattr(error, "status_code", None)
        if type(status) is int and 100 <= status <= 599:
            result["http_status"] = status
        return result
    if isinstance(error, ClientError):
        response = error.response if isinstance(error.response, dict) else {}
        detail = response.get("Error")
        code = detail.get("Code") if isinstance(detail, dict) else None
        category = (
            {
                "ExpiredToken": "r2_authentication_required",
                "InvalidAccessKeyId": "r2_authentication_required",
                "SignatureDoesNotMatch": "r2_authentication_required",
                "AccessDenied": "r2_access_denied",
                "SlowDown": "r2_rate_limited",
                "RequestTimeout": "r2_request_timeout",
                "NoSuchKey": "r2_object_missing",
                "NotFound": "r2_object_missing",
                "404": "r2_object_missing",
                "InternalError": "r2_service_failed",
                "ServiceUnavailable": "r2_service_failed",
            }.get(code, "r2_service_failed")
            if isinstance(code, str)
            else "r2_service_failed"
        )
        result = {"category": category, "component": "r2", "operation": "execute"}
        operation = getattr(error, "operation_name", None)
        mapped = (
            {
                "GetObject": "get",
                "HeadObject": "head",
                "PutObject": "put",
                "ListObjectsV2": "list",
            }.get(operation)
            if isinstance(operation, str)
            else None
        )
        if mapped:
            result["operation"] = mapped
        metadata = response.get("ResponseMetadata")
        status = metadata.get("HTTPStatusCode") if isinstance(metadata, dict) else None
        if type(status) is int and 100 <= status <= 599:
            result["http_status"] = status
        return result
    if isinstance(error, R2ReadBoundaryError):
        reason = (
            error.status
            if isinstance(error.status, str)
            and error.status in {"oversize", "schema_unverified", "read_failed"}
            else "unverified"
        )
        return {
            "category": "r2_read_boundary",
            "component": "r2",
            "operation": "get",
            "reason": reason,
        }
    health_error = getattr(sys.modules.get("pipeline.health_sync"), "HealthNotReady", None)
    if isinstance(health_error, type) and isinstance(error, health_error):
        status = error.status
        reason = (
            status
            if isinstance(status, str)
            and status in {"garmin_not_ready", "invalid_response", "wrong_date", "unsupported"}
            else "unverified"
        )
        return {
            "category": "source_not_ready"
            if reason == "garmin_not_ready"
            else "source_response_unverified",
            "reason": reason,
            "component": "garmin",
            "operation": "read",
        }
    native_error = getattr(sys.modules.get("pipeline.native_night"), "NativeInputError", None)
    if isinstance(native_error, type) and isinstance(error, native_error):
        return {
            "category": "r2_response_unverified",
            "component": "r2",
            "operation": "read",
            "reason": "native_schema",
        }
    if isinstance(error, R2BudgetError):
        reason = getattr(error, "reason", None)
        return {
            "category": "r2_" + reason
            if isinstance(reason, str) and reason in _BUDGET
            else "r2_budget_exhausted",
            "component": "r2",
            "operation": "put",
            **({"reason": reason} if isinstance(reason, str) and reason in _BUDGET else {}),
        }
    for kinds, category in (
        ((NoCredentialsError, PartialCredentialsError), "r2_credentials_missing"),
        ((ReadTimeoutError, ConnectTimeoutError), "r2_request_timeout"),
        ((EndpointConnectionError,), "r2_connection_failed"),
        ((ConnectionClosedError, IncompleteReadError), "r2_response_unverified"),
    ):
        if isinstance(error, kinds):
            return {"category": category, "component": "r2", "operation": "execute"}
    import subprocess

    if isinstance(error, subprocess.SubprocessError):
        detail = {
            "category": "subprocess_timeout"
            if isinstance(error, subprocess.TimeoutExpired)
            else "subprocess_failed",
            "component": "process",
            "operation": "execute",
        }
        code = getattr(error, "returncode", None)
        if type(code) is int and -255 <= code <= 255:
            detail["exit_code"] = code
        return detail
    from urllib.error import HTTPError

    import requests

    if isinstance(
        error, (requests.Timeout, requests.ConnectionError, requests.HTTPError, HTTPError)
    ):
        category = (
            "network_timeout"
            if isinstance(error, requests.Timeout)
            else "network_connection_failed"
            if isinstance(error, requests.ConnectionError)
            else "upstream_http_failed"
        )
        detail = {"category": category, "component": "network", "operation": "read"}
        response = getattr(error, "response", None)
        status = (
            response.status_code
            if isinstance(response, requests.Response)
            else error.code
            if isinstance(error, HTTPError)
            else None
        )
        if type(status) is int and 100 <= status <= 599:
            detail["http_status"] = status
        return detail
    return None


def failure_details(error, *, stage="cli", operation=None):
    result = None
    chain = list(_chain(error))
    for current in chain:
        result = _classify(current)
        if result is not None:
            break
    if result is None:
        categories = (
            (json.JSONDecodeError, "invalid_json"),
            (UnicodeError, "invalid_encoding"),
            (RecursionError, "structure_limit"),
            (OverflowError, "numeric_range_invalid"),
            (KeyError, "required_field_missing"),
            (TypeError, "invalid_type"),
            (ValueError, "invalid_value"),
            (FileNotFoundError, "local_file_missing"),
            (PermissionError, "local_access_denied"),
            (TimeoutError, "operation_timeout"),
            (OSError, "local_io_failed"),
            (MemoryError, "memory_limit"),
            (KeyboardInterrupt, "interrupted"),
        )
        for current in chain:
            backup_type = getattr(sys.modules.get("pipeline.backup_source"), "BackupError", None)
            if isinstance(backup_type, type) and isinstance(current, backup_type):
                result = {
                    "category": "backup_boundary",
                    "component": "backup",
                    "operation": "validate",
                }
                if (
                    current.args
                    and isinstance(current.args[0], str)
                    and current.args[0] in _BACKUP_CODES
                ):
                    result["reason"] = current.args[0]
                break
            history = sys.modules.get("pipeline.history_state")
            stop = getattr(history, "Stop", None)
            durability = getattr(history, "DurabilityError", None)
            baseline_stop = getattr(
                sys.modules.get("pipeline.night_baseline_refresh"), "BaselineRefreshStop", None
            )
            if isinstance(durability, type) and isinstance(current, durability):
                result = {
                    "category": "local_persistence_failed",
                    "component": "local",
                    "operation": "checkpoint",
                }
                break
            if isinstance(stop, type) and isinstance(current, stop):
                result = {
                    "category": "history_stopped",
                    "component": "history",
                    "operation": "execute",
                }
                category = current.category
                if isinstance(category, str) and category in {
                    "denied",
                    "rate_limited",
                    "unknown_write",
                    "invalid_source",
                    "invalid_null_hr",
                    "http_budget",
                    "wall_budget",
                    "byte_budget",
                    "state_binding_changed",
                    "owner_held",
                    "stopped",
                    "unsupported",
                }:
                    result["reason"] = category
                break
            if isinstance(baseline_stop, type) and isinstance(current, baseline_stop):
                result = {
                    "category": "baseline_refresh_stopped",
                    "component": "baseline",
                    "operation": "validate",
                }
                break
            for kind, category in categories:
                if isinstance(current, kind):
                    result = {
                        "category": category,
                        "component": "local" if isinstance(current, OSError) else "python",
                        "operation": "execute",
                    }
                    break
            if result is not None:
                break
    if result is None:
        result = {"category": "operation_failed", "component": "pipeline", "operation": "execute"}
    result["stage"] = stage if isinstance(stage, str) and stage in _STAGES else "unknown"
    if isinstance(operation, str) and operation in _OPERATIONS:
        result["operation"] = operation
    result["site"] = _site(error)
    return result


def log_failure(error, *, stage="cli", operation=None, force=False):
    if not force and not _ACTIVE.get():
        return None
    try:
        detail = failure_details(error, stage=stage, operation=operation)
    except Exception:
        detail = {
            "category": "diagnostic_unavailable",
            "component": "pipeline",
            "stage": "unknown",
            "operation": "execute",
            "site": {"module": "cli", "line": 0},
        }
    print("Pipeline diagnostic: " + json.dumps(detail, sort_keys=True), file=sys.stderr)
    return detail
