"""Offline feature coverage; false never causes automatic provider fallback."""


def capabilities(provider):
    if provider not in {"legacy", "personal"}:
        raise ValueError("Unknown Garmin provider.")
    complete = provider == "legacy"
    return {
        "provider": provider,
        "activity_summary": True,
        "activity_original": True,
        "activity_detail": complete,
        "sleep_summary": True,
        "sleep_stages": complete,
        "hrv_readings": True,
        "heart_rate_readings": True,
        "body_battery_readings": True,
        "raw_stress": complete,
        "training_readiness": complete,
        "weight": True,
        "body_composition_detail": complete,
        "local_clock_repair": complete,
        "tcx_export": complete,
        "gpx_export": complete,
    }


def capabilities_for(client=None):
    if client is None:
        import os

        from .provider_config import selection

        provider = selection(os.environ).provider
    else:
        from inspect import getattr_static

        # Existing injected clients may reject all unknown attributes as source calls.
        provider = getattr_static(client, "provider", "legacy")
        if isinstance(provider, property):
            provider = provider.__get__(client, type(client))
    return capabilities(provider)


def describe(provider=None):
    labels = {
        "activity_summary": "Activity summaries",
        "activity_original": "Original activity files",
        "activity_detail": "Full detailed activity import",
        "sleep_summary": "Sleep summaries",
        "sleep_stages": "Sleep stage timeline",
        "hrv_readings": "Night HRV readings",
        "heart_rate_readings": "Heart rate readings",
        "body_battery_readings": "Body Battery readings",
        "raw_stress": "Stress timeline",
        "training_readiness": "Training readiness",
        "weight": "Weight trend",
        "body_composition_detail": "Full body composition detail",
        "local_clock_repair": "Historical local-clock repair",
        "tcx_export": "TCX conversion",
        "gpx_export": "GPX conversion",
    }
    lines = []
    for name in ("legacy", "personal") if provider is None else (provider,):
        values = capabilities(name)
        lines.append(
            "Garmin Connect (existing connection)" if name == "legacy" else "Garmin Personal API"
        )
        for key, label in labels.items():
            lines.append(
                "  "
                + label
                + ": "
                + ("available" if values[key] else "not available in this adapter")
            )
        if name == "personal":
            lines.append(
                "  Body Battery may contain interval minima/maxima; full resolution is unverified."
            )
            lines.append(
                "  Original archive validation: FIT/TCX/GPX; independent activity association remains unverified."
            )
            lines.append(
                "  Garmin price, numerical quotas and Connect+ requirements remain undocumented in the reviewed sources."
            )
    return "\n".join(lines)


def unsupported_result(client, feature):
    values = capabilities_for(client)
    if feature not in values or type(values[feature]) is not bool:
        raise ValueError("Unknown Garmin feature.")
    if values[feature]:
        return None
    return {
        "schema_version": 1,
        "status": "unsupported",
        "garmin_provider": values["provider"],
        "unsupported_features": [feature],
    }
