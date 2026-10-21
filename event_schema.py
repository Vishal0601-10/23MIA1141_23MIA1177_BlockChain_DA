"""Structured event schema for the TATKAL-X live streaming pipeline.

This module does not calculate simulation metrics. It only converts the
existing per-tick TATKAL-X history record into a JSON-serializable event.
"""

from typing import Any, Dict


# The 26 fields produced by Engine.tick() in the existing TATKAL-X model.
EVENT_FIELDS = [
    "t",
    "pressure",
    "active",
    "inventory",
    "attempts",
    "initial_attempts",
    "retry_attempts",
    "activated",
    "completed",
    "pending",
    "service_rate",
    "robust_service_rate",
    "safe_concurrency",
    "latency_admission_capacity",
    "service_time_bound_s",
    "backend_workload_s",
    "credits",
    "arrival_rate",
    "admission_rate",
    "cpu_pct",
    "rss_mb",
    "cbf_headroom",
    "cbf_filtered",
    "safe_absorption",
    "mpc_objective",
    "safety_violation",
]


def build_event(
    row: Dict[str, Any],
    simulation_id: str = "tatkal_x",
    controller_mode: str = "unknown",
) -> Dict[str, Any]:
    """Build one structured TATKAL-X per-tick event from an existing record.

    The function intentionally performs no metric calculations. It preserves
    the values already produced by the research simulator and adds only
    stream-identification metadata.
    """
    event: Dict[str, Any] = {
        "event_type": "tatkal_x_tick",
        "simulation_id": simulation_id,
        "controller_mode": controller_mode,
    }

    for field in EVENT_FIELDS:
        event[field] = row.get(field)

    return event
