from __future__ import annotations

import csv
import itertools
import json
import math
import statistics
from pathlib import Path
from typing import Optional

from core import Config, Engine

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "results"
RAW = OUT / "raw_runs"

CONTROLLERS = [
    "fixed_rate",       # primary non-predictive rate baseline
    "admit_all",        # stress/no-admission-control baseline
    "greedy_capacity",  # instantaneous capacity baseline
    "cbf_only",         # safety-only ablation
    "mpc_only",         # prediction-only ablation
    "mpc_cbf",          # proposed controller
]

# The benchmark deliberately varies both crowd size and finite inventory.  The
# canonical case is 8,000 synchronized users competing for 1,000 tickets.
# Other scenarios test whether the controller is merely tuned to that ratio.
SCENARIOS = {
    # The canonical research case.
    "canonical_8k_1k": dict(burst=8000, inventory=1000, degradation_start=6.0, degradation_end=9.0,
                             shares=(0.20, 0.45, 0.20, 0.10, 0.05), retry_probability=0.50, payment_success=0.90),
    "high_crowd_20k_1k": dict(burst=20000, inventory=1000, degradation_start=6.0, degradation_end=9.0,
                               shares=(0.20, 0.45, 0.20, 0.10, 0.05), retry_probability=0.50, payment_success=0.90),
    # Independent inventory variation with the same 8k burst.
    "larger_inventory_8k_2k": dict(burst=8000, inventory=2000, degradation_start=6.0, degradation_end=9.0,
                                   shares=(0.20, 0.45, 0.20, 0.10, 0.05), retry_probability=0.50, payment_success=0.90),
    "scarce_inventory_8k_100": dict(burst=8000, inventory=100, degradation_start=6.0, degradation_end=9.0,
                                     shares=(0.20, 0.45, 0.20, 0.10, 0.05), retry_probability=0.50, payment_success=0.90),
}

SEEDS = list(range(42, 47))  # 5 paired seeds; df=4 for paired CI.

METRICS = [
    "successful_bookings",
    "goodput_per_s",
    "burst_absorption_rate",
    "final_inventory",
    "time_to_sellout_s",
    "p95_latency_s",
    "p99_latency_s",
    "safety_violations",
    "max_safe_violation",
    "retry_amplification",
    "latency_slo_violation_rate",
    "max_end_to_end_latency",
    "backlog_area_per_s",
    "peak_active",
    "controller_compute_ms_mean",
]

# Preferred direction. ``time_to_sellout_s`` is lower-is-better only when
# sell-out is actually reached; otherwise the raw value is kept as NaN and the
# metric is not used for a paired directional test.
METRIC_DIRECTION = {
    "successful_bookings": "higher",
    "goodput_per_s": "higher",
    "burst_absorption_rate": "higher",
    "final_inventory": "lower",
    "time_to_sellout_s": "lower",
    "p95_latency_s": "lower",
    "p99_latency_s": "lower",
    "safety_violations": "lower",
    "max_safe_violation": "lower",
    "retry_amplification": "lower",
    "latency_slo_violation_rate": "lower",
    "max_end_to_end_latency": "lower",
    "backlog_area_per_s": "lower",
    "peak_active": "none",
    "controller_compute_ms_mean": "lower",
}

_T95 = {
    1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571, 6: 2.447, 7: 2.365,
    8: 2.306, 9: 2.262, 10: 2.228, 11: 2.201, 12: 2.179, 13: 2.160,
    14: 2.145, 15: 2.131, 16: 2.120, 17: 2.110, 18: 2.101, 19: 2.093,
    20: 2.086, 21: 2.080, 22: 2.074, 23: 2.069, 24: 2.064, 25: 2.060,
    26: 2.056, 27: 2.052, 28: 2.048, 29: 2.045, 30: 2.042,
}


def _t_crit(df: int) -> float:
    return _T95.get(df, 1.96) if df >= 1 else float("nan")


def make_config(seed: int, mode: str, spec: dict) -> Config:
    fast, normal, slow, mobile, retry = spec["shares"]
    return Config(
        seed=seed,
        controller_mode=mode,
        population=max(10000, spec["burst"]),
        burst_size=spec["burst"],
        inventory=spec["inventory"],
        degradation_start_s=spec["degradation_start"],
        degradation_end_s=spec["degradation_end"],
        fast_user_share=fast,
        normal_user_share=normal,
        slow_user_share=slow,
        mobile_user_share=mobile,
        retry_prone_user_share=retry,
        retry_probability=float(spec.get("retry_probability", 0.10)),
        payment_success=float(spec.get("payment_success", 0.985)),
        max_simulation_steps=2000,
    )


def ci95(values: list[float]) -> float:
    vals = [x for x in values if math.isfinite(x)]
    if len(vals) < 2:
        return 0.0
    return _t_crit(len(vals) - 1) * statistics.stdev(vals) / math.sqrt(len(vals))


def exact_paired_sign_pvalue(diffs: list[float]) -> float:
    nz = [d for d in diffs if math.isfinite(d) and abs(d) > 1e-12]
    n = len(nz)
    if n == 0:
        return 1.0
    observed = abs(sum(nz))
    extreme = 0
    for signs in itertools.product((-1.0, 1.0), repeat=n):
        if abs(sum(s * d for s, d in zip(signs, nz))) >= observed - 1e-12:
            extreme += 1
    return extreme / (2 ** n)


def win_rate_for(metric: str, diffs: list[float]) -> Optional[float]:
    direction = METRIC_DIRECTION.get(metric, "higher")
    if direction == "none":
        return None
    vals = [d for d in diffs if math.isfinite(d)]
    if not vals:
        return None
    if direction == "higher":
        return sum(d > 0 for d in vals) / len(vals)
    return sum(d < 0 for d in vals) / len(vals)


def _run_one(job):
    """Run one deterministic controller/scenario/seed combination.

    Jobs are independent, so benchmark execution is parallelized without
    changing any seeded stochastic draws or paired-comparison semantics.
    """
    scenario, spec, seed, mode = job
    cfg = make_config(seed, mode, spec)
    engine = Engine(cfg)
    snap = engine.run_to_completion()
    raw_artifact = dict(snap)
    raw_artifact["controller"] = mode
    raw_artifact["history"] = []
    raw_artifact["seat_view"] = []
    raw_artifact["user_view"] = []
    raw_artifact["events"] = []
    return scenario, seed, mode, cfg, snap, raw_artifact


def run_all() -> None:
    OUT.mkdir(exist_ok=True)
    RAW.mkdir(exist_ok=True)
    raw_rows: list[dict] = []

    jobs = [(scenario, spec, seed, mode)
            for scenario, spec in SCENARIOS.items()
            for seed in SEEDS
            for mode in CONTROLLERS]
    # Sequential execution is intentional: it avoids host-specific process/BLAS
    # oversubscription and makes the benchmark's resource use reproducible.
    completed_jobs = [_run_one(job) for job in jobs]
    order = {(scenario, seed, mode): i for i, (scenario, spec, seed, mode) in enumerate(jobs)}
    completed_jobs.sort(key=lambda x: order[(x[0], x[1], x[2])])

    for scenario, seed, mode, cfg, snap, raw_artifact in completed_jobs:
        raw_path = RAW / f"{scenario}__{mode}__seed_{seed}.json"
        raw_path.write_text(json.dumps(raw_artifact, indent=2), encoding="utf-8")

        assert 0 <= snap["inventory"] <= cfg.inventory
        assert snap["successful_bookings"] <= cfg.inventory
        assert snap["successful_bookings"] <= cfg.burst_size
        assert abs(snap["inventory"] + snap["successful_bookings"] - cfg.inventory) < 1e-9
        assert 0.0 <= snap["burst_absorption_rate"] <= 1.0 + 1e-12
        assert snap["safety_violations"] >= 0
        assert snap["max_safe_violation"] >= 0
        if mode == "mpc_cbf":
            assert snap["safety_violations"] == 0, (
                f"CBF violation: {scenario}/{seed}: {snap['safety_violations']}"
            )

        row = {
            "scenario": scenario,
            "seed": seed,
            "controller": mode,
            "initial_users": cfg.burst_size,
            "initial_inventory": cfg.inventory,
            "demand_inventory_ratio": cfg.burst_size / cfg.inventory,
            "termination_reason": snap["termination_reason"],
            **{m: snap[m] for m in METRICS},
            "safe_absorption_start": snap["safe_absorption_start"],
            "safe_absorption_min": snap["safe_absorption_min"],
            "safe_absorption_max": snap["safe_absorption_max"],
        }
        raw_rows.append(row)

    with (OUT / "experiment_log.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(raw_rows[0].keys()))
        w.writeheader(); w.writerows(raw_rows)

    summary_rows: list[dict] = []
    for scenario in SCENARIOS:
        for mode in CONTROLLERS:
            subset = [r for r in raw_rows if r["scenario"] == scenario and r["controller"] == mode]
            row = {
                "scenario": scenario,
                "controller": mode,
                "n": len(subset),
                "initial_users": subset[0]["initial_users"],
                "initial_inventory": subset[0]["initial_inventory"],
                "demand_inventory_ratio": subset[0]["demand_inventory_ratio"],
            }
            for metric in METRICS:
                vals = [float(r[metric]) for r in subset if math.isfinite(float(r[metric]))]
                row[f"{metric}_mean"] = statistics.mean(vals) if vals else float("nan")
                row[f"{metric}_std"] = statistics.stdev(vals) if len(vals) > 1 else 0.0
                row[f"{metric}_ci95"] = ci95(vals)
                row[f"{metric}_n_finite"] = len(vals)
            summary_rows.append(row)

    with (OUT / "summary.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(summary_rows[0].keys()))
        w.writeheader(); w.writerows(summary_rows)

    impact_rows: list[dict] = []
    paired_summary: list[dict] = []
    for baseline_name in ("greedy_capacity", "fixed_rate", "cbf_only", "admit_all", "mpc_only"):
        for scenario in SCENARIOS:
            for seed in SEEDS:
                base = next(r for r in raw_rows if r["scenario"] == scenario and r["seed"] == seed and r["controller"] == baseline_name)
                prop = next(r for r in raw_rows if r["scenario"] == scenario and r["seed"] == seed and r["controller"] == "mpc_cbf")
                row = {"scenario": scenario, "seed": seed, "baseline": baseline_name}
                for metric in METRICS:
                    a, b = float(prop[metric]), float(base[metric])
                    row[f"delta_{metric}"] = a - b if math.isfinite(a) and math.isfinite(b) else float("nan")
                impact_rows.append(row)

            for metric in METRICS:
                diffs = [float(r[f"delta_{metric}"]) for r in impact_rows
                         if r["scenario"] == scenario and r["baseline"] == baseline_name and math.isfinite(float(r[f"delta_{metric}"]))]
                paired_summary.append({
                    "scenario": scenario,
                    "baseline": baseline_name,
                    "metric": metric,
                    "n": len(diffs),
                    "direction": METRIC_DIRECTION.get(metric, "higher"),
                    "mean_delta_proposed_minus_baseline": statistics.mean(diffs) if diffs else float("nan"),
                    "std_delta": statistics.stdev(diffs) if len(diffs) > 1 else 0.0,
                    "ci95": ci95(diffs),
                    "win_rate": win_rate_for(metric, diffs),
                    "exact_p_two_sided": exact_paired_sign_pvalue(diffs),
                })

    with (OUT / "paired_impact.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(impact_rows[0].keys()))
        w.writeheader(); w.writerows(impact_rows)
    with (OUT / "paired_summary.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(paired_summary[0].keys()))
        w.writeheader(); w.writerows(paired_summary)

    meta = {
        "controllers": CONTROLLERS,
        "seeds": SEEDS,
        "n_seeds": len(SEEDS),
        "scenarios": SCENARIOS,
        "n_runs": len(raw_rows),
        "execution": "sequential deterministic jobs",
        "common_random_numbers": True,
        "no_experiment_deadline": True,
        "natural_termination": ["inventory_sold_out", "demand_resolved"],
        "max_simulation_steps": 2000,
        "max_steps_role": "defensive runaway-loop guard only; not a controller constraint or reported time cutoff",
        "canonical_case": {"initial_users": 8000, "initial_inventory": 1000, "demand_inventory_ratio": 8.0},
        "benchmark_metrics": METRICS,
        "metric_direction": METRIC_DIRECTION,
        "bar_definition": "BAR = total successful bookings / initial synchronized users",
        "goodput_definition": "successful bookings / natural run duration",
        "time_to_sellout_definition": "first time inventory reaches zero; NaN when inventory does not naturally sell out",
        "safe_absorption_definition": "B_safe,H(k) is the MPC planning quantity: expected successful reservations over the next prediction horizon under the selected feasible plan; it is not a performance metric and is not divided into full-run successes",
        "optimization_scope": "MPC selects an absorption-first plan over the finite horizon; secondary regularizers break near-equal absorption ties. Candidate enumeration and deterministic rollout make this an approximate MPC, not a global optimum.",
        "novelty_scope": "The research contribution is the integrated formulation of synchronized finite-inventory transient absorption with endogenous retry pressure and a state-dependent safe admission envelope, optimized by receding-horizon control and enforced by a discrete CBF-style projection. Individual Hawkes, MPC, and CBF components are not claimed as individually novel.",
        "safety_scope": "Zero proposed-controller violations are claimed only for the implemented deterministic model and its explicitly stated worst-case service/network/latency assumptions. This is not a production safety guarantee.",
        "statistical_scope": "Paired seed comparisons use common random numbers, Student-t 95% confidence intervals on paired differences, and exact two-sided sign-permutation p-values. Results are simulation evidence, not universal superiority proofs.",
    }
    (OUT / "summary.json").write_text(json.dumps({"metadata": meta, "summary": summary_rows, "paired_summary": paired_summary}, indent=2, allow_nan=True), encoding="utf-8")
    print(f"Completed {len(raw_rows)} runs across {len(SCENARIOS)} scenarios, {len(CONTROLLERS)} controllers and {len(SEEDS)} paired seeds. Results written to {OUT}")


if __name__ == "__main__":
    run_all()
