from __future__ import annotations

"""TATKAL-X research simulator.

The simulator is deliberately separated from Flask so the exact same model is
used by the live UI and by results.py.  The proposed controller is a finite-
horizon MPC whose first action is filtered by a discrete-time CBF.

Research state:
    A_k  active backend transactions
    Q_k  unactivated demand attempts
    I_k  remaining inventory
    z_k  Hawkes excess intensity (lambda = mu + z)
Proposed planning quantity:
    B_safe,H(k) = expected successful reservations over the next MPC prediction
                  horizon H under the selected feasible action sequence. It is
                  a controller planning quantity, not an observed outcome.

Reported outcome metrics (see Engine.snapshot):
    BAR = total successful bookings / initial synchronized burst size.
    T_sellout = first time at which the finite inventory reaches zero, if it
                naturally sells out. No artificial experiment deadline is used.

The CBF is intentionally conservative: it assumes no completion during the
instantaneous safety step, so the hard filter cannot cross the safe-concurrency
boundary even under a worst-case no-departure step.
"""

from dataclasses import dataclass, asdict, fields
from collections import deque
import csv
import io
import json
import math
import os
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import psutil

from event_schema import build_event
from kafka_producer import KafkaEventProducer


USER_PROFILES: Dict[str, Dict[str, float]] = {
    "FAST": {"network_rtt_s": 0.035, "processing_scale": 0.85, "retry_mult": 0.65},
    "NORMAL": {"network_rtt_s": 0.080, "processing_scale": 1.00, "retry_mult": 1.00},
    "SLOW": {"network_rtt_s": 0.220, "processing_scale": 1.35, "retry_mult": 1.15},
    "MOBILE": {"network_rtt_s": 0.140, "processing_scale": 1.15, "retry_mult": 1.25},
    "RETRY_PRONE": {"network_rtt_s": 0.095, "processing_scale": 1.00, "retry_mult": 2.00},
}

# Human-readable labels for the seat-hover tooltip. These map 1:1 onto the
# USER_PROFILES keys already used to drive network_rtt/processing/retry
# behavior — no new user categories are invented for display purposes.
NETWORK_LABELS: Dict[str, str] = {
    "FAST": "Fast network",
    "NORMAL": "Normal network",
    "SLOW": "Slow network",
    "MOBILE": "Mobile network",
    "RETRY_PRONE": "Unstable connection",
}


@dataclass
class Config:
    seed: int = 42
    dt: float = 0.10
    horizon: int = 6
    max_prediction_steps: int = 240
    population: int = 10000
    burst_size: int = 8000
    burst_duration_s: float = 0.50
    max_simulation_steps: int = 2000

    inventory: int = 1000
    service_capacity_per_s: float = 50.0
    latency_limit_s: float = 4.0
    safe_margin: float = 0.10
    latency_guard_s: float = 0.05

    hawkes_mu: float = 0.5
    hawkes_alpha: float = 0.08
    hawkes_beta: float = 0.80
    retry_probability: float = 0.10
    retry_delay_s: float = 1.20
    max_payment_retries: int = 2

    degraded_capacity_factor: float = 0.45
    degradation_start_s: float = 6.0
    degradation_end_s: float = 9.0
    recovery_capacity_factor: float = 1.00

    payment_success: float = 0.985
    fast_user_share: float = 0.20
    normal_user_share: float = 0.45
    slow_user_share: float = 0.20
    mobile_user_share: float = 0.10
    retry_prone_user_share: float = 0.05

    controller_mode: str = "mpc_cbf"
    max_control_step: int = 1000
    pressure_weight: float = 0.20
    smooth_weight: float = 0.02
    risk_weight: float = 0.10
    terminal_backlog_weight: float = 0.50
    early_completion_weight: float = 0.20
    action_delay_weight: float = 0.05

    # Live-only telemetry; never changes benchmark model unless explicitly enabled.
    use_real_telemetry: bool = False
    kafka_enabled: bool = False
    cpu_limit_pct: float = 90.0
    memory_limit_mb: float = 0.0

    # Display metadata. Deliberately domain-generic: this is a finite-inventory
    # synchronized-burst reservation testbed, not a model of any specific
    # real-world booking system (railway or otherwise).
    service_name: str = "GENERIC RESERVATION SERVICE"
    origin: str = "SOURCE NODE"
    destination: str = "SINK NODE"
    travel_class: str = "STANDARD TIER"
    quota: str = "OPEN"
    visible_users: int = 80


class Telemetry:
    def __init__(self) -> None:
        self.p = psutil.Process(os.getpid())
        self.p.cpu_percent(None)
        self.cpu = 0.0
        self.rss_mb = self.p.memory_info().rss / 1e6
        self.tick_ms = 0.0

    def sample(self, elapsed: float) -> None:
        self.tick_ms = elapsed * 1000.0
        self.cpu = float(self.p.cpu_percent(None))
        self.rss_mb = self.p.memory_info().rss / 1e6


class HawkesProcess:
    """Discrete exponential Hawkes state.

    z_{k+1}=exp(-beta*dt) z_k + alpha*r_k.
    lambda_k = mu + z_k.
    The alpha/beta ratio is retained only as a model parameter ratio; this
    discrete recurrence is not asserted to have the standard continuous-time
    Hawkes branching-ratio interpretation.
    """

    def __init__(self, c: Config) -> None:
        self.mu = c.hawkes_mu
        self.alpha = c.hawkes_alpha
        self.beta = c.hawkes_beta
        self.z = 0.0
        self.events = 0

    @property
    def excitation_ratio(self) -> float:
        return self.alpha / max(self.beta, 1e-12)

    def decay(self, dt: float) -> None:
        self.z *= math.exp(-self.beta * dt)

    def add_events(self, n: int, weight: float = 1.0) -> None:
        if n > 0:
            self.z += self.alpha * float(n) * weight
            self.events += int(n)

    def intensity(self) -> float:
        return max(0.0, self.mu + self.z)


class Workload:
    """Seeded workload shared by paired controller experiments.

    Initial users are generated once from the seed. Each logical user keeps
    the same profile on retries. Payment and network randomness is pre-generated
    so paired controllers see the same stochastic user characteristics.
    """

    def __init__(self, c: Config) -> None:
        self.c = c
        self.rng = np.random.default_rng(c.seed + 17)
        self.profile_rng = np.random.default_rng(c.seed + 100003)
        self.profiles = self._generate_profiles(max(c.population, c.burst_size))
        self.network_factor = np.ones(len(self.profiles))
        self.processing_factor = np.ones(len(self.profiles))
        self.payment_u = self.rng.random(len(self.profiles))
        self.retry_u = self.rng.random(len(self.profiles))
        for i, p in enumerate(self.profiles):
            q = USER_PROFILES[p]
            self.network_factor[i] = max(0.005, self.rng.normal(q["network_rtt_s"], q["network_rtt_s"] * 0.20))
            self.processing_factor[i] = max(0.50, self.rng.normal(q["processing_scale"], 0.08))

    def _weights(self) -> Tuple[List[str], List[float]]:
        c = self.c
        raw = {
            "FAST": c.fast_user_share,
            "NORMAL": c.normal_user_share,
            "SLOW": c.slow_user_share,
            "MOBILE": c.mobile_user_share,
            "RETRY_PRONE": c.retry_prone_user_share,
        }
        total = sum(max(0.0, v) for v in raw.values())
        if total <= 0:
            return ["NORMAL"], [1.0]
        keys = list(raw)
        return keys, [max(0.0, raw[k]) / total for k in keys]

    def _generate_profiles(self, n: int) -> List[str]:
        keys, weights = self._weights()
        return [str(x) for x in self.profile_rng.choice(keys, size=n, p=weights)]

    def profile(self, user_id: int) -> str:
        return self.profiles[user_id % len(self.profiles)]

    def network_rtt(self, user_id: int) -> float:
        return float(self.network_factor[user_id % len(self.profiles)])

    def processing_scale(self, user_id: int) -> float:
        return float(self.processing_factor[user_id % len(self.profiles)])

    def _attempt_uniform(self, user_id: int, attempt_index: int, stream: int) -> float:
        # Deterministic, attempt-specific randomness. The same seed/user/attempt
        # produces the same draw across paired controller experiments, while
        # successive payment/retry attempts receive independent draws.
        seed = (
            int(self.c.seed)
            + 1_000_003
            + int(user_id) * 1_009
            + int(attempt_index) * 9_176
            + int(stream) * 104_729
        )
        return float(np.random.default_rng(seed).random())

    def payment_success(self, user_id: int, attempt_index: int = 0) -> bool:
        p = self.profile(user_id)
        penalty = {"FAST": 0.0, "NORMAL": 0.0, "SLOW": 0.012, "MOBILE": 0.008, "RETRY_PRONE": 0.004}[p]
        threshold = max(0.0, min(1.0, self.c.payment_success - penalty))
        return self._attempt_uniform(user_id, attempt_index, 1) <= threshold

    def retry_probability(self, user_id: int) -> float:
        p = self.profile(user_id)
        return min(1.0, self.c.retry_probability * USER_PROFILES[p]["retry_mult"])

    def should_retry(self, user_id: int, attempt_index: int = 0) -> bool:
        return self._attempt_uniform(user_id, attempt_index, 2) <= self.retry_probability(user_id)


class Controller:
    """Admission controller for the finite-inventory transient system.

    The proposed controller is deliberately split into two mathematically
    distinct layers:

      1. finite-horizon receding-horizon MPC chooses a nominal admission action;
      2. a discrete-time CBF-style safety projection clips that action to the
         current robust safe admission envelope.

    No artificial booking deadline is part of the plant model.  The only
    horizon is ``c.horizon * c.dt`` and it is used solely for MPC prediction.
    """

    MODES = ("mpc_cbf", "mpc_only", "cbf_only", "fixed_rate", "greedy_capacity", "admit_all")

    def __init__(self, c: Config) -> None:
        self.c = c
        self.prev_u = 0
        self.last_safe_absorption = 0.0
        self.last_plan: List[float] = []

    def capacity(self, t: float) -> float:
        c = self.c
        if c.degradation_start_s <= t < c.degradation_end_s:
            return max(0.1, c.service_capacity_per_s * c.degraded_capacity_factor)
        if t >= c.degradation_end_s:
            return max(0.1, c.service_capacity_per_s * c.recovery_capacity_factor)
        return max(0.1, c.service_capacity_per_s)

    def telemetry_factor(self, cpu: float, rss: float, tick_ms: float) -> float:
        c = self.c
        if not c.use_real_telemetry:
            return 1.0
        f = 1.0
        if c.cpu_limit_pct and cpu > c.cpu_limit_pct:
            f *= max(0.2, c.cpu_limit_pct / cpu)
        if c.memory_limit_mb and rss > c.memory_limit_mb:
            f *= 0.5
        if tick_ms > c.dt * 1000.0 * 1.5:
            f *= 0.75
        return f

    def robust_capacity(self, now: float, cpu: float, rss: float, tick_ms: float) -> float:
        """Minimum known service capacity over one latency-SLO horizon.

        This is a forward-looking *capacity* bound, not a booking deadline.
        The horizon is chosen because an admission made now can still be in
        flight for at most the modeled latency SLO.  Piecewise capacity changes
        are evaluated exactly at their regime boundaries.
        """
        c = self.c
        # Known capacity regimes are part of the model. If a degradation/recovery
        # boundary is scheduled within the next modeled transition, include it
        # now so the safety envelope anticipates the change rather than reacting
        # after the active set has already crossed the new bound.
        transition_ahead = max(0.0, c.degradation_start_s - now)
        end = now + max(c.dt, c.latency_limit_s, transition_ahead + c.dt)
        points = [now, end]
        for b in (c.degradation_start_s, c.degradation_end_s):
            if now <= b <= end:
                points.append(b)
        vals = [self.capacity(t) for t in points]
        return max(0.1, min(vals) * self.telemetry_factor(cpu, rss, tick_ms))

    def service_time_bound(self, robust_capacity: float) -> float:
        max_processing = max(float(v["processing_scale"]) for v in USER_PROFILES.values())
        return max(1e-6, max_processing / max(robust_capacity, 1e-9))

    def network_time_bound(self) -> float:
        return max(float(v["network_rtt_s"]) for v in USER_PROFILES.values())

    def safe_envelope(self, now: float, cpu: float, rss: float, tick_ms: float,
                      backend_workload_s: float) -> Tuple[float, int, int, float]:
        """Return robust service rate and two hard admission limits.

        Concurrency limit:
            C_A = floor(mu_robust * L_SLO * (1-m))

        Latency limit for the last newly admitted request:
            W + u*S_max + r_max + g <= L_SLO

        Hence the admissible action must satisfy both limits, in addition to
        finite inventory and pending demand.
        """
        c = self.c
        robust = self.robust_capacity(now, cpu, rss, tick_ms)
        conc = max(0, int(math.floor(robust * c.latency_limit_s * (1.0 - c.safe_margin))))
        s_max = self.service_time_bound(robust)
        r_max = self.network_time_bound()
        budget = c.latency_limit_s - c.latency_guard_s - float(backend_workload_s) - r_max
        lat_cap = max(0, int(math.floor(max(0.0, budget) / s_max)))
        return robust, conc, lat_cap, s_max

    def safe_concurrency(self, now: float, cpu: float, rss: float, tick_ms: float) -> Tuple[float, int]:
        robust = self.robust_capacity(now, cpu, rss, tick_ms)
        cap = max(0, int(math.floor(robust * self.c.latency_limit_s * (1.0 - self.c.safe_margin))))
        return robust, cap

    def expected_mix(self) -> Tuple[float, float]:
        c = self.c
        shares = {
            "FAST": c.fast_user_share, "NORMAL": c.normal_user_share,
            "SLOW": c.slow_user_share, "MOBILE": c.mobile_user_share,
            "RETRY_PRONE": c.retry_prone_user_share,
        }
        total = sum(max(0.0, v) for v in shares.values()) or 1.0
        p_success = 0.0
        retry_mult = 0.0
        penalties = {"FAST": 0.0, "NORMAL": 0.0, "SLOW": 0.012, "MOBILE": 0.008, "RETRY_PRONE": 0.004}
        for k, v in shares.items():
            w = max(0.0, v) / total
            p_success += w * max(0.0, min(1.0, c.payment_success - penalties[k]))
            retry_mult += w * USER_PROFILES[k]["retry_mult"]
        return p_success, retry_mult

    def _candidate_actions(self, upper: int) -> List[int]:
        if upper <= 0:
            return [0]
        # Exact small search; deterministic sparse search for large action spaces.
        if upper <= 10:
            return list(range(upper + 1))
        grid = np.linspace(0, upper, 5, dtype=int).tolist()
        return sorted(set(grid + [1, upper]))

    def _predict_mpc(self, now: float, active: int, inventory: int, pending: int,
                     hawkes_intensity: float, backend_workload_s: float,
                     cpu: float, rss: float, tick_ms: float) -> Tuple[float, List[float], float]:
        """Approximate finite-horizon MPC over the next H control steps.

        The optimization is over admission actions, not over an artificial
        experiment duration.  The prediction model conservatively propagates
        active work, finite inventory, FCFS workload and retry pressure.
        """
        c = self.c
        p_success, retry_mult = self.expected_mix()
        H = max(1, int(c.horizon))
        upper0 = min(int(pending), max(0, int(inventory - active)), int(c.max_control_step))
        # First-action safety envelope is part of the feasible set for MPC+CBF;
        # MPC-only intentionally omits that projection for the ablation.
        robust0, conc0, lat0, smax0 = self.safe_envelope(now, cpu, rss, tick_ms, backend_workload_s)
        safe_upper0 = min(upper0, max(0, conc0 - active), lat0)

        best_value = -float("inf")
        best_primary = -float("inf")
        best_penalty = float("inf")
        best_first = 0
        best_plan: List[float] = []
        candidate_upper = safe_upper0 if c.controller_mode == "mpc_cbf" else upper0
        # Work-conserving lower bound: if demand is waiting and at least one
        # transaction is safely admissible, do not repeatedly select an idle
        # first action. This is a physical service rule, not a deadline.
        lower0 = 1 if pending > 0 and candidate_upper >= 1 else 0
        candidates0 = [a for a in self._candidate_actions(candidate_upper) if a >= lower0]
        for u0 in candidates0:
            A = float(active)
            I = float(inventory)
            Q = float(pending)
            W = max(0.0, float(backend_workload_s))
            z = max(0.0, float(hawkes_intensity) - c.hawkes_mu)
            prev = float(self.prev_u)
            value = 0.0
            expected_total = 0.0
            regularization_cost = 0.0
            plan: List[float] = []

            for j in range(H):
                tj = now + j * c.dt
                robust, conc, lat_cap, smax = self.safe_envelope(tj, cpu, rss, tick_ms, W)
                upper = min(Q, max(0.0, I - A), float(c.max_control_step))
                if j == 0:
                    u = float(u0)
                else:
                    # Predicted CBF-feasible continuation. This is the nominal
                    # predictive policy; the actual CBF is still re-applied at
                    # execution time for the proposed controller.
                    if c.controller_mode == "mpc_cbf":
                        upper = min(upper, max(0.0, conc - A), float(lat_cap))
                    target = max(0.0, upper)
                    u = 0.75 * target + 0.25 * prev
                    u = min(u, upper)
                if j == 0 and c.controller_mode == "mpc_cbf":
                    u = min(u, max(0.0, conc - A), float(lat_cap))
                u = max(0.0, min(u, max(0.0, I - A), Q, float(c.max_control_step)))

                # Expected service completions during the next step.
                departure = min(A, robust * c.dt)
                expected_completed = p_success * departure
                expected_failure = max(0.0, departure - expected_completed)
                z = math.exp(-c.hawkes_beta * c.dt) * z + c.hawkes_alpha * expected_failure * retry_mult
                lam = c.hawkes_mu + z

                # FCFS workload update. Newly admitted requests append service
                # work; service capacity removes work during the step.
                W = max(0.0, W + u * smax - robust * c.dt)
                A = max(0.0, A + u - departure)
                I = max(0.0, I - expected_completed)
                Q = max(0.0, Q - u)

                # Absorption is the primary optimization quantity. The remaining
                # terms are secondary regularizers/tie-breakers; they cannot turn
                # the controller into a pure pressure-minimizer that intentionally
                # leaves safely absorbable demand untouched.
                expected_total += expected_completed
                regularization_cost += (
                    c.action_delay_weight * (j + 1) * u
                    + c.pressure_weight * lam / max(1.0, robust)
                    + c.smooth_weight * abs(u - prev)
                    + c.risk_weight * max(0.0, A - conc) / max(1.0, float(conc) + 1.0)
                    - c.early_completion_weight * (H - j) * expected_completed
                )
                value = expected_total - regularization_cost
                plan.append(u)
                prev = u
                if Q <= 1e-9 or I <= 1e-9:
                    break

            # Receding-horizon tie-breaker: leaving demand unserved at the end
            # of the prediction horizon is penalized so the controller does not
            # repeatedly choose an apparently attractive delayed plan while the
            # real plant remains unchanged between MPC calls.
            value -= c.terminal_backlog_weight * Q

            # Lexicographic selection: first maximize predicted successful
            # absorption over the horizon; only then use regularization to choose
            # among plans with effectively equal absorption. This aligns the
            # implemented optimization with the research question while retaining
            # smoothness/pressure preferences as secondary criteria.
            better = (expected_total > best_primary + 1e-12 or
                      (abs(expected_total - best_primary) <= 1e-12 and
                       regularization_cost < best_penalty - 1e-12))
            if better:
                best_primary = expected_total
                best_penalty = regularization_cost
                best_value = value
                best_first = int(round(u0))
                best_plan = plan

        # Planning quantity: expected successful reservations over the chosen
        # prediction sequence, capped by remaining finite inventory.
        A2 = float(active)
        I2 = float(inventory)
        W2 = max(0.0, float(backend_workload_s))
        expected_successes = 0.0
        for j, u in enumerate(best_plan):
            robust, _, _, smax = self.safe_envelope(now + j * c.dt, cpu, rss, tick_ms, W2)
            departure = min(A2, robust * c.dt)
            completed = p_success * departure
            expected_successes += completed
            W2 = max(0.0, W2 + float(u) * smax - robust * c.dt)
            A2 = max(0.0, A2 + float(u) - departure)
            I2 = max(0.0, I2 - completed)
        safe_absorption = min(float(inventory), expected_successes)
        self.last_plan = best_plan
        self.last_safe_absorption = safe_absorption
        return float(safe_absorption), best_plan, float(best_value)

    def decide(self, now: float, active: int, inventory: int, pending: int,
               hawkes_intensity: float, cpu: float, rss: float, tick_ms: float,
               backend_workload_s: float = 0.0) -> Dict[str, Any]:
        c = self.c
        robust, safe_c, latency_c, service_time_bound = self.safe_envelope(
            now, cpu, rss, tick_ms, backend_workload_s
        )
        p_success, _ = self.expected_mix()
        free_inventory = max(0, int(inventory) - int(active))
        cbf_limit = max(0, int(safe_c) - int(active))
        hard_limit = min(int(pending), free_inventory, cbf_limit, int(latency_c), int(c.max_control_step))

        if c.controller_mode == "fixed_rate":
            u = min(pending, free_inventory, max(0, int(math.floor(c.service_capacity_per_s * c.dt))))
            return self._result(u, robust, safe_c, cbf_limit, 0.0, 0.0, False, 0.0, p_success, latency_c, service_time_bound)
        if c.controller_mode == "greedy_capacity":
            u = min(pending, free_inventory, max(0, int(math.floor(robust * c.dt))))
            return self._result(u, robust, safe_c, cbf_limit, 0.0, 0.0, False, 0.0, p_success, latency_c, service_time_bound)
        if c.controller_mode == "admit_all":
            u = min(pending, free_inventory, c.max_control_step)
            return self._result(u, robust, safe_c, cbf_limit, 0.0, 0.0, False, 0.0, p_success, latency_c, service_time_bound)
        if c.controller_mode == "cbf_only":
            # Myopic safety-only baseline: begin from the instantaneous service
            # capacity and then apply the same hard safety constraints. It has no
            # predictive look-ahead and therefore cannot front-load safe work
            # merely because future capacity is known.
            nominal = min(pending, free_inventory, c.max_control_step, max(0, int(math.floor(robust * c.dt))))
            u = min(nominal, hard_limit)
            return self._result(u, robust, safe_c, cbf_limit, 0.0, 0.0, u < nominal, float(max(0, nominal-u)), p_success, latency_c, service_time_bound)

        safe_abs, plan, objective = self._predict_mpc(
            now, active, inventory, pending, hawkes_intensity,
            backend_workload_s, cpu, rss, tick_ms
        )
        nominal = int(round(plan[0])) if plan else 0
        nominal = min(nominal, pending, free_inventory, c.max_control_step)
        if c.controller_mode == "mpc_only":
            u = nominal
            filtered = False
            filter_amount = 0.0
        else:
            u = min(nominal, hard_limit)
            filtered = u != nominal
            filter_amount = float(max(0, nominal-u))
        self.prev_u = int(u)
        return self._result(int(u), robust, safe_c, cbf_limit, safe_abs, objective,
                            filtered, filter_amount, p_success, latency_c, service_time_bound)

    @staticmethod
    def _result(u: int, robust: float, safe_c: int, headroom: int,
                safe_absorption: float, objective: float, filtered: bool,
                filter_amount: float, p_success: float, latency_c: int = 0,
                service_time_bound: float = 0.0) -> Dict[str, Any]:
        return {
            "credits": int(max(0, u)),
            "robust_service_rate": float(robust),
            "safe_concurrency": int(max(0, safe_c)),
            "latency_admission_capacity": int(max(0, latency_c)),
            "service_time_bound_s": float(service_time_bound),
            "cbf_headroom": int(max(0, headroom)),
            "safe_absorption": float(max(0.0, safe_absorption)),
            "mpc_objective": float(objective),
            "cbf_filtered": bool(filtered),
            "cbf_filter_amount": float(filter_amount),
            "expected_payment_success": float(p_success),
        }


class Engine:
    """Discrete-event-style simulation advanced at fixed dt for live rendering."""

    def __init__(self, cfg: Optional[Config] = None) -> None:
        self.lock = threading.RLock()
        self.cfg = cfg or Config()
        self.telemetry = Telemetry()
        self.kafka_producer = KafkaEventProducer(enabled=self.cfg.kafka_enabled)
        self.rng = np.random.default_rng(self.cfg.seed + 7001)
        self.workload = Workload(self.cfg)
        self.hawkes = HawkesProcess(self.cfg)
        self.controller = Controller(self.cfg)
        self.running = False
        self.error: Optional[str] = None
        self.thread: Optional[threading.Thread] = None
        self.generation = 0
        self.reset()

    def configure(self, payload: Dict[str, Any]) -> None:
        with self.lock:
            if self.running:
                raise RuntimeError("stop the booking window before changing configuration")
            allowed = {f.name for f in fields(self.cfg)}
            cur = asdict(self.cfg)
            for k, v in (payload or {}).items():
                if k in allowed:
                    cur[k] = v
            ints = {"seed", "horizon", "max_prediction_steps", "population", "burst_size", "max_payment_retries", "max_control_step", "inventory", "visible_users", "max_simulation_steps"}
            strings = {"controller_mode", "service_name", "origin", "destination", "travel_class", "quota"}
            for k in list(cur):
                if k in ints:
                    cur[k] = int(cur[k])
                elif k in strings:
                    cur[k] = str(cur[k])
                elif k not in strings:
                    cur[k] = float(cur[k]) if isinstance(cur[k], (int, float, str)) else cur[k]
            if cur["controller_mode"] not in Controller.MODES:
                raise ValueError("invalid controller_mode")
            if not (0 < cur["dt"] <= 1.0):
                raise ValueError("dt must be in (0,1]")
            if cur["inventory"] < 1 or cur["burst_size"] < 1:
                raise ValueError("inventory and burst_size must be positive")
            if cur["max_simulation_steps"] < 1:
                raise ValueError("max_simulation_steps must be positive")
            shares = [cur["fast_user_share"], cur["normal_user_share"], cur["slow_user_share"], cur["mobile_user_share"], cur["retry_prone_user_share"]]
            if any(x < 0 for x in shares) or sum(shares) <= 0:
                raise ValueError("user shares must be nonnegative and have positive total")
            if cur["hawkes_beta"] <= 0 or cur["hawkes_alpha"] < 0:
                raise ValueError("Hawkes beta must be >0 and alpha >=0")
            self.cfg = Config(**cur)
            self.rng = np.random.default_rng(self.cfg.seed + 7001)
            self.workload = Workload(self.cfg)
            self.reset()

    def reset(self) -> None:
        with self.lock:
            self.running = False
            self.generation += 1
            self.error = None
            self.t = 0.0
            self.inventory = self.cfg.inventory
            self.attempts = 0
            self.initial_attempts = 0
            self.retry_attempts = 0
            self.activated = 0
            self.completed = 0
            self.failed = 0
            self.payment_failures = 0
            self.retry_events = 0
            self.peak_active = 0
            self.safety_violations = 0
            self.max_safe_violation = 0
            self.cbf_filter_events = 0
            self.total_controller_ms = 0.0
            self.controller_calls = 0
            self.latencies: List[float] = []
            self.sellout_time_s: Optional[float] = None
            self.latency_slo_violations = 0
            self.max_end_to_end_latency = 0.0
            self.backlog_area = 0.0
            self.termination_reason = "not_finished"
            self.user_status: Dict[int, str] = {}
            self.user_profile: Dict[int, str] = {}
            self.retry_schedule: List[Dict[str, Any]] = []
            self.retry_counts: Dict[int, int] = {}
            self.active: List[Dict[str, Any]] = []
            self.booked_info: Dict[int, Dict[str, Any]] = {}
            self.server_available_at = self.t
            self.inventory_slots = list(range(self.cfg.inventory))
            self.rng.shuffle(self.inventory_slots)
            self.booked_slots: set[int] = set()
            self.active_slots: set[int] = set()
            self.hawkes = HawkesProcess(self.cfg)
            self.controller = Controller(self.cfg)
            self.history = deque(maxlen=5000)
            self.events = deque(maxlen=180)
            self.last_event: Optional[Dict[str, Any]] = None
            self.max_pressure = 0.0
            self._make_user_view()

    def _make_user_view(self) -> None:
        self.visible_user_ids = list(range(min(self.cfg.visible_users, self.cfg.burst_size)))

    def _burst_arrivals_this_tick(self) -> int:
        if self.t > self.cfg.burst_duration_s or self.initial_attempts >= self.cfg.burst_size:
            return 0
        remaining = self.cfg.burst_size - self.initial_attempts
        rate = self.cfg.burst_size / max(self.cfg.burst_duration_s, 1e-9)
        n = min(remaining, max(0, int(round(rate * self.cfg.dt))))
        return n

    def _transaction(self, user_id: int) -> Dict[str, Any]:
        p = self.workload.profile(user_id)
        # A single FCFS reservation processor makes service_capacity_per_s an
        # actual throughput constraint. Transactions admitted faster than service
        # can finish accumulate queueing delay rather than magically completing.
        backend_start = max(self.t, self.server_available_at)
        service_rate = self.controller.capacity(backend_start)
        backend = max(0.003, self.workload.processing_scale(user_id) / max(service_rate, 1e-9))
        network = self.workload.network_rtt(user_id)
        backend_done = backend_start + backend
        self.server_available_at = backend_done
        return {
            "id": user_id,
            "profile": p,
            "start": self.t,
            "backend_done": backend_done,
            "commit_done": backend_done + network,
            "slot": None,
            "retries": int(self.retry_counts.get(user_id, 0)),
        }

    def _assign_free_slot(self) -> Optional[int]:
        candidates = [s for s in self.inventory_slots if s not in self.booked_slots and s not in self.active_slots]
        if not candidates:
            return None
        return int(self.rng.choice(candidates))

    def start(self) -> bool:
        with self.lock:
            if self.running:
                return False
            self.running = True
            self.error = None
            self.generation += 1
            g = self.generation
            self.events.append({"type": "BOOKING_WINDOW_OPEN", "t": round(self.t, 3)})
            self.thread = threading.Thread(target=self._run, args=(g,), daemon=True)
            self.thread.start()
            return True

    def stop(self) -> None:
        with self.lock:
            self.running = False
            self.events.append({"type": "STOP", "t": round(self.t, 3)})

    def _run(self, generation: int) -> None:
        try:
            while True:
                with self.lock:
                    if not self.running or generation != self.generation:
                        break
                started = time.perf_counter()
                self.tick()
                elapsed = time.perf_counter() - started
                self.telemetry.sample(elapsed)
                time.sleep(max(0.0, self.cfg.dt - elapsed))
        except Exception as exc:
            with self.lock:
                self.error = repr(exc)
                self.running = False
                self.events.append({"type": "ERROR", "error": repr(exc), "t": round(self.t, 3)})

    def tick(self) -> None:
        with self.lock:
            c = self.cfg
            self.t = round(self.t + c.dt, 10)
            self.hawkes.decay(c.dt)

            # Retry attempts become new demand exactly once: at their re-entry time.
            # NOTE: Hawkes excitation for a retry-generating event is applied
            # exactly once, at the payment-failure event that scheduled it (see
            # the single `self.hawkes.add_events(1)` call below, at the point of
            # failure). Reactivation here is the delayed consequence of that
            # same event, not a second independent excitation, so it must NOT
            # call hawkes.add_events again -- doing so previously double-counted
            # every retry-generating failure (once at failure, once at re-entry).
            ready = [r for r in self.retry_schedule if r["at"] <= self.t]
            self.retry_schedule = [r for r in self.retry_schedule if r["at"] > self.t]
            if ready:
                self.retry_events += len(ready)
                self.retry_attempts += len(ready)
                self.attempts += len(ready)
                for r in ready:
                    uid = int(r["id"])
                    self.retry_counts[uid] = int(r.get("retries", self.retry_counts.get(uid, 0)))
                    self.user_status[uid] = "RETRYING"
                self.events.append({"type": "RETRY_WAVE", "n": len(ready), "t": round(self.t, 3)})

            # Initial synchronized exogenous demand.
            n0 = self._burst_arrivals_this_tick()
            if n0:
                self.initial_attempts += n0
                self.attempts += n0
                for uid in range(self.initial_attempts - n0, self.initial_attempts):
                    self.user_profile[uid] = self.workload.profile(uid)
                    self.user_status[uid] = "WAITING"
                self.events.append({"type": "SYNCHRONIZED_BURST", "n": n0, "t": round(self.t, 3)})

            # New demand this tick, from either channel. Used only for the
            # reported (measured, not model-internal) arrival-rate metric.
            new_arrivals_this_tick = int(n0) + len(ready)

            pending = max(0, self.attempts - self.activated)
            self.backlog_area += pending * c.dt
            # A visible pressure field includes the synchronized shock while it is
            # being released and the endogenous Hawkes retry intensity thereafter.
            burst_rate = (c.burst_size / c.burst_duration_s) if self.t <= c.burst_duration_s and self.initial_attempts < c.burst_size else 0.0
            pressure = self.hawkes.intensity() + 0.02 * burst_rate
            self.max_pressure = max(self.max_pressure, pressure)

            s0 = time.perf_counter()
            free_inventory = max(0, self.inventory - len(self.active))
            decision = self.controller.decide(
                self.t, len(self.active), self.inventory, pending,
                self.hawkes.intensity(),
                self.telemetry.cpu, self.telemetry.rss_mb, self.telemetry.tick_ms,
                max(0.0, self.server_available_at - self.t),
            )
            self.total_controller_ms += (time.perf_counter() - s0) * 1000.0
            self.controller_calls += 1
            if decision["cbf_filtered"]:
                self.cbf_filter_events += 1

            # Activation is the control action. It is NOT a successful booking.
            credits = min(int(decision["credits"]), pending, free_inventory)
            for _ in range(max(0, credits)):
                uid = self._next_unactivated_user()
                if uid is None:
                    break
                slot = self._assign_free_slot()
                if slot is None:
                    break
                tx = self._transaction(uid)
                tx["slot"] = slot
                self.active.append(tx)
                self.active_slots.add(slot)
                self.activated += 1
                self.user_status[uid] = "PROCESSING"
                self.events.append({"type": "USER_ACTIVATED", "id": uid, "profile": tx["profile"], "t": round(self.t, 3)})

            self.peak_active = max(self.peak_active, len(self.active))

            # Successful backend/network completion -> payment/commit. The slot is
            # consumed only on success; a failed payment releases it.
            finished = [tx for tx in self.active if tx["commit_done"] <= self.t]
            self.active = [tx for tx in self.active if tx["commit_done"] > self.t]
            for tx in finished:
                uid = tx["id"]
                slot = tx["slot"]
                self.active_slots.discard(slot)
                latency = max(0.0, float(tx["commit_done"] - tx["start"]))
                self.latencies.append(latency)
                self.max_end_to_end_latency = max(self.max_end_to_end_latency, latency)
                if latency > c.latency_limit_s:
                    self.latency_slo_violations += 1
                attempt_index = int(tx["retries"])
                if self.workload.payment_success(uid, attempt_index):
                    assert self.inventory > 0, (
                        "invariant violated: booking committed with zero inventory remaining "
                        "(finite-inventory guard in the controller/activation path must have failed upstream)"
                    )
                    self.inventory -= 1
                    self.completed += 1
                    self.booked_slots.add(slot)
                    self.user_status[uid] = "BOOKED"
                    if self.inventory == 0 and self.sellout_time_s is None:
                        self.sellout_time_s = float(self.t)
                    # Retain the user characteristics that produced this booking.
                    # The active transaction is popped this same tick, so without
                    # this the seat map would lose per-user info the instant a
                    # ticket is confirmed.
                    self.booked_info[slot] = {
                        "user_id": uid,
                        "profile": tx["profile"],
                        "retries": int(tx["retries"]),
                        "network_rtt_s": round(self.workload.network_rtt(uid), 3),
                        "latency_s": round(latency, 3),
                    }
                    self.events.append({"type": "BOOKED", "id": uid, "profile": tx["profile"], "t": round(self.t, 3), "latency": round(latency, 4)})
                else:
                    self.payment_failures += 1
                    self.failed += 1
                    self.user_status[uid] = "PAYMENT_FAILED"
                    # Single point of Hawkes excitation for this retry-generating
                    # event (see the note in the retry re-entry block above: the
                    # later reactivation of this same failure must not excite
                    # the process again).
                    self.hawkes.add_events(1)
                    if tx["retries"] < c.max_payment_retries and self.workload.should_retry(uid, attempt_index):
                        next_retry = tx["retries"] + 1
                        self.retry_counts[uid] = next_retry
                        retry_time = self.t + c.retry_delay_s
                        self.retry_schedule.append({"at": retry_time, "id": uid, "retries": next_retry})
                        self.user_status[uid] = "RETRY_SCHEDULED"
                    self.events.append({"type": "PAYMENT_FAILURE", "id": uid, "profile": tx["profile"], "t": round(self.t, 3)})

            self.peak_active = max(self.peak_active, len(self.active))

            # Hard safety is checked after execution. Under the proposed CBF,
            # violations should remain zero because the filter is sufficient under
            # the no-departure assumption.
            robust, safe_c = self.controller.safe_concurrency(
                self.t, self.telemetry.cpu, self.telemetry.rss_mb, self.telemetry.tick_ms
            )
            active_now = len(self.active)
            excess = max(0, active_now - safe_c)
            if excess > 0:
                self.safety_violations += 1
                self.max_safe_violation = max(self.max_safe_violation, excess)

            safe_absorption = decision["safe_absorption"] if c.controller_mode == "mpc_cbf" else None
            record = {
                "t": self.t,
                "pressure": pressure,
                "active": active_now,
                "inventory": self.inventory,
                "attempts": self.attempts,
                "initial_attempts": self.initial_attempts,
                "retry_attempts": self.retry_attempts,
                "activated": self.activated,
                "completed": self.completed,
                "pending": pending,
                "service_rate": self.controller.capacity(self.t),
                "robust_service_rate": robust,
                "safe_concurrency": safe_c,
                "latency_admission_capacity": int(decision.get("latency_admission_capacity", 0)),
                "service_time_bound_s": float(decision.get("service_time_bound_s", 0.0)),
                "backend_workload_s": max(0.0, self.server_available_at - self.t),
                "credits": credits,
                "arrival_rate": new_arrivals_this_tick / max(c.dt, 1e-9),
                "admission_rate": credits / max(c.dt, 1e-9),
                "cpu_pct": self.telemetry.cpu,
                "rss_mb": self.telemetry.rss_mb,
                "cbf_headroom": max(0, safe_c - active_now),
                "cbf_filtered": int(decision["cbf_filtered"]),
                "safe_absorption": safe_absorption,
                "mpc_objective": decision["mpc_objective"],
                "safety_violation": int(excess > 0),
            }
            self.history.append(record)

            # Layer 1: expose the exact same per-tick research record as a
            # structured event. Kafka is intentionally not involved here.
            self.last_event = build_event(
                record,
                simulation_id=f"tatkal_x_{self.generation}",
                controller_mode=c.controller_mode,
            )
            self.kafka_producer.enabled = bool(c.kafka_enabled)
            self.kafka_producer.publish(self.last_event)

            # Natural terminal conditions only.  The system stops when the finite
            # demand has been fully resolved, or when the finite inventory has sold
            # out and no in-flight/retry work remains.  max_simulation_steps is a
            # defensive programming guard, never a control constraint.
            terminal_pending = max(0, self.attempts - self.activated)
            demand_resolved = (self.initial_attempts >= c.burst_size and
                               terminal_pending == 0 and
                               not self.active and
                               not self.retry_schedule)
            inventory_resolved = (self.inventory == 0 and
                                  not self.active and
                                  not self.retry_schedule)
            if demand_resolved or inventory_resolved:
                self.running = False
                self.termination_reason = "inventory_sold_out" if inventory_resolved else "demand_resolved"
                self.events.append({"type": "SIMULATION_COMPLETE", "reason": self.termination_reason, "t": round(self.t, 3)})

    def _next_unactivated_user(self) -> Optional[int]:
        # Search only the currently generated demand. Retries reuse the same id,
        # so they can be reactivated without creating fictitious new users.
        for uid in range(min(self.initial_attempts, self.cfg.burst_size)):
            if self.user_status.get(uid) in ("WAITING", "RETRYING"):
                return uid
        return None

    def run_to_completion(self, max_steps: Optional[int] = None) -> Dict[str, Any]:
        """Run deterministically until natural completion.

        ``max_simulation_steps`` is only a defensive runaway-loop guard; it is
        not visible to the controller and does not enter any metric definition.
        """
        steps = int(max_steps or self.cfg.max_simulation_steps)
        self.running = True
        for _ in range(steps):
            if not self.running:
                break
            self.tick()
        if self.running:
            self.termination_reason = "max_steps_guard"
            self.events.append({"type": "MAX_STEPS_GUARD", "t": round(self.t, 3), "steps": steps})
        self.running = False
        return self.snapshot()

    def snapshot(self) -> Dict[str, Any]:
        with self.lock:
            h = list(self.history)
            p95 = float(np.percentile(self.latencies, 95)) if self.latencies else 0.0
            p99 = float(np.percentile(self.latencies, 99)) if self.latencies else 0.0
            mean = float(np.mean(self.latencies)) if self.latencies else 0.0
            attempts = max(1, self.attempts)
            mode = self.cfg.controller_mode
            c = self.cfg
            safe_current = h[-1]["safe_absorption"] if (h and mode == "mpc_cbf") else None

            # Observed outcomes are based on the complete natural run, not an
            # arbitrary cutoff. Finite inventory conservation guarantees
            # successful_bookings <= initial inventory.
            successful_bookings = int(self.completed)
            burst_absorption_rate = successful_bookings / max(1, c.burst_size)
            assert 0.0 <= self.inventory <= c.inventory
            assert successful_bookings <= c.inventory
            assert successful_bookings <= c.burst_size + self.retry_attempts
            assert 0.0 <= burst_absorption_rate <= 1.0 + 1e-12

            timely_success_rate = successful_bookings / max(1, self.activated)
            t_run = max(self.t, c.dt)
            goodput = successful_bookings / t_run
            return {
                "config": asdict(self.cfg),
                "running": self.running,
                "error": self.error,
                "t": self.t,
                "final_time_s": self.t,
                "termination_reason": self.termination_reason,
                "mode": mode,
                "algorithm_enabled": mode == "mpc_cbf",
                "attempts": self.attempts,
                "initial_attempts": self.initial_attempts,
                "retry_attempts": self.retry_attempts,
                "activated": self.activated,
                "completed": self.completed,
                "successful_bookings": successful_bookings,
                "failed": self.failed,
                "payment_failures": self.payment_failures,
                "retry_events": self.retry_events,
                "pending": max(0, self.attempts - self.activated),
                "inventory": self.inventory,
                "final_inventory": self.inventory,
                "initial_inventory": c.inventory,
                "burst_absorption_rate": burst_absorption_rate,
                "goodput_per_s": goodput,
                "time_to_sellout_s": self.sellout_time_s,
                "timely_success_rate": timely_success_rate,
                "activation_rate": self.activated / attempts,
                "booking_success_rate": self.completed / max(1, self.activated),
                "unactivated": max(0, self.attempts - self.activated),
                "active": len(self.active),
                "peak_active": self.peak_active,
                "mean_latency": mean,
                "p95_latency": p95,
                "p99_latency": p99,
                "p95_latency_s": p95,
                "p99_latency_s": p99,
                "safety_violations": self.safety_violations,
                "max_safe_violation": self.max_safe_violation,
                "cbf_filter_events": self.cbf_filter_events,
                "retry_amplification": self.attempts / max(1, self.initial_attempts),
                "retry_attempt_rate": self.retry_attempts / max(1, self.initial_attempts),
                "latency_slo_violation_rate": self.latency_slo_violations / max(1, len(self.latencies)),
                "max_end_to_end_latency": self.max_end_to_end_latency,
                "backlog_area_per_s": self.backlog_area / t_run,
                "max_pressure": self.max_pressure,
                "safe_absorption_current": safe_current,
                "safe_absorption_start": (h[0]["safe_absorption"] if (h and mode == "mpc_cbf") else None),
                "safe_absorption_min": (min(x["safe_absorption"] for x in h if x["safe_absorption"] is not None) if mode == "mpc_cbf" and any(x["safe_absorption"] is not None for x in h) else None),
                "safe_absorption_max": (max(x["safe_absorption"] for x in h if x["safe_absorption"] is not None) if mode == "mpc_cbf" and any(x["safe_absorption"] is not None for x in h) else None),
                "service_rate": h[-1]["service_rate"] if h else self.cfg.service_capacity_per_s,
                "robust_service_rate": h[-1]["robust_service_rate"] if h else self.cfg.service_capacity_per_s,
                "arrival_rate": h[-1]["arrival_rate"] if h else 0.0,
                "admission_rate": h[-1]["admission_rate"] if h else 0.0,
                "safe_concurrency": h[-1]["safe_concurrency"] if h else 0,
                "latency_admission_capacity": h[-1].get("latency_admission_capacity", 0) if h else 0,
                "service_time_bound_s": h[-1].get("service_time_bound_s", 0.0) if h else 0.0,
                "backend_workload_s": h[-1].get("backend_workload_s", 0.0) if h else 0.0,
                "cbf_headroom": h[-1]["cbf_headroom"] if h else 0,
                "credits": h[-1]["credits"] if h else 0,
                "mpc_objective": h[-1]["mpc_objective"] if h else 0.0,
                "controller_compute_ms_mean": self.total_controller_ms / max(1, self.controller_calls),
                "cpu": self.telemetry.cpu,
                "rss_mb": self.telemetry.rss_mb,
                "tick_ms": self.telemetry.tick_ms,
                "history": h[-600:],
                "events": list(self.events)[-80:],
                "last_event": self.last_event,
                "kafka": self.kafka_producer.status(),
                "user_view": self.user_view(),
                "profiles": self.profile_counts(),
                "seat_view": self.seat_view(),
            }

    def seat_view(self) -> List[Dict[str, Any]]:
        # Visual inventory map: every displayed seat corresponds to a real
        # finite-inventory slot. Slot allocation is randomized by _assign_free_slot.
        visible = min(self.cfg.inventory, 120)
        active_by_slot = {int(tx["slot"]): tx for tx in self.active if tx.get("slot") is not None}
        seats: List[Dict[str, Any]] = []
        slot_labels = ("A", "B", "C", "D", "E", "F")
        for slot in range(visible):
            tx = active_by_slot.get(slot)
            row = slot // 6 + 1
            col = slot % 6
            base = {"slot": slot, "label": f"{row}{slot_labels[col]}"}
            if slot in self.booked_slots:
                info = self.booked_info.get(slot, {})
                base.update({
                    "status": "BOOKED",
                    "user_id": (int(info["user_id"]) + 1) if "user_id" in info else None,
                    "profile": info.get("profile"),
                    "network_label": NETWORK_LABELS.get(info.get("profile"), None),
                    "retries": info.get("retries", 0),
                    "network_rtt_s": info.get("network_rtt_s"),
                    "latency_s": info.get("latency_s"),
                })
            elif tx is not None:
                uid = tx["id"]
                base.update({
                    "status": "PROCESSING",
                    "user_id": int(uid) + 1,
                    "profile": tx["profile"],
                    "network_label": NETWORK_LABELS.get(tx["profile"]),
                    "retries": int(tx["retries"]),
                    "network_rtt_s": round(self.workload.network_rtt(uid), 3),
                    "latency_s": round(self.t - tx["start"], 3),
                })
            else:
                base.update({
                    "status": "AVAILABLE",
                    "user_id": None, "profile": None, "network_label": None,
                    "retries": None, "network_rtt_s": None, "latency_s": None,
                })
            seats.append(base)
        return seats

    def profile_counts(self) -> Dict[str, int]:
        counts = {k: 0 for k in USER_PROFILES}
        for uid, status in self.user_status.items():
            if status in ("PROCESSING", "BOOKED", "PAYMENT_FAILED", "RETRYING", "RETRY_SCHEDULED"):
                counts[self.workload.profile(uid)] += 1
        return counts

    def user_view(self) -> List[Dict[str, Any]]:
        # This is a user visualization, not a seat/coach visualization.
        ids = self.visible_user_ids
        rows = []
        for uid in ids:
            rows.append({
                "id": uid + 1,
                "profile": self.workload.profile(uid),
                "status": self.user_status.get(uid, "NOT_ARRIVED"),
            })
        # Add active/recently relevant users if they are outside the initial view.
        active_ids = [tx["id"] for tx in self.active]
        for uid in active_ids:
            if uid + 1 not in {r["id"] for r in rows}:
                rows.append({"id": uid + 1, "profile": self.workload.profile(uid), "status": self.user_status.get(uid, "PROCESSING")})
        return rows[: max(self.cfg.visible_users, 20)]

    def csv(self) -> str:
        out = io.StringIO()
        rows = list(self.history)
        if not rows:
            return ""
        cols = list(rows[0].keys())
        writer = csv.DictWriter(out, fieldnames=cols)
        writer.writeheader()
        writer.writerows(rows)
        return out.getvalue()


def run_single(cfg: Config) -> Dict[str, Any]:
    e = Engine(cfg)
    return e.run_to_completion()
