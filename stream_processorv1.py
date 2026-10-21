
from __future__ import annotations

import json
import os
import signal
import statistics
from collections import deque
from typing import Any, Deque, Dict, Optional

from kafka import KafkaConsumer


# =========================================================
# Configuration
# =========================================================

BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "tatkal-x-events",
)

WINDOW_SIZE = int(
    os.getenv("STREAM_WINDOW_SIZE", "20")
)

GROUP_ID = os.getenv(
    "KAFKA_GROUP_ID",
    "tatkal-x-stream-processor",
)


# =========================================================
# TATKAL-X Stream Processor
# =========================================================

class TATKALXStreamProcessor:

    def __init__(self, window_size: int = 20) -> None:

        self.window_size = max(
            2,
            int(window_size)
        )

        self.window: Deque[
            Dict[str, Any]
        ] = deque(
            maxlen=self.window_size
        )

        self.processed_events = 0

        self.last_event: Optional[
            Dict[str, Any]
        ] = None

        self.last_simulation_id = None

        self.last_t: Optional[float] = None

    # =====================================================
    # Numeric helpers
    # =====================================================

    @staticmethod
    def _number(
        value: Any,
        default: float = 0.0,
    ) -> float:

        try:

            if value is None:
                return default

            value = float(value)

            if value != value:
                return default

            return value

        except (
            TypeError,
            ValueError,
        ):
            return default

    # =====================================================
    # Extract field values
    # =====================================================

    @classmethod
    def _values(
        cls,
        window: Deque[Dict[str, Any]],
        field: str,
    ) -> list[float]:

        values = []

        for event in window:

            value = event.get(field)

            if value is None:
                continue

            try:

                value = float(value)

                if value == value:
                    values.append(value)

            except (
                TypeError,
                ValueError,
            ):
                continue

        return values

    @classmethod
    def _mean(
        cls,
        window: Deque[Dict[str, Any]],
        field: str,
    ) -> float:

        values = cls._values(
            window,
            field,
        )

        if not values:
            return 0.0

        return float(
            statistics.fmean(values)
        )

    @classmethod
    def _maximum(
        cls,
        window: Deque[Dict[str, Any]],
        field: str,
    ) -> float:

        values = cls._values(
            window,
            field,
        )

        return max(values) if values else 0.0

    # =====================================================
    # Calculate delta rate
    # =====================================================

    def _delta_rate(
        self,
        current: Dict[str, Any],
        field: str,
    ) -> float:

        if self.last_event is None:
            return 0.0

        current_value = self._number(
            current.get(field)
        )

        previous_value = self._number(
            self.last_event.get(field)
        )

        current_t = self._number(
            current.get("t")
        )

        previous_t = self._number(
            self.last_event.get("t")
        )

        dt = current_t - previous_t

        if dt <= 0:
            return 0.0

        delta = current_value - previous_value

        # Cumulative counters should not normally decrease.
        # If a new simulation starts, the processor will detect
        # the simulation change and avoid producing a negative rate.
        if delta < 0:
            return 0.0

        return delta / dt

    # =====================================================
    # Process one Kafka event
    # =====================================================

    def process(
        self,
        event: Dict[str, Any],
    ) -> Dict[str, Any]:

        simulation_id = event.get(
            "simulation_id"
        )

        # -------------------------------------------------
        # Detect a new simulation
        # -------------------------------------------------

        new_simulation = (
            self.last_simulation_id is not None
            and simulation_id != self.last_simulation_id
        )

        if new_simulation:

            self.window.clear()
            self.last_event = None
            self.last_t = None

        # -------------------------------------------------
        # Delta-based rates
        # -------------------------------------------------

        completion_rate = self._delta_rate(
            event,
            "completed",
        )

        retry_rate = self._delta_rate(
            event,
            "retry_attempts",
        )

        attempt_rate = self._delta_rate(
            event,
            "attempts",
        )

        activation_rate = self._delta_rate(
            event,
            "activated",
        )

        # -------------------------------------------------
        # Add current event to rolling window
        # -------------------------------------------------

        self.window.append(event)

        self.processed_events += 1

        self.last_simulation_id = simulation_id

        self.last_t = self._number(
            event.get("t")
        )

        # -------------------------------------------------
        # Rolling state metrics
        # -------------------------------------------------

        rolling_pressure = self._mean(
            self.window,
            "pressure",
        )

        rolling_active = self._mean(
            self.window,
            "active",
        )

        rolling_arrival_rate = self._mean(
            self.window,
            "arrival_rate",
        )

        rolling_admission_rate = self._mean(
            self.window,
            "admission_rate",
        )

        rolling_cbf_headroom = self._mean(
            self.window,
            "cbf_headroom",
        )

        rolling_backend_workload = self._mean(
            self.window,
            "backend_workload_s",
        )

        rolling_cpu = self._mean(
            self.window,
            "cpu_pct",
        )

        rolling_mpc_objective = self._mean(
            self.window,
            "mpc_objective",
        )

        rolling_safe_concurrency = self._mean(
            self.window,
            "safe_concurrency",
        )

        # -------------------------------------------------
        # Rolling extremes
        # -------------------------------------------------

        max_pressure = self._maximum(
            self.window,
            "pressure",
        )

        max_active = self._maximum(
            self.window,
            "active",
        )

        max_backend_workload = self._maximum(
            self.window,
            "backend_workload_s",
        )

        # -------------------------------------------------
        # Safety analytics
        # -------------------------------------------------

        safety_violations = sum(
            int(
                bool(
                    e.get(
                        "safety_violation",
                        0,
                    )
                )
            )
            for e in self.window
        )

        cbf_filtered_events = sum(
            int(
                bool(
                    e.get(
                        "cbf_filtered",
                        0,
                    )
                )
            )
            for e in self.window
        )

        # -------------------------------------------------
        # Current state
        # -------------------------------------------------

        active = self._number(
            event.get("active")
        )

        safe_concurrency = self._number(
            event.get("safe_concurrency")
        )

        cbf_headroom = self._number(
            event.get("cbf_headroom")
        )

        pressure = self._number(
            event.get("pressure")
        )

        safety_violation = int(
            bool(
                event.get(
                    "safety_violation",
                    0,
                )
            )
        )

        # -------------------------------------------------
        # Safety / anomaly signals
        # -------------------------------------------------

        concurrency_pressure = (
            safe_concurrency > 0
            and active > safe_concurrency
        )

        high_pressure = (
            rolling_pressure > 0
            and pressure > rolling_pressure * 1.5
        )

        low_cbf_headroom = (
            cbf_headroom <= 0
        )

        anomaly = (
            concurrency_pressure
            or high_pressure
            or safety_violation
        )

        # -------------------------------------------------
        # Retry amplification
        # -------------------------------------------------

        attempts = self._number(
            event.get("attempts")
        )

        initial_attempts = self._number(
            event.get("initial_attempts")
        )

        retry_amplification = (
            attempts / initial_attempts
            if initial_attempts > 0
            else 1.0
        )

        # -------------------------------------------------
        # Processed streaming event
        # -------------------------------------------------

        processed = {

            "event_type":
                "tatkal_x_stream_metrics",

            "simulation_id":
                simulation_id,

            "controller_mode":
                event.get(
                    "controller_mode"
                ),

            "source_t":
                event.get("t"),

            # ---------------------------------------------
            # Window
            # ---------------------------------------------

            "window_size":
                len(self.window),

            "processed_events":
                self.processed_events,

            # ---------------------------------------------
            # Rolling state metrics
            # ---------------------------------------------

            "rolling_pressure":
                rolling_pressure,

            "rolling_active":
                rolling_active,

            "rolling_arrival_rate":
                rolling_arrival_rate,

            "rolling_admission_rate":
                rolling_admission_rate,

            "rolling_cbf_headroom":
                rolling_cbf_headroom,

            "rolling_safe_concurrency":
                rolling_safe_concurrency,

            "rolling_backend_workload_s":
                rolling_backend_workload,

            "rolling_cpu_pct":
                rolling_cpu,

            "rolling_mpc_objective":
                rolling_mpc_objective,

            # ---------------------------------------------
            # TRUE DERIVED RATES
            # ---------------------------------------------

            "completion_rate_per_s":
                completion_rate,

            "retry_rate_per_s":
                retry_rate,

            "attempt_rate_per_s":
                attempt_rate,

            "activation_rate_per_s":
                activation_rate,

            # ---------------------------------------------
            # Extremes
            # ---------------------------------------------

            "max_pressure_window":
                max_pressure,

            "max_active_window":
                max_active,

            "max_backend_workload_s":
                max_backend_workload,

            # ---------------------------------------------
            # Safety
            # ---------------------------------------------

            "safety_violation_count":
                safety_violations,

            "cbf_filtered_count":
                cbf_filtered_events,

            # ---------------------------------------------
            # Current state
            # ---------------------------------------------

            "active":
                active,

            "safe_concurrency":
                safe_concurrency,

            "cbf_headroom":
                cbf_headroom,

            "pressure":
                pressure,

            # ---------------------------------------------
            # Derived forensic / operational indicators
            # ---------------------------------------------

            "retry_amplification":
                retry_amplification,

            "concurrency_pressure":
                bool(
                    concurrency_pressure
                ),

            "high_pressure":
                bool(
                    high_pressure
                ),

            "low_cbf_headroom":
                bool(
                    low_cbf_headroom
                ),

            "anomaly":
                bool(anomaly),
        }

        # Save AFTER calculating deltas.
        self.last_event = event

        return processed


# =========================================================
# Console display
# =========================================================

def print_metrics(
    metrics: Dict[str, Any],
) -> None:

    print(
        "\n"
        "============================================================\n"
        " TATKAL-X REAL-TIME STREAM METRICS\n"
        "============================================================\n"
        f" Simulation       : {metrics['simulation_id']}\n"
        f" Controller       : {metrics['controller_mode']}\n"
        f" Source t         : {metrics['source_t']}\n"
        f" Window           : {metrics['window_size']}\n"
        "------------------------------------------------------------\n"
        f" Rolling Pressure : "
        f"{metrics['rolling_pressure']:.4f}\n"
        f" Rolling Active   : "
        f"{metrics['rolling_active']:.2f}\n"
        f" Arrival Rate     : "
        f"{metrics['rolling_arrival_rate']:.2f}/s\n"
        f" Admission Rate   : "
        f"{metrics['rolling_admission_rate']:.2f}/s\n"
        f" Completion Rate  : "
        f"{metrics['completion_rate_per_s']:.2f}/s\n"
        f" Retry Rate       : "
        f"{metrics['retry_rate_per_s']:.2f}/s\n"
        f" Attempt Rate     : "
        f"{metrics['attempt_rate_per_s']:.2f}/s\n"
        f" Activation Rate  : "
        f"{metrics['activation_rate_per_s']:.2f}/s\n"
        "------------------------------------------------------------\n"
        f" Backend Workload : "
        f"{metrics['rolling_backend_workload_s']:.4f}s\n"
        f" Safe Concurrency : "
        f"{metrics['safe_concurrency']:.0f}\n"
        f" CBF Headroom     : "
        f"{metrics['cbf_headroom']:.0f}\n"
        f" Safety Violations: "
        f"{metrics['safety_violation_count']}\n"
        f" CBF Filtered     : "
        f"{metrics['cbf_filtered_count']}\n"
        "------------------------------------------------------------\n"
        f" Retry Amplif.    : "
        f"{metrics['retry_amplification']:.4f}x\n"
        f" Concurrency Risk : "
        f"{metrics['concurrency_pressure']}\n"
        f" High Pressure    : "
        f"{metrics['high_pressure']}\n"
        f" Low CBF Headroom : "
        f"{metrics['low_cbf_headroom']}\n"
        f" STREAM ANOMALY   : "
        f"{metrics['anomaly']}\n"
        "============================================================"
    )


# =========================================================
# Kafka consumer
# =========================================================

running = True


def stop_processor(
    signum: int,
    frame: Any,
) -> None:

    global running

    running = False

    print(
        "\nStopping TATKAL-X stream processor..."
    )


def main() -> None:

    global running

    signal.signal(
        signal.SIGINT,
        stop_processor,
    )

    signal.signal(
        signal.SIGTERM,
        stop_processor,
    )

    print(
        "Starting TATKAL-X stream processor..."
    )

    print(
        f"Kafka       : "
        f"{BOOTSTRAP_SERVERS}"
    )

    print(
        f"Topic       : "
        f"{TOPIC}"
    )

    print(
        f"Window      : "
        f"{WINDOW_SIZE}"
    )

    print(
        f"Consumer ID : "
        f"{GROUP_ID}"
    )

    print()

    processor = (
        TATKALXStreamProcessor(
            window_size=WINDOW_SIZE
        )
    )

    consumer = KafkaConsumer(
        TOPIC,

        bootstrap_servers=
            BOOTSTRAP_SERVERS,

        group_id=
            GROUP_ID,

        # Start from the newest events.
        # This avoids replaying an old simulation
        # every time the processor starts.
        auto_offset_reset="latest",

        enable_auto_commit=True,

        value_deserializer=
            lambda value:
                json.loads(
                    value.decode(
                        "utf-8"
                    )
                ),
    )

    print(
        "Connected to Kafka."
    )

    print(
        "Waiting for TATKAL-X events...\n"
    )

    try:

        while running:

            records = consumer.poll(
                timeout_ms=1000
            )

            for _, messages in records.items():

                for message in messages:

                    try:

                        event = message.value

                        metrics = (
                            processor.process(
                                event
                            )
                        )

                        print_metrics(
                            metrics
                        )

                    except Exception as exc:

                        print(
                            "[PROCESSING ERROR] "
                            f"{type(exc).__name__}: "
                            f"{exc}"
                        )

    finally:

        consumer.close()

        print(
            "\nProcessed events: "
            f"{processor.processed_events}"
        )

        print(
            "Stream processor stopped."
        )

if __name__ == "__main__":
    main()