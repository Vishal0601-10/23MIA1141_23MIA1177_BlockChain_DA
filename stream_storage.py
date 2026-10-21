from __future__ import annotations

import json
import os
import signal
import sqlite3
from typing import Any, Dict

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

GROUP_ID = os.getenv(
    "KAFKA_STORAGE_GROUP_ID",
    "tatkal-x-persistent-storage",
)

DB_PATH = os.getenv(
    "TATKAL_X_DB",
    "tatkal_x_stream.db",
)


# =========================================================
# SQLite Storage
# =========================================================

class TATKALXStorage:

    def __init__(self, db_path: str) -> None:

        self.db_path = db_path

        self.connection = sqlite3.connect(
            self.db_path,
            check_same_thread=False,
        )

        self._create_tables()

        self.stored_events = 0

    # -----------------------------------------------------
    # Database schema
    # -----------------------------------------------------

    def _create_tables(self) -> None:

        cursor = self.connection.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS tatkal_x_events (

                id INTEGER PRIMARY KEY AUTOINCREMENT,

                event_type TEXT NOT NULL,

                simulation_id TEXT,

                controller_mode TEXT,

                t REAL,

                pressure REAL,

                active INTEGER,

                inventory INTEGER,

                attempts INTEGER,

                initial_attempts INTEGER,

                retry_attempts INTEGER,

                activated INTEGER,

                completed INTEGER,

                pending INTEGER,

                service_rate REAL,

                robust_service_rate REAL,

                safe_concurrency INTEGER,

                latency_admission_capacity INTEGER,

                service_time_bound_s REAL,

                backend_workload_s REAL,

                credits INTEGER,

                arrival_rate REAL,

                admission_rate REAL,

                cpu_pct REAL,

                rss_mb REAL,

                cbf_headroom INTEGER,

                cbf_filtered INTEGER,

                safe_absorption REAL,

                mpc_objective REAL,

                safety_violation INTEGER,

                raw_event TEXT NOT NULL,

                stored_at TEXT DEFAULT CURRENT_TIMESTAMP

            )
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_simulation_id
            ON tatkal_x_events(simulation_id)
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_event_time
            ON tatkal_x_events(t)
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_controller_mode
            ON tatkal_x_events(controller_mode)
            """
        )

        self.connection.commit()

    # -----------------------------------------------------
    # Store one event
    # -----------------------------------------------------

    def store(self, event: Dict[str, Any]) -> None:

        cursor = self.connection.cursor()

        cursor.execute(
            """
            INSERT INTO tatkal_x_events (

                event_type,
                simulation_id,
                controller_mode,
                t,
                pressure,
                active,
                inventory,
                attempts,
                initial_attempts,
                retry_attempts,
                activated,
                completed,
                pending,
                service_rate,
                robust_service_rate,
                safe_concurrency,
                latency_admission_capacity,
                service_time_bound_s,
                backend_workload_s,
                credits,
                arrival_rate,
                admission_rate,
                cpu_pct,
                rss_mb,
                cbf_headroom,
                cbf_filtered,
                safe_absorption,
                mpc_objective,
                safety_violation,
                raw_event

            )

            VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,

            (
                event.get("event_type"),
                event.get("simulation_id"),
                event.get("controller_mode"),
                event.get("t"),
                event.get("pressure"),
                event.get("active"),
                event.get("inventory"),
                event.get("attempts"),
                event.get("initial_attempts"),
                event.get("retry_attempts"),
                event.get("activated"),
                event.get("completed"),
                event.get("pending"),
                event.get("service_rate"),
                event.get("robust_service_rate"),
                event.get("safe_concurrency"),
                event.get("latency_admission_capacity"),
                event.get("service_time_bound_s"),
                event.get("backend_workload_s"),
                event.get("credits"),
                event.get("arrival_rate"),
                event.get("admission_rate"),
                event.get("cpu_pct"),
                event.get("rss_mb"),
                event.get("cbf_headroom"),
                event.get("cbf_filtered"),
                event.get("safe_absorption"),
                event.get("mpc_objective"),
                event.get("safety_violation"),
                json.dumps(
                    event,
                    separators=(",", ":"),
                    allow_nan=False,
                ),
            ),
        )

        self.connection.commit()

        self.stored_events += 1

    # -----------------------------------------------------
    # Close
    # -----------------------------------------------------

    def close(self) -> None:

        self.connection.commit()
        self.connection.close()


# =========================================================
# Consumer
# =========================================================

running = True


def stop_storage(
    signum: int,
    frame: Any,
) -> None:

    global running

    running = False

    print(
        "\nStopping TATKAL-X persistent storage..."
    )


def main() -> None:

    global running

    signal.signal(
        signal.SIGINT,
        stop_storage,
    )

    signal.signal(
        signal.SIGTERM,
        stop_storage,
    )

    print(
        "Starting TATKAL-X persistent storage..."
    )

    print(
        f"Kafka       : {BOOTSTRAP_SERVERS}"
    )

    print(
        f"Topic       : {TOPIC}"
    )

    print(
        f"Consumer ID : {GROUP_ID}"
    )

    print(
        f"Database    : {DB_PATH}"
    )

    print()

    storage = TATKALXStorage(
        DB_PATH
    )

    consumer = KafkaConsumer(
        TOPIC,

        bootstrap_servers=
            BOOTSTRAP_SERVERS,

        group_id=
            GROUP_ID,

        auto_offset_reset="earliest",

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
        "Persistent storage is ready."
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

                        storage.store(
                            event
                        )

                        print(
                            f"[STORED] "
                            f"simulation="
                            f"{event.get('simulation_id')} "
                            f"t="
                            f"{event.get('t')} "
                            f"total="
                            f"{storage.stored_events}"
                        )

                    except Exception as exc:

                        print(
                            "[STORAGE ERROR] "
                            f"{type(exc).__name__}: "
                            f"{exc}"
                        )

    finally:

        consumer.close()

        storage.close()

        print(
            f"\nStored events: "
            f"{storage.stored_events}"
        )

        print(
            f"Database: {DB_PATH}"
        )

        print(
            "Persistent storage stopped."
        )


if __name__ == "__main__":
    main()