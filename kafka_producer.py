"""Kafka producer for the TATKAL-X live event stream.

This module transports the already-created event from event_schema.py.
It does not calculate simulation metrics or alter controller behaviour.
"""

from __future__ import annotations

import json
import os
import threading
import time
from typing import Any, Dict, Optional


class KafkaEventProducer:
    """Failure-tolerant Kafka producer for TATKAL-X tick events."""

    def __init__(
        self,
        enabled: bool = False,
        bootstrap_servers: Optional[str] = None,
        topic: Optional[str] = None,
    ) -> None:
        self.enabled = bool(enabled)

        self.bootstrap_servers = bootstrap_servers or os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            "localhost:9092",
        )

        self.topic = topic or os.getenv(
            "KAFKA_TOPIC",
            "tatkal-x-events",
        )

        self._producer = None
        self._lock = threading.RLock()

        self._last_error: Optional[str] = None
        self._last_success_at: Optional[float] = None

        self._published = 0
        self._last_attempt_at = 0.0
        self._retry_interval_s = 3.0

    def _connect(self) -> bool:
        if not self.enabled:
            return False

        with self._lock:
            if self._producer is not None:
                return True

            now = time.monotonic()

            if now - self._last_attempt_at < self._retry_interval_s:
                return False

            self._last_attempt_at = now

            try:
                from kafka import KafkaProducer
                from kafka.admin import KafkaAdminClient, NewTopic

                # Create the topic automatically if it does not exist.
                admin = KafkaAdminClient(
                    bootstrap_servers=self.bootstrap_servers,
                    client_id="tatkal-x-admin",
                    request_timeout_ms=1500,
                )

                try:
                    existing = admin.list_topics()

                    if self.topic not in existing:
                        admin.create_topics(
                            new_topics=[
                                NewTopic(
                                    name=self.topic,
                                    num_partitions=1,
                                    replication_factor=1,
                                )
                            ],
                            validate_only=False,
                        )
                finally:
                    admin.close()

                self._producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,

                    key_serializer=lambda value:
                        value.encode("utf-8"),

                    value_serializer=lambda value:
                        json.dumps(
                            value,
                            separators=(",", ":"),
                            allow_nan=False,
                        ).encode("utf-8"),

                    acks="all",
                    retries=3,
                    linger_ms=5,
                    request_timeout_ms=1500,
                    api_version_auto_timeout_ms=1500,
                )

                self._last_error = None

                return True

            except Exception as exc:
                self._producer = None
                self._last_error = (
                    f"{type(exc).__name__}: {exc}"
                )

                return False

    def publish(self, event: Dict[str, Any]) -> bool:
        """Publish one TATKAL-X event to Kafka."""

        if not self.enabled:
            return False

        if not self._connect():
            return False

        try:
            simulation_id = str(
                event.get(
                    "simulation_id",
                    "tatkal_x",
                )
            )

            future = self._producer.send(
                self.topic,
                key=simulation_id,
                value=event,
            )

            # Wait for broker acknowledgement.
            future.get(timeout=2.0)

            with self._lock:
                self._published += 1
                self._last_success_at = time.time()
                self._last_error = None

            return True

        except Exception as exc:
            with self._lock:
                self._last_error = (
                    f"{type(exc).__name__}: {exc}"
                )

                producer = self._producer
                self._producer = None

            try:
                if producer is not None:
                    producer.close(timeout=0.5)
            except Exception:
                pass

            return False

    def status(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "enabled": self.enabled,
                "connected": self._producer is not None,
                "bootstrap_servers": self.bootstrap_servers,
                "topic": self.topic,
                "published": self._published,
                "last_success_at": self._last_success_at,
                "last_error": self._last_error,
            }

    def close(self) -> None:
        with self._lock:
            producer = self._producer
            self._producer = None

        if producer is not None:
            try:
                producer.flush(timeout=2.0)
                producer.close(timeout=2.0)
            except Exception:
                pass