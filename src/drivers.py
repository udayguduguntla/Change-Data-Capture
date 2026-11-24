"""Neo4j driver pool, keyed by instance."""

from __future__ import annotations

import threading
from typing import Any

from .errors import ConfigError

import logging

# Neo4j warns when a query mentions a label, relationship type or property that does not
# exist in the database YET (01N50 / 01N51 / 01N52). That is normal for an empty or new
# tenant database: the embedder and the pipeline ask about every label, and the data simply
# has not arrived. These warnings carry no information, so they are dropped; every other
# Neo4j notification (deprecations, performance hints, errors) is still logged.
_NOT_YET = ("01N50", "01N51", "01N52")


class _DropNotYetExisting(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        return not any(f"gql_status='{c}'" in msg for c in _NOT_YET)


logging.getLogger("neo4j.notifications").addFilter(_DropNotYetExisting())


class DriverPool:
    def __init__(self, neo4j_config: dict):
        self._cfg = neo4j_config
        self._instances = neo4j_config.get("instances") or {}
        if not self._instances:
            raise ConfigError("connections.yaml: neo4j.instances is empty")
        self._default = neo4j_config.get("default_instance") or next(iter(self._instances))
        if self._default not in self._instances:
            raise ConfigError(
                f"neo4j.default_instance {self._default!r} is not present in neo4j.instances"
            )
        self._drivers: dict[str, Any] = {}
        self._lock = threading.Lock()

    @property
    def default_instance(self) -> str:
        return self._default

    @property
    def metadata_database(self) -> str:
        # Fallback to neo4j for community edition
        return "neo4j"


    def instance_names(self) -> list[str]:
        return list(self._instances)

    def get(self, instance: str | None = None):
        name = instance or self._default
        if name not in self._instances:
            raise ConfigError(f"unknown Neo4j instance {name!r}")
        with self._lock:
            if name not in self._drivers:
                from neo4j import GraphDatabase          # imported lazily
                c = self._instances[name]
                self._drivers[name] = GraphDatabase.driver(
                    c["uri"],
                    auth=(c["user"], c["password"]),
                    max_connection_pool_size=c.get("max_connection_pool_size", 50),
                    connection_acquisition_timeout=c.get("connection_acquisition_timeout", 60),
                )
            return self._drivers[name]

    def close(self) -> None:
        with self._lock:
            for d in self._drivers.values():
                try:
                    d.close()
                except Exception:                        # pragma: no cover
                    pass
            self._drivers.clear()
