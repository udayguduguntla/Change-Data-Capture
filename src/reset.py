"""``python main.py reset``: remove everything the pipeline has written, to start clean.

Per tenant database:
  * Neo4j Enterprise: DROP DATABASE (the next `run` / `backfill` re-creates and provisions it)
  * otherwise, or when the drop is not allowed: delete every node in batches, then drop
    every constraint and index in that database
  * delete its PipelineState record, so provisioning runs in full next time

On Community edition every tenant shares one database, so `--org` cannot separate them:
reset refuses `--org` there and only `--all` is allowed.
"""
from __future__ import annotations

import logging

from .graph_state import LABEL as STATE_LABEL

log = logging.getLogger(__name__)
BATCH = 10000


def _count(session) -> int:
    rec = session.run("MATCH (n) RETURN count(n) AS c").single()
    return int(rec["c"]) if rec else 0


def _database_exists(driver, name: str) -> bool | None:
    """True/False when the server can tell (Enterprise system database), else None."""
    try:
        with driver.session(database="system") as s:
            rec = s.run("SHOW DATABASES YIELD name WHERE name = $n RETURN count(*) AS c", n=name).single()
            return bool(rec and rec["c"])
    except Exception:                                   # noqa: BLE001 - Community / no rights
        return None


def _wipe_in_place(driver, database: str) -> dict:
    """Batched delete of every node, then every constraint and index."""
    with driver.session(database=database) as s:
        before = _count(s)
        while True:
            rec = s.run(f"MATCH (n) WITH n LIMIT {BATCH} DETACH DELETE n RETURN count(*) AS c").single()
            if not rec or rec["c"] == 0:
                break
        constraints = [r["name"] for r in s.run("SHOW CONSTRAINTS YIELD name RETURN name")]
        for name in constraints:
            s.run(f"DROP CONSTRAINT `{name}` IF EXISTS").consume()
        indexes = [r["name"] for r in s.run(
            "SHOW INDEXES YIELD name, type WHERE type <> 'LOOKUP' RETURN name")]
        for name in indexes:
            s.run(f"DROP INDEX `{name}` IF EXISTS").consume()
    return {"nodes_deleted": before, "constraints_dropped": len(constraints),
            "indexes_dropped": len(indexes)}


def reset_database(pool, database: str, instance: str | None = None, dry_run: bool = False) -> dict:
    driver = pool.get(instance)
    exists = _database_exists(driver, database)
    report: dict = {"database": database}
    if exists is False:
        report["result"] = "not present"
    else:
        try:
            with driver.session(database=database) as s:
                report["nodes"] = _count(s)
        except Exception as exc:                        # noqa: BLE001
            report["nodes"] = f"unreadable: {exc}"
        if not dry_run:
            dropped = False
            if exists and database not in ("neo4j", "system"):
                try:
                    with driver.session(database="system") as s:
                        s.run(f"DROP DATABASE `{database}` IF EXISTS WAIT").consume()
                    dropped = True
                    report["result"] = "database dropped"
                except Exception as exc:                # noqa: BLE001 - fall back to in-place
                    log.warning("DROP DATABASE %s failed (%s); deleting in place", database, exc)
            if not dropped:
                report.update(_wipe_in_place(driver, database))
                report["result"] = "wiped in place"
    if not dry_run:
        meta = getattr(pool, "metadata_database", "neo4j")
        try:
            with driver.session(database=meta) as s:
                s.run(f"MATCH (p:{STATE_LABEL} {{database: $db}}) DETACH DELETE p", db=database).consume()
        except Exception as exc:                        # noqa: BLE001
            log.warning("could not clear pipeline state for %s: %s", database, exc)
    return report


def shared_database(pool, orgs) -> bool:
    """True when tenants share one database: Community edition cannot create more."""
    try:
        with pool.get(None).session(database="system") as s:
            rec = s.run("CALL dbms.components() YIELD edition RETURN edition").single()
            return bool(rec) and str(rec["edition"]).lower() == "community"
    except Exception:                                   # noqa: BLE001 - be safe: assume shared
        return True
