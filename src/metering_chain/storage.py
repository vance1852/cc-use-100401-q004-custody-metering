"""计量监管链的 SQLite 模式和事务辅助。"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS meter_users (
    user_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    role TEXT NOT NULL CHECK(role IN ('operator','metrologist','officer','manager','auditor')),
    active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1)),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS metering_points (
    point_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    stage TEXT NOT NULL CHECK(stage IN
        ('wellhead','platform-separation','pipeline-inlet','pipeline-outlet',
         'floating-processing','storage-tank','offtake')),
    facility_id TEXT NOT NULL,
    well_group_id TEXT,
    created_by TEXT NOT NULL REFERENCES meter_users(user_id),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS chain_links (
    link_id TEXT PRIMARY KEY,
    from_point_id TEXT NOT NULL REFERENCES metering_points(point_id),
    to_point_id TEXT NOT NULL REFERENCES metering_points(point_id),
    loss_basis_points INTEGER NOT NULL CHECK(loss_basis_points BETWEEN 0 AND 1000),
    created_by TEXT NOT NULL REFERENCES meter_users(user_id),
    created_at TEXT NOT NULL,
    UNIQUE(from_point_id),
    CHECK(from_point_id <> to_point_id)
);

CREATE TABLE IF NOT EXISTS calibration_versions (
    version_id INTEGER PRIMARY KEY AUTOINCREMENT,
    point_id TEXT NOT NULL REFERENCES metering_points(point_id),
    version_no INTEGER NOT NULL,
    coefficient TEXT NOT NULL,
    note TEXT NOT NULL DEFAULT '',
    state TEXT NOT NULL DEFAULT 'draft' CHECK(state IN ('draft','active','retired')),
    created_by TEXT NOT NULL REFERENCES meter_users(user_id),
    created_at TEXT NOT NULL,
    activated_at TEXT,
    UNIQUE(point_id, version_no)
);

CREATE INDEX IF NOT EXISTS idx_calibration_active
ON calibration_versions(point_id, state);

CREATE TABLE IF NOT EXISTS crude_batches (
    batch_id TEXT PRIMARY KEY,
    wellhead_point_id TEXT NOT NULL REFERENCES metering_points(point_id),
    well_group_id TEXT NOT NULL,
    product TEXT NOT NULL DEFAULT 'crude-oil',
    state TEXT NOT NULL DEFAULT 'open' CHECK(state IN ('open','frozen')),
    revision INTEGER NOT NULL DEFAULT 1,
    created_by TEXT NOT NULL REFERENCES meter_users(user_id),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS settlements (
    settlement_id INTEGER PRIMARY KEY AUTOINCREMENT,
    point_id TEXT NOT NULL REFERENCES metering_points(point_id),
    batch_id TEXT NOT NULL REFERENCES crude_batches(batch_id),
    revision INTEGER NOT NULL,
    quantity_units TEXT NOT NULL,
    delta_units TEXT NOT NULL,
    reading_count INTEGER NOT NULL,
    state TEXT NOT NULL DEFAULT 'closed' CHECK(state IN ('closed','superseded')),
    supersedes_settlement_id INTEGER REFERENCES settlements(settlement_id),
    closed_by TEXT NOT NULL REFERENCES meter_users(user_id),
    closed_at TEXT NOT NULL,
    UNIQUE(point_id, batch_id, revision)
);

CREATE TABLE IF NOT EXISTS meter_readings (
    reading_id INTEGER PRIMARY KEY AUTOINCREMENT,
    point_id TEXT NOT NULL REFERENCES metering_points(point_id),
    batch_id TEXT NOT NULL REFERENCES crude_batches(batch_id),
    observed_at TEXT NOT NULL,
    raw_quantity TEXT NOT NULL,
    calibration_version_id INTEGER NOT NULL REFERENCES calibration_versions(version_id),
    coefficient TEXT NOT NULL,
    corrected_quantity TEXT NOT NULL,
    settlement_id INTEGER REFERENCES settlements(settlement_id),
    idempotency_key TEXT NOT NULL UNIQUE,
    uploaded_by TEXT NOT NULL REFERENCES meter_users(user_id),
    uploaded_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_readings_point_batch
ON meter_readings(point_id, batch_id, reading_id);

CREATE INDEX IF NOT EXISTS idx_readings_batch
ON meter_readings(batch_id, point_id);

CREATE TABLE IF NOT EXISTS liftings (
    voyage_id TEXT PRIMARY KEY,
    tank_point_id TEXT NOT NULL REFERENCES metering_points(point_id),
    vessel_name TEXT NOT NULL,
    created_by TEXT NOT NULL REFERENCES meter_users(user_id),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS handovers (
    handover_id INTEGER PRIMARY KEY AUTOINCREMENT,
    voyage_id TEXT NOT NULL REFERENCES liftings(voyage_id),
    revision INTEGER NOT NULL,
    quantity_units TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'draft' CHECK(state IN ('draft','signed','superseded')),
    calibration_version_id INTEGER REFERENCES calibration_versions(version_id),
    supersedes_handover_id INTEGER REFERENCES handovers(handover_id),
    created_by TEXT NOT NULL REFERENCES meter_users(user_id),
    created_at TEXT NOT NULL,
    signed_by TEXT REFERENCES meter_users(user_id),
    signed_at TEXT,
    UNIQUE(voyage_id, revision)
);

CREATE TABLE IF NOT EXISTS lifting_attributions (
    attribution_id INTEGER PRIMARY KEY AUTOINCREMENT,
    handover_id INTEGER NOT NULL REFERENCES handovers(handover_id),
    batch_id TEXT NOT NULL REFERENCES crude_batches(batch_id),
    well_group_id TEXT NOT NULL,
    quantity_units TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'active' CHECK(state IN ('active','reversed')),
    UNIQUE(handover_id, batch_id)
);

CREATE INDEX IF NOT EXISTS idx_attributions_batch
ON lifting_attributions(batch_id, state);

CREATE TABLE IF NOT EXISTS metering_disputes (
    dispute_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL REFERENCES crude_batches(batch_id),
    point_id TEXT REFERENCES metering_points(point_id),
    reason TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'open' CHECK(state IN ('open','resolved')),
    opened_by TEXT NOT NULL REFERENCES meter_users(user_id),
    opened_at TEXT NOT NULL,
    resolved_by TEXT REFERENCES meter_users(user_id),
    resolved_at TEXT,
    resolution_note TEXT
);

CREATE INDEX IF NOT EXISTS idx_disputes_batch
ON metering_disputes(batch_id, state);

CREATE TABLE IF NOT EXISTS meter_idempotency (
    scope TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_sha256 TEXT NOT NULL,
    response_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(scope, idempotency_key)
);

CREATE TABLE IF NOT EXISTS meter_audit_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    previous_hash TEXT NOT NULL,
    event_hash TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_meter_audit_entity
ON meter_audit_events(entity_type, entity_id, event_id);
"""


def connect(path: str | Path) -> sqlite3.Connection:
    connection = sqlite3.connect(str(path), isolation_level=None, timeout=10)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA busy_timeout=5000")
    initialize(connection)
    return connection


def initialize(connection: sqlite3.Connection) -> None:
    connection.executescript(SCHEMA)


@contextmanager
def transaction(connection: sqlite3.Connection, *, immediate: bool = False) -> Iterator[None]:
    connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
    try:
        yield
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()
