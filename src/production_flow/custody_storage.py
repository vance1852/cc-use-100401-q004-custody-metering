"""端到端计量监管链的 SQLite 模式。"""

from __future__ import annotations

import sqlite3


CUSTODY_SCHEMA = """
CREATE TABLE IF NOT EXISTS calibration_versions (
    calibration_id TEXT PRIMARY KEY,
    meter_kind TEXT NOT NULL,
    correction_factor TEXT NOT NULL,
    basis TEXT NOT NULL,
    effective_from TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'active' CHECK(state IN ('active','retired')),
    revision INTEGER NOT NULL DEFAULT 1,
    created_by TEXT NOT NULL REFERENCES supply_users(user_id),
    created_at TEXT NOT NULL,
    UNIQUE(meter_kind, calibration_id)
);

CREATE TABLE IF NOT EXISTS meter_readings (
    reading_id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT REFERENCES custody_batches(batch_id),
    meter_id TEXT NOT NULL,
    calibration_id TEXT NOT NULL REFERENCES calibration_versions(calibration_id),
    meter_kind TEXT NOT NULL,
    factor_snapshot TEXT NOT NULL,
    gross_reading TEXT NOT NULL,
    net_reading TEXT NOT NULL,
    reading_role TEXT NOT NULL CHECK(reading_role IN ('primary','late')),
    observed_at TEXT NOT NULL,
    late_settlement_id INTEGER,
    recorded_by TEXT NOT NULL REFERENCES supply_users(user_id),
    recorded_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_readings_batch ON meter_readings(batch_id, reading_id);
CREATE INDEX IF NOT EXISTS idx_readings_meter ON meter_readings(meter_id, observed_at);

CREATE TABLE IF NOT EXISTS custody_batches (
    batch_id TEXT PRIMARY KEY,
    segment TEXT NOT NULL CHECK(segment IN (
        'well_production','platform_separation','pipeline_batch',
        'floating_processing','tank_blend','lifting_transfer')),
    product TEXT NOT NULL DEFAULT 'crude-oil',
    well_group TEXT,
    gross_quota_units TEXT NOT NULL,
    quantity_quota_units TEXT NOT NULL,
    factor_set_sha256 TEXT NOT NULL,
    allowed_loss_basis_points INTEGER NOT NULL,
    observed_at TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'recorded'
        CHECK(state IN ('recorded','frozen','settlement','void')),
    revision INTEGER NOT NULL DEFAULT 1,
    idempotency_key TEXT NOT NULL UNIQUE,
    created_by TEXT NOT NULL REFERENCES supply_users(user_id),
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_custody_segment ON custody_batches(segment, batch_id);

CREATE TABLE IF NOT EXISTS custody_links (
    link_id INTEGER PRIMARY KEY AUTOINCREMENT,
    parent_batch_id TEXT NOT NULL REFERENCES custody_batches(batch_id),
    child_batch_id TEXT NOT NULL REFERENCES custody_batches(batch_id),
    consumed_quota_units TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    created_by TEXT NOT NULL REFERENCES supply_users(user_id),
    created_at TEXT NOT NULL,
    UNIQUE(parent_batch_id, child_batch_id),
    CHECK(parent_batch_id <> child_batch_id)
);

CREATE INDEX IF NOT EXISTS idx_custody_links_child ON custody_links(child_batch_id);
CREATE INDEX IF NOT EXISTS idx_custody_links_parent ON custody_links(parent_batch_id);

CREATE TABLE IF NOT EXISTS lifting_handovers (
    handover_id INTEGER PRIMARY KEY AUTOINCREMENT,
    lifting_batch_id TEXT NOT NULL UNIQUE REFERENCES custody_batches(batch_id),
    signed_quantity_quota_units TEXT NOT NULL,
    vessel_voyage TEXT NOT NULL,
    receiver TEXT NOT NULL,
    terminal TEXT NOT NULL,
    bill_of_lading TEXT,
    signed_by TEXT NOT NULL REFERENCES supply_users(user_id),
    signed_at TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'signed' CHECK(state IN ('signed','void')),
    idempotency_key TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS late_settlements (
    settlement_id INTEGER PRIMARY KEY AUTOINCREMENT,
    original_batch_id TEXT NOT NULL REFERENCES custody_batches(batch_id),
    successor_batch_id TEXT NOT NULL UNIQUE REFERENCES custody_batches(batch_id),
    delta_gross_quota_units TEXT NOT NULL,
    delta_net_quota_units TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    note TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    created_by TEXT NOT NULL REFERENCES supply_users(user_id),
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_settlements_original ON late_settlements(original_batch_id);

CREATE TABLE IF NOT EXISTS measurement_disputes (
    dispute_id INTEGER PRIMARY KEY AUTOINCREMENT,
    scope TEXT NOT NULL CHECK(scope IN ('single','lineage')),
    anchor_batch_id TEXT NOT NULL REFERENCES custody_batches(batch_id),
    affected_batch_ids_json TEXT NOT NULL,
    note TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'open' CHECK(state IN ('open','resolved')),
    resolution_note TEXT,
    created_by TEXT NOT NULL REFERENCES supply_users(user_id),
    created_at TEXT NOT NULL,
    resolved_by TEXT REFERENCES supply_users(user_id),
    resolved_at TEXT,
    idempotency_key TEXT NOT NULL UNIQUE
);

CREATE INDEX IF NOT EXISTS idx_disputes_state ON measurement_disputes(state, dispute_id);

CREATE TABLE IF NOT EXISTS custody_idempotency (
    scope TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_sha256 TEXT NOT NULL,
    response_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(scope, idempotency_key)
);
"""


def initialize_custody(connection: sqlite3.Connection) -> None:
    connection.executescript(CUSTODY_SCHEMA)
