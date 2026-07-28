-- Shelby blob indexer tables. Move numeric types (u8/u32/u64) map to NUMERIC.
-- last_transaction_version backs the latest-version-wins upsert guard.

-- Current state, keyed by the per-registration uid (object_name is reusable).
CREATE TABLE blobs (
    uid                      NUMERIC NOT NULL,
    object_name              TEXT NOT NULL,
    owner                    VARCHAR(66) NOT NULL,
    blob_commitment          TEXT NOT NULL,
    encoding                 TEXT NOT NULL,
    encryption               TEXT NOT NULL,
    slice_address            VARCHAR(66) NOT NULL,
    placement_group          VARCHAR(66) NOT NULL,
    created_at               NUMERIC NOT NULL,
    updated_at               NUMERIC NOT NULL,
    expires_at               NUMERIC NOT NULL,
    size                     NUMERIC NOT NULL,
    num_chunksets            NUMERIC NOT NULL,
    payment_amount           NUMERIC NOT NULL,
    is_persisted             NUMERIC NOT NULL,
    is_committed             NUMERIC NOT NULL,
    is_deleted               NUMERIC NOT NULL,
    etag                     TEXT,
    deletion_reason          TEXT,
    last_transaction_version BIGINT NOT NULL,
    inserted_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (uid)
);

CREATE INDEX idx_blobs_object_name ON blobs (object_name);
CREATE INDEX idx_blobs_owner_object_name ON blobs (owner, object_name);
CREATE INDEX idx_blobs_created_at_active ON blobs (created_at DESC) WHERE is_deleted = 0;
-- The "current objects" view: a name resolves to the one committed, undeleted
-- blob bound to it. Overwrites leave a dead row per version, so prefix listing
-- needs the predicate in the index rather than as a post-scan filter.
CREATE INDEX idx_blobs_current_objects ON blobs (owner, object_name)
    WHERE is_committed = 1 AND is_deleted = 0;

-- Append-only activity log.
CREATE TABLE blob_activities (
    transaction_hash    VARCHAR(66) NOT NULL,
    event_type          TEXT NOT NULL,
    event_index         NUMERIC NOT NULL,
    uid                 NUMERIC NOT NULL,
    object_name         TEXT NOT NULL,
    owner               VARCHAR(66),
    transaction_version NUMERIC NOT NULL,
    timestamp           TIMESTAMP NOT NULL,
    inserted_at         TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (transaction_hash, event_type, event_index)
);

CREATE INDEX idx_blob_act_uid ON blob_activities (uid);
CREATE INDEX idx_blob_act_owner_version ON blob_activities (owner, transaction_version DESC);
CREATE INDEX idx_blob_act_version ON blob_activities (transaction_version DESC);

CREATE TABLE placement_group_slots (
    placement_group          VARCHAR(66) NOT NULL,
    slot_index               NUMERIC NOT NULL,
    storage_provider         VARCHAR(66) NOT NULL,
    status                   TEXT NOT NULL,
    updated_at               NUMERIC NOT NULL,
    last_transaction_version BIGINT NOT NULL,
    inserted_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (placement_group, slot_index)
);

CREATE INDEX idx_pg_slots_status ON placement_group_slots (status);
