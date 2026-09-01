DROP TABLE IF EXISTS shelby_object_activities;
DROP TABLE IF EXISTS shelby_open_multipart_parts;
DROP TABLE IF EXISTS shelby_open_multipart_uploads;
DROP TABLE IF EXISTS shelby_objects;

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
CREATE INDEX idx_blobs_current_objects ON blobs (owner, object_name)
    WHERE is_committed = 1 AND is_deleted = 0;

CREATE TABLE blob_activities (
    transaction_hash    VARCHAR(66) NOT NULL,
    event_type          TEXT NOT NULL,
    event_index         BIGINT NOT NULL,
    uid                 NUMERIC NOT NULL,
    object_name         TEXT NOT NULL,
    owner               VARCHAR(66),
    transaction_version BIGINT NOT NULL,
    timestamp           TIMESTAMP NOT NULL,
    inserted_at         TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (transaction_hash, event_type, event_index)
);

CREATE INDEX idx_blob_act_uid ON blob_activities (uid);
CREATE INDEX idx_blob_act_owner_version ON blob_activities (owner, transaction_version DESC);
CREATE INDEX idx_blob_act_version ON blob_activities (transaction_version DESC);
