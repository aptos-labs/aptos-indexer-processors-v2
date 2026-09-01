-- Shelby indexes objects: what a name resolves to, and the uploads on their way
-- to becoming one. Blobs are how bytes are stored, which is the storage
-- providers' concern and reaches them from the chain, so they are absent here.
--
-- Move u8/u32/u64 map to NUMERIC, addresses to VARCHAR(66), and on-chain
-- microsecond clocks keep their Move names. `last_transaction_version` backs
-- the latest-version-wins guard on every table that is written more than once.

DROP TABLE IF EXISTS blobs;
DROP TABLE IF EXISTS blob_activities;


-- What a name resolves to, now. One row per live object, updated in place on
-- overwrite and removed on delete, so it stays proportional to what exists.
--
-- Written by ObjectCommittedEvent (upsert on `name`) and ObjectDeletedEvent
-- (delete, guarded on the stored version). Both guards are what make
-- reprocessing safe; a delete can drop the row rather than tombstone it
-- because the processor replays a contiguous range forward from its
-- checkpoint, so a re-applied commit is always followed by the delete that
-- came after it.
--
-- No lifecycle columns: `commit_object` asserts the blob is already durable, so
-- a row exists exactly when the name resolves to something readable.
CREATE TABLE shelby_objects (
    -- Object names are `@<owner>/<suffix>`, so a name is unique across accounts
    -- and one account's objects are a contiguous range of this key.
    name                     TEXT NOT NULL,
    owner                    VARCHAR(66) NOT NULL,
    etag                     TEXT NOT NULL,
    -- How the object's bytes are encrypted, which is what turns the stored
    -- length below into the plaintext length a listing reports.
    encryption               TEXT NOT NULL,

    -- ObjectContent::Blob -- the object resolves to one blob.
    blob_uid                 NUMERIC,
    -- Bytes stored for that blob, encryption container included.
    stored_size              NUMERIC,

    -- ObjectContent::Multipart -- the object resolves to a record's parts.
    multipart_uid            NUMERIC,
    part_count               NUMERIC,
    -- Sum of the parts' plaintext sizes. A multipart object reports plaintext
    -- directly because its parts declared it, while a single blob reports
    -- stored bytes, which is why these are two columns and not one.
    total_size               NUMERIC,

    -- Derived by the database so it cannot drift from the columns it describes.
    kind                     TEXT GENERATED ALWAYS AS (
                                 CASE WHEN multipart_uid IS NULL
                                      THEN 'blob' ELSE 'multipart' END
                             ) STORED,

    committed_at_micros      NUMERIC NOT NULL,
    last_transaction_version BIGINT NOT NULL,
    inserted_at              TIMESTAMP NOT NULL DEFAULT NOW(),

    -- Resolves a name, orders a listing, carries its cursor, and bounds a
    -- prefix as `name >= 'pfx' AND name < 'pfx' || high-sentinel`. Writing that
    -- prefix as `LIKE 'pfx%'` instead cannot use this index outside a
    -- C-collation database, and an index that could would not also serve the
    -- listing's collation-ordered scan.
    PRIMARY KEY (name),

    -- The row is one ObjectContent variant or the other, never a mixture and
    -- never neither.
    CONSTRAINT shelby_objects_content_variant CHECK (
        (blob_uid      IS NOT NULL AND stored_size IS NOT NULL
         AND multipart_uid IS NULL AND part_count IS NULL AND total_size IS NULL)
        OR
        (multipart_uid IS NOT NULL AND part_count IS NOT NULL AND total_size IS NOT NULL
         AND blob_uid IS NULL AND stored_size IS NULL)
    )
);


-- The uploads a client can still add parts to.
--
-- A multipart upload accumulates parts under a name that does not resolve yet,
-- and can end by being abandoned rather than committed, so its lifecycle is not
-- an object's. A row lasts only as long as its upload: an upload that completed
-- belongs to an object and one that was aborted belongs to nothing, and S3
-- answers NoSuchUpload for either, so the table is bounded by how many uploads
-- are in flight rather than by how many there have ever been.
--
-- Written by MultipartUploadCreatedEvent (insert), and deleted by
-- ObjectCommittedEvent when its content is multipart, or by
-- MultipartUploadAbortedEvent.
CREATE TABLE shelby_open_multipart_uploads (
    multipart_uid            NUMERIC NOT NULL,
    -- The name this upload will claim if it completes. Not a foreign key: no
    -- object exists under it yet, and one may never exist.
    object_name              TEXT NOT NULL,
    owner                    VARCHAR(66) NOT NULL,
    -- Scheme every part of this upload carries.
    encryption               TEXT NOT NULL,
    created_at_micros        NUMERIC NOT NULL,
    last_transaction_version BIGINT NOT NULL,
    inserted_at              TIMESTAMP NOT NULL DEFAULT NOW(),

    -- Every multipart request arrives as an uploadId and resolves through this.
    -- ListMultipartUploads scans by name instead, but this table holds only
    -- uploads in flight, so there is not enough of it to index.
    PRIMARY KEY (multipart_uid)
);


-- What an open upload has accumulated so far.
--
-- Lives and dies with its upload, for the same reason it does: ListParts asks
-- what an upload in progress has received, and completing or aborting one
-- leaves no upload to ask about. A committed object's parts are still
-- reachable, through the contract's parts view that GetObject reads to serve
-- `?partNumber=N`, which lasts as long as the object does.
--
-- Written by PartCommittedEvent, whose `replaced_uid` reports that the part
-- number was already taken and which the upsert handles by overwriting.
-- Deleted for the whole upload by ObjectCommittedEvent or
-- MultipartUploadAbortedEvent.
CREATE TABLE shelby_open_multipart_parts (
    multipart_uid            NUMERIC NOT NULL,
    part_number              NUMERIC NOT NULL,
    -- The blob holding this part's bytes.
    blob_uid                 NUMERIC NOT NULL,
    -- Bytes of the object this part supplies, encryption excluded.
    plaintext_size           NUMERIC NOT NULL,
    etag                     TEXT NOT NULL,
    committed_at_micros      NUMERIC NOT NULL,
    last_transaction_version BIGINT NOT NULL,
    inserted_at              TIMESTAMP NOT NULL DEFAULT NOW(),

    -- The ListParts query: the parts of one upload, ascending from a
    -- part-number marker. Also the bulk delete, on the leading column alone.
    PRIMARY KEY (multipart_uid, part_number)
);


-- When each object appeared and went away.
--
-- Records ObjectCommittedEvent and ObjectDeletedEvent, and those two only.
-- Storing bytes and assembling an upload both happen a layer below an object:
-- a blob is the storage providers' concern, and opening an upload, adding a
-- part to one or abandoning one are steps toward an object rather than
-- something that happened to one.
--
-- The only unbounded table here, and the only one no other component reads, so
-- a deployment with no explorer switches it off in the processor config and
-- leaves it empty.
CREATE TABLE shelby_object_activities (
    transaction_version      BIGINT NOT NULL,
    -- Position of the event within its transaction, so with the version it
    -- names the event uniquely.
    event_index              BIGINT NOT NULL,
    event_type               TEXT NOT NULL,
    transaction_hash         VARCHAR(66) NOT NULL,
    object_name              TEXT NOT NULL,
    owner                    VARCHAR(66) NOT NULL,
    -- What the object resolved to, exactly one of the two: a commit reports the
    -- content it bound, a deletion the binding it released.
    blob_uid                 NUMERIC,
    multipart_uid            NUMERIC,
    timestamp                TIMESTAMP NOT NULL,
    inserted_at              TIMESTAMP NOT NULL DEFAULT NOW(),

    -- Keyed on where the event sits in the chain rather than on its hash, as
    -- every other activity table in this database is. It makes reprocessing
    -- idempotent the same way, but it ascends with every insert, so the index
    -- grows at its right edge instead of being written all over. Read
    -- backwards it is also the newest-first feed, and ordering on the pair
    -- keeps that feed stable across pages when a transaction emits several
    -- events.
    PRIMARY KEY (transaction_version, event_index)
);

-- One account's history, newest first. The leading `owner` is what the primary
-- key cannot serve, since that orders by version across every account.
CREATE INDEX shelby_object_activities_owner_version
    ON shelby_object_activities (owner, transaction_version DESC, event_index DESC);
