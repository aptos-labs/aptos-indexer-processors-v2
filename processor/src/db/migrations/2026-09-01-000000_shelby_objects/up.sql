-- Shelby indexes objects: what a name resolves to, the uploads on their way to
-- becoming one, and the parts a completed multipart object is made of. A
-- committed blob gets no row of its own; storage providers read those from the
-- chain. The one exception is a blob still waiting to be committed, which
-- nothing else can enumerate -- see shelby_pending_blobs.
--
-- A uid is a Move `u64` and does not fit BIGINT in general, but `test_uid_layout`
-- in the contract pins the snowflake's fields at 63 bits, leaving the sign bit
-- clear.
--
-- `last_transaction_version` backs the latest-version-wins guard on every table
-- written more than once. `inserted_at` carries a default and is never in an
-- insert's values, so the chain-derived data stays a deterministic function of
-- the event stream; only the wall clock differs, as on every other table here.

DROP TABLE IF EXISTS blobs;
DROP TABLE IF EXISTS blob_activities;


-- What a name resolves to, now. One row per live object, updated in place on
-- overwrite and removed on delete, so it stays proportional to what exists.
-- Written by ObjectCommittedEvent (upsert on `name`) and ObjectDeletedEvent
-- (delete, guarded on the stored version).
CREATE TABLE shelby_objects (
    -- Object names are `@<owner>/<suffix>`, so a name is unique across accounts
    -- and one account's objects are a contiguous range of this key.
    --
    -- `COLLATE "C"` is byte order, which S3 requires and which a linguistic
    -- collation would break rather than merely reorder: a delimited listing
    -- jumps over each directory, assuming every key under a prefix is
    -- contiguous, and a collation that de-prioritises punctuation sorts `ab`
    -- between `a/b` and `a/c`, so jumping `a/` drops `ab` from the results. It
    -- is also what lets `name LIKE 'pfx%'` seek this column's indexes.
    name                     TEXT COLLATE "C" NOT NULL,
    owner                    VARCHAR(66) NOT NULL,
    -- The etag S3 reports: hex the first ETAG_LENGTH (16) bytes (a digest),
    -- then append the rest as ASCII. A single-blob etag has no rest; a
    -- multipart one carries `-<part count>`.
    etag                     TEXT NOT NULL,
    -- How the object's bytes are encrypted. Unconstrained on purpose: the
    -- indexer never reads it, so a scheme added to the contract must not stop
    -- this processor.
    encryption               TEXT NOT NULL,
    -- How the object's bytes are erasure coded; fixes the chunkset size, which
    -- maps a plaintext range onto stored bytes. Unconstrained for the same
    -- reason as encryption.
    encoding                 TEXT NOT NULL,
    -- Region holding the object's bytes. One name for either kind of object: a
    -- single blob has its slice's region, and a multipart object's parts were all
    -- written to the one its upload fixed.
    location_name            TEXT NOT NULL,
    -- Bytes the object carries, encryption container excluded. The same
    -- measurement whichever variant below is populated.
    plaintext_size           BIGINT NOT NULL,
    -- Bytes holding the object's bytes, encryption container included: what
    -- reading the whole object transfers, and what a payment for it is
    -- quantized against. Stored rather than derived because a multipart
    -- object's containers are its parts', one per part, so the overhead does
    -- not follow from the object's own plaintext length.
    stored_size              BIGINT NOT NULL,

    -- ObjectContent::Blob -- the object resolves to one blob.
    blob_uid                 BIGINT,

    -- ObjectContent::Multipart -- the object resolves to the rows in
    -- shelby_object_parts under this uid.
    multipart_uid            BIGINT,
    -- HeadObject answers `x-amz-mp-parts-count` without reading that table.
    part_count               INTEGER,

    -- Which ObjectContent variant the row holds. Generated, so it cannot drift
    -- and the storer never writes it.
    kind                     TEXT GENERATED ALWAYS AS (
                                 CASE WHEN multipart_uid IS NULL
                                      THEN 'blob' ELSE 'multipart' END
                             ) STORED,

    committed_at_micros      BIGINT NOT NULL,
    last_transaction_version BIGINT NOT NULL,
    inserted_at              TIMESTAMP NOT NULL DEFAULT NOW(),

    -- Resolves a name, orders a listing, carries its cursor, and bounds a
    -- prefix as `name >= 'pfx' AND name < 'pfx' || high-sentinel`.
    PRIMARY KEY (name),

    -- The row is one ObjectContent variant or the other, never a mixture and
    -- never neither.
    CONSTRAINT shelby_objects_content_variant CHECK (
        (blob_uid IS NOT NULL AND multipart_uid IS NULL AND part_count IS NULL)
        OR
        (blob_uid IS NULL AND multipart_uid IS NOT NULL AND part_count IS NOT NULL)
    )
);

-- One account's objects, ascending by name: every bucket listing.
CREATE INDEX shelby_objects_owner_name
    ON shelby_objects (owner, name);

-- Newest objects first, across every account: the explorer's feed, which has
-- no owner filter and so can use neither the index above nor the primary key.
CREATE INDEX shelby_objects_committed_at
    ON shelby_objects (committed_at_micros DESC);


-- The uploads a client can still add parts to. ListMultipartUploads is
-- answered from this table alone.
--
-- Written by MultipartUploadCreatedEvent, deleted by ObjectCommittedEvent when
-- its content is multipart, or by MultipartUploadAbortedEvent. There is no
-- expiry or admin sweep, so an abandoned upload stays here for good; the table
-- is bounded by uploads ever started, which is why the listing below is
-- indexed rather than left to a scan.
CREATE TABLE shelby_open_multipart_uploads (
    multipart_uid            BIGINT NOT NULL,
    -- The name this upload will claim if it completes. Not a foreign key: no
    -- object exists under it yet, and one may never exist. `COLLATE "C"` for
    -- the reasons shelby_objects.name carries it.
    object_name              TEXT COLLATE "C" NOT NULL,
    owner                    VARCHAR(66) NOT NULL,
    -- Scheme every part of this upload carries.
    encryption               TEXT NOT NULL,
    -- Coding every part of this upload carries.
    encoding                 TEXT NOT NULL,
    -- Region every part of this upload is written to, resolved when it was
    -- opened. A part takes it from the record, so the parts cannot disagree
    -- and the part tables do not repeat it.
    location_name            TEXT NOT NULL,
    created_at_micros        BIGINT NOT NULL,
    last_transaction_version BIGINT NOT NULL,
    inserted_at              TIMESTAMP NOT NULL DEFAULT NOW(),

    -- Every multipart request arrives as an uploadId and resolves through this.
    PRIMARY KEY (multipart_uid)
);

-- ListMultipartUploads: one bucket's uploads, by key and then by upload id.
-- `object_name` carries the prefix and the `key-marker`, and `multipart_uid`
-- is both the `upload-id-marker` and S3's order within a key, a uid being a
-- snowflake with the millisecond in its high bits.
CREATE INDEX shelby_open_multipart_uploads_owner_name
    ON shelby_open_multipart_uploads (owner, object_name, multipart_uid);


-- What an open upload has accumulated so far: staging, not a manifest. It
-- holds everything uploaded, including parts a completion goes on to leave
-- out, and it dies with its upload.
--
-- Written by PartCommittedEvent, where re-uploading a part number is an upsert
-- on the same key. Deleted for the whole upload by ObjectCommittedEvent or
-- MultipartUploadAbortedEvent.
CREATE TABLE shelby_open_multipart_parts (
    multipart_uid            BIGINT NOT NULL,
    part_number              INTEGER NOT NULL,
    -- The blob holding this part's bytes.
    blob_uid                 BIGINT NOT NULL,
    -- Bytes of the object this part supplies, encryption excluded.
    plaintext_size           BIGINT NOT NULL,
    -- Bytes the part's blob holds them in, container included: what reading this
    -- part transfers.
    stored_size              BIGINT NOT NULL,
    -- The bare digest, `0x` and thirty-two hex characters. A part's tag is
    -- pinned at ETAG_LENGTH by E_INVALID_PART_ETAG_LENGTH, so unlike an
    -- object's it never carries a suffix.
    etag                     TEXT NOT NULL,
    committed_at_micros      BIGINT NOT NULL,
    last_transaction_version BIGINT NOT NULL,
    inserted_at              TIMESTAMP NOT NULL DEFAULT NOW(),

    -- The ListParts query: the parts of one upload, ascending from a
    -- part-number marker. Also the bulk delete, on the leading column alone.
    PRIMARY KEY (multipart_uid, part_number)
);


-- The parts a committed multipart object resolves to: what GetObject reads to
-- know which blobs hold the bytes it was asked for.
--
-- Its own table rather than a column on shelby_objects because a ranged read
-- wants only the parts covering its range; held on the object row, every read
-- would fetch the whole manifest.
--
-- Promoted from shelby_open_multipart_parts at completion, restricted to the
-- part numbers the commit event leaves unpruned, and never updated after.
CREATE TABLE shelby_object_parts (
    multipart_uid            BIGINT NOT NULL,
    part_number              INTEGER NOT NULL,
    -- The blob holding this part's bytes.
    blob_uid                 BIGINT NOT NULL,

    -- Where this part sits in the object: `[offset_in_object, end_offset)`.
    -- Both bounds are stored and the size derived, because a ranged read needs
    -- both to be indexed predicates: `offset < :end AND offset +
    -- plaintext_size > :start` puts an expression on the second comparison,
    -- which no index serves. A part's plaintext size is `end_offset -
    -- offset_in_object`.
    offset_in_object         BIGINT NOT NULL,
    end_offset               BIGINT NOT NULL,

    -- Bytes reading this part transfers, container included, carried across
    -- from the staging row so a part reports the length its blob really has.
    stored_size              BIGINT NOT NULL,
    inserted_at              TIMESTAMP NOT NULL DEFAULT NOW(),

    -- No `last_transaction_version`: a row is written once under a
    -- `multipart_uid` that is never reused, so no two writes ever contend.

    -- `?partNumber=N` is a point lookup on this, and the whole-object read an
    -- ordered scan of the leading column.
    PRIMARY KEY (multipart_uid, part_number),

    CONSTRAINT shelby_object_parts_span CHECK (end_offset > offset_in_object)
);

-- The ranged read: the parts of one object intersecting [start, end). Seeks on
-- `end_offset > :start` and scans forward while `offset_in_object < :end`, so
-- it reads the parts in the range rather than all of them.
CREATE INDEX shelby_object_parts_range
    ON shelby_object_parts (multipart_uid, end_offset);


-- Blobs registered but not yet committed: the candidate set for
-- `garbage_collect_blobs`, which collects a pending blob once the contract's
-- `PENDING_GC_GRACE_MICROS` window has elapsed since it was registered.
-- Collection is permissionless, so anyone running this query can reclaim.
--
-- Written by BlobRegisteredEvent and deleted by the two events that end a
-- blob's wait: BlobPersistedEvent when it becomes durable, BlobDeletedEvent
-- when it is torn down. The delete is keyed on uid alone, so the teardown of
-- a blob that was never here is a no-op.
CREATE TABLE shelby_pending_blobs (
    uid                      BIGINT NOT NULL,
    owner                    VARCHAR(66) NOT NULL,
    -- Region the blob was registered into, so a backlog can be read per region.
    location_name            TEXT NOT NULL,
    -- Registration time, which the grace window is measured from.
    creation_micros          BIGINT NOT NULL,
    -- Bytes the blob was registered to hold, container included: what a
    -- sweep reclaims by collecting it.
    stored_size              BIGINT NOT NULL,
    last_transaction_version BIGINT NOT NULL,
    inserted_at              TIMESTAMP NOT NULL DEFAULT NOW(),

    -- A uid identifies a blob, and a sweep passes uids to the entry function.
    PRIMARY KEY (uid)
);

-- The sweep: the blobs registered longest ago, which are the ones whose grace
-- window has elapsed.
CREATE INDEX shelby_pending_blobs_creation
    ON shelby_pending_blobs (creation_micros);


-- When each object appeared and went away: ObjectCommittedEvent and
-- ObjectDeletedEvent only, since opening an upload or adding a part to one is
-- a step toward an object rather than something that happened to one.
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
    blob_uid                 BIGINT,
    multipart_uid            BIGINT,
    timestamp                TIMESTAMP NOT NULL,
    inserted_at              TIMESTAMP NOT NULL DEFAULT NOW(),

    -- Keyed on where the event sits in the chain rather than on its hash:
    -- reprocessing stays idempotent, and read backwards the key is itself the
    -- newest-first feed, stable across pages when one transaction emits
    -- several events.
    PRIMARY KEY (transaction_version, event_index)
);

-- One account's history, newest first.
CREATE INDEX shelby_object_activities_owner_version
    ON shelby_object_activities (owner, transaction_version DESC, event_index DESC);
