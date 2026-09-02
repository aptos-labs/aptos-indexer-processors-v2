-- Shelby indexes objects: what a name resolves to, the uploads on their way to
-- becoming one, and the parts a completed multipart object is made of. Blobs
-- get no table; storage providers read those from the chain.
--
-- A uid is a Move `u64` and does not fit BIGINT in general, but `test_uid_layout`
-- in the contract pins the snowflake's fields at 63 bits, leaving the sign bit
-- clear.
--
-- `last_transaction_version` backs the latest-version-wins guard on every table
-- written more than once. There is no `inserted_at`: a wall-clock default would
-- stop this index being a deterministic function of the event stream, and so
-- rebuildable from the chain alone.

DROP TABLE IF EXISTS blobs;
DROP TABLE IF EXISTS blob_activities;


-- What a name resolves to, now. One row per live object, updated in place on
-- overwrite and removed on delete, so it stays proportional to what exists.
--
-- Written by ObjectCommittedEvent (upsert on `name`) and ObjectDeletedEvent
-- (delete, guarded on the stored version). A delete can drop the row rather
-- than tombstone it because the processor replays a contiguous range forward,
-- so a re-applied commit is always followed by the delete that came after it.
--
-- No lifecycle columns: `commit_object` asserts the blob is already durable, so
-- a row exists exactly when the name resolves to something readable.
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
    -- The etag S3 reports, `0x` and the hex of the event's bytes. The first
    -- ETAG_LENGTH (16) bytes are a digest and anything after them is the suffix
    -- in ASCII, so one rule renders either kind of object: hex the first
    -- sixteen bytes, then append the rest. A single-blob etag has no rest; a
    -- multipart one carries `-<part count>`.
    etag                     TEXT NOT NULL,
    -- How the object's bytes are encrypted, which a reader needs to interpret
    -- them. Unconstrained on purpose: the indexer never reads it, so a scheme
    -- added to the contract must not stop this processor.
    encryption               TEXT NOT NULL,
    -- Bytes the object carries, encryption container excluded. The same
    -- measurement whichever variant below is populated.
    plaintext_size           BIGINT NOT NULL,

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

-- One account's objects, ascending by name: every bucket listing. A name
-- embeds its owner, so these rows are also a contiguous range of the primary
-- key, but that is a convention of how names are built and not something the
-- planner can see.
CREATE INDEX shelby_objects_owner_name
    ON shelby_objects (owner, name);

-- Newest objects first, across every account: the explorer's feed, which has no
-- owner filter and so can use neither the index above nor the primary key.
CREATE INDEX shelby_objects_committed_at
    ON shelby_objects (committed_at_micros DESC);


-- The uploads a client can still add parts to. An upload accumulates parts
-- under a name that does not resolve yet and may be abandoned rather than
-- committed, so its lifecycle is not an object's, and it has no shelby_objects
-- row to join to -- ListMultipartUploads is answered from this table alone.
--
-- Written by MultipartUploadCreatedEvent, deleted by ObjectCommittedEvent when
-- its content is multipart, or by MultipartUploadAbortedEvent. Those are the
-- only ways a row leaves, and neither fires on its own: `abort_multipart_upload`
-- takes the client's signer and there is no expiry or admin sweep, so an
-- abandoned upload stays here for good. The table is bounded by uploads ever
-- started rather than by uploads in flight, which is why the listing below is
-- indexed rather than left to a scan.
CREATE TABLE shelby_open_multipart_uploads (
    multipart_uid            BIGINT NOT NULL,
    -- The name this upload will claim if it completes. Not a foreign key: no
    -- object exists under it yet, and one may never exist. `COLLATE "C"` for
    -- the reasons shelby_objects.name carries it -- ListMultipartUploads walks
    -- the key space the same way, with a prefix and a delimiter.
    object_name              TEXT COLLATE "C" NOT NULL,
    owner                    VARCHAR(66) NOT NULL,
    -- Scheme every part of this upload carries.
    encryption               TEXT NOT NULL,
    created_at_micros        BIGINT NOT NULL,
    last_transaction_version BIGINT NOT NULL,

    -- Every multipart request arrives as an uploadId and resolves through this.
    PRIMARY KEY (multipart_uid)
);

-- ListMultipartUploads: one bucket's uploads, by key and then by upload id.
-- `owner` leads because a bucket is an account, `object_name` carries the
-- prefix and the `key-marker`, and `multipart_uid` is both the
-- `upload-id-marker` and S3's order within a key, a uid being a snowflake with
-- the millisecond in its high bits.
CREATE INDEX shelby_open_multipart_uploads_owner_name
    ON shelby_open_multipart_uploads (owner, object_name, multipart_uid);


-- What an open upload has accumulated so far: staging, not a manifest. It holds
-- everything uploaded, including parts a completion goes on to leave out, and
-- it dies with its upload. It inherits the growth described above, at up to
-- MAX_OBJECT_PARTS rows per abandoned upload; every query against it is keyed
-- by `multipart_uid`, so the cost is storage rather than latency.
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
    -- The bare digest, `0x` and thirty-two hex characters. A part's tag is
    -- pinned at ETAG_LENGTH by E_INVALID_PART_ETAG_LENGTH, so unlike an
    -- object's it never carries a suffix, and ListParts quotes it as it stands.
    etag                     TEXT NOT NULL,
    committed_at_micros      BIGINT NOT NULL,
    last_transaction_version BIGINT NOT NULL,

    -- The ListParts query: the parts of one upload, ascending from a
    -- part-number marker. Also the bulk delete, on the leading column alone.
    PRIMARY KEY (multipart_uid, part_number)
);


-- The parts a committed multipart object resolves to: what GetObject reads to
-- know which blobs hold the bytes it was asked for.
--
-- Its own table rather than a column on shelby_objects because a ranged read
-- wants only the parts covering its range; held on the object row, every read
-- would fetch the whole manifest, which at the part ceiling is on the order of
-- 100 KB to serve an 8 MiB range.
--
-- Promoted from shelby_open_multipart_parts at completion, restricted to the
-- part numbers the commit event leaves unpruned, and never updated after.
-- Removing them is driven off the ObjectRef that ObjectDeletedEvent and an
-- overwriting ObjectCommittedEvent carry: an overwrite replaces the object row
-- in place and takes the old uid with it, so nothing here could be found again.
CREATE TABLE shelby_object_parts (
    multipart_uid            BIGINT NOT NULL,
    part_number              INTEGER NOT NULL,
    -- The blob holding this part's bytes.
    blob_uid                 BIGINT NOT NULL,

    -- Where this part sits in the object: `[offset_in_object, end_offset)`.
    -- Both bounds are stored and the size derived, rather than the other way
    -- round, because a ranged read needs both to be indexed predicates. The
    -- natural `offset < :end AND offset + plaintext_size > :start` puts an
    -- expression on the second comparison, which no index serves and Hasura
    -- cannot generate; storing the end makes it two plain column comparisons. A
    -- part's plaintext size is `end_offset - offset_in_object`.
    offset_in_object         BIGINT NOT NULL,
    end_offset               BIGINT NOT NULL,

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


-- When each object appeared and went away: ObjectCommittedEvent and
-- ObjectDeletedEvent only, since opening an upload or adding a part to one is a
-- step toward an object rather than something that happened to one.
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

    -- Keyed on where the event sits in the chain rather than on its hash, as
    -- every other activity table here is: reprocessing stays idempotent, the
    -- index grows at its right edge, and read backwards the key is itself the
    -- newest-first feed, stable across pages when one transaction emits several
    -- events.
    PRIMARY KEY (transaction_version, event_index)
);

-- One account's history, newest first. The leading `owner` is what the primary
-- key cannot serve, since that orders by version across every account.
CREATE INDEX shelby_object_activities_owner_version
    ON shelby_object_activities (owner, transaction_version DESC, event_index DESC);
