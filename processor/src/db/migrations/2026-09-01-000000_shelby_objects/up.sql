-- Shelby indexes objects: what a name resolves to, the uploads on their way to
-- becoming one, and the parts a completed multipart object is made of. Blobs
-- are how bytes are stored, which is the storage providers' concern and reaches
-- them from the chain, so they are absent here.
--
-- Move uids, sizes, offsets and microsecond clocks map to BIGINT, part numbers
-- to INTEGER, addresses to VARCHAR(66). A uid is a `u64` and so does not fit a
-- signed 64-bit column in general, but `test_uid_layout` in the contract pins
-- the snowflake's fields at exactly 63 bits, leaving the sign bit clear.
--
-- `last_transaction_version` backs the latest-version-wins guard on every table
-- whose rows are written more than once. No `inserted_at` anywhere: nothing
-- reads it, and a wall-clock default would make this index something other than
-- a deterministic function of the event stream, so two indexers fed the same
-- events would disagree and neither could be rebuilt from the chain alone.

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
--
-- Everything HeadObject answers is here. A multipart object's parts live in
-- shelby_object_parts, keyed by the `multipart_uid` this row carries, so a
-- whole-object or ranged GET is this row plus the parts covering its range.
CREATE TABLE shelby_objects (
    -- Object names are `@<owner>/<suffix>`, so a name is unique across accounts
    -- and one account's objects are a contiguous range of this key.
    --
    -- `COLLATE "C"` -- byte order, rather than the database's linguistic
    -- collation -- for three reasons, one of which is correctness.
    --
    -- S3 returns keys in UTF-8 binary order, so any other ordering makes a
    -- listing non-conformant and its pagination unstable against a client that
    -- expects S3's.
    --
    -- A delimited listing walks the key space and jumps over each directory,
    -- which assumes every key under a prefix is contiguous. A linguistic
    -- collation de-prioritises punctuation, so `ab` can sort between `a/b` and
    -- `a/c`, inside the `a/` range. Jumping the directory then skips `ab`
    -- entirely, and the listing loses a key rather than merely misordering it.
    --
    -- And it is what lets `name LIKE 'pfx%'` use this column's indexes at all;
    -- under any other collation that predicate is a filter, not a seek.
    name                     TEXT COLLATE "C" NOT NULL,
    owner                    VARCHAR(66) NOT NULL,
    -- The etag S3 reports, in two pieces: the first ETAG_LENGTH (16) bytes are
    -- the digest, and whatever follows is the suffix in ASCII. A single-blob
    -- object's is the merkle-root commitment's prefix and stops there. A
    -- multipart object's is a digest over its parts' tags followed by
    -- `-<part count>`, which is why the contract bounds a caller-supplied etag
    -- at MAX_ETAG_LENGTH rather than fixing its length.
    --
    -- Held as the event serializes it: `0x`, then the hex of every byte. One
    -- rule renders either kind -- hex the first sixteen bytes, then append the
    -- rest as ASCII -- because a single-blob etag has no rest. Nothing reading
    -- this column has to know which variant the row holds.
    etag                     TEXT NOT NULL,
    -- How the object's bytes are encrypted, which a reader needs in order to
    -- interpret the bytes it downloads. Carried verbatim from the chain and
    -- deliberately unconstrained: the indexer never interprets it, so a scheme
    -- added to the contract must not stop this processor. The gateway is what
    -- has to reject a scheme it cannot read.
    encryption               TEXT NOT NULL,
    -- Bytes the object carries, encryption container excluded. The same
    -- measurement whichever variant below is populated.
    plaintext_size           BIGINT NOT NULL,

    -- ObjectContent::Blob -- the object resolves to one blob.
    blob_uid                 BIGINT,

    -- ObjectContent::Multipart -- the object resolves to the rows in
    -- shelby_object_parts under this uid.
    multipart_uid            BIGINT,
    -- Kept here rather than counted from that table, so HeadObject answers
    -- `x-amz-mp-parts-count` without touching it.
    part_count               INTEGER,

    -- Which ObjectContent variant the row holds, stated rather than left to be
    -- inferred from which uid column is null. Derived by the database so it
    -- cannot drift from the columns it describes, and generated, so the storer
    -- never writes it and it stays out of the Insertable.
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

-- One account's objects, ascending by name: every bucket listing, and the
-- account views in the explorer and the SDK.
--
-- A name embeds its owner, so one account's objects are also a contiguous range
-- of the primary key -- but that is a convention of how names are built, not
-- something the database enforces or the planner can see. Queries written the
-- way callers think, `WHERE owner = $1`, need this index.
CREATE INDEX shelby_objects_owner_name
    ON shelby_objects (owner, name);

-- Newest objects first, across every account: the explorer's feed, which orders
-- on committed_at_micros with no owner filter and so cannot use the index above
-- or the primary key.
CREATE INDEX shelby_objects_committed_at
    ON shelby_objects (committed_at_micros DESC);


-- The uploads a client can still add parts to.
--
-- A multipart upload accumulates parts under a name that does not resolve yet,
-- and can end by being abandoned rather than committed, so its lifecycle is not
-- an object's.
--
-- Self-contained by necessity. An upload in flight has no shelby_objects row to
-- join to -- that is the whole reason this table exists -- so
-- ListMultipartUploads is answered from here alone, and `object_name` is
-- carried on MultipartUploadCreatedEvent rather than looked up.
--
-- Written by that event, and deleted by ObjectCommittedEvent when its content
-- is multipart, or by MultipartUploadAbortedEvent. Those two are the only ways
-- a row leaves, and neither fires on its own: nothing on chain reclaims an
-- upload whose owner walked away, since `abort_multipart_upload` takes the
-- client's signer and there is no expiry or admin sweep. So this table is
-- bounded by uploads ever started and never finished, not by uploads in flight,
-- which is why the listing below is indexed rather than left to a scan.
CREATE TABLE shelby_open_multipart_uploads (
    multipart_uid            BIGINT NOT NULL,
    -- The name this upload will claim if it completes. Not a foreign key: no
    -- object exists under it yet, and one may never exist.
    --
    -- `COLLATE "C"` for the reasons shelby_objects.name carries it.
    -- ListMultipartUploads takes a prefix and a delimiter, so it walks the key
    -- space the same way: byte ordering is what keeps its pagination stable and
    -- what stops a delimiter jump from skipping a key that a linguistic
    -- collation sorted into the middle of the range being jumped.
    object_name              TEXT COLLATE "C" NOT NULL,
    owner                    VARCHAR(66) NOT NULL,
    -- Scheme every part of this upload carries.
    encryption               TEXT NOT NULL,
    created_at_micros        BIGINT NOT NULL,
    last_transaction_version BIGINT NOT NULL,

    -- Every multipart request arrives as an uploadId and resolves through this.
    -- ListParts reads the key it reports back from here too.
    PRIMARY KEY (multipart_uid)
);

-- ListMultipartUploads: one bucket's uploads, by key and then by upload id.
--
-- `owner` leads because a bucket is an account. `object_name` carries the
-- prefix range and the `key-marker`, and its collation is what lets a prefix
-- predicate seek rather than filter. `multipart_uid` breaks ties within a key,
-- which is at once the `upload-id-marker` a client pages with and the order S3
-- requires within a key: a uid is a snowflake with the millisecond in its high
-- bits, so uid order is initiation order.
CREATE INDEX shelby_open_multipart_uploads_owner_name
    ON shelby_open_multipart_uploads (owner, object_name, multipart_uid);


-- What an open upload has accumulated so far.
--
-- Lives and dies with its upload: ListParts asks what an upload in progress has
-- received, and completing or aborting one leaves no upload to ask about. It
-- inherits the growth described above, at up to MAX_OBJECT_PARTS rows per
-- abandoned upload. Every query against it is keyed by `multipart_uid`, so no
-- further index is wanted; the cost is storage, not latency.
--
-- Written by PartCommittedEvent, whose `replaced_uid` reports that the part
-- number was already taken and which the upsert handles by overwriting. Deleted
-- for the whole upload by ObjectCommittedEvent or MultipartUploadAbortedEvent.
--
-- This is staging, not a manifest. It holds what was uploaded, including parts
-- a completion goes on to leave out, and it is gone once the upload ends.
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
-- wants only the parts covering its range. Held on the object row, as JSON or
-- as arrays, every read would fetch the whole manifest -- at the part ceiling,
-- on the order of 100 KB to serve an 8 MiB range -- and gateway-side caching
-- would become a requirement of the design rather than an optimization.
--
-- Promoted from shelby_open_multipart_parts at completion, restricted to the
-- part numbers the commit event leaves unpruned. Nothing is copied that the
-- staging table did not already hold: the offsets are the running sum, which
-- only becomes known once the object's part list is fixed.
--
-- Rows are written once and never updated. They are deleted when the object
-- stops pointing at them, which the storer drives off the ObjectRef that
-- ObjectDeletedEvent and an overwriting ObjectCommittedEvent both carry --
-- nothing in this table or in shelby_objects can find them otherwise, since an
-- overwrite replaces the object row in place and takes the old uid with it.
CREATE TABLE shelby_object_parts (
    multipart_uid            BIGINT NOT NULL,
    part_number              INTEGER NOT NULL,
    -- The blob holding this part's bytes.
    blob_uid                 BIGINT NOT NULL,

    -- Where this part sits in the object: `[offset_in_object, end_offset)`.
    --
    -- Both bounds are stored and the size is derived, rather than the other way
    -- round, because a ranged read needs both to be indexed predicates. The
    -- natural form -- `offset < :end AND offset + plaintext_size > :start` --
    -- has an expression on the second comparison, which no index serves and
    -- which Hasura cannot generate at all. Storing the end instead makes it
    -- `offset_in_object < :end AND end_offset > :start`, two plain comparisons
    -- on columns.
    --
    -- Two of the three values, so nothing is redundant: a part's plaintext size
    -- is `end_offset - offset_in_object`.
    offset_in_object         BIGINT NOT NULL,
    end_offset               BIGINT NOT NULL,

    -- No `last_transaction_version`. That column arbitrates between an older
    -- write and a newer one, and there is no such contest here: a row is
    -- written once, at completion, under a `multipart_uid` that is never
    -- reused, and is never updated.

    -- `?partNumber=N` is a point lookup on this, and the whole-object read is
    -- an ordered scan of the leading column. The bulk delete uses it too.
    PRIMARY KEY (multipart_uid, part_number),

    CONSTRAINT shelby_object_parts_span CHECK (end_offset > offset_in_object)
);

-- The ranged read: the parts of one object intersecting [start, end). Seeks on
-- `end_offset > :start` and scans forward while `offset_in_object < :end`, so
-- it reads the parts in the range rather than all of them.
CREATE INDEX shelby_object_parts_range
    ON shelby_object_parts (multipart_uid, end_offset);


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
    blob_uid                 BIGINT,
    multipart_uid            BIGINT,
    timestamp                TIMESTAMP NOT NULL,

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
