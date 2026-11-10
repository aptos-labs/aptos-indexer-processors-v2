-- The on-chain write format a row was recorded in, bumped at each
-- compatibility transition in the contract's write path:
--   1: `commit_object`, `commit_object_part`, `finalize_multipart_object`. The
--      chain derives a single-blob object's etag from its blob commitment.
--   2: `commit_object_v2`, `commit_object_part_v2`,
--      `finalize_multipart_object_v2`. Adds caller-supplied single-blob etags
--      and opaque metadata on commits.
-- The default covers rows written before this column and writers that predate
-- it, all of which only ever see format 1 events.
ALTER TABLE shelby_objects
    ADD COLUMN format_version INTEGER NOT NULL DEFAULT 1;

ALTER TABLE shelby_open_multipart_parts
    ADD COLUMN format_version INTEGER NOT NULL DEFAULT 1;
