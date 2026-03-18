# Event File Consumer Guide

## Bucket layout

```
{bucket_root}/
  metadata.json                          # root metadata
  0/                                     # folder 0
    metadata.json                        # folder metadata
    {first_version}.pb.lz4              # data file
    {first_version}.pb.lz4              # ...
  1/                                     # folder 1
    metadata.json
    ...
```

Folders are numbered sequentially starting from 0. Each folder holds up to `max_txns_per_folder` filtered transactions. Once full, `is_complete` is set and a new folder begins. The folder prefix does not correspond to txn ledger version or anything like that (unlike the filestore worker in indexer-grpc-v2). Doing so would make folders too sparse.

## Data files

Each data file is a serialized `EventFile` proto (see `processor/proto/indexer/v1/event_file.proto`) containing a flat list of `EventWithContext` messages. The proto definition for `Event` is from [aptos-core](https://github.com/aptos-labs/aptos-core/tree/main/protos/proto/aptos/transaction/v1).

**Format & compression** are declared in root `metadata.json` under `config.output_format` and `config.compression`. Current options:

| output_format | compression | extension    |
|---------------|-------------|--------------|
| `protobuf`    | `lz4`       | `.pb.lz4`    |
| `protobuf`    | `none`      | `.pb`        |
| `json`        | `lz4`       | `.json.lz4`  |
| `json`        | `none`      | `.json`      |

LZ4 compression uses the standard **LZ4 frame format** (magic bytes `\x04\x22\x4D\x18`). Files can be decompressed with the `lz4` CLI (`lz4 -d file.pb.lz4 file.pb`) or any LZ4 frame-compatible library (e.g. `lz4_flex::frame` in Rust, `lz4.frame` in Python).

## Key invariants

### Write ordering

The writer follows this sequence on each flush:

1. **Data file written** to `{folder}/{version}{ext}`.
2. **Folder `metadata.json` updated** — the new file appears in `files[]`.
3. **Root `metadata.json` updated**.

**If you take away anything, let it be this**: If a file appears in folder metadata, the data file has already been fully written and will never be modified. Folder metadata is always updated *after* the data file is persisted. You can safely read any file listed in folder metadata.

### Data files are immutable once mentioned in metadata

Once a data file is written it is **never modified or rewritten** under normal operation. You can cache them indefinitely. After a crash, an orphaned file (written but not yet in metadata) may be overwritten on recovery, but files referenced by metadata are final.

### Metadata files are mutable

Both `metadata.json` (root and folder) are **overwritten in place** as new data arrives. Always re-read them to get the latest state. Once a folder is sealed (`is_complete: true`), it is **never modified** again.

### Caching

Metadata objects are written with `Cache-Control: no-store` so GCS does not serve stale copies. Data files (immutable) use the GCS default (`public, max-age=3600`).

### Complete transactions

Every data file contains **complete transactions** — if any event from a transaction appears in a file, all matching events from that transaction are in the same file. Flushes only happen at transaction boundaries.

### Version semantics

All `version` fields use the Aptos transaction ledger version (a globally unique, monotonically increasing u64).

| Field | Meaning |
|-------|---------|
| `root.latest_committed_version` | All matching events with `version <= latest_committed_version` have been written to files. If this is 12, versions 0–12 are fully covered (though only versions matching the filter will have events). Root metadata is only written after the first successful flush, so this field is always meaningful. |
| `root.latest_processed_version` | The processor has scanned all transactions with `version <= latest_processed_version`. May be ahead of `latest_committed_version` during stretches with no matching events. Tells you how far the indexer has progressed regardless of event density. |
| `folder_metadata.first_version` | Version of the first event in this folder (inclusive). |
| `folder_metadata.last_version` | Version of the last event in this folder (inclusive). |
| `file.first_version` | Version of the first event in this file (inclusive). Also encoded in the filename. |
| `file.last_version` | Version of the last event in this file (inclusive). |

All version fields are **inclusive** — they are the actual transaction versions of the first and last events. For example, if `first_version` is 10 and `last_version` is 12, the file contains events from versions 10, 11, and 12 (though there may be gaps — not every version in the range necessarily has an event).

### Folder lifecycle

- `is_complete: false` — the folder is still being written to.
- `is_complete: true` — the folder is sealed. No more files will be added. A new folder with `folder_index + 1` has been (or will be) created.

A folder transitions to complete when its accumulated filtered transaction
count reaches `max_txns_per_folder`.

### Gaps in versions

There may be **large gaps** between consecutive `version` values within and across files. The processor only writes events matching its configured filters (specific module addresses, module names, event names). Transactions without matching events are skipped entirely. Use `latest_processed_version` on root metadata to see how far the indexer has scanned regardless of event density.

### Event filtering

The filters applied are stored in `root.config.event_filter_config`. Only events from **successful** transactions matching these filters are included. Events from failed transactions are always excluded.

### Config immutability

The fields under `root.config` (`event_filter_config`, `output_format`, `compression`, `max_txns_per_folder`) are **immutable** for the lifetime of a bucket. The processor refuses to start if they differ from what's stored. If you see these values in root metadata, they apply to every file in the bucket.

## Fetching strategy

This assumes that you want to download all the data, rather than some kind of polling approach. This describes the simplest approach.

1. Clone the entire bucket.
2. Delete the folder with the highest index since it may be incomplete.

The rest of the data will be a complete, consistent set of events. You can look at `metadata.json` in the highest index folder to see what the latest txn is.

If you want to be less wasteful, you could keep the latest folder and parse the `metadata.json` file in that folder based on the above rules.

### Example script

This implements the above dumb approach.

```bash
#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  download_events.sh -s <rclone_source> -d <destination_dir>

Example:
  download_events.sh \
    -s ':gcs:aptos-indexer-event-files-shelbynet/shelbynet' \
    -d './shelbynet'
EOF
}

human_size() {
  du -sh "$1" | awk '{print $1}'
}

bytes_size() {
  du -sb "$1" | awk '{print $1}'
}

format_duration_ms() {
  local total_ms="$1"
  awk -v ms="$total_ms" 'BEGIN {
    minutes = int(ms / 60000)
    seconds = (ms % 60000) / 1000
    printf "%dm %.3fs", minutes, seconds
  }'
}

SRC=""
DEST=""

while getopts ":s:d:h" opt; do
  case "$opt" in
    s) SRC="$OPTARG" ;;
    d) DEST="$OPTARG" ;;
    h)
      usage
      exit 0
      ;;
    \?)
      echo "Error: invalid option -$OPTARG" >&2
      usage >&2
      exit 1
      ;;
    :)
      echo "Error: option -$OPTARG requires an argument" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "$SRC" || -z "$DEST" ]]; then
  echo "Error: both -s and -d are required" >&2
  usage >&2
  exit 1
fi

command -v rclone >/dev/null 2>&1 || { echo "rclone not found"; exit 1; }
command -v lz4 >/dev/null 2>&1 || { echo "lz4 not found"; exit 1; }
command -v find >/dev/null 2>&1 || { echo "find not found"; exit 1; }
command -v du >/dev/null 2>&1 || { echo "du not found"; exit 1; }
command -v date >/dev/null 2>&1 || { echo "date not found"; exit 1; }
command -v awk >/dev/null 2>&1 || { echo "awk not found"; exit 1; }

mkdir -p "$DEST"

echo "Downloading from: $SRC"
echo "Destination: $DEST"

download_start_ms=$(python3 - <<'PY'
import time
print(int(time.time() * 1000))
PY
)

rclone copy \
  --gcs-anonymous \
  --transfers 64 \
  --checkers 64 \
  --fast-list \
  --no-traverse \
  "$SRC" "$DEST"

download_end_ms=$(python3 - <<'PY'
import time
print(int(time.time() * 1000))
PY
)
download_ms=$((download_end_ms - download_start_ms))

echo "Looking for highest-numbered folder under $DEST ..."
highest_dir=""
highest_idx=""

while IFS= read -r -d '' dir; do
  base="$(basename "$dir")"
  if [[ "$base" =~ ^[0-9]+$ ]]; then
    if [[ -z "$highest_idx" || "$base" -gt "$highest_idx" ]]; then
      highest_idx="$base"
      highest_dir="$dir"
    fi
  fi
done < <(find "$DEST" -mindepth 1 -maxdepth 1 -type d -print0)

if [[ -n "$highest_dir" ]]; then
  echo "Removing highest-numbered folder (may be incomplete): $highest_dir"
  rm -rf -- "$highest_dir"
else
  echo "No numbered top-level folders found under $DEST"
fi

before_human="$(human_size "$DEST")"
before_bytes="$(bytes_size "$DEST")"

echo "Size on disk before decompressing: $before_human ($before_bytes bytes)"

decompress_start_ms=$(python3 - <<'PY'
import time
print(int(time.time() * 1000))
PY
)

find "$DEST" -type f \( -name '*.pb.lz4' -o -name '*.json.lz4' \) -print0 |
while IFS= read -r -d '' file; do
  out="${file%.lz4}"
  echo "Decompressing: $file -> $out"
  lz4 -d --rm "$file" "$out"
done

decompress_end_ms=$(python3 - <<'PY'
import time
print(int(time.time() * 1000))
PY
)
decompress_ms=$((decompress_end_ms - decompress_start_ms))

after_human="$(human_size "$DEST")"
after_bytes="$(bytes_size "$DEST")"

echo
echo "Summary"
echo "-------"
echo "Source:                       $SRC"
echo "Destination:                  $DEST"
if [[ -n "$highest_dir" ]]; then
  echo "Removed highest folder:       $highest_dir"
else
  echo "Removed highest folder:       none"
fi
echo "Size before decompressing:    $before_human ($before_bytes bytes)"
echo "Size after decompressing:     $after_human ($after_bytes bytes)"
echo "Download time:                $(format_duration_ms "$download_ms") (${download_ms} ms)"
echo "Decompression time:           $(format_duration_ms "$decompress_ms") (${decompress_ms} ms)"
echo "Done."
```