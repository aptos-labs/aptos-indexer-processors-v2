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

Folders are numbered sequentially starting from 0. Each folder holds up to `max_txns_per_folder` filtered transactions. Once full, `is_sealed` is set and a new folder begins. The folder prefix does not correspond to txn ledger version or anything like that (unlike the filestore worker in indexer-grpc-v2). Doing so would make folders too sparse.

## Data files

Each data file is a serialized `EventFile` proto (see `processor/proto/aptos/indexer/event_file/v1/event_file.proto`) containing a flat list of `EventWithContext` messages. The proto definition for `Event` is from [aptos-core](https://github.com/aptos-labs/aptos-core/tree/main/protos/proto/aptos/transaction/v1).

**Format & compression** are declared in root `metadata.json` under `config.output_format` and `config.compression`. Current options:

| output_format | compression | extension    |
|---------------|-------------|--------------|
| `protobuf`    | `lz4`       | `.pb.lz4`    |
| `protobuf`    | `none`      | `.pb`        |
| `json`        | `lz4`       | `.json.lz4`  |
| `json`        | `none`      | `.json`      |

LZ4 compression uses the standard **LZ4 frame format** (magic bytes `\x04\x22\x4D\x18`). Files can be decompressed with the `lz4` CLI (`lz4 -d file.pb.lz4 file.pb`) or any LZ4 frame-compatible library (e.g. `lz4_flex::frame` in Rust, `lz4.frame` in Python).

## Root metadata structure

Root `metadata.json` has two top-level keys:

- **`config`** — immutable identity, set once when the data store is created. Consumers can hash this block to detect whether the data store has changed.
- **`tracking`** — mutable version-tracking state, updated on every flush.

```json
{
  "config": {
    "chain_id": 2,
    "initial_starting_version": 0,
    "event_filter_config": {
      "filters": [
        { "module_address": "0xbcdef...", "module_name": "emojicoin_dot_fun", "event_name": null }
      ]
    },
    "output_format": "protobuf",
    "compression": "lz4",
    "max_txns_per_folder": 100000,
    "max_file_size_bytes": 52428800,
    "max_seconds_between_flushes": 600
  },
  "tracking": {
    "latest_committed_version": 123456,
    "latest_processed_version": 130000,
    "current_folder_index": 3,
    "current_folder_txn_count": 4200
  }
}
```

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

Both `metadata.json` (root and folder) are **overwritten in place** as new data arrives. Always re-read them to get the latest state. Once a folder is sealed (`is_sealed: true`), it is **never modified** again.

### Caching

Metadata objects are written with `Cache-Control: no-store` so GCS does not serve stale copies. Data files (immutable) use the GCS default (`public, max-age=3600`).

### Complete transactions

Every data file contains **complete transactions** — if any event from a transaction appears in a file, all matching events from that transaction are in the same file. Flushes only happen at transaction boundaries.

### Version semantics

All `version` fields use the Aptos transaction ledger version (a globally unique, monotonically increasing u64).

**Root tracking fields** (under `root.tracking`):

| Field | Meaning |
|-------|---------|
| `latest_committed_version` | All matching events with `version <= latest_committed_version` have been written to files. If this is 12, versions 0–12 are fully covered (though only versions matching the filter will have events). |
| `latest_processed_version` | The processor has scanned all transactions with `version <= latest_processed_version`. May be ahead of `latest_committed_version` during stretches with no matching events. Tells you how far the indexer has progressed regardless of event density. |
| `current_folder_index` | Index of the folder currently being written to. |
| `current_folder_txn_count` | Number of filtered transactions accumulated in the current (possibly incomplete) folder. |

**Folder metadata fields** (per-folder `metadata.json`):

| Field | Meaning |
|-------|---------|
| `first_version` | Version of the first event in this folder (inclusive). |
| `last_version` | Version of the last event in this folder (inclusive). |
| `total_transactions` | Total number of filtered transactions across all files in the folder. |
| `is_sealed` | Whether the folder is sealed (see folder lifecycle below). |

**File metadata fields** (entries in `folder_metadata.files[]`):

| Field | Meaning |
|-------|---------|
| `filename` | Name of the data file, e.g. `10.pb.lz4`. Encodes `{first_version}{ext}`. |
| `first_version` | Version of the first event in this file (inclusive). Also encoded in the filename. |
| `last_version` | Version of the last event in this file (inclusive). |
| `num_events` | Total number of events in this file. |
| `num_transactions` | Number of distinct filtered transactions that contributed events. |
| `size_bytes` | Size of the serialized (and possibly compressed) file in bytes. |

All version fields are **inclusive** — they are the actual transaction versions of the first and last events. For example, if `first_version` is 10 and `last_version` is 12, the file contains events from versions 10, 11, and 12 (though there may be gaps — not every version in the range necessarily has an event).

### Folder lifecycle

- `is_sealed: false` — the folder is still being written to.
- `is_sealed: true` — the folder is sealed. No more files will be added. A new folder with `folder_index + 1` has been (or will be) created.

A folder transitions to complete when its accumulated filtered transaction count reaches `max_txns_per_folder`.

### Gaps in versions

There may be **large gaps** between consecutive `version` values within and across files. The processor only writes events matching its configured filters (specific module addresses, module names, event names). Transactions without matching events are skipped entirely. Use `tracking.latest_processed_version` in root metadata to see how far the indexer has scanned regardless of event density.

### Event filtering

The filters applied are stored in `root.config.event_filter_config`. Only events from **successful** transactions matching these filters are included. Events from failed transactions are always excluded.

### Config identity and immutability

The `root.config` block is **immutable** for the lifetime of a data store. It contains:

- `chain_id` — the Aptos chain this data was indexed from.
- `initial_starting_version` — the ledger version the processor was configured to start from.
- `event_filter_config` — the event filters determining which events are included.
- `output_format`, `compression` — serialization format and compression of data files.
- `max_txns_per_folder`, `max_file_size_bytes`, `max_seconds_between_flushes` — flush/folder tuning.

The processor refuses to start if the processor config fields differ from what is stored. All fields in `root.config` apply to every file in the bucket.

**Consumer identity check**: you can hash or fingerprint the entire `config` JSON block to detect whether you are reading from the same data store as before. If the hash changes, the data store's identity has changed (different chain, different filters, different starting version, etc.).

## Fetching strategy

This assumes that you want to download all the data, rather than some kind of polling approach.

1. Clone the entire bucket.
2. For the highest-numbered folder, check its `metadata.json`:
   - If `is_sealed` is `true`, the folder is sealed and safe to use as-is.
   - If `is_sealed` is `false`, the folder is still being written to. Every file listed in its `files[]` array is fully written and safe to read (see write ordering above), but there may be orphaned data files on disk that are **not** listed in the metadata. Delete any data files not referenced by the metadata.
   - If there is no `metadata.json` at all, the folder has no committed data. Delete it.

### Example script

Downloads the bucket and trims the highest folder to only files referenced by
its `metadata.json` (safe even when there is only one folder).

```bash
#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  download_events.sh -s <rclone_source> -d <destination_dir>

Example:
  download_events.sh \
    -s ':gcs:aptos-indexer-event-files-shelbynet/shelbynet/v2' \
    -d './out'
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
command -v python3 >/dev/null 2>&1 || { echo "python3 not found"; exit 1; }

mkdir -p "$DEST"

echo "Downloading from: $SRC"
echo "Destination: $DEST"

download_start_ms=$(python3 -c 'import time; print(int(time.time() * 1000))')

rclone copy \
  --gcs-anonymous \
  --transfers 64 \
  --checkers 64 \
  --fast-list \
  "$SRC" "$DEST"

download_end_ms=$(python3 -c 'import time; print(int(time.time() * 1000))')
download_ms=$((download_end_ms - download_start_ms))

# --- Trim the highest-numbered folder to only metadata-referenced files ---
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

trimmed_info="none"
if [[ -n "$highest_dir" ]]; then
  meta="$highest_dir/metadata.json"
  if [[ -f "$meta" ]]; then
    is_sealed=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['is_sealed'])" "$meta")
    if [[ "$is_sealed" == "True" ]]; then
      echo "Highest folder $highest_idx is sealed (is_sealed=true), keeping it."
      trimmed_info="folder $highest_idx: sealed, kept as-is"
    else
      echo "Highest folder $highest_idx is incomplete, trimming to metadata-referenced files ..."
      # Build set of filenames listed in metadata, then delete anything else.
      keep_files=$(python3 -c "
import json, sys
m = json.load(open(sys.argv[1]))
for f in m.get('files', []):
    print(f['filename'])
" "$meta")
      removed=0
      while IFS= read -r -d '' f; do
        fname="$(basename "$f")"
        if [[ "$fname" == "metadata.json" ]]; then
          continue
        fi
        if ! echo "$keep_files" | grep -qxF "$fname"; then
          rm -f "$f"
          removed=$((removed + 1))
        fi
      done < <(find "$highest_dir" -maxdepth 1 -type f -print0)
      trimmed_info="folder $highest_idx: incomplete, removed $removed orphaned file(s)"
      echo "$trimmed_info"
    fi
  else
    echo "Highest folder $highest_idx has no metadata.json, removing entirely."
    rm -rf -- "$highest_dir"
    trimmed_info="folder $highest_idx: no metadata, removed"
  fi
else
  echo "No numbered top-level folders found under $DEST"
fi

before_human="$(human_size "$DEST")"
before_bytes="$(bytes_size "$DEST")"

echo "Size on disk before decompressing: $before_human ($before_bytes bytes)"

decompress_start_ms=$(python3 -c 'import time; print(int(time.time() * 1000))')

find "$DEST" -type f \( -name '*.pb.lz4' -o -name '*.json.lz4' \) -print0 |
while IFS= read -r -d '' file; do
  out="${file%.lz4}"
  echo "Decompressing: $file -> $out"
  lz4 -d --rm "$file" "$out"
done

decompress_end_ms=$(python3 -c 'import time; print(int(time.time() * 1000))')
decompress_ms=$((decompress_end_ms - decompress_start_ms))

after_human="$(human_size "$DEST")"
after_bytes="$(bytes_size "$DEST")"

echo
echo "Summary"
echo "-------"
echo "Source:                       $SRC"
echo "Destination:                  $DEST"
echo "Highest folder:               $trimmed_info"
echo "Size before decompressing:    $before_human ($before_bytes bytes)"
echo "Size after decompressing:     $after_human ($after_bytes bytes)"
echo "Download time:                $(format_duration_ms "$download_ms") (${download_ms} ms)"
echo "Decompression time:           $(format_duration_ms "$decompress_ms") (${decompress_ms} ms)"
echo "Done."
```