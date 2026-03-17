#!/usr/bin/env python3

# Copyright (c) Aptos Foundation
# Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

import os
import re
import sys

LICENSE_SLASH = (
    "// Copyright (c) Aptos Foundation\n"
    "// Licensed pursuant to the Innovation-Enabling Source Code License, "
    "available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE"
)

LICENSE_HASH = (
    "# Copyright (c) Aptos Foundation\n"
    "# Licensed pursuant to the Innovation-Enabling Source Code License, "
    "available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE"
)

EXTENSIONS_SLASH = {".rs", ".mjs"}
EXTENSIONS_HASH = {".sh"}
ALL_EXTENSIONS = EXTENSIONS_SLASH | EXTENSIONS_HASH

EXCLUDE_DIRS = {"target", "node_modules", ".git"}

# Files that are auto-generated and should be skipped.
EXCLUDE_FILES = {"schema.rs"}

STALE_HEADER_PATTERNS = [
    re.compile(r"^//\s*SPDX-License-Identifier.*$"),
    re.compile(r"^#\s*SPDX-License-Identifier.*$"),
]

OLD_HEADER_PATTERNS = [
    re.compile(r"^//\s*Copyright.*$"),
    re.compile(r"^#\s*Copyright.*$"),
    *STALE_HEADER_PATTERNS,
    re.compile(r"^//\s*Licensed\s+pursuant.*$"),
    re.compile(r"^#\s*Licensed\s+pursuant.*$"),
]


def get_license(ext):
    if ext in EXTENSIONS_SLASH:
        return LICENSE_SLASH
    if ext in EXTENSIONS_HASH:
        return LICENSE_HASH
    return None


def has_correct_license(lines, license_text):
    """Check if the file already starts with the correct license (after optional shebang)."""
    license_lines = license_text.split("\n")
    start = 0

    if lines and lines[0].startswith("#!"):
        start = 1
        while start < len(lines) and lines[start].strip() == "":
            start += 1

    for i, expected in enumerate(license_lines):
        idx = start + i
        if idx >= len(lines) or lines[idx] != expected:
            return False

    # Also reject files that have stale SPDX or extra copyright lines after the new license.
    after = start + len(license_lines)
    while after < len(lines) and lines[after].strip() == "":
        after += 1
    if after < len(lines) and is_old_header_line(lines[after]):
        return False

    return True


def is_old_header_line(line):
    return any(p.match(line) for p in OLD_HEADER_PATTERNS)


def fix_license(lines, license_text):
    """Return new lines with old license removed and new license prepended."""
    license_lines = license_text.split("\n")
    start = 0
    shebang = None

    if lines and lines[0].startswith("#!"):
        shebang = lines[0]
        start = 1

    # Skip blank lines after shebang or at the very top.
    while start < len(lines) and lines[start].strip() == "":
        start += 1

    # Remove old copyright / license header lines, including blank lines between them.
    made_progress = True
    while made_progress:
        made_progress = False
        while start < len(lines) and is_old_header_line(lines[start]):
            start += 1
            made_progress = True
        while start < len(lines) and lines[start].strip() == "":
            start += 1
            made_progress = True

    result = []
    if shebang is not None:
        result.append(shebang)
        result.append("")
    result.extend(license_lines)
    result.append("")  # blank line after license

    result.extend(lines[start:])
    return result


def find_files(root):
    result = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in EXCLUDE_DIRS]
        for f in filenames:
            if f in EXCLUDE_FILES:
                continue
            ext = os.path.splitext(f)[1]
            if ext in ALL_EXTENSIONS:
                result.append(os.path.join(dirpath, f))
    return sorted(result)


def main():
    check_mode = "--check" in sys.argv
    root = "."
    files = find_files(root)

    errors = 0
    fixed = 0

    for filepath in files:
        ext = os.path.splitext(filepath)[1]
        license_text = get_license(ext)
        if license_text is None:
            continue

        with open(filepath, "r") as f:
            content = f.read()

        # Split into lines, preserving the trailing newline status.
        lines = content.split("\n")
        # Remove the phantom empty string from trailing newline.
        if lines and lines[-1] == "":
            lines = lines[:-1]

        if has_correct_license(lines, license_text):
            continue

        if check_mode:
            print(f"Missing or incorrect license header: {filepath}")
            errors += 1
        else:
            new_lines = fix_license(lines, license_text)
            with open(filepath, "w") as f:
                f.write("\n".join(new_lines) + "\n")
            print(f"Fixed: {filepath}")
            fixed += 1

    if check_mode and errors > 0:
        print(f"\n{errors} file(s) have missing or incorrect license headers.")
        print("Run `python3 scripts/check_license.py` to fix them.")
        sys.exit(1)
    elif check_mode:
        print("All files have correct license headers.")
    else:
        if fixed > 0:
            print(f"\nFixed {fixed} file(s).")
        else:
            print("All files already have correct license headers.")


if __name__ == "__main__":
    main()
