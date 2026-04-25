// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstdint>
#include <string>

#include "common/status.h"
#include "gen_cpp/persistent_index.pb.h"

namespace starrocks {
namespace lake {

// On-disk framing for a `LakePersistentIndexSnapshotMetaPB` blob. Layout (little-endian):
//
//   offset  size  field
//   ------  ----  ------------------------------------------------------------
//        0    8   magic = "SRPISNP1" (kSnapshotMagic, ASCII, no NUL)
//        8    4   format_version (kSnapshotFormatVersion at write time)
//       12    4   meta_pb_size (bytes of the serialised proto that follows)
//       16    N   meta_pb_bytes (serialised LakePersistentIndexSnapshotMetaPB)
//   16 + N    4   crc32c over bytes [0, 16 + N) (header + payload, excludes the
//                 checksum itself). Computed via base/hash/crc32c.h.
//
// Readers must verify magic, format_version <= kSnapshotFormatVersion, and the
// CRC before parsing the proto. Any mismatch yields a non-OK status; callers
// fall back to the full cold-rebuild path on any error.
//
// PR-2 ships only the framing, the proto, and these I/O helpers. PR-3 wires
// them into `LakePersistentIndex::try_serialize_to_local_snapshot` /
// `try_restore_from_local_snapshot` and adds the validity-rule checks.

static constexpr char kSnapshotMagic[] = "SRPISNP1";
static constexpr size_t kSnapshotMagicLen = 8; // strlen("SRPISNP1"), no NUL
static constexpr uint32_t kSnapshotFormatVersion = 1;
static constexpr size_t kSnapshotHeaderLen = kSnapshotMagicLen + sizeof(uint32_t) + sizeof(uint32_t);
static constexpr size_t kSnapshotChecksumLen = sizeof(uint32_t);

// Write `meta` to `path` using the format above. Truncates an existing file.
// Returns IOError on filesystem failure; the file is fsynced before return so
// readers cannot observe a partial blob after a crash.
Status write_lake_persistent_index_snapshot(const std::string& path, const LakePersistentIndexSnapshotMetaPB& meta);

// Read and validate a snapshot file produced by `write_lake_persistent_index_snapshot`.
// On success, `meta` is populated. Returns Corruption on magic / format_version /
// CRC / parse mismatch; NotFound when the file does not exist; IOError on other
// filesystem failures.
Status read_lake_persistent_index_snapshot(const std::string& path, LakePersistentIndexSnapshotMetaPB* meta);

// Derive the on-disk path for the snapshot of `tablet_id` at `captured_version`.
// Layout: `<root>/lake_pk_snapshot/<tablet_id>/v<captured_version>.snapshot` where
// `<root>` is `config::pk_index_snapshot_local_dir` if non-empty, or the first
// `config::storage_root_path` entry otherwise. Returns InvalidArgument when no
// usable root path can be derived.
Status get_lake_persistent_index_snapshot_path(int64_t tablet_id, int64_t captured_version, std::string* path);

// Derive the per-tablet snapshot root: `<root>/lake_pk_snapshot/`. Same root rules as
// `get_lake_persistent_index_snapshot_path`. Trailing slash is included.
Status get_lake_persistent_index_snapshot_root(std::string* root);

// Walk the snapshot tree under `snapshot_root` (`<root>/lake_pk_snapshot/`) and remove
// any `v<version>.snapshot` file whose mtime is older than `max_age_sec` seconds. A
// `max_age_sec <= 0` is treated as "GC disabled" and the function returns OK without
// touching the filesystem. `removed_count` (optional) reports how many files were
// deleted in this pass; useful for tests and operator visibility. Best-effort: per-file
// failures are logged but do not abort the pass.
Status gc_stale_lake_persistent_index_snapshots(const std::string& snapshot_root, int64_t max_age_sec,
                                                int64_t* removed_count = nullptr);

// Pure helper that decides whether a previously captured snapshot is safe to use
// for a load at `expected_version` on `expected_tablet_id` with `expected_schema_id`.
// `now_unix_sec` and `max_age_sec` drive the age check (set `max_age_sec <= 0` to
// disable). Returns OK on a usable snapshot; NotFound on any incompatibility so
// the caller falls back to the full cold-rebuild path.
Status validate_lake_persistent_index_snapshot(const LakePersistentIndexSnapshotMetaPB& meta,
                                               int64_t expected_tablet_id, int64_t expected_version,
                                               int64_t expected_schema_id, int64_t now_unix_sec, int64_t max_age_sec);

} // namespace lake
} // namespace starrocks
