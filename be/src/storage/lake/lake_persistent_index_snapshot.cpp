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

#include "storage/lake/lake_persistent_index_snapshot.h"

#include <chrono>
#include <cstring>
#include <limits>
#include <string>

#include "base/coding.h"
#include "base/hash/crc32c.h"
#include "common/config.h"
#include "fs/fs.h"
#include "fs/fs_factory.h"
#include "fs/fs_util.h"
#include "gen_cpp/lake_types.pb.h"

namespace starrocks {
namespace lake {

namespace {

// Pack header (magic + format_version + meta_pb_size) into a fixed-size buffer.
void encode_header(uint32_t format_version, uint32_t meta_pb_size, char buf[kSnapshotHeaderLen]) {
    std::memcpy(buf, kSnapshotMagic, kSnapshotMagicLen);
    encode_fixed32_le(reinterpret_cast<uint8_t*>(buf + kSnapshotMagicLen), format_version);
    encode_fixed32_le(reinterpret_cast<uint8_t*>(buf + kSnapshotMagicLen + sizeof(uint32_t)), meta_pb_size);
}

} // namespace

Status write_lake_persistent_index_snapshot(const std::string& path, const LakePersistentIndexSnapshotMetaPB& meta) {
    std::string payload;
    if (!meta.SerializeToString(&payload)) {
        return Status::InternalError("failed to serialise LakePersistentIndexSnapshotMetaPB");
    }
    if (payload.size() > std::numeric_limits<uint32_t>::max()) {
        return Status::InvalidArgument("snapshot payload exceeds 4 GiB");
    }

    char header[kSnapshotHeaderLen];
    encode_header(kSnapshotFormatVersion, static_cast<uint32_t>(payload.size()), header);

    uint32_t crc = crc32c::Value(header, kSnapshotHeaderLen);
    crc = crc32c::Extend(crc, payload.data(), payload.size());

    char crc_buf[kSnapshotChecksumLen];
    encode_fixed32_le(reinterpret_cast<uint8_t*>(crc_buf), crc);

    WritableFileOptions wopts;
    wopts.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE;
    ASSIGN_OR_RETURN(auto wf, fs::new_writable_file(wopts, path));
    Slice frames[3] = {Slice(header, kSnapshotHeaderLen), Slice(payload), Slice(crc_buf, kSnapshotChecksumLen)};
    RETURN_IF_ERROR(wf->appendv(frames, 3));
    RETURN_IF_ERROR(wf->sync());
    return wf->close();
}

Status read_lake_persistent_index_snapshot(const std::string& path, LakePersistentIndexSnapshotMetaPB* meta) {
    if (meta == nullptr) {
        return Status::InvalidArgument("meta out-param is null");
    }
    ASSIGN_OR_RETURN(auto rf, fs::new_random_access_file(path));
    ASSIGN_OR_RETURN(const int64_t file_size, rf->get_size());
    if (file_size < static_cast<int64_t>(kSnapshotHeaderLen + kSnapshotChecksumLen)) {
        return Status::Corruption("snapshot file too small for header + checksum");
    }

    char header[kSnapshotHeaderLen];
    RETURN_IF_ERROR(rf->read_at_fully(0, header, kSnapshotHeaderLen));
    if (std::memcmp(header, kSnapshotMagic, kSnapshotMagicLen) != 0) {
        return Status::Corruption("snapshot magic mismatch");
    }
    const uint32_t format_version = decode_fixed32_le(reinterpret_cast<const uint8_t*>(header + kSnapshotMagicLen));
    if (format_version > kSnapshotFormatVersion) {
        return Status::Corruption("snapshot format_version newer than reader supports");
    }
    const uint32_t meta_pb_size =
            decode_fixed32_le(reinterpret_cast<const uint8_t*>(header + kSnapshotMagicLen + sizeof(uint32_t)));
    if (file_size != static_cast<int64_t>(kSnapshotHeaderLen + meta_pb_size + kSnapshotChecksumLen)) {
        return Status::Corruption("snapshot file size does not match declared meta_pb_size");
    }

    std::string payload(meta_pb_size, '\0');
    RETURN_IF_ERROR(rf->read_at_fully(kSnapshotHeaderLen, payload.data(), meta_pb_size));

    char crc_buf[kSnapshotChecksumLen];
    RETURN_IF_ERROR(rf->read_at_fully(kSnapshotHeaderLen + meta_pb_size, crc_buf, kSnapshotChecksumLen));
    const uint32_t stored_crc = decode_fixed32_le(reinterpret_cast<const uint8_t*>(crc_buf));
    uint32_t expected_crc = crc32c::Value(header, kSnapshotHeaderLen);
    expected_crc = crc32c::Extend(expected_crc, payload.data(), payload.size());
    if (stored_crc != expected_crc) {
        return Status::Corruption("snapshot crc32c mismatch");
    }

    if (!meta->ParseFromString(payload)) {
        return Status::Corruption("failed to parse LakePersistentIndexSnapshotMetaPB");
    }
    return Status::OK();
}

int64_t parse_snapshot_version_from_filename(std::string_view name) {
    constexpr std::string_view kSuffix = ".snapshot";
    if (name.size() <= kSuffix.size() + 1 || name[0] != 'v' ||
        name.substr(name.size() - kSuffix.size()) != kSuffix) {
        return -1;
    }
    int64_t version = 0;
    for (size_t i = 1; i < name.size() - kSuffix.size(); i++) {
        const char c = name[i];
        if (c < '0' || c > '9') {
            return -1;
        }
        // Bound the accumulator by INT63_MAX/10 so digit overflow is detected before it wraps.
        if (version > (std::numeric_limits<int64_t>::max() - 9) / 10) {
            return -1;
        }
        version = version * 10 + (c - '0');
    }
    return version;
}

Status get_lake_persistent_index_snapshot_root(std::string* root) {
    if (root == nullptr) {
        return Status::InvalidArgument("root out-param is null");
    }
    std::string base = config::pk_index_snapshot_local_dir;
    if (base.empty()) {
        const std::string& roots = config::storage_root_path;
        if (roots.empty()) {
            return Status::InvalidArgument("no storage_root_path configured for pk-index snapshot");
        }
        size_t semi = roots.find(';');
        base = (semi == std::string::npos) ? roots : roots.substr(0, semi);
        // Trim a trailing comma-suffix used by some deployments (e.g. "/data;medium:HDD").
        size_t comma = base.find(',');
        if (comma != std::string::npos) {
            base = base.substr(0, comma);
        }
    }
    if (base.empty()) {
        return Status::InvalidArgument("derived snapshot root path is empty");
    }
    if (base.back() == '/') {
        base.pop_back();
    }
    *root = base + "/lake_pk_snapshot/";
    return Status::OK();
}

Status get_lake_persistent_index_snapshot_path(int64_t tablet_id, int64_t captured_version, std::string* path) {
    if (path == nullptr) {
        return Status::InvalidArgument("path out-param is null");
    }
    std::string root;
    RETURN_IF_ERROR(get_lake_persistent_index_snapshot_root(&root));
    *path = root + std::to_string(tablet_id) + "/v" + std::to_string(captured_version) + ".snapshot";
    return Status::OK();
}

Status gc_stale_lake_persistent_index_snapshots(const std::string& snapshot_root, int64_t max_age_sec,
                                                int64_t* removed_count) {
    if (removed_count != nullptr) {
        *removed_count = 0;
    }
    if (max_age_sec <= 0) {
        return Status::OK();
    }
    if (snapshot_root.empty()) {
        return Status::InvalidArgument("snapshot_root is empty");
    }
    if (!fs::path_exist(snapshot_root)) {
        // Nothing to GC yet — directory is created lazily when the first snapshot is written.
        return Status::OK();
    }
    ASSIGN_OR_RETURN(auto fs, FileSystemFactory::CreateSharedFromString(snapshot_root));
    const int64_t now_sec =
            std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch())
                    .count();

    // Top-level entries under <root>/lake_pk_snapshot/ are per-tablet directories.
    std::vector<std::string> tablet_dirs;
    Status iter_st = fs->iterate_dir(snapshot_root, [&](std::string_view name) -> bool {
        if (name == "." || name == "..") return true;
        tablet_dirs.emplace_back(name);
        return true;
    });
    if (!iter_st.ok()) {
        return iter_st;
    }

    for (const auto& tablet_subdir : tablet_dirs) {
        const std::string tablet_dir = snapshot_root + tablet_subdir;
        // Skip non-directories defensively (older deployments may have left flat files).
        auto is_dir_or = fs->is_directory(tablet_dir);
        if (!is_dir_or.ok() || !is_dir_or.value()) {
            continue;
        }
        Status per_tablet_iter = fs->iterate_dir2(tablet_dir, [&](DirEntry entry) -> bool {
            if (entry.name == "." || entry.name == "..") return true;
            if (entry.is_dir.value_or(false)) return true;
            // Only touch snapshot files; ignore unknown content (someone may use the
            // same root for other purposes; principle of least surprise).
            if (entry.name.size() <= 9 || entry.name.substr(entry.name.size() - 9) != ".snapshot") {
                return true;
            }
            if (!entry.mtime.has_value()) {
                // Cannot evaluate age — skip rather than risk deleting fresh files.
                return true;
            }
            const int64_t age = now_sec - entry.mtime.value();
            if (age <= max_age_sec) {
                return true;
            }
            const std::string path = tablet_dir + "/" + std::string(entry.name);
            Status del_st = fs->delete_file(path);
            if (del_st.ok()) {
                if (removed_count != nullptr) {
                    (*removed_count)++;
                }
            } else {
                LOG(WARNING) << "gc_stale_lake_persistent_index_snapshots: failed to delete " << path << ": "
                             << del_st.to_string();
            }
            return true;
        });
        if (!per_tablet_iter.ok()) {
            LOG(WARNING) << "gc_stale_lake_persistent_index_snapshots: iterate_dir2 failed for " << tablet_dir << ": "
                         << per_tablet_iter.to_string();
        }
    }
    return Status::OK();
}

Status write_stub_lake_persistent_index_snapshot(int64_t tablet_id, const TabletMetadataPB& metadata,
                                                 std::string* path_out) {
    // Snapshot the metadata-derived state without instantiating LakePersistentIndex.
    // This is the cheapest possible capture for an evicted tablet: no SST opens,
    // no rowset reads — just protobuf assembly + a single local file write.
    LakePersistentIndexSnapshotMetaPB meta;
    meta.set_format_version(kSnapshotFormatVersion);
    meta.set_tablet_id(tablet_id);
    meta.set_captured_version(static_cast<int64_t>(metadata.version()));
    meta.set_schema_id(metadata.schema().id());
    meta.set_captured_at_unix_sec(
            std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch())
                    .count());
    // memtable_entries: intentionally empty. The restore-side bulk_insert path
    // is gated on `memtable_entries_size() > 0`, so an empty list translates to
    // "the freshly-init()'d empty memtable is the correct post-restore state".
    // Caller's need_rebuild_counts == (0, 0) gate is what guarantees correctness.
    for (const auto& sst_meta : metadata.sstable_meta().sstables()) {
        auto* fs_ref = meta.add_filesets();
        fs_ref->set_fileset_version(static_cast<int64_t>(metadata.version()));
        fs_ref->add_sst_filenames(sst_meta.filename());
        fs_ref->set_max_rss_rowid(static_cast<int64_t>(sst_meta.max_rss_rowid()));
    }

    std::string snapshot_path;
    RETURN_IF_ERROR(get_lake_persistent_index_snapshot_path(tablet_id, meta.captured_version(), &snapshot_path));
    const auto last_slash = snapshot_path.find_last_of('/');
    if (last_slash != std::string::npos) {
        const std::string dir = snapshot_path.substr(0, last_slash);
        RETURN_IF_ERROR(fs::create_directories(dir));
    }
    RETURN_IF_ERROR(write_lake_persistent_index_snapshot(snapshot_path, meta));
    if (path_out != nullptr) {
        *path_out = snapshot_path;
    }
    return Status::OK();
}

Status validate_lake_persistent_index_snapshot(const LakePersistentIndexSnapshotMetaPB& meta,
                                               int64_t expected_tablet_id, int64_t expected_version,
                                               int64_t expected_schema_id, int64_t now_unix_sec, int64_t max_age_sec) {
    if (meta.format_version() > kSnapshotFormatVersion) {
        return Status::NotFound("snapshot format_version newer than reader supports");
    }
    if (meta.tablet_id() != expected_tablet_id) {
        return Status::NotFound("snapshot tablet_id mismatch");
    }
    if (meta.captured_version() != expected_version) {
        return Status::NotFound("snapshot captured_version does not match base_version");
    }
    if (meta.schema_id() != expected_schema_id) {
        return Status::NotFound("snapshot schema_id mismatch");
    }
    if (max_age_sec > 0 && now_unix_sec > 0) {
        const int64_t age = now_unix_sec - meta.captured_at_unix_sec();
        if (age < 0 || age > max_age_sec) {
            return Status::NotFound("snapshot is older than max_age_sec");
        }
    }
    return Status::OK();
}

} // namespace lake
} // namespace starrocks
