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

#include <cstring>
#include <limits>
#include <string>

#include "base/coding.h"
#include "base/hash/crc32c.h"
#include "fs/fs.h"

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
    const uint64_t file_size = rf->size();
    if (file_size < kSnapshotHeaderLen + kSnapshotChecksumLen) {
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
    if (file_size != kSnapshotHeaderLen + meta_pb_size + kSnapshotChecksumLen) {
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

} // namespace lake
} // namespace starrocks
