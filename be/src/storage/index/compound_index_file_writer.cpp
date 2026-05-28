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

#include "storage/index/compound_index_file_writer.h"

#include <cstdint>
#include <cstring>
#include <vector>

#include "fs/fs.h"
#include "gutil/endian.h"
#include "util/slice.h"

namespace starrocks {

namespace {

// Streamed subfile copy buffer. 64 KiB matches the typical kernel page-cache
// readahead grain and avoids a noticeable syscall storm without inflating BE
// resident size during pack.
constexpr size_t kCopyBufferBytes = 64 * 1024;

// In-memory header builder. We grow a contiguous Vec<u8> and use BigEndian
// helpers to encode integers; the final header is written to disk in one
// `WritableFile::append` call.
class HeaderBuilder {
public:
    void put_u8(uint8_t v) { _buf.push_back(v); }

    void put_zero(size_t n) { _buf.insert(_buf.end(), n, 0u); }

    void put_u32(uint32_t v) {
        size_t off = _buf.size();
        _buf.resize(off + 4);
        BigEndian::Store32(_buf.data() + off, v);
    }

    void put_u64(uint64_t v) {
        size_t off = _buf.size();
        _buf.resize(off + 8);
        BigEndian::Store64(_buf.data() + off, v);
    }

    void put_str(std::string_view s) {
        put_u32(static_cast<uint32_t>(s.size()));
        _buf.insert(_buf.end(), s.begin(), s.end());
    }

    const std::vector<uint8_t>& buf() const { return _buf; }
    size_t size() const { return _buf.size(); }

private:
    std::vector<uint8_t> _buf;
};

// Pre-compute the byte size of the header without actually building it. Used
// so we can assign absolute data-region offsets to every subfile before any
// bytes are written.
size_t header_byte_size(const std::vector<CompoundIndexEntry>& entries) {
    // Fixed prefix: magic(4) + version(4) + num_indices(4)
    size_t bytes = 4 + 4 + 4;
    for (const auto& e : entries) {
        // kind(1) + reserved(3) + index_id(8) + suffix_len(4) + suffix bytes
        // + num_files(4)
        bytes += 1 + 3 + 8 + 4 + e.suffix.size() + 4;
        for (const auto& f : e.files) {
            // name_len(4) + name bytes + offset(8) + length(8)
            bytes += 4 + f.name.size() + 8 + 8;
        }
    }
    return bytes;
}

} // namespace

Status CompoundIndexFileWriter::pack(const std::vector<CompoundIndexEntry>& entries, WritableFile* out) {
    if (out == nullptr) {
        return Status::InvalidArgument("CompoundIndexFileWriter::pack out is null");
    }
    if (entries.empty()) {
        return Status::InvalidArgument("CompoundIndexFileWriter::pack entries is empty");
    }

    // Step 1: open every subfile to read its size, and assign absolute offsets.
    // We open via the same FileSystem the WritableFile lives in (defaults to
    // local FS for temp dirs, which is what our writer uses). Sizes are
    // mandatory upfront because the format embeds them in the header.
    auto fs_or = FileSystem::CreateSharedFromString("posix://");
    if (!fs_or.ok()) {
        return fs_or.status();
    }
    std::shared_ptr<FileSystem> fs = std::move(fs_or).value();

    struct ResolvedFile {
        const CompoundFileRef* ref;
        uint64_t size;
        uint64_t offset; // assigned after header sizing
    };
    std::vector<std::vector<ResolvedFile>> resolved;
    resolved.reserve(entries.size());

    uint64_t cursor = static_cast<uint64_t>(header_byte_size(entries));
    for (const auto& e : entries) {
        std::vector<ResolvedFile> per_index;
        per_index.reserve(e.files.size());
        for (const auto& f : e.files) {
            ASSIGN_OR_RETURN(uint64_t sz, fs->get_file_size(f.local_path));
            per_index.push_back({&f, sz, cursor});
            cursor += sz;
        }
        resolved.push_back(std::move(per_index));
    }

    // Step 2: build the header in memory using the now-known offsets.
    HeaderBuilder hb;
    hb.put_u32(COMPOUND_BIN_MAGIC);
    hb.put_u32(COMPOUND_BIN_VERSION);
    hb.put_u32(static_cast<uint32_t>(entries.size()));
    for (size_t i = 0; i < entries.size(); ++i) {
        const auto& e = entries[i];
        const auto& files = resolved[i];
        hb.put_u8(static_cast<uint8_t>(e.kind));
        hb.put_zero(3); // reserved
        hb.put_u64(static_cast<uint64_t>(e.index_id));
        hb.put_str(e.suffix);
        hb.put_u32(static_cast<uint32_t>(files.size()));
        for (const auto& rf : files) {
            hb.put_str(rf.ref->name);
            hb.put_u64(rf.offset);
            hb.put_u64(rf.size);
        }
    }
    // Sanity check: the size we precomputed must match what we actually built.
    if (hb.size() != header_byte_size(entries)) {
        return Status::InternalError("compound bin header size mismatch (precompute vs actual)");
    }

    // Step 3: write the header.
    Slice header_slice(reinterpret_cast<const char*>(hb.buf().data()), hb.size());
    RETURN_IF_ERROR(out->append(header_slice));

    // Step 4: stream every subfile's bytes into the data region in the same
    // order we recorded their offsets. A 64 KiB ping-pong buffer keeps
    // resident memory bounded.
    std::vector<char> copy_buf(kCopyBufferBytes);
    for (size_t i = 0; i < entries.size(); ++i) {
        for (const auto& rf : resolved[i]) {
            ASSIGN_OR_RETURN(auto raf, fs->new_random_access_file(rf.ref->local_path));
            uint64_t remaining = rf.size;
            int64_t pos = 0;
            while (remaining > 0) {
                int64_t want = static_cast<int64_t>(std::min<uint64_t>(remaining, copy_buf.size()));
                RETURN_IF_ERROR(raf->read_at_fully(pos, copy_buf.data(), want));
                RETURN_IF_ERROR(out->append(Slice(copy_buf.data(), want)));
                pos += want;
                remaining -= static_cast<uint64_t>(want);
            }
        }
    }

    return Status::OK();
}

} // namespace starrocks
