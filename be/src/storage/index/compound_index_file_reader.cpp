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

#include "storage/index/compound_index_file_reader.h"

#include <cstdint>
#include <cstring>

#include "fs/fs.h"
#include "gutil/endian.h"

namespace starrocks {

namespace {

class HeaderParser {
public:
    HeaderParser(const uint8_t* data, size_t size) : _data(data), _size(size) {}

    bool truncated() const { return _truncated; }

    Status need(size_t n) {
        if (_pos + n > _size) {
            _truncated = true;
            return Status::ResourceBusy("compound bin: header probe too small at offset " +
                                        std::to_string(_pos));
        }
        return Status::OK();
    }

    StatusOr<uint8_t> get_u8() {
        RETURN_IF_ERROR(need(1));
        return _data[_pos++];
    }

    Status skip(size_t n) {
        RETURN_IF_ERROR(need(n));
        _pos += n;
        return Status::OK();
    }

    StatusOr<uint32_t> get_u32() {
        RETURN_IF_ERROR(need(4));
        uint32_t v = BigEndian::Load32(_data + _pos);
        _pos += 4;
        return v;
    }

    StatusOr<uint64_t> get_u64() {
        RETURN_IF_ERROR(need(8));
        uint64_t v = BigEndian::Load64(_data + _pos);
        _pos += 8;
        return v;
    }

    StatusOr<std::string> get_str() {
        ASSIGN_OR_RETURN(uint32_t len, get_u32());
        RETURN_IF_ERROR(need(len));
        std::string s(reinterpret_cast<const char*>(_data + _pos), len);
        _pos += len;
        return s;
    }

    size_t pos() const { return _pos; }

private:
    const uint8_t* _data;
    size_t _size;
    size_t _pos = 0;
    bool _truncated = false;
};


// Header sniff buffer — large enough for typical layouts (a few hundred files
// across a handful of indices). If we hit the end of this buffer mid-parse
// the reader transparently re-reads with a doubled cap, up to 16 MiB.
constexpr size_t kInitialHeaderProbeBytes = 64 * 1024;
constexpr size_t kMaxHeaderProbeBytes = 16 * 1024 * 1024;

} // namespace

StatusOr<std::unique_ptr<CompoundIndexFileReader>> CompoundIndexFileReader::open(
        const std::string& bin_path) {
    auto fs_or = FileSystem::CreateSharedFromString(bin_path);
    if (!fs_or.ok()) {
        return fs_or.status();
    }
    return open(bin_path, fs_or.value().get());
}

StatusOr<std::unique_ptr<CompoundIndexFileReader>> CompoundIndexFileReader::open(
        const std::string& bin_path, FileSystem* fs) {
    ASSIGN_OR_RETURN(auto raf, fs->new_random_access_file(bin_path));
    ASSIGN_OR_RETURN(uint64_t total_size, raf->get_size());
    if (total_size < 12) {
        return Status::Corruption("compound bin too small: " + bin_path);
    }

    // We don't know the header length up-front; speculatively read a chunk
    // big enough for the common case and grow if we run off the end.
    size_t probe = std::min<size_t>(kInitialHeaderProbeBytes, total_size);
    std::vector<uint8_t> buf(probe);

    bool truncated = false;
    auto parse_with_buffer = [&](size_t buf_size) -> StatusOr<std::vector<CompoundIndexLayout>> {
        RETURN_IF_ERROR(raf->read_at_fully(0, buf.data(), buf_size));
        HeaderParser p(buf.data(), buf_size);

        auto run = [&]() -> StatusOr<std::vector<CompoundIndexLayout>> {
            ASSIGN_OR_RETURN(uint32_t magic, p.get_u32());
            if (magic != COMPOUND_BIN_MAGIC) {
                return Status::Corruption("compound bin: bad magic " + std::to_string(magic));
            }
            ASSIGN_OR_RETURN(uint32_t version, p.get_u32());
            if (version != COMPOUND_BIN_VERSION) {
                return Status::Corruption("compound bin: unsupported version " +
                                          std::to_string(version));
            }
            ASSIGN_OR_RETURN(uint32_t num_indices, p.get_u32());

            std::vector<CompoundIndexLayout> out;
            out.reserve(num_indices);
            for (uint32_t i = 0; i < num_indices; ++i) {
                CompoundIndexLayout layout;
                ASSIGN_OR_RETURN(uint8_t kind_byte, p.get_u8());
                switch (kind_byte) {
                case static_cast<uint8_t>(CompoundIndexKind::INVERTED_TANTIVY):
                    layout.kind = CompoundIndexKind::INVERTED_TANTIVY;
                    break;
                default:
                    layout.kind = CompoundIndexKind::UNKNOWN;
                    break;
                }
                RETURN_IF_ERROR(p.skip(3)); // reserved
                ASSIGN_OR_RETURN(uint64_t idx_id, p.get_u64());
                layout.index_id = static_cast<int64_t>(idx_id);
                ASSIGN_OR_RETURN(layout.suffix, p.get_str());
                ASSIGN_OR_RETURN(uint32_t num_files, p.get_u32());
                layout.files.reserve(num_files);
                for (uint32_t k = 0; k < num_files; ++k) {
                    CompoundFileEntry fe;
                    ASSIGN_OR_RETURN(fe.name, p.get_str());
                    ASSIGN_OR_RETURN(fe.offset, p.get_u64());
                    ASSIGN_OR_RETURN(fe.length, p.get_u64());
                    if (fe.offset + fe.length > total_size) {
                        return Status::Corruption(
                                "compound bin: file '" + fe.name + "' overruns file end");
                    }
                    layout.files.push_back(std::move(fe));
                }
                out.push_back(std::move(layout));
            }
            return out;
        };
        auto result = run();
        truncated = p.truncated();
        return result;
    };

    StatusOr<std::vector<CompoundIndexLayout>> layouts_or = parse_with_buffer(probe);
    while (!layouts_or.ok() && truncated && probe < kMaxHeaderProbeBytes &&
           probe < total_size) {
        // Header runs longer than our probe. Double the buffer and retry.
        probe = std::min(std::min(probe * 2, kMaxHeaderProbeBytes), static_cast<size_t>(total_size));
        buf.assign(probe, 0u);
        layouts_or = parse_with_buffer(probe);
    }
    if (!layouts_or.ok()) {
        // If the loop exhausted (probe hit cap / file size with truncation
        // still flagged), surface a clear corruption status rather than the
        // ResourceBusy sentinel.
        if (truncated) {
            return Status::Corruption("compound bin: header exceeds " +
                                      std::to_string(probe) + " bytes (probe cap reached)");
        }
        return layouts_or.status();
    }

    return std::unique_ptr<CompoundIndexFileReader>(
            new CompoundIndexFileReader(bin_path, std::move(layouts_or).value()));
}

StatusOr<CompoundIndexLayout> CompoundIndexFileReader::find_index(CompoundIndexKind kind,
                                                                  int64_t index_id) const {
    for (const auto& layout : _layouts) {
        if (layout.kind == kind && layout.index_id == index_id) {
            return layout;
        }
    }
    return Status::NotFound("compound bin: no entry for kind=" +
                            std::to_string(static_cast<int>(kind)) +
                            " index_id=" + std::to_string(index_id) + " in " + _bin_path);
}

} // namespace starrocks
