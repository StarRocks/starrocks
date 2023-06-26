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

#include <cstddef>
#include <cstdint>
#include <memory>

#include "common/status.h"
#include "io/seekable_input_stream.h"

namespace starrocks::io {

class SharedBufferedInputStream : public SeekableInputStream {
public:
    struct IORange {
        int64_t offset;
        int64_t size;
        bool operator<(const IORange& x) const { return offset < x.offset; }
    };
    struct CoalesceOptions {
        static constexpr int64_t MB = 1024 * 1024;
        int64_t max_dist_size = 1 * MB;
        int64_t max_buffer_size = 8 * MB;
    };
    struct SharedBuffer {
        // request range
        int64_t raw_offset;
        int64_t raw_size;
        // request range after alignment
        int64_t offset;
        int64_t size;
        int64_t ref_count;
        std::vector<uint8_t> buffer;
        void align(int64_t align_size, int64_t file_size);
    };

    SharedBufferedInputStream(std::shared_ptr<SeekableInputStream> stream, const std::string& filename,
                              size_t file_size);
    ~SharedBufferedInputStream() override = default;

    Status seek(int64_t position) override {
        _offset = position;
        return _stream->seek(position);
    }
    StatusOr<int64_t> position() override { return _offset; }
    StatusOr<int64_t> read(void* data, int64_t count) override;
    Status read_at_fully(int64_t offset, void* out, int64_t count) override;
    StatusOr<int64_t> get_size() override;
    Status skip(int64_t count) override {
        _offset += count;
        return _stream->skip(count);
    }

    Status get_bytes(const uint8_t** buffer, size_t offset, size_t nbytes);
    StatusOr<SharedBuffer*> find_shared_buffer(size_t offset, size_t count);

    StatusOr<std::unique_ptr<NumericStatistics>> get_numeric_statistics() override {
        return _stream->get_numeric_statistics();
    }

    Status set_io_ranges(const std::vector<IORange>& ranges);
    void release_to_offset(int64_t offset);
    void release();
    void set_coalesce_options(const CoalesceOptions& options) { _options = options; }
    void set_align_size(int64_t size) { _align_size = size; }

    int64_t shared_io_count() const { return _shared_io_count; }
    int64_t shared_io_bytes() const { return _shared_io_bytes; }
    int64_t shared_io_timer() const { return _shared_io_timer; }
    int64_t direct_io_count() const { return _direct_io_count; }
    int64_t direct_io_bytes() const { return _direct_io_bytes; }
    int64_t direct_io_timer() const { return _direct_io_timer; }
    int64_t estimated_mem_usage() const { return _estimated_mem_usage; }

    StatusOr<std::string_view> peek(int64_t count) override;

private:
    void _update_estimated_mem_usage();
    const std::shared_ptr<SeekableInputStream> _stream;
    const std::string _filename;
    std::map<int64_t, SharedBuffer> _map;
    CoalesceOptions _options;
    int64_t _offset = 0;
    int64_t _file_size = 0;
    int64_t _shared_io_count = 0;
    int64_t _shared_io_bytes = 0;
    int64_t _shared_io_timer = 0;
    int64_t _direct_io_count = 0;
    int64_t _direct_io_bytes = 0;
    int64_t _direct_io_timer = 0;
    int64_t _align_size = 0;
    int64_t _estimated_mem_usage = 0;
};

} // namespace starrocks::io
