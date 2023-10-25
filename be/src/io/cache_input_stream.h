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

#include <memory>
#include <string>

#include "block_cache/io_buffer.h"
#include "io/shared_buffered_input_stream.h"

namespace starrocks::io {

class CacheInputStream final : public SeekableInputStreamWrapper {
public:
    struct Stats {
        int64_t read_cache_ns = 0;
        int64_t write_cache_ns = 0;
        int64_t read_cache_count = 0;
        int64_t write_cache_count = 0;
        int64_t read_cache_bytes = 0;
        int64_t write_cache_bytes = 0;
        int64_t write_cache_fail_count = 0;
        int64_t write_cache_fail_bytes = 0;
        int64_t read_block_buffer_bytes = 0;
        int64_t read_block_buffer_count = 0;
    };

    explicit CacheInputStream(const std::shared_ptr<SharedBufferedInputStream>& stream, const std::string& filename,
                              size_t size, int64_t modification_time);

    ~CacheInputStream() override = default;

    StatusOr<int64_t> read(void* data, int64_t count) override;

    Status read_at_fully(int64_t offset, void* data, int64_t count) override;

    Status seek(int64_t offset) override;

    StatusOr<int64_t> position() override;

    StatusOr<int64_t> get_size() override;

    const Stats& stats() { return _stats; }

    void set_enable_populate_cache(bool v) { _enable_populate_cache = v; }

    void set_enable_block_buffer(bool v) { _enable_block_buffer = v; }

    int64_t get_align_size() const;

    StatusOr<std::string_view> peek(int64_t count) override;

    Status skip(int64_t count) override {
        _offset += count;
        return _sb_stream->skip(count);
    }

private:
    struct BlockBuffer {
        int64_t offset;
        IOBuffer buffer;
    };

    Status _read_block(int64_t offset, int64_t size, char* out, bool single_read);
    void _populate_cache_from_zero_copy_buffer(const char* p, int64_t offset, int64_t count);
    void _deduplicate_shared_buffer(SharedBufferedInputStream::SharedBuffer* sb);

    std::string _cache_key;
    std::string _filename;
    std::shared_ptr<SharedBufferedInputStream> _sb_stream;
    int64_t _offset;
    std::string _buffer;
    Stats _stats;
    int64_t _size;
    bool _enable_populate_cache = false;
    bool _enable_block_buffer = false;
    int64_t _block_size = 0;
    std::unordered_map<int64_t, BlockBuffer> _block_map;
};

} // namespace starrocks::io
