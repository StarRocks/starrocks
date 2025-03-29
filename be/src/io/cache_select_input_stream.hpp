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

#include "io/cache_input_stream.h"
#include "util/runtime_profile.h"

namespace starrocks::io {

class CacheSelectInputStream final : public CacheInputStream {
public:
    explicit CacheSelectInputStream(const std::shared_ptr<SharedBufferedInputStream>& stream,
                                    const std::string& filename, size_t size, int64_t modification_time)
            : CacheInputStream(stream, filename, size, modification_time) {
        set_enable_populate_cache(true);
        set_enable_async_populate_mode(false);
        set_enable_cache_io_adaptor(false);
        set_enable_block_buffer(false);
        // Set a high frequecy for cache item that will be populated by cache select.
        set_frequency(1);
    }

    ~CacheSelectInputStream() override = default;

    Status write_at_fully(int64_t offset, int64_t count) {
        RETURN_IF_ERROR(read_at_fully(offset, nullptr, count));
        _sb_stream->release_to_offset(offset + count);
        return Status::OK();
    }

protected:
    Status _read_block_from_local(const int64_t offset, const int64_t size, char* out) override {
        if (UNLIKELY(size == 0)) {
            return Status::OK();
        }
        DCHECK(size <= _block_size);
        const int64_t block_id = offset / _block_size;
        const int64_t block_offset = block_id * _block_size;
        const int64_t load_size = std::min(_block_size, _size - block_offset);

        // check block existed
        int64_t read_cache_ns = 0;
        bool existed = false;
        {
            SCOPED_RAW_TIMER(&read_cache_ns);
            existed = _cache->exist(_cache_key, block_offset, load_size);
        }

        _stats.read_cache_ns += read_cache_ns;
        if (existed) {
            _stats.read_cache_bytes += load_size;
            _stats.read_cache_count += 1;
            return Status::OK();
        } else {
            return Status::NotFound("Not Found");
        }
    }

    Status _read_blocks_from_remote(const int64_t offset, const int64_t size, char* out) override {
        const int64_t start_block_id = offset / _block_size;
        const int64_t end_block_id = (offset + size - 1) / _block_size;

        // We will load range=[read_start_offset, read_end_offset) from remote
        const int64_t block_start_offset = start_block_id * _block_size;
        const int64_t block_end_offset = std::min(end_block_id * _block_size + _block_size, _size);

        for (int64_t read_offset_cursor = block_start_offset; read_offset_cursor < block_end_offset;) {
            // Everytime read at most one buffer size
            const int64_t read_size = std::min(_buffer_size, block_end_offset - read_offset_cursor);
            RETURN_IF_ERROR(_sb_stream->read_at_fully(read_offset_cursor, _buffer.data(), read_size));
            char* src = _buffer.data();

            _populate_to_cache(src, read_offset_cursor, read_size, nullptr);
            read_offset_cursor += read_size;
        }
        return Status::OK();
    }
};

} // namespace starrocks::io
