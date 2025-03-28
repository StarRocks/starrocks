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

#include "cache/block_cache/block_cache.h"
#include "cache/block_cache/io_buffer.h"
#include "io/shared_buffered_input_stream.h"

namespace starrocks::io {

class CacheInputStream : public SeekableInputStreamWrapper {
public:
    struct Stats {
        int64_t read_cache_ns = 0;
        int64_t write_cache_ns = 0;
        int64_t read_cache_count = 0;
        int64_t write_cache_count = 0;
        int64_t write_mem_cache_bytes = 0;
        int64_t write_disk_cache_bytes = 0;
        int64_t read_cache_bytes = 0;
        int64_t read_mem_cache_bytes = 0;
        int64_t read_disk_cache_bytes = 0;
        int64_t write_cache_bytes = 0;
        int64_t skip_read_cache_count = 0;
        int64_t skip_read_cache_bytes = 0;
        int64_t skip_write_cache_count = 0;
        int64_t skip_write_cache_bytes = 0;
        int64_t write_cache_fail_count = 0;
        int64_t write_cache_fail_bytes = 0;
        int64_t read_block_buffer_bytes = 0;
        int64_t read_block_buffer_count = 0;
    };

    explicit CacheInputStream(const std::shared_ptr<SharedBufferedInputStream>& stream, const std::string& filename,
                              size_t size, int64_t modification_time);

    ~CacheInputStream() override;

    StatusOr<int64_t> read(void* data, int64_t count) override;

    Status read_at_fully(int64_t offset, void* data, int64_t count) override;

    Status seek(int64_t offset) override;

    StatusOr<int64_t> position() override;

    StatusOr<int64_t> get_size() override;

    const Stats& stats() { return _stats; }

    void set_enable_populate_cache(bool v) { _enable_populate_cache = v; }

    void set_enable_async_populate_mode(bool v) { _enable_async_populate_mode = v; }

    void set_enable_block_buffer(bool v) { _enable_block_buffer = v; }

    void set_enable_cache_io_adaptor(bool v) { _enable_cache_io_adaptor = v; }

    void set_datacache_evict_probability(int32_t v) { _datacache_evict_probability = v; }

    void set_priority(const int8_t priority) { _priority = priority; }

    void set_frequency(const int8_t frequency) { _frequency = frequency; }

    void set_ttl_seconds(const uint64_t ttl_seconds) { _ttl_seconds = ttl_seconds; }

    int64_t get_align_size() const;

    StatusOr<std::string_view> peek(int64_t count) override;

    Status skip(int64_t count) override {
        _offset += count;
        return _sb_stream->skip(count);
    }

protected:
    struct BlockBuffer {
        int64_t offset;
        IOBuffer buffer;
    };
    using SharedBufferPtr = SharedBufferedInputStream::SharedBufferPtr;

    // Read block from local, if not found, will return Status::NotFound();
    virtual Status _read_block_from_local(const int64_t offset, const int64_t size, char* out);
    // Read multiple blocks from remote
    virtual Status _read_blocks_from_remote(const int64_t offset, const int64_t size, char* out);
    void _populate_to_cache(const char* src, int64_t offset, int64_t count, const SharedBufferPtr& sb);
    void _deduplicate_shared_buffer(const SharedBufferPtr& sb);
    bool _can_ignore_populate_error(const Status& status) const;

    std::string _cache_key;
    std::string _filename;
    std::shared_ptr<SharedBufferedInputStream> _sb_stream;
    int64_t _offset;
    int64_t _buffer_size;
    std::string _buffer;
    Stats _stats;
    int64_t _size;
    bool _enable_populate_cache = false;
    bool _enable_async_populate_mode = false;
    bool _enable_block_buffer = false;
    bool _enable_cache_io_adaptor = false;
    int32_t _datacache_evict_probability = 100;
    BlockCache* _cache = nullptr;
    int64_t _block_size = 0;
    std::unordered_map<int64_t, BlockBuffer> _block_map;
    int8_t _priority = 0;
    uint64_t _ttl_seconds = 0;
    int8_t _frequency = 0;

private:
    inline int64_t _calculate_remote_latency_per_block(int64_t io_bytes, int64_t read_time_ns);
    // Record already populated blocks, avoid duplicate populate
    std::unordered_set<int64_t> _already_populated_blocks{};
};

} // namespace starrocks::io
