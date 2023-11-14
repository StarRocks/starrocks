// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <memory>
#include <string>

#include "io/seekable_input_stream.h"

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
    };

    explicit CacheInputStream(std::shared_ptr<SeekableInputStream> stream, const std::string& filename, size_t size,
                              int64_t modification_time);

    ~CacheInputStream() override = default;

    StatusOr<int64_t> read(void* data, int64_t count) override;

    Status read_at_fully(int64_t offset, void* data, int64_t count) override;

    Status seek(int64_t offset) override;

    StatusOr<int64_t> position() override;

    StatusOr<int64_t> get_size() override;

    const Stats& stats() { return _stats; }

    void set_enable_populate_cache(bool v) { _enable_populate_cache = v; }

    int64_t get_align_size() const;

    StatusOr<std::string_view> peek(int64_t count) override;

    Status skip(int64_t count) override {
        _offset += count;
        return _stream->skip(count);
    }

private:
    void _populate_cache_from_zero_copy_buffer(const char* p, int64_t offset, int64_t count);

    std::string _cache_key;
    std::string _filename;
    std::shared_ptr<SeekableInputStream> _stream;
    int64_t _offset;
    std::string _buffer;
    Stats _stats;
    int64_t _size;
    bool _enable_populate_cache = false;
};

} // namespace starrocks::io
