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

#include "io/seekable_input_stream.h"

namespace starrocks::io {

class CacheInputStream final : public SeekableInputStream {
public:
    struct Stats {
        int64_t read_cache_ns = 0;
        int64_t write_cache_ns = 0;
        int64_t read_cache_count = 0;
        int64_t write_cache_count = 0;
        int64_t read_cache_bytes = 0;
        int64_t write_cache_bytes = 0;
    };

    static constexpr int64_t BLOCK_SIZE = 1 * 1024 * 1024;
    explicit CacheInputStream(const std::string& filename, std::shared_ptr<SeekableInputStream> stream);

    ~CacheInputStream() override = default;

    StatusOr<int64_t> read(void* data, int64_t count) override;

    Status seek(int64_t offset) override;

    StatusOr<int64_t> position() override;

    StatusOr<int64_t> get_size() override;

    const Stats& stats() { return _stats; }

    void set_enable_populate_cache(bool v) { _enable_populate_cache = v; }

private:
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
