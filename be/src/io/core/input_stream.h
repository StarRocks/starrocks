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

#include <cassert>
#include <string_view>

#include "common/ownership.h"
#include "io/core/readable.h"

namespace starrocks::io {

class NumericStatistics;

// Plain-old-data snapshot of cumulative read/write IO counters. Cheap drop-in alternative
// to `get_numeric_statistics()` for hot paths that only need a fixed set of counters
// (e.g. publish-time tracing): a single struct copy with no heap allocation, no string
// construction, and no vector growth. Streams that do not expose IO breakdown leave every
// field at zero. Adding a counter only requires extending this struct and the relevant
// concrete-stream override; the abstract interface stays the same.
struct IoStatsSnapshot {
    // bytes
    int64_t bytes_read_local_disk = 0;
    int64_t bytes_read_remote = 0;
    int64_t bytes_write_local_disk = 0;
    int64_t bytes_write_remote = 0;
    // op counts
    int64_t io_count_local_disk = 0;
    int64_t io_count_remote = 0;
    // time (ns)
    int64_t io_ns_read_local_disk = 0;
    int64_t io_ns_read_remote = 0;
    int64_t io_ns_write_local_disk = 0;
    int64_t io_ns_write_remote = 0;
    // prefetch
    int64_t prefetch_hit_count = 0;
    int64_t prefetch_wait_finish_ns = 0;
    int64_t prefetch_pending_ns = 0;
};

// InputStream is the superclass of all classes representing an input stream of bytes.
class InputStream : public Readable {
public:
    ~InputStream() override = default;

    // Skips a number of bytes.
    // This is guaranteed to be no slower that reading the same data, but may be faster.
    // If end of stream is reached, skipping will stop at the end of the stream, and skip
    // will return OK.
    // Returns error if an underlying read error occurs.
    virtual Status skip(int64_t count) = 0;

    // Return zero-copy string_view to upcoming bytes.
    //
    // Do not modify the stream position. The view becomes invalid after
    // any operation on the stream. May trigger buffering if the requested
    // size is larger than the number of buffered bytes.
    //
    // May return NotSupported on streams that don't support it.
    //
    virtual StatusOr<std::string_view> peek(int64_t nbytes) { return Status::NotSupported("InputStream::peek"); }

    // Get statistics about the reads which this InputStream has done.
    // If the InputStream implementation doesn't support statistics, a null pointer or
    // an empty statistics is returned.
    virtual StatusOr<std::unique_ptr<NumericStatistics>> get_numeric_statistics() { return nullptr; }

    // Cheap fixed-shape variant of `get_numeric_statistics()`. Default returns all zeros.
    // Prefer this over `get_numeric_statistics()` on hot paths (publish, multi_get, …) —
    // it avoids the heap-allocated NumericStatistics object and the 11 std::string keys
    // built by adapter streams. Out-of-line so gcov can credit a `.cpp` line; see
    // `input_stream.cpp` for the definition.
    virtual IoStatsSnapshot get_io_stats_snapshot() const;
};

class InputStreamWrapper : public InputStream {
public:
    explicit InputStreamWrapper(std::unique_ptr<InputStream> stream)
            : _impl(stream.release()), _ownership(kTakesOwnership) {}
    explicit InputStreamWrapper(InputStream* stream, Ownership ownership) : _impl(stream), _ownership(ownership) {}

    ~InputStreamWrapper() override {
        if (_ownership == kTakesOwnership) delete _impl;
    }

    // Disallow copy and assignment
    InputStreamWrapper(const InputStreamWrapper&) = delete;
    void operator=(const InputStreamWrapper&) = delete;
    // Disallow move
    InputStreamWrapper(InputStreamWrapper&&) = delete;
    void operator=(InputStreamWrapper&&) = delete;

    StatusOr<int64_t> read(void* data, int64_t count) override { return _impl->read(data, count); }

    Status read_fully(void* data, int64_t count) override { return _impl->read_fully(data, count); }

    Status skip(int64_t count) override { return _impl->skip(count); }

    StatusOr<std::string_view> peek(int64_t nbytes) override { return _impl->peek(nbytes); }

    StatusOr<std::unique_ptr<NumericStatistics>> get_numeric_statistics() override {
        return _impl->get_numeric_statistics();
    }

    IoStatsSnapshot get_io_stats_snapshot() const override;

private:
    InputStream* _impl;
    Ownership _ownership;
};

class NumericStatistics {
public:
    NumericStatistics() = default;
    ~NumericStatistics() = default;

    NumericStatistics(const NumericStatistics&) = default;
    NumericStatistics& operator=(const NumericStatistics&) = default;
    NumericStatistics(NumericStatistics&&) = default;
    NumericStatistics& operator=(NumericStatistics&&) = default;

    void append(std::string_view name, int64_t value);

    int64_t size() const;

    const std::string& name(int64_t idx) const;

    int64_t value(int64_t idx) const;

    void reserve(int64_t size);

private:
    std::vector<std::string> _names;
    std::vector<int64_t> _values;
};

inline void NumericStatistics::append(std::string_view name, int64_t value) {
    _names.emplace_back(name);
    _values.emplace_back(value);
}

inline int64_t NumericStatistics::size() const {
    return static_cast<int64_t>(_names.size());
}

inline const std::string& NumericStatistics::name(int64_t idx) const {
    assert(idx >= 0 && idx < size());
    return _names[idx];
}

inline int64_t NumericStatistics::value(int64_t idx) const {
    assert(idx >= 0 && idx < size());
    return _values[idx];
}

inline void NumericStatistics::reserve(int64_t size) {
    _names.reserve(size);
    _values.reserve(size);
}

} // namespace starrocks::io
