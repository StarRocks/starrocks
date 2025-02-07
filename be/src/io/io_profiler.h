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

#include <atomic>

#include "common/status.h"
#include "common/statusor.h"
#include "util/system_metrics.h"

namespace starrocks {

struct IOStatEntry;

class IOProfiler {
public:
    enum IOMode {
        IOMODE_NONE = 0,
        IOMODE_READ = 1,
        IOMODE_WRITE = 2,
        IOMODE_ALL = 3,
    };

    enum TAG {
        TAG_NONE = 0,
        TAG_QUERY,
        TAG_LOAD,
        TAG_PKINDEX,
        TAG_COMPACTION,
        TAG_CLONE,
        TAG_ALTER,
        TAG_MIGRATE,
        TAG_SIZE,
        TAG_SPILL,

        TAG_END,
    };

    struct IOStat {
        uint64_t read_ops;
        uint64_t read_bytes;
        uint64_t read_time_ns;
        uint64_t write_ops;
        uint64_t write_bytes;
        uint64_t write_time_ns;
        uint64_t sync_ops;
        uint64_t sync_time_ns;
    };

    static const char* tag_to_string(uint32_t tag);

    static Status start(IOMode op);
    static void stop();
    static void reset();
    static IOMode get_context_io_mode() { return static_cast<IOMode>(_context_io_mode.load()); }

    static void set_context(uint32_t tag, uint64_t tablet_id);
    static void set_context(IOStatEntry* entry);
    static void set_tag(uint32_t tag);
    static IOStatEntry* get_context();
    static IOStat get_context_io();
    static void clear_context();

    static void take_tls_io_snapshot(IOStat* snapshot);
    static IOStat calculate_scoped_tls_io(const IOStat& snapshot);

    static bool is_empty();

    class Scope {
    public:
        Scope() = delete;
        Scope(const Scope&) = delete;
        Scope(Scope&& other) {
            _old = other._old;
            _tls_io_snapshot = other._tls_io_snapshot;
            other._old = nullptr;
        }
        Scope(uint32_t tag, uint64_t tablet_id) {
            _old = get_context();
            set_context(tag, tablet_id);
            take_tls_io_snapshot(&_tls_io_snapshot);
        }
        Scope(IOStatEntry* entry) {
            _old = get_context();
            set_context(entry);
            take_tls_io_snapshot(&_tls_io_snapshot);
        }
        ~Scope() { set_context(_old); }

        IOStat current_scoped_tls_io() { return calculate_scoped_tls_io(_tls_io_snapshot); }

        IOStat current_context_io() { return get_context_io(); }

    private:
        IOStatEntry* _old{nullptr};
        // A snapshot of the thread local io stat when this scope is created
        IOStat _tls_io_snapshot;
    };

    static Scope scope(uint32_t tag, uint64_t tablet_id) { return Scope(tag, tablet_id); }
    static Scope scope(IOStatEntry* entry) { return Scope(entry); }

    static inline void add_read(int64_t bytes, int64_t latency_ns) {
        _add_tls_read(bytes, latency_ns);
        if (_context_io_mode & IOMode::IOMODE_READ) {
            _add_context_read(bytes);
        }
    }

    static inline void add_write(int64_t bytes, int64_t latency_ns) {
        _add_tls_write(bytes, latency_ns);
        if (_context_io_mode & IOMode::IOMODE_WRITE) {
            _add_context_write(bytes);
        }
    }

    static inline void add_sync(int64_t latency_ns) { _add_tls_sync(latency_ns); }

    static StatusOr<std::vector<std::string>> get_topn_read_stats(size_t n);
    static StatusOr<std::vector<std::string>> get_topn_write_stats(size_t n);
    static StatusOr<std::vector<std::string>> get_topn_total_stats(size_t n);

    /**
     * profile and get topn stats
     * @param mode read, write, all
     * @param seconds profile seconds
     * @param topn get topn stats
     * @return stats information as tabular format string
     */
    static std::string profile_and_get_topn_stats_str(const std::string& mode, int seconds, size_t topn);

protected:
    static StatusOr<std::vector<std::string>> get_topn_stats(size_t n,
                                                             const std::function<int64_t(const IOStatEntry&)>& func);

    // Update thread local io statistics
    static void _add_tls_read(int64_t bytes, int64_t latency_ns);
    static void _add_tls_write(int64_t bytes, int64_t latency_ns);
    static void _add_tls_sync(int64_t latency_ns);

    // Update io statistics associated with a context, such as tag + tablet_id
    static void _add_context_read(int64_t bytes);
    static void _add_context_write(int64_t bytes);

    static std::atomic<uint32_t> _context_io_mode;
};

} // namespace starrocks
