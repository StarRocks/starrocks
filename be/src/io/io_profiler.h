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

namespace starrocks {

class IOStatEntry;

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
    };
    static const char* tag_to_string(uint32_t tag);

    static Status start(IOMode op);
    static void stop();
    static void reset();

    static void set_context(uint32_t tag, uint64_t tablet_id);
    static void set_context(IOStatEntry* entry);
    static IOStatEntry* get_context();
    static void clear_context();

    class Scope {
    public:
        Scope() = delete;
        Scope(const Scope&) = delete;
        Scope(Scope&& other) {
            _old = other._old;
            other._old = nullptr;
        }
        Scope(uint32_t tag, uint64_t tablet_id) {
            _old = get_context();
            set_context(tag, tablet_id);
        }
        Scope(IOStatEntry* entry) {
            _old = get_context();
            set_context(entry);
        }
        ~Scope() { set_context(_old); }

    private:
        IOStatEntry* _old{nullptr};
    };

    static Scope scope(uint32_t tag, uint64_t tablet_id) { return Scope(tag, tablet_id); }
    static Scope scope(IOStatEntry* entry) { return Scope(entry); }

    static inline void add_read(int64_t bytes) {
        if (_mode & IOMode::IOMODE_READ) {
            _add_read(bytes);
        }
    }

    static inline void add_write(int64_t bytes) {
        if (_mode & IOMode::IOMODE_WRITE) {
            _add_write(bytes);
        }
    }

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

    static void _add_read(int64_t bytes);
    static void _add_write(int64_t bytes);

    static std::atomic<uint32_t> _mode;
};

} // namespace starrocks
