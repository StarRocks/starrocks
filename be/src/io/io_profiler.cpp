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

#include "io_profiler.h"

#include <mutex>
#include <thread>
#include <unordered_set>

#include "fmt/format.h"

namespace starrocks {

std::atomic<uint32_t> IOProfiler::_context_io_mode(0);

Status IOProfiler::start(IOProfiler::IOMode op) {
    if (op == IOMode::IOMODE_NONE) {
        return Status::InternalError("invalid io profiler mode");
    }
    uint32_t old_mode = _context_io_mode.load();
    if (old_mode != IOMode::IOMODE_NONE) {
        return Status::InternalError("io profiler is already started");
    }
    if (_context_io_mode.compare_exchange_strong(old_mode, (uint32_t)op)) {
        LOG(INFO) << "io profiler started, mode=" << op;
        return Status::OK();
    } else {
        LOG(WARNING) << "io profiler already started, mode=" << old_mode;
        return Status::InternalError("io profiler is already started");
    }
}

void IOProfiler::stop() {
    uint32_t old_mode = _context_io_mode.load();
    if (old_mode == IOMode::IOMODE_NONE) {
        LOG(INFO) << "io profiler is not started";
        return;
    }
    if (_context_io_mode.compare_exchange_strong(old_mode, IOMode::IOMODE_NONE)) {
        LOG(INFO) << "io profiler stopped";
    } else {
        LOG(WARNING) << "io profiler already stopped";
    }
    // simple solution to sleep some time to prevent race condition
    ::usleep(50000);
}

struct IOStatEntry {
    uint64_t id{0};
    std::atomic<uint64_t> read_bytes{0};
    std::atomic<uint64_t> write_bytes{0};
    std::atomic<uint32_t> read_ops{0};
    std::atomic<uint32_t> write_ops{0};

    IOStatEntry(uint64_t id) : id(id) {}

    bool operator==(const IOStatEntry& other) const { return id == other.id; }

    void add_read(uint64_t bytes) {
        this->read_bytes.fetch_add(bytes);
        this->read_ops.fetch_add(1);
    }

    void add_write(uint64_t bytes) {
        this->write_bytes.fetch_add(bytes);
        this->write_ops.fetch_add(1);
    }

    void clear() {
        this->read_bytes = 0;
        this->read_ops = 0;
        this->write_bytes = 0;
        this->write_ops = 0;
    }

    static std::string header_string() {
        return fmt::format("{:>10} {:>10} {:>16} {:>8} {:>16} {:>8} {:>16} {:>8}", "Tablet", "TAG", "read_bytes", "ops",
                           "write_bytes", "ops", "total_bytes", "ops");
    }

    std::string to_string() const {
        uint32_t tag = id >> 48UL;
        uint64_t tablet_id = id & 0x0000FFFFFFFFFFFFUL;
        return fmt::format("{:>10} {:>10} {:>16} {:>8} {:>16} {:>8} {:>16} {:>8}", tablet_id,
                           IOProfiler::tag_to_string(tag), read_bytes.load(), read_ops.load(), write_bytes.load(),
                           write_ops.load(), read_bytes + write_bytes, read_ops + write_ops);
    }
};

class IOStatEntryHash {
public:
    size_t operator()(const IOStatEntry& entry) const { return std::hash<uint64_t>()(entry.id); }
};

static std::mutex _io_stats_mutex;
static std::unordered_set<IOStatEntry, IOStatEntryHash> _io_stats;

void IOProfiler::reset() {
    uint32_t old_mode = _context_io_mode.load();
    if (old_mode != IOMode::IOMODE_NONE) {
        LOG(WARNING) << "stop io profiler before reset";
        return;
    }
    {
        std::lock_guard<std::mutex> l(_io_stats_mutex);
        _io_stats.clear();
    }
    LOG(INFO) << "io profiler reset";
}

thread_local IOStatEntry* current_io_stat = nullptr;

void IOProfiler::set_context(uint32_t tag, uint64_t tablet_id) {
    if (tablet_id == 0) {
        return;
    }
    uint64_t key = ((uint64_t)tag << 48UL) | tablet_id;
    std::lock_guard<std::mutex> l(_io_stats_mutex);
    auto it = _io_stats.find(IOStatEntry{key});
    if (it == _io_stats.end()) {
        it = _io_stats.emplace(key).first;
    }
    current_io_stat = const_cast<IOStatEntry*>(&(*it));
}

IOStatEntry* IOProfiler::get_context() {
    return current_io_stat;
}

void IOProfiler::set_context(IOStatEntry* entry) {
    current_io_stat = entry;
}

void IOProfiler::clear_context() {
    current_io_stat = nullptr;
}

void IOProfiler::_add_context_read(int64_t bytes) {
    if (current_io_stat != nullptr) {
        current_io_stat->add_read(bytes);
    }
}

void IOProfiler::_add_context_write(int64_t bytes) {
    if (current_io_stat != nullptr) {
        current_io_stat->add_write(bytes);
    }
}

// Thread local IO statistics which accumulates all IO since the thread is started
thread_local IOProfiler::IOStat tls_io_stat{0, 0, 0, 0, 0, 0};

void IOProfiler::take_tls_io_snapshot(IOStat* snapshot) {
    snapshot->read_ops = tls_io_stat.read_ops;
    snapshot->read_bytes = tls_io_stat.read_bytes;
    snapshot->read_time_ns = tls_io_stat.read_time_ns;
    snapshot->write_ops = tls_io_stat.write_ops;
    snapshot->write_bytes = tls_io_stat.write_bytes;
    snapshot->write_time_ns = tls_io_stat.write_time_ns;
}

IOProfiler::IOStat IOProfiler::calculate_scoped_tls_io(const IOStat& snapshot) {
    IOStat io_stat{
            tls_io_stat.read_ops - snapshot.read_ops,         tls_io_stat.read_bytes - snapshot.read_bytes,
            tls_io_stat.read_time_ns - snapshot.read_time_ns, tls_io_stat.write_ops - snapshot.write_ops,
            tls_io_stat.write_bytes - snapshot.write_bytes,   tls_io_stat.write_time_ns - snapshot.write_time_ns};
    return io_stat;
}

void IOProfiler::_add_tls_read(int64_t bytes, int64_t latency_ns) {
    tls_io_stat.read_ops += 1;
    tls_io_stat.read_bytes += bytes;
    tls_io_stat.read_time_ns += latency_ns;
}

void IOProfiler::_add_tls_write(int64_t bytes, int64_t latency_ns) {
    tls_io_stat.write_ops += 1;
    tls_io_stat.write_bytes += bytes;
    tls_io_stat.write_time_ns += latency_ns;
}

const char* IOProfiler::tag_to_string(uint32_t tag) {
    switch (tag) {
    case TAG_NONE:
        return "NONE";
    case TAG_QUERY:
        return "QUERY";
    case TAG_LOAD:
        return "LOAD";
    case TAG_PKINDEX:
        return "PKINDEX";
    case TAG_COMPACTION:
        return "COMPACTION";
    case TAG_CLONE:
        return "CLONE";
    case TAG_ALTER:
        return "ALTER";
    case TAG_MIGRATE:
        return "MIGRATE";
    case TAG_SIZE:
        return "SIZE";
    default:
        return "UNKNOWN";
    }
};

StatusOr<std::vector<std::string>> IOProfiler::get_topn_stats(size_t n,
                                                              const std::function<int64_t(const IOStatEntry&)>& func) {
    std::vector<std::pair<uint64_t, const IOStatEntry*>> stats;
    std::lock_guard<std::mutex> l(_io_stats_mutex);
    if (_context_io_mode.load() != IOMode::IOMODE_NONE) {
        return Status::InternalError("io profiler still running");
    }
    stats.reserve(_io_stats.size());
    for (auto& it : _io_stats) {
        auto v = func(it);
        if (v > 0) {
            stats.emplace_back(v, &it);
        }
    }
    std::sort(stats.begin(), stats.end(), [](const auto& a, const auto& b) { return a.first > b.first; });
    std::vector<std::string> result;
    n = std::min(n, stats.size());
    result.reserve(n + 1);
    result.emplace_back(IOStatEntry::header_string());
    for (size_t i = 0; i < n; ++i) {
        result.emplace_back(stats[i].second->to_string());
    }
    return result;
}

StatusOr<std::vector<std::string>> IOProfiler::get_topn_read_stats(size_t n) {
    return IOProfiler::get_topn_stats(n, [](const IOStatEntry& e) { return e.read_bytes.load(); });
}

StatusOr<std::vector<std::string>> IOProfiler::get_topn_write_stats(size_t n) {
    return IOProfiler::get_topn_stats(n, [](const IOStatEntry& e) { return e.write_bytes.load(); });
}

StatusOr<std::vector<std::string>> IOProfiler::get_topn_total_stats(size_t n) {
    return IOProfiler::get_topn_stats(n,
                                      [](const IOStatEntry& e) { return e.read_bytes.load() + e.write_bytes.load(); });
}

std::string IOProfiler::profile_and_get_topn_stats_str(const std::string& mode, int seconds, size_t topn) {
    StatusOr<std::vector<std::string>> ret;
    if (mode == "read") {
        auto st = IOProfiler::start(IOProfiler::IOMODE_READ);
        if (!st.ok()) {
            ret = st;
        } else {
            sleep(seconds);
            IOProfiler::stop();
            ret = IOProfiler::get_topn_read_stats(topn);
        }
    } else if (mode == "write") {
        auto st = IOProfiler::start(IOProfiler::IOMODE_WRITE);
        if (!st.ok()) {
            ret = st;
        } else {
            sleep(seconds);
            IOProfiler::stop();
            ret = IOProfiler::get_topn_write_stats(topn);
        }
    } else {
        auto st = IOProfiler::start(IOProfiler::IOMODE_ALL);
        if (!st.ok()) {
            ret = st;
        } else {
            sleep(seconds);
            IOProfiler::stop();
            ret = IOProfiler::get_topn_total_stats(topn);
        }
    }

    std::stringstream ss;
    if (!ret.ok()) {
        ss << "Unable to get io profile: " << ret.status().to_string() << std::endl;
    } else {
        for (const auto& it : ret.value()) {
            ss << it << "\n";
        }
    }
    return ss.str();
}

} // namespace starrocks
