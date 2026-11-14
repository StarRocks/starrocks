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
#include <cstdint>

namespace starrocks {

// Who a shared-data remote read is for, as seen by the remote read scheduler of starlet. It is fixed when a file is
// opened and travels with the open options (LakeIOOptions -> RandomAccessFileOptions -> starlet ReadOptions). The
// only thing derived from the calling thread is the fallback of ScopedRemoteReadContext below, for files opened
// without a context. This is the BE-side mirror of staros::starlet::fslib::RemoteReadContext, so that storage code
// needs no StarOS header.
struct RemoteReadContext {
    enum class Workload : uint8_t {
        QUERY = 0,
        COMPACTION = 1,
        OTHER_BACKGROUND = 2,
        // Publish, primary key indexes, delete files and the spill of a load: the remote reads a load costs.
        LOAD = 3,
    };

    // False when nobody attributed the read. The scheduler still admits it, as OTHER_BACKGROUND.
    bool is_set = false;
    Workload workload = Workload::OTHER_BACKGROUND;
    // Resource group of a query, with its cpu_weight and configuration version (WorkGroup::version()) when the
    // query started. Load and background reads leave them at their defaults.
    int64_t group_id = 0;
    uint32_t group_weight = 1;
    uint64_t group_config_version = 0;
    // Shared by every read of one query (or one load or background task), whatever tablet or split it is for.
    uint64_t flow_id = 0;

    static RemoteReadContext for_query(int64_t group_id, uint32_t group_weight, uint64_t group_config_version,
                                       uint64_t flow_id) {
        RemoteReadContext ctx;
        ctx.is_set = true;
        ctx.workload = Workload::QUERY;
        ctx.group_id = group_id;
        ctx.group_weight = group_weight == 0 ? 1 : group_weight;
        ctx.group_config_version = group_config_version;
        ctx.flow_id = flow_id;
        return ctx;
    }

    static RemoteReadContext for_background(Workload workload, uint64_t flow_id) {
        RemoteReadContext ctx;
        ctx.is_set = true;
        ctx.workload = workload;
        ctx.flow_id = flow_id;
        return ctx;
    }

    static RemoteReadContext for_load(uint64_t flow_id) { return for_background(Workload::LOAD, flow_id); }

    // The flow id of a query or a load, from the two halves of its unique id.
    static uint64_t flow_id_of(int64_t id_hi, int64_t id_lo) {
        return static_cast<uint64_t>(id_hi) ^ (static_cast<uint64_t>(id_lo) * 0x9e3779b97f4a7c15ULL);
    }
};

// While some work runs on this thread -- a query's fragment prepare, morsel pickup or scan IO task, a publish, a
// task of a ParallelTaskRunner -- the files it opens without a context of their own (tablet metadata, split-planning
// footers and indexes, primary key index files, delete files...) are attributed to that work. Consulted only when such
// a file is opened; its reads keep what was fixed then, on any thread. Scopes nest: the innermost set context wins,
// and an unset one leaves the enclosing context in place.
class ScopedRemoteReadContext {
public:
    explicit ScopedRemoteReadContext(const RemoteReadContext& ctx) : _ctx(ctx), _prev(tls_current) {
        tls_current = _ctx.is_set ? &_ctx : _prev;
    }
    ~ScopedRemoteReadContext() { tls_current = _prev; }
    ScopedRemoteReadContext(const ScopedRemoteReadContext&) = delete;
    ScopedRemoteReadContext& operator=(const ScopedRemoteReadContext&) = delete;

    // `ctx` when it is set, else the context of the work running on this thread, if any.
    static const RemoteReadContext& or_current(const RemoteReadContext& ctx) {
        return ctx.is_set || tls_current == nullptr ? ctx : *tls_current;
    }

    // For the reads of a load -- publish, primary key indexes, delete files -- on whatever thread they run: the work
    // running on this thread when there is some (the publish that does them), else a load of their own.
    static RemoteReadContext current_or_load() {
        return tls_current == nullptr ? RemoteReadContext::for_load(0) : *tls_current;
    }

private:
    static inline thread_local const RemoteReadContext* tls_current = nullptr;
    const RemoteReadContext _ctx;
    const RemoteReadContext* const _prev;
};

// Set once at startup, before any lake read, when the remote I/O scheduler of starlet was enabled
// (remote_io_scheduler_enable); it never changes afterwards. While it is set, the lake scans read ahead through
// SegmentPrefetchWindow, and the read-ahead strategies it replaces are off: io_coalesce_lake_read_enable was set to
// false, and the starlet buffer prefetch is forced off.
inline std::atomic<bool> g_remote_io_scheduler_enabled{false};

inline bool remote_io_scheduler_enabled() {
    return g_remote_io_scheduler_enabled.load(std::memory_order_relaxed);
}

} // namespace starrocks
