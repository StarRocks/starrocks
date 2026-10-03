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

#include "runtime/runtime_env.h"

#include <algorithm>
#include <sstream>

#include "base/compression/block_compression.h"
#include "base/string/parse_util.h"
#include "base/utility/pretty_printer.h"
#include "common/config_exec_env_fwd.h"
#include "common/config_lake_fwd.h"
#include "common/config_metrics_fwd.h"
#include "common/logging.h"
#include "common/mem_chunk.h"
#include "common/statusor.h"
#include "common/system/mem_info.h"
#include "platform/python/env.h"
#include "runtime/current_thread.h"
#include "runtime/diagnose_daemon.h"
#include "runtime/heartbeat_flags.h"
#include "runtime/mem_tracker.h"
#include "runtime/memory/mem_chunk_allocator.h"
#include "runtime/process_memory_metrics.h"
#include "types/hll.h"

namespace starrocks {

namespace {
// The per-thread ZSTD dictionary decompression contexts (base/compression) are created
// on whichever thread first decodes a dictionary page and live until that thread exits.
// Global malloc is hooked, so without this they would be charged to whatever tracker was
// current at creation -- usually a query's -- and stay charged there long after the query
// finished. Switching the thread-local tracker across the allocation MOVES that charge to
// the process tracker; it does not add a second one. set_mem_tracker() commits whatever
// the allocation cache is holding to the OLD tracker before it switches, so nothing lands
// on the wrong side of the swap.
//
// The tracker is resolved on every enter rather than cached: a thread can exit after the
// environment is torn down (its thread_local cache destructor frees the contexts then), and
// a cached MemTracker* would dangle. When the environment is gone the scope does nothing and
// the free is charged through the malloc hook's own fallback, which resolves to the process
// tracker -- the same place the allocation was charged.
//
// The depth counter keeps a future nested use honest: only the outermost scope saves and
// restores, so an inner one cannot overwrite what the outer one has to put back.
thread_local MemTracker* tls_saved_dctx_tracker = nullptr;
thread_local int tls_dctx_scope_depth = 0;
thread_local bool tls_dctx_switched = false;

void enter_dctx_alloc_scope() {
    if (tls_dctx_scope_depth++ > 0) return;
    // Two guards, in this order, and neither is optional.
    //
    // tls_is_thread_status_init is the same POD thread_local the malloc hook checks
    // (mem_hook.cpp MEMORY_CONSUME_SIZE / MEMORY_RELEASE_SIZE): CurrentThread's
    // constructor sets it, its destructor clears it, and it has no destructor of its own
    // so it is always readable. It matters because dict_dctx_cache() can be the first
    // dynamically initialized thread_local on a thread -- the cache is created, and only
    // then does this scope initialize CurrentThread -- which makes CurrentThread destroyed
    // FIRST at thread exit and the cache second. The cache's destructor frees its contexts,
    // so without this guard it would touch tls_thread_status after that object's lifetime
    // ended. The hook stops touching it at the same moment for the same reason, and its
    // fallback resolves to the process tracker, so the free is still attributed correctly
    // while nothing dead is read.
    //
    // CurrentThread::mem_tracker() is then the env check as well: it loads the
    // is-initialized function with acquire ordering and returns null once the environment
    // is gone, so nothing here reads RuntimeEnv::_is_init (a plain bool) directly.
    if (!tls_is_thread_status_init) return;
    MemTracker* saved = CurrentThread::mem_tracker();
    if (saved == nullptr) return;
    tls_saved_dctx_tracker = saved;
    tls_dctx_switched = true;
    (void)tls_thread_status.set_mem_tracker(RuntimeEnv::GetInstance()->process_mem_tracker());
}

void leave_dctx_alloc_scope() {
    if (--tls_dctx_scope_depth > 0) return;
    tls_dctx_scope_depth = 0;
    // Restore only what was actually switched. A scope that bailed out above -- nested, or
    // CurrentThread already destroyed, or the environment gone -- must not put anything back:
    // it would touch the same state the guards exist to avoid, and flush the thread's
    // allocation cache for a swap that never happened.
    if (!tls_dctx_switched) return;
    tls_dctx_switched = false;
    (void)tls_thread_status.set_mem_tracker(tls_saved_dctx_tracker);
    tls_saved_dctx_tracker = nullptr;
}

} // namespace

RuntimeEnv::RuntimeEnv() : _heartbeat_flags(std::make_unique<HeartbeatFlags>()) {}

RuntimeEnv::~RuntimeEnv() {
    _is_init = false;
}

int64_t RuntimeEnv::process_mem_limit() const {
    return _process_mem_tracker->limit();
}

ProcessMemoryMetrics* RuntimeEnv::process_memory_metrics() const {
    return ProcessMemoryMetrics::instance();
}

// Calculate the total memory limit of all load tasks on this BE
static int64_t calc_max_load_memory(int64_t process_mem_limit) {
    if (process_mem_limit == -1) {
        // no limit
        return -1;
    }
    int32_t max_load_memory_percent = config::load_process_max_memory_limit_percent;
    int64_t max_load_memory_bytes = process_mem_limit * max_load_memory_percent / 100;
    return std::min<int64_t>(max_load_memory_bytes, config::load_process_max_memory_limit_bytes);
}

static int64_t calc_max_compaction_memory(int64_t process_mem_limit) {
    int64_t limit = config::compaction_max_memory_limit;
    int64_t percent = config::compaction_max_memory_limit_percent;

    if (config::compaction_memory_limit_per_worker < 0) {
        config::compaction_memory_limit_per_worker = 2147483648; // 2G
    }

    if (process_mem_limit < 0) {
        return -1;
    }
    if (limit < 0) {
        limit = process_mem_limit;
    }
    if (percent < 0 || percent > 100) {
        percent = 100;
    }
    return std::min<int64_t>(limit, process_mem_limit * percent / 100);
}

static StatusOr<int64_t> calc_max_consistency_memory(int64_t process_mem_limit) {
    ASSIGN_OR_RETURN(int64_t limit, ParseUtil::parse_mem_spec(config::consistency_max_memory_limit, process_mem_limit));
    int64_t percent = config::consistency_max_memory_limit_percent;

    if (process_mem_limit < 0) {
        return -1;
    }
    if (limit < 0) {
        limit = process_mem_limit;
    }
    if (percent < 0 || percent > 100) {
        percent = 100;
    }
    return std::min<int64_t>(limit, process_mem_limit * percent / 100);
}

namespace {

bool allocate_hll_registers_with_mem_chunk_allocator(size_t size, void* /*ctx*/, MemChunk* chunk) {
    return MemChunkAllocator::allocate(size, chunk);
}

void free_hll_registers_with_mem_chunk_allocator(const MemChunk& chunk, void* /*ctx*/) {
    MemChunkAllocator::free(chunk);
}

void register_hll_registers_allocator() {
    HyperLogLog::RegistersAllocator allocator;
    allocator.allocate = allocate_hll_registers_with_mem_chunk_allocator;
    allocator.free = free_hll_registers_with_mem_chunk_allocator;
    Status st = HyperLogLog::set_registers_allocator(allocator);
    CHECK(st.ok()) << "failed to register hll registers allocator: " << st.to_string();
}

MemTracker* process_mem_tracker_provider() {
    return RuntimeEnv::GetInstance()->process_mem_tracker();
}

} // namespace

bool RuntimeEnv::_is_init = false;

JavaEnv* JavaEnv::GetInstance() {
    return RuntimeEnv::GetInstance()->java_env();
}

bool RuntimeEnv::is_init() {
    return _is_init;
}

Status RuntimeEnv::init(MetricRegistry* metrics) {
    _heartbeat_flags->update(0);
    RETURN_IF_ERROR(_init_mem_tracker(metrics));
    RETURN_IF_ERROR(global_python_env_registry().init(config::python_envs));
    CurrentThread::set_mem_tracker_source(&RuntimeEnv::is_init, process_mem_tracker_provider);
    if (_diagnose_daemon != nullptr) {
        _diagnose_daemon->stop();
        _diagnose_daemon.reset();
    }
    auto diagnose_daemon = std::make_unique<DiagnoseDaemon>();
    RETURN_IF_ERROR(diagnose_daemon->init());
    _diagnose_daemon = std::move(diagnose_daemon);
    _is_init = true;
    return Status::OK();
}

void RuntimeEnv::stop() {
    if (_diagnose_daemon != nullptr) {
        _diagnose_daemon->stop();
        _diagnose_daemon.reset();
    }
    _java_env.shutdown();
    _thread_pools.shutdown();
    _java_env.destroy();
    _thread_pools.destroy();
    _is_init = false;
    _reset_tracker();
}

Status RuntimeEnv::init_execution_thread_pools(MetricRegistry* metrics) {
    RETURN_IF_ERROR(_thread_pools.init_execution_thread_pools(metrics));
    return _java_env.init(metrics, config::enable_jvm_metrics);
}

Status RuntimeEnv::init_lake_thread_pools(MetricRegistry* metrics) {
    return _thread_pools.init_lake_thread_pools(metrics);
}

Status RuntimeEnv::_init_mem_tracker(MetricRegistry* metrics) {
    MemTracker::init_type_label_map();

    int64_t bytes_limit = 0;
    std::stringstream ss;
    // --mem_limit="" means no memory limit
    ASSIGN_OR_RETURN(bytes_limit, ParseUtil::parse_mem_spec(config::mem_limit, MemInfo::physical_mem()));
    // use 90% of mem_limit as the soft mem limit of BE
    bytes_limit = bytes_limit * 0.9;
    if (bytes_limit <= 0) {
        ss << "Failed to parse mem limit from '" + config::mem_limit + "'.";
        return Status::InternalError(ss.str());
    }

    if (bytes_limit > MemInfo::physical_mem()) {
        LOG(WARNING) << "Memory limit " << PrettyPrinter::print(bytes_limit, TUnit::BYTES)
                     << " exceeds physical memory of " << PrettyPrinter::print(MemInfo::physical_mem(), TUnit::BYTES)
                     << ". Using physical memory instead";
        bytes_limit = MemInfo::physical_mem();
    }

    if (bytes_limit <= 0) {
        ss << "Invalid mem limit: " << bytes_limit;
        return Status::InternalError(ss.str());
    }

    _process_mem_tracker = regist_tracker(MemTrackerType::PROCESS, bytes_limit, nullptr);
    // Give the dictionary decompression contexts a stable home before anything can read
    // a segment; see the hooks above.
    set_dict_dctx_alloc_scope(&enter_dctx_alloc_scope, &leave_dctx_alloc_scope);
    _jemalloc_metadata_tracker = regist_tracker(MemTrackerType::JEMALLOC, -1, process_mem_tracker());
    int64_t query_pool_mem_limit =
            calc_max_query_memory(_process_mem_tracker->limit(), config::query_max_memory_limit_percent);
    _query_pool_mem_tracker = regist_tracker(MemTrackerType::QUERY_POOL, query_pool_mem_limit, process_mem_tracker());
    int64_t query_pool_spill_limit = query_pool_mem_limit * config::query_pool_spill_mem_limit_threshold;
    _query_pool_mem_tracker->set_reserve_limit(query_pool_spill_limit);
    _connector_scan_pool_mem_tracker = regist_tracker(MemTrackerType::CONNECTOR_SCAN, query_pool_mem_limit, nullptr);
    _connector_scan_pool_mem_tracker->set_level(2);

    int64_t load_mem_limit = calc_max_load_memory(_process_mem_tracker->limit());
    _load_mem_tracker = regist_tracker(MemTrackerType::LOAD, load_mem_limit, process_mem_tracker());

    // Metadata statistics memory statistics do not use new mem statistics framework with hook
    _metadata_mem_tracker = regist_tracker(MemTrackerType::METADATA, -1, nullptr);
    _metadata_mem_tracker->set_level(2);

    _tablet_metadata_mem_tracker = regist_tracker(MemTrackerType::TABLET_METADATA, -1, metadata_mem_tracker());
    _rowset_metadata_mem_tracker = regist_tracker(MemTrackerType::ROWSET_METADATA, -1, metadata_mem_tracker());
    _segment_metadata_mem_tracker = regist_tracker(MemTrackerType::SEGMENT_METADATA, -1, metadata_mem_tracker());
    _column_metadata_mem_tracker = regist_tracker(MemTrackerType::COLUMN_METADATA, -1, metadata_mem_tracker());

    _tablet_schema_mem_tracker = regist_tracker(MemTrackerType::TABLET_SCHEMA, -1, tablet_metadata_mem_tracker());
    _segment_zonemap_mem_tracker = regist_tracker(MemTrackerType::SEGMENT_METADATA, -1, segment_metadata_mem_tracker());
    _short_key_index_mem_tracker = regist_tracker(MemTrackerType::SHORT_KEY_INDEX, -1, segment_metadata_mem_tracker());
    _column_zonemap_index_mem_tracker =
            regist_tracker(MemTrackerType::COLUMN_ZONEMAP_INDEX, -1, column_metadata_mem_tracker());
    _ordinal_index_mem_tracker = regist_tracker(MemTrackerType::ORDINAL_INDEX, -1, column_metadata_mem_tracker());
    _bitmap_index_mem_tracker = regist_tracker(MemTrackerType::BITMAP_INDEX, -1, column_metadata_mem_tracker());
    _bloom_filter_index_mem_tracker =
            regist_tracker(MemTrackerType::BLOOM_FILTER_INDEX, -1, column_metadata_mem_tracker());
    _builtin_inverted_index_mem_tracker =
            regist_tracker(MemTrackerType::BUILTIN_INVERTED_INDEX, -1, column_metadata_mem_tracker());

    int64_t compaction_mem_limit = calc_max_compaction_memory(_process_mem_tracker->limit());
    _compaction_mem_tracker = regist_tracker(MemTrackerType::COMPACTION, compaction_mem_limit, process_mem_tracker());
    _schema_change_mem_tracker = regist_tracker(MemTrackerType::SCHEMA_CHANGE, -1, process_mem_tracker());
    _page_cache_mem_tracker = regist_tracker(MemTrackerType::PAGE_CACHE, -1, process_mem_tracker());
    _jit_cache_mem_tracker = regist_tracker(MemTrackerType::JIT_CACHE, -1, process_mem_tracker());
    int32_t update_mem_percent = std::max(std::min(100, config::update_memory_limit_percent), 0);
    _update_mem_tracker = regist_tracker(MemTrackerType::UPDATE, bytes_limit * update_mem_percent / 100, nullptr);
    _update_mem_tracker->set_level(2);
    // P3: off-tree, byte-limited admission tracker for the lake publish path. pct is clamped to
    // [0,100]; pct==0 maps to an unlimited (-1) tracker -- the deliberate kill switch, NOT a 0-byte limit
    // that would freeze all publishing on the node.
    int32_t lake_publish_pct = std::max(0, std::min(100, config::lake_publish_memory_limit_percent));
    int64_t lake_publish_limit = (lake_publish_pct == 0) ? -1 : bytes_limit * lake_publish_pct / 100;
    _lake_publish_mem_tracker = regist_tracker(MemTrackerType::LAKE_PUBLISH, lake_publish_limit, nullptr);
    _lake_publish_mem_tracker->set_level(2);
    LOG(INFO) << "lake publish mem gate limit=" << lake_publish_limit << " (pct=" << lake_publish_pct << ")";
    _passthrough_mem_tracker = regist_tracker(MemTrackerType::PASSTHROUGH, -1, nullptr);
    _passthrough_mem_tracker->set_level(2);
    _brpc_iobuf_mem_tracker = regist_tracker(MemTrackerType::BRPC_IOBUF, -1, nullptr);
    _brpc_iobuf_mem_tracker->set_level(2);
    _clone_mem_tracker = regist_tracker(MemTrackerType::CLONE, -1, process_mem_tracker());
    ASSIGN_OR_RETURN(int64_t consistency_mem_limit, calc_max_consistency_memory(_process_mem_tracker->limit()));
    _consistency_mem_tracker =
            regist_tracker(MemTrackerType::CONSISTENCY, consistency_mem_limit, process_mem_tracker());
    _datacache_mem_tracker = regist_tracker(MemTrackerType::DATACACHE, -1, process_mem_tracker());
    _replication_mem_tracker = regist_tracker(MemTrackerType::REPLICATION, -1, process_mem_tracker());
    _vector_index_mem_tracker = regist_tracker(MemTrackerType::VECTOR_INDEX, -1, process_mem_tracker());

    register_hll_registers_allocator();
    if (metrics != nullptr) {
        register_mem_chunk_allocator_metrics(metrics);
    }

    return Status::OK();
}

std::vector<std::shared_ptr<MemTracker>> RuntimeEnv::mem_trackers() const {
    std::vector<std::shared_ptr<MemTracker>> mem_trackers;
    mem_trackers.reserve(_mem_tracker_map.size());
    for (auto& item : _mem_tracker_map) {
        mem_trackers.emplace_back(item.second);
    }
    return mem_trackers;
}

std::shared_ptr<MemTracker> RuntimeEnv::get_mem_tracker_by_type(MemTrackerType type) const {
    auto iter = _mem_tracker_map.find(type);
    if (iter != _mem_tracker_map.end()) {
        return iter->second;
    } else {
        return nullptr;
    }
}

void RuntimeEnv::_reset_tracker() {
    for (auto& iter : _mem_tracker_map) {
        iter.second.reset();
    }
}

std::shared_ptr<MemTracker> RuntimeEnv::regist_tracker(MemTrackerType type, int64_t bytes_limit, MemTracker* parent) {
    auto mem_tracker = std::make_shared<MemTracker>(type, bytes_limit, MemTracker::type_to_label(type), parent);
    _mem_tracker_map[type] = mem_tracker;
    return mem_tracker;
}

int64_t RuntimeEnv::calc_max_query_memory(int64_t process_mem_limit, int64_t percent) {
    if (process_mem_limit <= 0) {
        // -1 means no limit
        return -1;
    }
    if (percent < 0 || percent > 100) {
        percent = 90;
    }
    return process_mem_limit * percent / 100;
}

} // namespace starrocks
