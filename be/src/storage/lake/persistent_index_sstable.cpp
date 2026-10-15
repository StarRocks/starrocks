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

#include "storage/lake/persistent_index_sstable.h"

#include <butil/time.h> // NOLINT

#include <algorithm>
#include <chrono>
#include <limits>

#include "base/container/lru_cache.h"
#include "base/debug/trace.h"
#include "base/testutil/sync_point.h"
#include "base/utility/defer_op.h"
#include "common/config_primary_key_fwd.h"
#include "common/config_starlet_fwd.h"
#include "common/process_exit.h"
#include "fs/fs.h"
#include "fs/fs_factory.h"
#include "gen_cpp/types.pb.h"
#include "io/input_stream.h"
#include "platform/key_cache.h"
#include "storage/lake/lake_delvec_loader.h"
#include "storage/lake/utils.h"
#include "storage/sstable/comparator.h"
#include "storage/sstable/format.h"
#include "storage/sstable/iterator.h"
#include "storage/sstable/table_builder.h"
#include "storage/storage_metrics.h"

namespace starrocks::lake {

namespace {

// Cheap drop-in for `get_numeric_statistics()` on the publish hot path: a single
// virtual call returning a 13×int64 struct, no heap alloc / no strings / no vector.
// Plain POSIX (shared-nothing UT) returns all zero via the base default. Callers
// always pass non-null `_rf.get()` (set in `init()`) or non-null fresh `rf.get()`
// (from `ASSIGN_OR_RETURN`), so the only guard we need is against the rare case
// where the underlying file's stream pointer comes back null.
io::IoStatsSnapshot take_sstable_io_snapshot(RandomAccessFile* rf) {
    auto stream = rf->stream();
    return stream ? stream->get_io_stats_snapshot() : io::IoStatsSnapshot{};
}

void trace_open_io(const io::IoStatsSnapshot& before, const io::IoStatsSnapshot& after, bool probe) {
    TRACE_COUNTER_INCREMENT(probe ? "sst_probe_io_local_disk_bytes" : "sst_open_io_local_disk_bytes",
                            std::max<int64_t>(0, after.bytes_read_local_disk - before.bytes_read_local_disk));
    TRACE_COUNTER_INCREMENT(probe ? "sst_probe_io_remote_bytes" : "sst_open_io_remote_bytes",
                            std::max<int64_t>(0, after.bytes_read_remote - before.bytes_read_remote));
    TRACE_COUNTER_INCREMENT(probe ? "sst_probe_io_count_local_disk" : "sst_open_io_count_local_disk",
                            std::max<int64_t>(0, after.io_count_local_disk - before.io_count_local_disk));
    TRACE_COUNTER_INCREMENT(probe ? "sst_probe_io_count_remote" : "sst_open_io_count_remote",
                            std::max<int64_t>(0, after.io_count_remote - before.io_count_remote));
}

} // namespace

Status drop_corrupted_sstable_cache(const std::string& path) {
#if defined(USE_STAROS) && !defined(BUILD_FORMAT_LIB)
    if (!config::lake_clear_corrupted_cache_data) {
        return Status::NotSupported("lake_clear_corrupted_cache_data is turned off");
    }
    auto fs_or = FileSystemFactory::CreateSharedFromString(path);
    if (!fs_or.ok()) {
        LOG(INFO) << "clear corrupted cache for " << path << ", error:" << fs_or.status();
        return fs_or.status();
    }
    auto drop_status = (*fs_or)->drop_local_cache(path);
    TEST_SYNC_POINT_CALLBACK("PersistentIndexSstable::drop_corrupted_cache", &drop_status);
    LOG(INFO) << "clear corrupted cache for " << path << ", error:" << drop_status;
    return drop_status;
#else
    return Status::NotSupported("clear corrupted cache is only supported in shared-data mode");
#endif
}

Status SstReadStateMemoryContext::check_cancelled() const {
    return process_exit_in_progress() || (wait_scope && wait_scope->cancelled && wait_scope->cancelled())
                   ? Status::Cancelled("SST byte reservation cancelled")
                   : Status::OK();
}

SstReadStateMemoryContext::Reservation::~Reservation() {
    shrink_to(0);
}

void SstReadStateMemoryContext::Reservation::shrink_to(size_t bytes) {
    DCHECK_LE(bytes, _bytes);
    _context->tracker.release(_bytes - bytes);
    _bytes = bytes;
    _context->memory_cv.notify_all();
}

bool SstReadStateMemoryContext::Reservation::try_add(size_t bytes) {
    if (_context->refresh_usage) _context->refresh_usage();
    if (bytes > static_cast<size_t>(std::numeric_limits<int64_t>::max()) - _bytes ||
        !_context->check_cancelled().ok() || _context->tracker.try_consume(bytes)) {
        return false;
    }
    _bytes += bytes;
    TRACE_COUNTER_INCREMENT("pindex_sst_reserved_bytes", bytes);
    return true;
}

Status SstReadStateMemoryContext::Reservation::add(size_t bytes, bool wait, const std::function<bool()>& cancelled,
                                                   int64_t wait_timeout_ms) {
    TRACE_COUNTER_SCOPE_LATENCY_US("pindex_sst_byte_reservation_us");
    auto deadline =
            std::chrono::steady_clock::now() +
            std::chrono::milliseconds(wait_timeout_ms < 0 ? std::max(0, config::lake_pk_index_publish_wait_timeout_ms)
                                                          : wait_timeout_ms);
    if (_context->wait_scope && _context->wait_scope->deadline_ms >= 0) {
        deadline = std::min(deadline, std::chrono::steady_clock::now() +
                                              std::chrono::milliseconds(std::max<int64_t>(
                                                      0, _context->wait_scope->deadline_ms - MonotonicMillis())));
    }
    bool waited = false;
    if (_context->refresh_usage) _context->refresh_usage();
    auto trace_rejection = [&](MemTracker* exceeded) {
        TRACE_COUNTER_INCREMENT("pindex_sst_rejected_request_bytes", bytes);
        TRACE_COUNTER_INCREMENT("pindex_sst_rejected_reserved_bytes", _bytes);
        TRACE_COUNTER_INCREMENT("pindex_sst_rejected_tracker_limit_bytes", exceeded->limit());
        TRACE_COUNTER_INCREMENT("pindex_sst_rejected_tracker_used_bytes", exceeded->consumption());
        TRACE_COUNTER_INCREMENT("pindex_sst_rejected_read_state_used_bytes", _context->tracker.consumption());
        // Existing cache gauges are snapshots, not an atomic breakdown of the failing tracker.
        // Keep them separate from the exact reservation and from transient/open scratch bytes.
        auto* metrics = StorageMetrics::instance();
        TRACE_COUNTER_INCREMENT("pindex_sst_rejected_cached_memtable_bytes",
                                metrics->lake_pk_index_cache_memtable_bytes.value());
        TRACE_COUNTER_INCREMENT("pindex_sst_rejected_cached_index_bytes",
                                metrics->lake_pk_index_cache_index_block_bytes.value());
        TRACE_COUNTER_INCREMENT("pindex_sst_rejected_cached_filter_bytes",
                                metrics->lake_pk_index_cache_filter_bytes.value());
        TRACE_COUNTER_INCREMENT("pindex_sst_rejected_cached_delvec_bytes",
                                metrics->lake_pk_index_cache_delvec_bytes.value());
        TRACE_COUNTER_INCREMENT("pindex_sst_rejected_cached_overhead_bytes",
                                metrics->lake_pk_index_cache_overhead_bytes.value());
    };
    for (;;) {
        RETURN_IF_ERROR(_context->check_cancelled());
        if (cancelled && cancelled()) return Status::Cancelled("SST byte reservation cancelled");
        if (bytes > static_cast<size_t>(std::numeric_limits<int64_t>::max()) - _bytes) {
            return Status::MemoryLimitExceeded("SST read-state reservation overflows");
        }
        auto* exceeded = _context->tracker.try_consume(bytes);
        if (!exceeded) {
            _bytes += bytes;
            TRACE_COUNTER_INCREMENT("pindex_sst_reserved_bytes", bytes);
            return Status::OK();
        }
        if (_context->reclaim) _context->reclaim(bytes);
        exceeded = _context->tracker.try_consume(bytes);
        if (!exceeded) {
            _bytes += bytes;
            TRACE_COUNTER_INCREMENT("pindex_sst_reserved_bytes", bytes);
            return Status::OK();
        }
        // The global sweep skips the publish-pinned entry. Its lock owner can still drop its
        // own unpinned SSTs; otherwise one publisher can wait forever for memory it could free.
        if (wait && _reclaim_self) _reclaim_self(bytes);
        exceeded = _context->tracker.try_consume(bytes);
        if (!exceeded) {
            _bytes += bytes;
            TRACE_COUNTER_INCREMENT("pindex_sst_reserved_bytes", bytes);
            return Status::OK();
        }
        // Optional filters fall back immediately. This open implementation retains compressed
        // input while allocating decoded output; a rejected peak does not prove that the decoded
        // index alone exceeds the budget.
        if (!wait || (exceeded->limit() >= 0 && _bytes + bytes > exceeded->limit())) {
            TRACE_COUNTER_INCREMENT("pindex_sst_byte_reservation_rejected", 1);
            trace_rejection(exceeded);
            return Status::MemoryLimitExceeded(
                    fmt::format("SST read-state working set needs {} bytes; tracker {} limit={} consumption={}",
                                _bytes + bytes, exceeded->label(), exceeded->limit(), exceeded->consumption()));
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            TRACE_COUNTER_INCREMENT("pindex_sst_byte_reservation_timeout", 1);
            trace_rejection(exceeded);
            return Status::TimedOut("SST read-state byte reservation exceeded its bounded wait");
        }
        if (!waited) {
            TRACE_COUNTER_INCREMENT("pindex_sst_byte_reservation_wait_count", 1);
            waited = true;
            TEST_SYNC_POINT_CALLBACK("SstReadStateMemoryContext::Reservation::waiting", nullptr);
        }
        std::unique_lock<std::mutex> lock(_context->memory_mutex);
        const auto wait_begin =
                _context->wait_scope ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{};
        _context->memory_cv.wait_until(
                lock, std::min(deadline, std::chrono::steady_clock::now() + std::chrono::milliseconds(100)));
        if (_context->wait_scope) {
            _context->wait_scope->memory_wait_us.fetch_add(
                    std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - wait_begin)
                            .count(),
                    std::memory_order_relaxed);
        }
    }
}

struct PersistentIndexSstable::ReadState {
    ~ReadState() {
        table.reset();
        reservation.reset();
        permit.reset();
    }
    std::shared_ptr<SstReadStateMemoryContext::Reservation> reservation;
    std::unique_ptr<PkIndexPublishLimiter::Permit> permit;
    // Destroy the table before the file and filter policy its iterators/options refer to.
    std::unique_ptr<sstable::FilterPolicy> filter_policy;
    std::unique_ptr<RandomAccessFile> rf;
    std::unique_ptr<sstable::Table> table;
};

StatusOr<std::unique_ptr<RandomAccessFile>> PersistentIndexSstable::_open_file() const {
    RandomAccessFileOptions opts;
    if (!_sstable_pb.encryption_meta().empty()) {
        ASSIGN_OR_RETURN(auto info, KeyCache::instance().unwrap_encryption_meta(_sstable_pb.encryption_meta()));
        opts.encryption_info = std::move(info);
    }
    auto rf = fs::new_random_access_file(opts, _filename);
    if (!rf.ok()) {
        // File-open failures return before Table::Open can report a read error. Count them here,
        // including failures while reopening after a corrupted-cache retry, and preserve the cause.
        StorageMetrics::instance()->pk_index_sst_read_error_total.increment(1);
        LOG(WARNING) << "Failed to open PersistentIndex SST file: " << _sstable_pb.filename()
                     << ", error: " << rf.status();
    }
    return rf;
}

StatusOr<std::shared_ptr<PersistentIndexSstable::ReadState>> PersistentIndexSstable::_open_read_state(
        std::unique_ptr<RandomAccessFile> rf, const std::shared_ptr<SstReadStateMemoryContext>& context,
        const std::function<void(size_t)>& reclaim_self) const {
    auto state = std::make_shared<ReadState>();
    if (context && config::lake_pk_index_partial_eviction) {
        state->reservation = std::make_shared<SstReadStateMemoryContext::Reservation>(context, reclaim_self);
        // Table/iterator/filter-policy objects and open-time bookkeeping are small but nonzero.
        RETURN_IF_ERROR(state->reservation->add(kReadStateObjectMemoryReserve, true));
    }
    state->rf = std::move(rf);
    sstable::Options options;
    if (state->reservation) {
        options.reserve_read_state_memory = [reservation = state->reservation](size_t bytes, bool wait) {
            return reservation->add(bytes, wait);
        };
        options.try_reserve_read_state_memory = [reservation = state->reservation](size_t bytes) {
            return reservation->try_add(bytes);
        };
        options.check_read_state_cancelled = [context] {
            return context->check_cancelled().ok() ? Status::OK() : Status::Cancelled("SST windowed read cancelled");
        };
    }
    if (_need_filter) {
        state->filter_policy.reset(const_cast<sstable::FilterPolicy*>(sstable::NewBloomFilterPolicy(10)));
        options.filter_policy = state->filter_policy.get();
    }
    options.block_cache = _cache;
    // Preserve checksum verification and the local-cache corruption retry on both initial open and reopen.
    options.paranoid_checks = config::lake_pk_index_sst_verify_checksum;
    auto open_table = [&] {
        const auto before = take_sstable_io_snapshot(state->rf.get());
        DeferOp report_io([&] { trace_open_io(before, take_sstable_io_snapshot(state->rf.get()), false); });
        return sstable::Table::Open(options, state->rf.get(), _sstable_pb.filesize(), state->table, _block_cache_id);
    };
    auto open_st = open_table();
    TEST_SYNC_POINT_CALLBACK("PersistentIndexSstable::init:table_open_error", &open_st);
    if (open_st.is_corruption()) {
        auto drop_status = drop_corrupted_sstable_cache(_filename);
        if (drop_status.ok()) {
            state->table.reset();
            if (state->reservation) state->reservation->shrink_to(kReadStateObjectMemoryReserve);
            ASSIGN_OR_RETURN(state->rf, _open_file());
            open_st = open_table();
            TEST_SYNC_POINT_CALLBACK("PersistentIndexSstable::init:table_open_retry_error", &open_st);
        }
    }
    if (!open_st.ok()) {
        if (!open_st.is_mem_limit_exceeded() && !open_st.is_time_out() && !open_st.is_cancelled()) {
            StorageMetrics::instance()->pk_index_sst_read_error_total.increment(1);
        }
        LOG(WARNING) << "Failed to open PersistentIndex SST file: " << _sstable_pb.filename() << ", error: " << open_st;
        return open_st;
    }
    _read_state_memory_demand.store(state->table->memory_usage());
    if (state->reservation) {
        // Open scratch and the metaindex block are gone; retain only resident bytes plus objects.
        state->reservation->shrink_to(state->table->memory_usage() + kReadStateObjectMemoryReserve);
    }
    return state;
}

StatusOr<size_t> PersistentIndexSstable::probe_minimum_read_memory() const {
    if (const auto known = _minimum_read_memory.load(); known != 0) return known;
    TRACE_COUNTER_SCOPE_LATENCY_US("pindex_sst_working_set_probe_us");
    auto probe = [&](RandomAccessFile* file) -> StatusOr<size_t> {
        const auto before = take_sstable_io_snapshot(file);
        DeferOp report_io([&] { trace_open_io(before, take_sstable_io_snapshot(file), true); });
        if (_sstable_pb.filesize() < sstable::Footer::kEncodedLength) {
            return Status::Corruption("file is too short to be an sstable");
        }
        char footer_bytes[sstable::Footer::kEncodedLength];
        RETURN_IF_ERROR(
                file->read_at_fully(_sstable_pb.filesize() - sizeof(footer_bytes), footer_bytes, sizeof(footer_bytes)));
        Slice footer_input(footer_bytes, sizeof(footer_bytes));
        sstable::Footer footer;
        RETURN_IF_ERROR(footer.DecodeFrom(&footer_input));
        sstable::BlockReadMemory memory;
        RETURN_IF_ERROR(sstable::ProbeBlockMemory(file, footer.index_handle(), &memory));
        if (memory.minimum_open_bytes() > std::numeric_limits<size_t>::max() - kReadStateObjectMemoryReserve) {
            return Status::Corruption("SST minimum read-state size overflows");
        }
        return memory.minimum_open_bytes() + kReadStateObjectMemoryReserve;
    };
    ASSIGN_OR_RETURN(auto file, _open_file());
    auto result = probe(file.get());
    if (!result.ok() && result.status().is_corruption() && drop_corrupted_sstable_cache(_filename).ok()) {
        ASSIGN_OR_RETURN(file, _open_file());
        result = probe(file.get());
    }
    if (!result.ok()) {
        StorageMetrics::instance()->pk_index_sst_read_error_total.increment(1);
        return result.status();
    }
    // This is an immutable-file sizing hint. Actual input/checksum and byte reservations are
    // still validated by ReadBlock; it never authorizes an allocation from this hint alone.
    _minimum_read_memory.store(*result);
    TRACE_COUNTER_INCREMENT("pindex_sst_working_set_probe_count", 1);
    return *result;
}

StatusOr<std::shared_ptr<PersistentIndexSstable::ReadState>> PersistentIndexSstable::_get_read_state() const {
    std::unique_lock<std::mutex> lock(_read_state_mutex);
    if (!config::lake_pk_index_partial_eviction) {
        _read_state_cv.wait(lock, [&] { return !_read_state_opening; });
    } else {
        TRACE_COUNTER_SCOPE_LATENCY_US("pindex_sst_shared_reopen_wait_us");
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(std::max(0, config::lake_pk_index_publish_wait_timeout_ms));
        while (_read_state_opening) {
            if (process_exit_in_progress()) return Status::Cancelled("SST shared reopen cancelled");
            if (std::chrono::steady_clock::now() >= deadline) {
                TRACE_COUNTER_INCREMENT("pindex_sst_shared_reopen_timeout", 1);
                return Status::TimedOut("SST shared reopen exceeded its bounded wait");
            }
            _read_state_cv.wait_until(
                    lock, std::min(deadline, std::chrono::steady_clock::now() + std::chrono::milliseconds(100)));
        }
    }
    if (_read_state) {
        return _read_state;
    }
    if (_filename.empty()) {
        return Status::InvalidArgument("SSTable is not initialized");
    }
    if (auto state = _transient_read_state.lock()) {
        return state;
    }
    const bool cacheable = _read_state_cacheable || !config::lake_pk_index_partial_eviction;
    const auto context = _memory_context;
    const auto reclaim_self = _reclaim_self;
    _read_state_opening = true;
    // Publish a single-flight marker, then drop the SST lock before IO/reservation. Owner-side
    // reclamation can now inspect charges and try to evict other SSTs without a lock inversion.
    lock.unlock();
    DeferOp finish_open([&] {
        if (!lock.owns_lock()) lock.lock();
        _read_state_opening = false;
        _read_state_cv.notify_all();
    });
    std::unique_ptr<PkIndexPublishLimiter::Permit> permit;
    if (!cacheable && context) {
        ASSIGN_OR_RETURN(permit, context->limiter.acquire(
                                         std::max(0, config::lake_pk_index_publish_wait_timeout_ms), [] { return 1; },
                                         [] { return process_exit_in_progress(); }, true));
    }
    TRACE_COUNTER_SCOPE_LATENCY_US("pindex_sst_reopen_us");
    TRACE_COUNTER_INCREMENT("pindex_sst_reopen_count", 1);
    ASSIGN_OR_RETURN(auto rf, _open_file());
    ASSIGN_OR_RETURN(auto state, _open_read_state(std::move(rf), context, reclaim_self));
    TRACE_COUNTER_INCREMENT("pindex_sst_reopen_bytes", state->table->memory_usage());
    lock.lock();
    if (cacheable || !context) {
        _read_state = state;
    } else {
        state->permit = std::move(permit);
        _transient_read_state = state;
        TRACE_COUNTER_INCREMENT("pindex_sst_transient_bytes", state->table->memory_usage());
    }
    return state;
}

void PersistentIndexSstable::set_read_state_cacheable(bool cacheable,
                                                      const std::shared_ptr<SstReadStateMemoryContext>& context) const {
    std::lock_guard<std::mutex> lock(_read_state_mutex);
    _memory_context = context;
    _read_state_cacheable = cacheable;
    if (!cacheable && _read_state && _read_state.use_count() == 1) {
        TRACE_COUNTER_INCREMENT("pindex_sst_read_state_evict_bytes", _read_state->table->memory_usage());
        _read_state.reset();
    }
}

Status PersistentIndexSstable::reopen_read_state() const {
    ASSIGN_OR_RETURN(auto state, _get_read_state());
    return Status::OK();
}

size_t PersistentIndexSstable::evict_read_state() const {
    std::unique_lock<std::mutex> lock(_read_state_mutex, std::try_to_lock);
    if (!lock.owns_lock() || _read_state_opening || !_read_state || _read_state.use_count() != 1) {
        return 0;
    }
    const size_t bytes = _read_state->table->memory_usage();
    _read_state.reset();
    // A lazy fallback after eviction uses a single transient SST at a time. prepare_primary_index
    // can explicitly reserve a resident batch and turn caching back on before its parallel reopen.
    if (_memory_context) {
        _read_state_cacheable = false;
    }
    TRACE_COUNTER_INCREMENT("pindex_sst_read_state_evict_bytes", bytes);
    return bytes;
}

sstable::Iterator* PersistentIndexSstable::new_iterator(const sstable::ReadOptions& options) {
    auto state_or = _get_read_state();
    if (!state_or.ok()) {
        return sstable::NewErrorIterator(state_or.status());
    }
    auto state = std::move(state_or).value();
    auto* iterator = state->table->NewIterator(options);
    iterator->RegisterCleanup([](void* ptr, void*) { delete static_cast<std::shared_ptr<ReadState>*>(ptr); },
                              new std::shared_ptr<ReadState>(std::move(state)), nullptr);
    return iterator;
}

Status PersistentIndexSstable::init(std::unique_ptr<RandomAccessFile> rf, const PersistentIndexSstablePB& sstable_pb,
                                    Cache* cache, bool need_filter, DelVectorPtr delvec,
                                    const TabletMetadataPtr& metadata, TabletManager* tablet_mgr,
                                    const std::shared_ptr<SstReadStateMemoryContext>& memory_context,
                                    bool defer_read_state, std::function<void(size_t)> reclaim_self) {
    _filename = rf->filename();
    _sstable_pb.CopyFrom(sstable_pb);
    _cache = cache;
    _need_filter = need_filter;
    _block_cache_id = 0;
    _memory_context = memory_context;
    _reclaim_self = std::move(reclaim_self);
    _read_state_cacheable = !defer_read_state;
    if (defer_read_state) {
        _block_cache_id = cache ? cache->new_id() : 0;
    } else {
        ASSIGN_OR_RETURN(_read_state, _open_read_state(std::move(rf), _memory_context, _reclaim_self));
        _block_cache_id = _read_state->table->cache_id();
    }
    // load delvec
    if (_sstable_pb.has_delvec() && _sstable_pb.delvec().size() > 0) {
        if (delvec) {
            // If delvec is already provided, use it directly.
            _delvec = std::move(delvec);
        } else {
            if (metadata == nullptr) {
                return Status::InvalidArgument("metadata is null when loading delvec from file");
            }
            if (tablet_mgr == nullptr) {
                return Status::InvalidArgument("tablet_mgr is null when loading delvec from file");
            }
            // otherwise, load delvec from file
            LakeIOOptions lake_io_opts{.fill_data_cache = true, .skip_disk_cache = false};
            auto delvec_loader =
                    std::make_unique<LakeDelvecLoader>(tablet_mgr, nullptr, true /* fill cache */, lake_io_opts);
            RETURN_IF_ERROR(delvec_loader->load_from_meta(metadata, _sstable_pb.delvec(), &_delvec));
        }
    }
    return Status::OK();
}

Status PersistentIndexSstable::build_sstable(const phmap::btree_map<std::string, IndexValueWithVer, std::less<>>& map,
                                             WritableFile* wf, uint64_t* filesz,
                                             PersistentIndexSstableRangePB* range_pb) {
    std::unique_ptr<sstable::FilterPolicy> filter_policy;
    filter_policy.reset(const_cast<sstable::FilterPolicy*>(sstable::NewBloomFilterPolicy(10)));
    sstable::Options options;
    options.filter_policy = filter_policy.get();
    sstable::TableBuilder builder(options, wf);
    for (const auto& [k, v] : map) {
        IndexValuesWithVerPB index_value_pb;
        auto* value = index_value_pb.add_values();
        value->set_version(v.first);
        value->set_rssid(v.second.get_rssid());
        value->set_rowid(v.second.get_rowid());
        RETURN_IF_ERROR(builder.Add(Slice(k), Slice(index_value_pb.SerializeAsString())));
    }
    if (auto st = builder.Finish(); !st.ok()) {
        StorageMetrics::instance()->pk_index_sst_write_error_total.increment(1);
        LOG(WARNING) << "Failed to finish PersistentIndex SST, error: " << st;
        return st;
    }
    *filesz = builder.FileSize();
    if (range_pb != nullptr) {
        auto [key_start, key_end] = builder.KeyRange();
        range_pb->set_start_key(key_start.to_string());
        range_pb->set_end_key(key_end.to_string());
    }
    return Status::OK();
}

Status PersistentIndexSstable::build_tombstone_sstable(const Slice* sorted_keys, size_t n, int64_t version,
                                                       WritableFile* wf, uint64_t* filesz,
                                                       PersistentIndexSstableRangePB* range_pb) {
    std::unique_ptr<sstable::FilterPolicy> filter_policy;
    filter_policy.reset(const_cast<sstable::FilterPolicy*>(sstable::NewBloomFilterPolicy(10)));
    sstable::Options options;
    options.filter_policy = filter_policy.get();
    sstable::TableBuilder builder(options, wf);
    // The tombstone value is identical for every key (|version| + the NullIndexValue split), so serialize
    // it once and reuse the same bytes for every Add. Over a large delete this avoids a per-key protobuf
    // serialization -- the dominant cost when the sort is skipped.
    IndexValuesWithVerPB index_value_pb;
    auto* value = index_value_pb.add_values();
    value->set_version(version);
    // Tombstone encoding: rssid == rowid == UINT32_MAX (== NullIndexValue split), see is_index_tombstone().
    value->set_rssid(std::numeric_limits<uint32_t>::max());
    value->set_rowid(std::numeric_limits<uint32_t>::max());
    std::string serialized;
    index_value_pb.SerializeToString(&serialized);
    const Slice value_slice(serialized);
    // |sorted_keys| must be ascending; skip adjacent duplicates so a repeated key does not trip the
    // strictly-increasing check in TableBuilder::Add. Any out-of-order key (a broken caller assumption)
    // still surfaces as an Add error rather than silent corruption.
    for (size_t i = 0; i < n; ++i) {
        if (i > 0 && options.comparator->Compare(sorted_keys[i], sorted_keys[i - 1]) == 0) {
            continue;
        }
        RETURN_IF_ERROR(builder.Add(sorted_keys[i], value_slice));
    }
    if (auto st = builder.Finish(); !st.ok()) {
        StorageMetrics::instance()->pk_index_sst_write_error_total.increment(1);
        LOG(WARNING) << "Failed to finish PersistentIndex tombstone SST, error: " << st;
        return st;
    }
    *filesz = builder.FileSize();
    if (range_pb != nullptr) {
        auto [key_start, key_end] = builder.KeyRange();
        range_pb->set_start_key(key_start.to_string());
        range_pb->set_end_key(key_end.to_string());
    }
    return Status::OK();
}

Status PersistentIndexSstable::multi_get(const Slice* keys, const KeyIndexSet& key_indexes, int64_t version,
                                         IndexValue* values, KeyIndexSet* found_key_indexes) const {
    ASSIGN_OR_RETURN(auto state, _get_read_state());
    std::vector<std::string> index_value_with_vers(key_indexes.size());
    sstable::ReadIOStat stat;
    sstable::ReadOptions options;
    options.stat = &stat;
    if (state->reservation && config::lake_pk_index_partial_eviction) {
        const auto limit = state->reservation->parent_limit();
        if (_skip_data_block_cache.load() ||
            (limit > 0 && state->table->memory_usage() > static_cast<size_t>(limit / 2))) {
            options.fill_cache = false;
            TRACE_COUNTER_INCREMENT("pindex_large_sst_data_cache_bypass_count", 1);
        }
    }
    // Catch corrupted data blocks as Corruption (instead of silently returning wrong
    // index values) so the drop-cache-and-retry below can heal a bad local cache.
    options.verify_checksums = config::lake_pk_index_sst_verify_checksum;
    // Read through a per-call file handle instead of the shared `_rf`: multi_get runs concurrently
    // on the pk index execution pool, and file-class state must not be shared across those readers.
    // A fresh handle also starts its IO counters at 0, so the deltas below are the absolute IO done
    // by this multi_get.
    RandomAccessFileOptions opts;
    if (!_sstable_pb.encryption_meta().empty()) {
        ASSIGN_OR_RETURN(auto info, KeyCache::instance().unwrap_encryption_meta(_sstable_pb.encryption_meta()));
        opts.encryption_info = std::move(info);
    }
    ASSIGN_OR_RETURN(auto rf, fs::new_random_access_file(opts, _filename));
    // Test seam: the per-call handle is the only thing the IO-breakdown counters below measure, so a
    // UT has to be able to wrap it in a stream that reports synthetic local/remote statistics. A
    // shared-nothing POSIX file exposes none, which would leave the counters pinned at zero.
    TEST_SYNC_POINT_CALLBACK("PersistentIndexSstable::multi_get:opened_file", &rf);
    options.file = rf.get();
    io::IoStatsSnapshot io_snap_before = take_sstable_io_snapshot(rf.get());
    // Currently, there is no need to set predicate for MultiGet of persistent index sstable. Because predicate
    // only used for sstable compaction to filter out some keys for tablet split purpose and such keys can not
    // be read by the persistent index by designed. So even we provide a predicate, all keys read by multi_get
    // will always meet the condition.
    auto start_ts = butil::gettimeofday_us();
    auto multiget_st =
            state->table->MultiGet(options, keys, key_indexes.begin(), key_indexes.end(), &index_value_with_vers);
    TEST_SYNC_POINT_CALLBACK("PersistentIndexSstable::multi_get:error", &multiget_st);
    if (multiget_st.is_corruption()) {
        auto drop_status = drop_corrupted_sstable_cache(_filename);
        if (drop_status.ok()) {
            multiget_st = state->table->MultiGet(options, keys, key_indexes.begin(), key_indexes.end(),
                                                 &index_value_with_vers);
            TEST_SYNC_POINT_CALLBACK("PersistentIndexSstable::multi_get:retry_error", &multiget_st);
        }
    }
    if (!multiget_st.ok()) {
        StorageMetrics::instance()->pk_index_sst_read_error_total.increment(1);
        LOG(WARNING) << "Failed to multi_get from PersistentIndex SST file: " << _sstable_pb.filename()
                     << ", error: " << multiget_st;
        return multiget_st;
    }
    auto end_ts = butil::gettimeofday_us();
    io::IoStatsSnapshot io_snap_after = take_sstable_io_snapshot(rf.get());
    TRACE_COUNTER_INCREMENT("multi_get_us", end_ts - start_ts);
    TRACE_COUNTER_INCREMENT("read_block_hit_cache_cnt", stat.block_cnt_from_cache);
    TRACE_COUNTER_INCREMENT("read_block_miss_cache_cnt", stat.block_cnt_from_file);
    // Break down the misses into reads served by the local data cache vs. reads that went out to
    // the remote object store (S3/OSS/etc.). The per-call handle above makes the before-snapshot all
    // zero and the same object is measured twice, so the deltas cannot go negative on their own; the
    // clamp stays as a cheap guard against a stream implementation reporting non-monotonic counters.
    TRACE_COUNTER_INCREMENT(
            "sstable_io_local_disk_bytes",
            std::max<int64_t>(0, io_snap_after.bytes_read_local_disk - io_snap_before.bytes_read_local_disk));
    TRACE_COUNTER_INCREMENT("sstable_io_remote_bytes",
                            std::max<int64_t>(0, io_snap_after.bytes_read_remote - io_snap_before.bytes_read_remote));
    TRACE_COUNTER_INCREMENT(
            "sstable_io_count_local_disk",
            std::max<int64_t>(0, io_snap_after.io_count_local_disk - io_snap_before.io_count_local_disk));
    TRACE_COUNTER_INCREMENT("sstable_io_count_remote",
                            std::max<int64_t>(0, io_snap_after.io_count_remote - io_snap_before.io_count_remote));
    size_t i = 0;
    for (auto& key_index : key_indexes) {
        // Index_value_with_vers is empty means key is not found in sst.
        // Value in sst can not be empty.
        if (index_value_with_vers[i].empty()) {
            ++i;
            continue;
        }
        IndexValuesWithVerPB index_value_with_ver_pb;
        if (!index_value_with_ver_pb.ParseFromString(index_value_with_vers[i])) {
            return Status::InternalError("parse index value info failed");
        }
        // Check if this rowid is already filtered by delvec
        if (_delvec && !_delvec->empty()) {
            if (_delvec->roaring()->contains(index_value_with_ver_pb.values(0).rowid())) {
                ++i;
                continue;
            }
        }
        // Tombstone-aware projection: see is_index_tombstone() in storage/lake/utils.h
        // for why the rssid/rowid sentinel must be preserved through both the
        // shared_rssid overwrite and the rssid_offset shift. Version is independent of
        // the NullIndexValue encoding and is projected onto every entry (including
        // tombstones) so that the version-equality lookup below matches them.
        if (_sstable_pb.has_shared_version() && _sstable_pb.shared_version() > 0) {
            DCHECK(_sstable_pb.has_shared_rssid());
            for (size_t j = 0; j < index_value_with_ver_pb.values_size(); ++j) {
                index_value_with_ver_pb.mutable_values(j)->set_version(_sstable_pb.shared_version());
                if (is_index_tombstone(index_value_with_ver_pb.values(j))) continue;
                index_value_with_ver_pb.mutable_values(j)->set_rssid(_sstable_pb.shared_rssid());
            }
        }
        if (_sstable_pb.rssid_offset() != 0) {
            for (size_t j = 0; j < index_value_with_ver_pb.values_size(); ++j) {
                if (is_index_tombstone(index_value_with_ver_pb.values(j))) continue;
                const int64_t rssid =
                        static_cast<int64_t>(index_value_with_ver_pb.values(j).rssid()) + _sstable_pb.rssid_offset();
                index_value_with_ver_pb.mutable_values(j)->set_rssid(static_cast<uint32_t>(rssid));
            }
        }

        if (index_value_with_ver_pb.values_size() > 0) {
            if (version < 0) {
                values[key_index] = build_index_value(index_value_with_ver_pb.values(0));
                found_key_indexes->insert(key_index);
            } else {
                for (size_t j = 0; j < index_value_with_ver_pb.values_size(); ++j) {
                    if (index_value_with_ver_pb.values(j).version() == version) {
                        values[key_index] = build_index_value(index_value_with_ver_pb.values(j));
                        found_key_indexes->insert(key_index);
                        break;
                    }
                }
            }
        }
        ++i;
    }
    return Status::OK();
}

size_t PersistentIndexSstable::index_block_memory_usage() const {
    std::lock_guard<std::mutex> lock(_read_state_mutex);
    return _read_state ? _read_state->table->index_block_memory_usage() : 0;
}

size_t PersistentIndexSstable::filter_memory_usage() const {
    std::lock_guard<std::mutex> lock(_read_state_mutex);
    return _read_state ? _read_state->table->filter_memory_usage() : 0;
}

size_t PersistentIndexSstable::memory_usage() const {
    std::lock_guard<std::mutex> lock(_read_state_mutex);
    return _read_state ? _read_state->table->memory_usage() : 0;
}

size_t PersistentIndexSstable::metadata_memory_usage() const {
    std::lock_guard<std::mutex> lock(_read_state_mutex);
    size_t bytes = sizeof(*this) + _sstable_pb.SpaceUsedLong() + _filename.capacity() + delvec_memory_usage();
    if (_read_state && _read_state->reservation) {
        bytes += _read_state->reservation->bytes() - _read_state->table->memory_usage();
    }
    return bytes;
}

size_t PersistentIndexSstable::directly_accounted_memory_usage() const {
    std::lock_guard<std::mutex> lock(_read_state_mutex);
    return _read_state && _read_state->reservation ? _read_state->reservation->bytes() : 0;
}

size_t PersistentIndexSstable::reserved_read_state_overhead() const {
    std::lock_guard<std::mutex> lock(_read_state_mutex);
    return _read_state && _read_state->reservation
                   ? _read_state->reservation->bytes() - _read_state->table->memory_usage()
                   : 0;
}

PersistentIndexSstable::ReadStateMemoryUsage PersistentIndexSstable::read_state_memory_usage() const {
    std::lock_guard<std::mutex> lock(_read_state_mutex);
    ReadStateMemoryUsage usage;
    if (_read_state) {
        usage.index = _read_state->table->index_block_memory_usage();
        usage.filter = _read_state->table->filter_memory_usage();
        if (_read_state->reservation) {
            usage.direct = _read_state->reservation->bytes();
            usage.objects = usage.direct - usage.index - usage.filter;
        }
    }
    return usage;
}

size_t PersistentIndexSstable::delvec_memory_usage() const {
    return (_delvec != nullptr) ? _delvec->memory_usage() : 0;
}

Status PersistentIndexSstable::sample_keys(std::vector<std::string>* keys, size_t sample_interval_bytes) const {
    ASSIGN_OR_RETURN(auto state, _get_read_state());
    return state->table->sample_keys(keys, sample_interval_bytes);
}

Status PersistentIndexSstable::sample_data_keys(std::vector<std::string>* keys, const Slice& seek_key,
                                                const Slice& stop_key, size_t max_samples) const {
    ASSIGN_OR_RETURN(auto state, _get_read_state());
    std::vector<std::string> separators;
    RETURN_IF_ERROR(state->table->sample_keys_in_range(&separators, seek_key, stop_key, max_samples));

    sstable::ReadOptions options;
    options.fill_cache = false;
    // The seeks below read real data blocks; catch corrupted bytes as Corruption
    // instead of letting them feed wrong keys into the split samples.
    options.verify_checksums = config::lake_pk_index_sst_verify_checksum;
    std::unique_ptr<sstable::Iterator> iterator(state->table->NewIterator(options));
    for (const auto& separator : separators) {
        iterator->Seek(Slice(separator));
        if (!iterator->Valid()) {
            RETURN_IF_ERROR(iterator->status());
            continue;
        }
        keys->emplace_back(iterator->key().to_string());
    }
    return iterator->status();
}

StatusOr<PersistentIndexSstableUniquePtr> PersistentIndexSstable::new_sstable(
        const PersistentIndexSstablePB& sstable_pb, const std::string& location, Cache* cache, bool need_filter,
        const DelVectorPtr& delvec, const TabletMetadataPtr& metadata, TabletManager* tablet_mgr,
        const std::shared_ptr<SstReadStateMemoryContext>& memory_context, bool defer_read_state,
        std::function<void(size_t)> reclaim_self) {
    auto sstable = std::make_unique<PersistentIndexSstable>();
    RandomAccessFileOptions opts;
    if (!sstable_pb.encryption_meta().empty()) {
        ASSIGN_OR_RETURN(auto info, KeyCache::instance().unwrap_encryption_meta(sstable_pb.encryption_meta()));
        opts.encryption_info = std::move(info);
    }
    ASSIGN_OR_RETURN(auto rf, fs::new_random_access_file(opts, location));
    RETURN_IF_ERROR(sstable->init(std::move(rf), sstable_pb, cache, need_filter, delvec, metadata, tablet_mgr,
                                  memory_context, defer_read_state, std::move(reclaim_self)));
    return std::move(sstable);
}

PersistentIndexSstableStreamBuilder::PersistentIndexSstableStreamBuilder(std::unique_ptr<WritableFile> wf,
                                                                         std::string encryption_meta)
        : _wf(std::move(wf)), _encryption_meta(std::move(encryption_meta)) {
    _filter_policy.reset(const_cast<sstable::FilterPolicy*>(sstable::NewBloomFilterPolicy(10)));
    sstable::Options options;
    options.filter_policy = _filter_policy.get();
    _table_builder = std::make_unique<sstable::TableBuilder>(options, _wf.get());
}

Status PersistentIndexSstableStreamBuilder::add(const Slice& key) {
    if (_finished) {
        return Status::InvalidArgument("Builder already finished");
    }

    IndexValuesWithVerPB index_value_pb;
    auto* val = index_value_pb.add_values();
    val->set_rowid(_sst_rowid++);

    RETURN_IF_ERROR(_table_builder->Add(key, Slice(index_value_pb.SerializeAsString())));
    return _table_builder->status();
}

Status PersistentIndexSstableStreamBuilder::finish(uint64_t* file_size) {
    if (_finished) {
        return Status::InvalidArgument("Builder already finished");
    }

    RETURN_IF_ERROR(_table_builder->Finish());
    _finished = true;
    if (file_size != nullptr) {
        *file_size = _table_builder->FileSize();
    }
    return _table_builder->status();
}

uint64_t PersistentIndexSstableStreamBuilder::num_entries() const {
    return _table_builder ? _table_builder->NumEntries() : 0;
}

FileInfo PersistentIndexSstableStreamBuilder::file_info() const {
    FileInfo file_info;
    file_info.path = file_name(_wf->filename());
    file_info.size = _table_builder ? _table_builder->FileSize() : 0;
    file_info.encryption_meta = _encryption_meta;
    return file_info;
}

std::pair<Slice, Slice> PersistentIndexSstableStreamBuilder::key_range() const {
    DCHECK(_table_builder != nullptr);
    return _table_builder->KeyRange();
}

} // namespace starrocks::lake
