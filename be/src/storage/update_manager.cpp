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

#include "storage/update_manager.h"

#include <limits>
#include <memory>
#include <numeric>

#include "gutil/endian.h"
#include "storage/chunk_helper.h"
#include "storage/del_vector.h"
#include "storage/kv_store.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage/tablet_meta_manager.h"
#include "util/pretty_printer.h"
#include "util/starrocks_metrics.h"
#include "util/time.h"

namespace starrocks {

Status LocalDelvecLoader::load(const TabletSegmentId& tsid, int64_t version, DelVectorPtr* pdelvec) {
    return StorageEngine::instance()->update_manager()->get_del_vec(_meta, tsid, version, pdelvec);
}

UpdateManager::UpdateManager(MemTracker* mem_tracker)
        : _index_cache(std::numeric_limits<size_t>::max()), _update_state_cache(std::numeric_limits<size_t>::max()) {
    _update_mem_tracker = mem_tracker;
    _update_state_mem_tracker = std::make_unique<MemTracker>(-1, "rowset_update_state", mem_tracker);
    _index_cache_mem_tracker = std::make_unique<MemTracker>(-1, "index_cache", mem_tracker);
    _del_vec_cache_mem_tracker = std::make_unique<MemTracker>(-1, "del_vec_cache", mem_tracker);
    _compaction_state_mem_tracker = std::make_unique<MemTracker>(-1, "compaction_state", mem_tracker);

    _index_cache.set_mem_tracker(_index_cache_mem_tracker.get());
    _update_state_cache.set_mem_tracker(_update_state_mem_tracker.get());

    int64_t byte_limits = ParseUtil::parse_mem_spec(config::mem_limit, MemInfo::physical_mem());
    int32_t update_mem_percent = std::max(std::min(100, config::update_memory_limit_percent), 0);
    _index_cache.set_capacity(byte_limits * update_mem_percent);
}

UpdateManager::~UpdateManager() {
    if (_apply_thread_pool != nullptr) {
        // DynamicCache may be still used by apply thread.
        // Before deconstrut the DynamicCache, apply thread
        // should be shutdown.
        _apply_thread_pool->shutdown();
    }
    if (_get_pindex_thread_pool) {
        _get_pindex_thread_pool->shutdown();
    }
    clear_cache();
    if (_compaction_state_mem_tracker) {
        _compaction_state_mem_tracker.reset();
    }
    if (_del_vec_cache_mem_tracker) {
        _del_vec_cache_mem_tracker.reset();
    }
    if (_update_state_mem_tracker) {
        _update_state_mem_tracker.reset();
    }
    if (_index_cache_mem_tracker) {
        _index_cache_mem_tracker.reset();
    }
}

Status UpdateManager::init() {
    int max_thread_cnt = CpuInfo::num_cores();
    if (config::transaction_apply_worker_count > 0) {
        max_thread_cnt = config::transaction_apply_worker_count;
    }
    RETURN_IF_ERROR(ThreadPoolBuilder("update_apply").set_max_threads(max_thread_cnt).build(&_apply_thread_pool));
    REGISTER_GAUGE_STARROCKS_METRIC(update_apply_queue_count,
                                    [this]() { return _apply_thread_pool->num_queued_tasks(); });
    max_thread_cnt = CpuInfo::num_cores();
    int max_get_thread_cnt =
            config::get_pindex_worker_count > max_thread_cnt ? config::get_pindex_worker_count : max_thread_cnt * 2;
    RETURN_IF_ERROR(
            ThreadPoolBuilder("get_pindex").set_max_threads(max_get_thread_cnt).build(&_get_pindex_thread_pool));

    return Status::OK();
}

Status UpdateManager::get_del_vec_in_meta(KVStore* meta, const TabletSegmentId& tsid, int64_t version,
                                          DelVector* delvec, int64_t* latest_version) {
    return TabletMetaManager::get_del_vector(meta, tsid.tablet_id, tsid.segment_id, version, delvec, latest_version);
}

Status UpdateManager::set_del_vec_in_meta(KVStore* meta, const TabletSegmentId& tsid, const DelVector& delvec) {
    // TODO: support batch transaction with tablet/rowset meta save
    return TabletMetaManager::set_del_vector(meta, tsid.tablet_id, tsid.segment_id, delvec);
}

Status UpdateManager::get_del_vec(KVStore* meta, const TabletSegmentId& tsid, int64_t version, DelVectorPtr* pdelvec) {
    {
        std::lock_guard<std::mutex> lg(_del_vec_cache_lock);
        auto itr = _del_vec_cache.find(tsid);
        if (itr != _del_vec_cache.end()) {
            if (version >= itr->second->version()) {
                VLOG(3) << strings::Substitute("get_del_vec cached tablet_segment=$0 version=$1 actual_version=$2",
                                               tsid.to_string(), version, itr->second->version());
                // cache valid
                // TODO(cbl): add cache hit stats
                *pdelvec = itr->second;
                return Status::OK();
            }
        }
    }
    (*pdelvec).reset(new DelVector());
    int64_t latest_version = 0;
    RETURN_IF_ERROR(get_del_vec_in_meta(meta, tsid, version, pdelvec->get(), &latest_version));
    if ((*pdelvec)->version() == latest_version) {
        std::lock_guard<std::mutex> lg(_del_vec_cache_lock);
        auto itr = _del_vec_cache.find(tsid);
        if (itr == _del_vec_cache.end()) {
            _del_vec_cache.emplace(tsid, *pdelvec);
            _del_vec_cache_mem_tracker->consume((*pdelvec)->memory_usage());
        } else if (latest_version > itr->second->version()) {
            // should happen rarely
            _del_vec_cache_mem_tracker->release(itr->second->memory_usage());
            itr->second = (*pdelvec);
            _del_vec_cache_mem_tracker->consume(itr->second->memory_usage());
        }
    }
    return Status::OK();
}

void UpdateManager::clear_cache() {
    _update_state_cache.clear();
    if (_update_state_mem_tracker) {
        _update_state_mem_tracker->release(_update_state_mem_tracker->consumption());
    }
    _index_cache.clear();
    if (_index_cache_mem_tracker) {
        _index_cache_mem_tracker->release(_index_cache_mem_tracker->consumption());
    }
    StarRocksMetrics::instance()->update_primary_index_num.set_value(0);
    StarRocksMetrics::instance()->update_primary_index_bytes_total.set_value(0);
    {
        std::lock_guard<std::mutex> lg(_del_vec_cache_lock);
        _del_vec_cache.clear();
        if (_del_vec_cache_mem_tracker) {
            _del_vec_cache_mem_tracker->release(_del_vec_cache_mem_tracker->consumption());
        }
        StarRocksMetrics::instance()->update_del_vector_num.set_value(0);
        StarRocksMetrics::instance()->update_del_vector_bytes_total.set_value(0);
    }
}

void UpdateManager::clear_cached_del_vec(const std::vector<TabletSegmentId>& tsids) {
    std::lock_guard<std::mutex> lg(_del_vec_cache_lock);
    for (const auto& tsid : tsids) {
        auto itr = _del_vec_cache.find(tsid);
        if (itr != _del_vec_cache.end()) {
            _del_vec_cache_mem_tracker->release(itr->second->memory_usage());
            _del_vec_cache.erase(itr);
        }
    }
}

void UpdateManager::expire_cache() {
    StarRocksMetrics::instance()->update_primary_index_num.set_value(_index_cache.object_size());
    StarRocksMetrics::instance()->update_primary_index_bytes_total.set_value(_index_cache.size());
    {
        std::lock_guard<std::mutex> lg(_del_vec_cache_lock);
        StarRocksMetrics::instance()->update_del_vector_num.set_value(_del_vec_cache.size());
        StarRocksMetrics::instance()->update_del_vector_bytes_total.set_value(std::accumulate(
                _del_vec_cache.cbegin(), _del_vec_cache.cend(), 0,
                [](const int& accumulated, const auto& p) { return accumulated + p.second->memory_usage(); }));
    }
    if (MonotonicMillis() - _last_clear_expired_cache_millis > _cache_expire_ms) {
        _update_state_cache.clear_expired();

        ssize_t orig_size = _index_cache.size();
        ssize_t orig_obj_size = _index_cache.object_size();
        _index_cache.clear_expired();
        ssize_t size = _index_cache.size();
        ssize_t obj_size = _index_cache.object_size();
        LOG(INFO) << strings::Substitute("index cache expire: before:($0 $1) after:($2 $3) expire: ($4 $5)",
                                         orig_obj_size, PrettyPrinter::print_bytes(orig_size), obj_size,
                                         PrettyPrinter::print_bytes(size), orig_obj_size - obj_size,
                                         PrettyPrinter::print_bytes(orig_size - size));

        _last_clear_expired_cache_millis = MonotonicMillis();
    }
}

void UpdateManager::evict_cache(int64_t memory_urgent_level, int64_t memory_high_level) {
    int64_t capacity = _index_cache.capacity();
    int64_t size = _index_cache.size();
    int64_t memory_urgent = capacity * memory_urgent_level / 100;
    int64_t memory_high = capacity * memory_high_level / 100;

    if (size > memory_urgent) {
        _index_cache.try_evict(memory_urgent);
    }

    size = _index_cache.size();
    if (size > memory_high) {
        int64_t target_memory = std::max((size * 9 / 10), memory_high);
        _index_cache.try_evict(target_memory);
    }
    return;
}

string UpdateManager::memory_stats() {
    return strings::Substitute("index:$0 rowset:$1 compaction:$2 delvec:$3 total:$4/$5",
                               PrettyPrinter::print_bytes(_index_cache_mem_tracker->consumption()),
                               PrettyPrinter::print_bytes(_update_state_mem_tracker->consumption()),
                               PrettyPrinter::print_bytes(_compaction_state_mem_tracker->consumption()),
                               PrettyPrinter::print_bytes(_del_vec_cache_mem_tracker->consumption()),
                               PrettyPrinter::print_bytes(_update_mem_tracker->consumption()),
                               PrettyPrinter::print_bytes(_update_mem_tracker->limit()));
}

string UpdateManager::detail_memory_stats() {
    auto primary_index_stats = _index_cache.get_entry_sizes();
    std::sort(primary_index_stats.begin(), primary_index_stats.end(),
              [](const std::pair<uint64_t, size_t>& lhs, const std::pair<uint64_t, size_t>& rhs) {
                  return lhs.second > rhs.second;
              });
    size_t total_memory = 0;
    for (const auto& e : primary_index_stats) {
        total_memory += e.second;
    }
    string ret;
    StringAppendF(&ret, "primary index stats: total:%zu memory:%zu\n  tabletid       memory\n",
                  primary_index_stats.size(), total_memory);
    for (size_t i = 0; i < std::min(primary_index_stats.size(), (size_t)200); i++) {
        auto& e = primary_index_stats[i];
        StringAppendF(&ret, "%10lu %12zu\n", (unsigned long)e.first, e.second);
    }
    return ret;
}

string UpdateManager::topn_memory_stats(size_t topn) {
    auto primary_index_stats = _index_cache.get_entry_sizes();
    std::sort(primary_index_stats.begin(), primary_index_stats.end(),
              [](const std::pair<uint64_t, size_t>& lhs, const std::pair<uint64_t, size_t>& rhs) {
                  return lhs.second > rhs.second;
              });
    string ret;
    for (size_t i = 0; i < std::min(primary_index_stats.size(), topn); i++) {
        auto& e = primary_index_stats[i];
        StringAppendF(&ret, "%lu(%zuM)", (unsigned long)e.first, e.second / (1024 * 1024));
    }
    return ret;
}

Status UpdateManager::get_latest_del_vec(KVStore* meta, const TabletSegmentId& tsid, DelVectorPtr* pdelvec) {
    std::lock_guard<std::mutex> lg(_del_vec_cache_lock);
    auto itr = _del_vec_cache.find(tsid);
    if (itr != _del_vec_cache.end()) {
        *pdelvec = itr->second;
        return Status::OK();
    } else {
        // TODO(cbl): move get_del_vec_in_meta out of lock
        (*pdelvec).reset(new DelVector());
        int64_t latest_version = 0;
        RETURN_IF_ERROR(get_del_vec_in_meta(meta, tsid, INT64_MAX, pdelvec->get(), &latest_version));
        _del_vec_cache.emplace(tsid, *pdelvec);
        _del_vec_cache_mem_tracker->consume((*pdelvec)->memory_usage());
    }
    return Status::OK();
}

Status UpdateManager::set_cached_del_vec(const TabletSegmentId& tsid, const DelVectorPtr& delvec) {
    VLOG(1) << "set_cached_del_vec tablet:" << tsid.tablet_id << " rss:" << tsid.segment_id
            << " version:" << delvec->version() << " #del:" << delvec->cardinality();
    std::lock_guard<std::mutex> lg(_del_vec_cache_lock);
    auto itr = _del_vec_cache.find(tsid);
    if (itr != _del_vec_cache.end()) {
        if (delvec->version() <= itr->second->version()) {
            string msg = strings::Substitute("UpdateManager::set_cached_del_vec: new version($0) < old version($1)",
                                             delvec->version(), itr->second->version());
            LOG(ERROR) << msg;
            return Status::InternalError(msg);
        } else {
            _del_vec_cache_mem_tracker->release(itr->second->memory_usage());
            itr->second = delvec;
            _del_vec_cache_mem_tracker->consume(itr->second->memory_usage());
        }
    } else {
        _del_vec_cache.emplace(tsid, delvec);
        _del_vec_cache_mem_tracker->consume(delvec->memory_usage());
    }
    return Status::OK();
}

Status UpdateManager::on_rowset_finished(Tablet* tablet, Rowset* rowset) {
    if (!rowset->has_data_files() || tablet->tablet_state() == TABLET_NOTREADY) {
        // if rowset is empty or tablet is in schemachange, we can skip preparing updatestates and pre-loading primary index
        return Status::OK();
    }

    string rowset_unique_id = rowset->rowset_id().to_string();
    VLOG(1) << "UpdateManager::on_rowset_finished start tablet:" << tablet->tablet_id()
            << " rowset:" << rowset_unique_id;
    // Prepare apply required resources, load updatestate, primary index into cache,
    // so apply can run faster. Since those resources are in cache, they can get evicted
    // before used in apply process, in that case, these will be loaded again in apply
    // process.
    auto state_entry =
            _update_state_cache.get_or_create(strings::Substitute("$0_$1", tablet->tablet_id(), rowset_unique_id));
    auto st = state_entry->value().load(tablet, rowset);
    state_entry->update_expire_time(MonotonicMillis() + _cache_expire_ms);
    _update_state_cache.update_object_size(state_entry, state_entry->value().memory_usage());
    if (st.ok()) {
        _update_state_cache.release(state_entry);
    } else {
        LOG(WARNING) << "load RowsetUpdateState error: " << st << " tablet: " << tablet->tablet_id();
        _update_state_cache.remove(state_entry);
    }
    if (st.ok()) {
        auto index_entry = _index_cache.get_or_create(tablet->tablet_id());
        st = index_entry->value().load(tablet);
        index_entry->update_expire_time(MonotonicMillis() + _cache_expire_ms);
        _index_cache.update_object_size(index_entry, index_entry->value().memory_usage());
        if (st.ok()) {
            _index_cache.release(index_entry);
        } else {
            LOG(WARNING) << "load primary index error: " << st << " tablet: " << tablet->tablet_id();
            _index_cache.remove(index_entry);
        }
    }
    VLOG(1) << "UpdateManager::on_rowset_finished finish tablet:" << tablet->tablet_id()
            << " rowset:" << rowset_unique_id;
    return st;
}

void UpdateManager::on_rowset_cancel(Tablet* tablet, Rowset* rowset) {
    string rowset_unique_id = rowset->rowset_id().to_string();
    VLOG(1) << "UpdateManager::on_rowset_error remove state tablet:" << tablet->tablet_id()
            << " rowset:" << rowset_unique_id;
    auto state_entry = _update_state_cache.get(strings::Substitute("$0_$1", tablet->tablet_id(), rowset_unique_id));
    if (state_entry != nullptr) {
        _update_state_cache.remove(state_entry);
    }
}

} // namespace starrocks
