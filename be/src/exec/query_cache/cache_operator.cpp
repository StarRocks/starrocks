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

#include "exec/query_cache/cache_operator.h"

#include <glog/logging.h>

#include <vector>

#include "column/vectorized_fwd.h"
#include "common/compiler_util.h"
#include "exec/pipeline/pipeline_driver.h"
#include "storage/rowset/rowset.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "util/time.h"
namespace starrocks::query_cache {
enum PerLaneBufferState {
    PLBS_INIT,
    PLBS_MISS,
    PLBS_HIT_PARTIAL,
    PLBS_HIT_TOTAL,
    PLBS_PARTIAL,
    PLBS_TOTAL,
    PLBS_POPULATE,
    PLBS_PASSTHROUGH,
};
struct PerLaneBuffer {
    LaneOwnerType lane_owner{-1};
    int lane;
    PerLaneBufferState state;
    TabletSharedPtr tablet;
    std::vector<RowsetSharedPtr> rowsets;
    RowsetsAcqRelPtr rowsets_acq_rel;
    int64_t required_version;
    int64_t cached_version;
    std::vector<ChunkPtr> chunks;
    size_t next_gc_chunk_idx;
    size_t next_chunk_idx;
    size_t num_rows;
    size_t num_bytes;

    void reset() {
        lane = -1;
        state = PLBS_INIT;
        tablet.reset();
        rowsets_acq_rel.reset();
        rowsets.clear();
        required_version = 0;
        cached_version = 0;
        chunks.clear();
        next_gc_chunk_idx = 0;
        next_chunk_idx = 0;
        num_rows = 0;
        num_bytes = 0;
    }
    void clear_chunks() {
        DCHECK(state == PLBS_HIT_PARTIAL);
        num_rows = 0;
        num_bytes = 0;
        chunks.clear();
    }
    bool should_populate_cache() const {
        return cached_version < required_version && (state == PLBS_HIT_TOTAL || state == PLBS_TOTAL);
    }
    bool is_partial() const { return state == PLBS_MISS || state == PLBS_HIT_PARTIAL || state == PLBS_PARTIAL; }
    void append_chunk(const ChunkPtr& chunk) {
        if (!is_partial()) {
            return;
        }
        // CRITICAL!!!: we should append last empty chunk to the empty buffer to guarantee that pull_chunk method
        // would fetch a chunk and reset_lane.
        // We must guarantee happen-before invariant as follows:
        // append EOS chunk[PLBS_TOTAL] -> propulate_cache[PLBS_POPULATE] -> pull last EOS chunk -> release_lane
        if (!chunk->is_empty() || chunk->owner_info().is_last_chunk()) {
            chunks.push_back(chunk);
            num_rows += chunk->num_rows();
            num_bytes += chunk->bytes_usage();
        }

        state = chunk->owner_info().is_last_chunk() ? PLBS_TOTAL : PLBS_PARTIAL;
    }

    bool has_chunks() const { return next_chunk_idx < chunks.size(); }

    void set_passthrough() {
        if (is_partial()) {
            state = PLBS_PASSTHROUGH;
        }
    }

    bool can_release() const {
        return (state == PLBS_POPULATE || state == PLBS_PASSTHROUGH || state == PLBS_HIT_TOTAL) && !has_chunks();
    }

    ChunkPtr get_next_chunk() {
        if (UNLIKELY(state == PLBS_POPULATE || state == PLBS_PASSTHROUGH)) {
            while (UNLIKELY(next_gc_chunk_idx < next_chunk_idx)) {
                chunks[next_gc_chunk_idx++].reset();
            }
            ++next_gc_chunk_idx;
        }

        if (state == PLBS_PASSTHROUGH) {
            // In passthrough mode, chunks are used exclusively by post-aggregate operator, so it need not be cloned
            return std::move(chunks[next_chunk_idx++]);
        } else {
            // CRITICAL(by satanson): The method is invoked when cloning chunk, when query cache enabled, chunk
            // may be accessed by multi-thread, so we must clone a chunk when pull chunk from cache operator.
            ChunkPtr chunk = chunks[next_chunk_idx++]->clone_unique();
            return chunk;
        }
    }
};

CacheOperator::CacheOperator(pipeline::OperatorFactory* factory, int32_t driver_sequence, CacheManagerRawPtr cache_mgr,
                             const CacheParam& cache_param)
        : pipeline::Operator(factory, factory->id(), factory->get_raw_name(), factory->plan_node_id(), true,
                             driver_sequence),
          _cache_mgr(cache_mgr),
          _cache_param(cache_param),
          _lane_arbiter(std::make_shared<LaneArbiter>(_cache_param.num_lanes)) {
    _per_lane_buffers.reserve(_lane_arbiter->num_lanes());
    for (auto i = 0; i < _lane_arbiter->num_lanes(); ++i) {
        _per_lane_buffers.push_back(std::make_unique<PerLaneBuffer>());
    }
}

static inline Chunks remap_chunks(const Chunks& chunks, const SlotRemapping& slot_remapping) {
    std::vector<ChunkPtr> new_chunks;
    new_chunks.reserve(chunks.size());
    for (auto i = 0; i < chunks.size(); ++i) {
        const auto& chunk = chunks[i];
        DCHECK(chunk != nullptr);
        auto new_chunk = std::make_shared<Chunk>();
        auto is_last_chunk = (i == (chunks.size() - 1));
        new_chunk->owner_info().set_owner_id(chunk->owner_info().owner_id(), is_last_chunk);
        if (chunk->is_empty()) {
            new_chunks.push_back(std::move(new_chunk));
            continue;
        }

        // SlotId remapping always happens inside cache operator, this can lead to chunks from passthrough, cache and
        // ScanOperator with inconsistent maps from SlotIds to column index, however post-cache agg always produces
        // chunks with consistent maps from SlotIds to column index on which Chunk::append_selectivity relies.
        for (auto& [slot_id, new_slot_id] : slot_remapping) {
            auto column = chunk->get_column_by_slot_id(slot_id);
            new_chunk->append_column(column, new_slot_id);
        }
        new_chunks.push_back(std::move(new_chunk));
    }
    return new_chunks;
}
Status CacheOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    _push_chunk_num_counter = ADD_COUNTER(_common_metrics, "PushChunkNum", TUnit::UNIT);
    _cache_probe_timer = ADD_TIMER(_unique_metrics, "CacheProbeTime");
    _cache_probe_chunks_counter = ADD_COUNTER(_unique_metrics, "CacheProbeChunkNum", TUnit::UNIT);
    _cache_probe_tablets_counter = ADD_COUNTER(_unique_metrics, "CacheProbeTabletNum", TUnit::UNIT);
    _cache_probe_rows_counter = ADD_COUNTER(_unique_metrics, "CacheProbeRowNum", TUnit::UNIT);
    _cache_probe_bytes_counter = ADD_COUNTER(_unique_metrics, "CacheProbeBytes", TUnit::BYTES);

    _cache_populate_timer = ADD_TIMER(_unique_metrics, "CachePopulateTime");
    _cache_populate_chunks_counter = ADD_COUNTER(_unique_metrics, "CachePopulateChunkNum", TUnit::UNIT);
    _cache_populate_tablets_counter = ADD_COUNTER(_unique_metrics, "CachePopulateTabletNum", TUnit::UNIT);
    _cache_populate_rows_counter = ADD_COUNTER(_unique_metrics, "CachePopulateRowNum", TUnit::UNIT);
    _cache_populate_bytes_counter = ADD_COUNTER(_unique_metrics, "CachePopulateBytes", TUnit::BYTES);

    _cache_passthrough_timer = ADD_TIMER(_unique_metrics, "CachePassthroughTime");
    _cache_passthrough_chunks_counter = ADD_COUNTER(_unique_metrics, "CachePassthroughChunkNum", TUnit::UNIT);
    _cache_passthrough_tablets_counter = ADD_COUNTER(_unique_metrics, "CachePassthroughTabletNum", TUnit::UNIT);
    _cache_passthrough_rows_counter = ADD_COUNTER(_unique_metrics, "CachePassthroughRowNum", TUnit::UNIT);
    _cache_passthrough_bytes_counter = ADD_COUNTER(_unique_metrics, "CachePassthroughBytes", TUnit::BYTES);

    return Status::OK();
}

static inline std::string flatten_tablet_set(const std::unordered_set<int64_t>& tablets) {
    return fmt::format("{}", fmt::join(tablets.begin(), tablets.end(), ","));
}

void CacheOperator::close(RuntimeState* state) {
    std::unordered_set<int64_t> passthrough_tablets;
    for (auto tablet_id : _all_tablets) {
        if (!_populate_tablets.count(tablet_id) && !_probe_tablets.count(tablet_id)) {
            passthrough_tablets.insert(tablet_id);
        }
    }

    _cache_populate_tablets_counter->update(_populate_tablets.size());
    _cache_probe_tablets_counter->update(_probe_tablets.size());
    _cache_passthrough_tablets_counter->update(passthrough_tablets.size());
    _unique_metrics->add_info_string("CacheProbeTablets", flatten_tablet_set(_probe_tablets));
    _unique_metrics->add_info_string("CachePopulateTablets", flatten_tablet_set(_populate_tablets));
    _unique_metrics->add_info_string("CachePassthroughTablets", flatten_tablet_set(passthrough_tablets));

    Operator::close(state);
}

void CacheOperator::_handle_stale_cache_value(int64_t tablet_id, CacheValue& cache_value, PerLaneBufferPtr& buffer,
                                              int64_t version) {
// In BE unittest, stale value is considered as partial-hit
#ifdef BE_TEST
    {
        buffer->state = PLBS_HIT_PARTIAL;
        buffer->required_version = version;
        buffer->cached_version = cache_value.version;
        buffer->num_rows = 0;
        buffer->num_bytes = 0;
        auto chunks = remap_chunks(cache_value.result, _cache_param.reverse_slot_remapping);
        buffer->chunks = std::move(chunks);
        for (const auto& chunk : buffer->chunks) {
            buffer->num_rows += chunk->num_rows();
            buffer->num_bytes += chunk->bytes_usage();
        }
        buffer->chunks.back()->owner_info().set_last_chunk(false);
        return;
    }
#endif

    if (_cache_param.keys_type != TKeysType::PRIMARY_KEYS) {
        _handle_stale_cache_value_for_non_pk(tablet_id, cache_value, buffer, version);
    } else {
        _handle_stale_cache_value_for_pk(tablet_id, cache_value, buffer, version);
    }
}

void CacheOperator::_handle_stale_cache_value_for_non_pk(int64_t tablet_id, CacheValue& cache_value,
                                                         PerLaneBufferPtr& buffer, int64_t version) {
    // Try to reuse partial cache result when cached version is less than required version, delta versions
    // should be captured at first.
    auto status = StorageEngine::instance()->tablet_manager()->capture_tablet_and_rowsets(
            tablet_id, cache_value.version + 1, version);

    // Cache MISS if delta versions are not captured, because aggressive cumulative compactions.
    if (!status.ok()) {
        buffer->state = PLBS_MISS;
        buffer->cached_version = 0;
        return;
    }

    // Delta versions are captured, several situations should be taken into consideration.
    auto& [tablet, rowsets, rowsets_acq_rel] = status.value();
    auto all_rs_empty = true;
    auto min_version = std::numeric_limits<int64_t>::max();
    auto max_version = std::numeric_limits<int64_t>::min();
    for (const auto& rs : rowsets) {
        all_rs_empty &= !rs->has_data_files();
        min_version = std::min(min_version, rs->start_version());
        max_version = std::max(max_version, rs->end_version());
    }
    Version delta_versions(min_version, max_version);
    buffer->tablet = tablet;
    auto has_delete_predicates = tablet->has_delete_predicates(delta_versions);
    // case 1: there exist delete predicates in delta versions, or data model can not support multiversion cache and
    // the tablet has non-empty delta rowsets; then cache result is not reuse, so cache miss.
    if (has_delete_predicates || (!_cache_param.can_use_multiversion && !all_rs_empty)) {
        buffer->state = PLBS_MISS;
        buffer->cached_version = 0;
        return;
    }

    buffer->cached_version = cache_value.version;
    auto chunks = remap_chunks(cache_value.result, _cache_param.reverse_slot_remapping);
    _update_probe_metrics(tablet_id, chunks);
    buffer->chunks = std::move(chunks);
    // case 2: all delta versions are empty rowsets, so the cache result is hit totally.
    if (all_rs_empty) {
        buffer->state = PLBS_HIT_TOTAL;
        buffer->chunks.back()->owner_info().set_last_chunk(true);
        return;
    }

    // case 3: otherwise, the cache result is partial result of per-tablet computation, so delta versions must
    //  be scanned and merged with cache result to generate total result.
    buffer->state = PLBS_HIT_PARTIAL;
    buffer->rowsets = std::move(rowsets);
    buffer->rowsets_acq_rel = std::move(rowsets_acq_rel);
    buffer->num_rows = 0;
    buffer->num_bytes = 0;
    for (const auto& chunk : buffer->chunks) {
        buffer->num_rows += chunk->num_rows();
        buffer->num_bytes += chunk->bytes_usage();
    }
    buffer->chunks.back()->owner_info().set_last_chunk(false);
}

void CacheOperator::_handle_stale_cache_value_for_pk(int64_t tablet_id, starrocks::query_cache::CacheValue& cache_value,
                                                     starrocks::query_cache::PerLaneBufferPtr& buffer,
                                                     int64_t version) {
    DCHECK(_cache_param.keys_type == TKeysType::PRIMARY_KEYS);
    // At the present, PRIMARY_KEYS can not support merge-on-read, so we can not merge stale cache values and delta
    // rowsets. Capturing delta rowsets is meaningless and unsupported, thus we capture all rowsets of the PK tablet.
    auto status = StorageEngine::instance()->tablet_manager()->capture_tablet_and_rowsets(tablet_id, 0, version);
    if (!status.ok()) {
        buffer->state = PLBS_MISS;
        buffer->cached_version = 0;
        return;
    }
    auto& [tablet, rowsets, rowsets_acq_rel] = status.value();
    const auto snapshot_version = cache_value.version;
    bool can_pickup_delta_rowsets = false;
    bool exists_non_empty_delta_rowsets = false;
    for (auto& rs : rowsets) {
        can_pickup_delta_rowsets |= rs->start_version() == snapshot_version + 1;
        exists_non_empty_delta_rowsets |= rs->start_version() > snapshot_version && rs->has_data_files();
    }
    if (exists_non_empty_delta_rowsets || !can_pickup_delta_rowsets) {
        buffer->state = PLBS_MISS;
        buffer->cached_version = 0;
        return;
    }

    buffer->cached_version = cache_value.version;
    auto chunks = remap_chunks(cache_value.result, _cache_param.reverse_slot_remapping);
    _update_probe_metrics(tablet_id, chunks);
    buffer->chunks = std::move(chunks);
    buffer->state = PLBS_HIT_TOTAL;
    buffer->chunks.back()->owner_info().set_last_chunk(true);
}

void CacheOperator::_update_probe_metrics(int64_t tablet_id, const std::vector<ChunkPtr>& chunks) {
    auto num_bytes = 0L;
    auto num_rows = 0L;
    for (const auto& chunk : chunks) {
        num_bytes += chunk->bytes_usage();
        num_rows += chunk->num_rows();
    }
    _cache_probe_bytes_counter->update(num_bytes);
    _cache_probe_chunks_counter->update(chunks.size());
    _cache_probe_rows_counter->update(num_rows);
    _probe_tablets.insert(tablet_id);
}

bool CacheOperator::probe_cache(int64_t tablet_id, int64_t version) {
    _all_tablets.insert(tablet_id);
    // allocate lane and PerLaneBuffer for tablet_id
    int64_t lane = _lane_arbiter->must_acquire_lane(tablet_id);

    _owner_to_lanes[tablet_id] = lane;
    auto& buffer = _per_lane_buffers[lane];
    buffer->reset();
    buffer->lane = lane;
    buffer->required_version = version;

    if (_cache_param.force_populate || !_cache_param.cache_key_prefixes.count(tablet_id)) {
        buffer->state = PLBS_MISS;
        return false;
    }
    // probe cache
    const std::string& cache_key = _cache_param.digest + _cache_param.cache_key_prefixes.at(tablet_id);
    auto probe_status = _cache_mgr->probe(cache_key);

    // Cache MISS when failed to probe
    if (!probe_status.ok()) {
        buffer->state = PLBS_MISS;
        return false;
    }

    auto& cache_value = probe_status.value();
    if (cache_value.version == version) {
        // Cache HIT_TOTAL when cached version equals to required version
        buffer->state = PLBS_HIT_TOTAL;
        buffer->cached_version = cache_value.version;
        auto chunks = remap_chunks(cache_value.result, _cache_param.reverse_slot_remapping);
        _update_probe_metrics(tablet_id, chunks);
        buffer->chunks = std::move(chunks);
    } else if (cache_value.version > version) {
        // It rarely happens that required version is less that cached version, the required version become
        // stale when the query is postponed to be processed because of some reasons, for examples, non-deterministic
        // query scheduling, network congestion etc. make queries be executed out-of-order. so we must prevent stale
        // result from replacing fresh cached result.
        buffer->state = PLBS_MISS;
        buffer->cached_version = 0;
    } else {
        // Incremental updating cause the cached value become stale, It is a very critical and complex situation.
        // here we support a multi-version cache mechanism.
        _handle_stale_cache_value(tablet_id, cache_value, buffer, version);
    }

    // return true on cache hit, false on cache miss
    if (buffer->state == PLBS_HIT_TOTAL) {
        _lane_arbiter->mark_processed(tablet_id);
        return true;
    } else if (buffer->state == PLBS_HIT_PARTIAL) {
        return true;
    } else {
        return false;
    }
}

void CacheOperator::populate_cache(int64_t tablet_id) {
    DCHECK(_owner_to_lanes.count(tablet_id));
    auto lane = _owner_to_lanes[tablet_id];
    auto& buffer = _per_lane_buffers[lane];
    DCHECK(buffer->should_populate_cache());

    const auto cache_key_suffix_it = _cache_param.cache_key_prefixes.find(tablet_id);
    // uncacheable
    if (cache_key_suffix_it == _cache_param.cache_key_prefixes.end()) {
        buffer->state = PLBS_POPULATE;
        return;
    }
    const std::string cache_key = _cache_param.digest + cache_key_suffix_it->second;
    int64_t current = GetMonoTimeMicros();
    auto chunks = remap_chunks(buffer->chunks, _cache_param.slot_remapping);
    CacheValue cache_value(current, buffer->required_version, std::move(chunks));
    // If the cache implementation is global, populate method must be asynchronous and try its best to
    // update the cache.
    _cache_populate_bytes_counter->update(buffer->num_bytes);
    _cache_populate_chunks_counter->update(buffer->chunks.size());
    _cache_populate_rows_counter->update(buffer->num_rows);
    _populate_tablets.insert(tablet_id);
    _cache_mgr->populate(cache_key, cache_value);
    buffer->state = PLBS_POPULATE;
}

int64_t CacheOperator::cached_version(int64_t tablet_id) {
    auto& buffer = _per_lane_buffers[_owner_to_lanes[tablet_id]];
    if (buffer->state == PLBS_HIT_TOTAL) {
        return buffer->required_version;
    } else {
        return buffer->cached_version;
    }
}

std::tuple<int64_t, vector<RowsetSharedPtr>> CacheOperator::delta_version_and_rowsets(int64_t tablet_id) {
    auto lane_it = _owner_to_lanes.find(tablet_id);
    if (lane_it == _owner_to_lanes.end()) {
        return make_tuple(0, vector<RowsetSharedPtr>{});
    } else {
        auto& buffer = _per_lane_buffers[lane_it->second];
        return make_tuple(buffer->cached_version + 1, buffer->rowsets);
    }
}

Status CacheOperator::set_finishing(RuntimeState* state) {
    _is_input_finished = true;
    return Status::OK();
}

Status CacheOperator::set_finished(RuntimeState* state) {
    _is_input_finished = true;
    for (auto& buffer : _per_lane_buffers) {
        buffer->reset();
    }
    _passthrough_chunk.reset();
    DCHECK(is_finished());
    return Status::OK();
}

bool CacheOperator::is_finished() const {
    return _is_input_finished && !has_output();
}

bool CacheOperator::has_output() const {
    for (const auto& [_, lane_id] : _owner_to_lanes) {
        auto& buffer = _per_lane_buffers[lane_id];
        if (buffer->has_chunks() || buffer->can_release()) {
            return true;
        }
    }
    return _passthrough_chunk != nullptr;
}

bool CacheOperator::need_input() const {
    return !_is_input_finished && (!_lane_arbiter->in_passthrough_mode() || _passthrough_chunk == nullptr);
}

bool CacheOperator::_should_passthrough(size_t num_rows, size_t num_bytes) {
    return _cache_param.entry_max_rows <= 0 || num_rows > _cache_param.entry_max_rows ||
           _cache_param.entry_max_bytes <= 0 || num_bytes > _cache_param.entry_max_bytes;
}

Status CacheOperator::reset_lane(RuntimeState* state, LaneOwnerType lane_owner) {
    DCHECK(_multilane_operators.size() > 0);
    for (auto i = 0; i < _multilane_operators.size() - 1; ++i) {
        RETURN_IF_ERROR(_multilane_operators[i]->reset_lane(state, lane_owner, {}));
    }

    auto& buffer = _per_lane_buffers[_owner_to_lanes[lane_owner]];
    _owner_to_lanes.erase(buffer->lane_owner);
    buffer->lane_owner = lane_owner;

    if (buffer->state == PLBS_HIT_PARTIAL) {
        RETURN_IF_ERROR(_multilane_operators.back()->reset_lane(state, lane_owner, buffer->chunks));
        buffer->clear_chunks();
    } else {
        RETURN_IF_ERROR(_multilane_operators.back()->reset_lane(state, lane_owner, {}));
    }

    // When build-side of HashJoin/NLJoin is empty, the Driver should set operators to finished from
    // the source operator till the prematurely-finished HashJoinProbeOperator/NLJoinProbeOperator
    // in short-circuit style.

    // seek for the index of the prematurely-finished operator in _multilane_operators.
    size_t premature_finished_idx = 0;
    for (auto i = 0; i < _multilane_operators.size() - 1; ++i) {
        auto& multi_op = _multilane_operators[i];
        auto op = multi_op->get_internal_op(_owner_to_lanes[lane_owner]);
        if (op->is_finished()) {
            premature_finished_idx = i;
        }
    }

    // if we found the prematurely-finished operator, we invoke set-finished method of the operators from source
    // to prematurely-finished operator.
    // NOTICE: the source operator ScanOperator is not wrapped in MultilaneOperator, so we must process
    // ScanOperator and MultilaneOperators.
    if (premature_finished_idx > 0) {
        _lane_arbiter->enable_passthrough_mode();
        for (auto i = 0; i <= premature_finished_idx; ++i) {
            auto& multi_op = _multilane_operators[i];
            (void)multi_op->set_finished(state);
        }
        (void)_scan_operator->set_finished(state);
    }
    return Status::OK();
}

Status CacheOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    DCHECK(chunk != nullptr);
    if (_lane_arbiter->in_passthrough_mode()) {
        DCHECK(_passthrough_chunk == nullptr);
        _passthrough_chunk = chunk;
        return Status::OK();
    }
    auto lane_owner = chunk->owner_info().owner_id();
    DCHECK(_owner_to_lanes.count(lane_owner));
    auto lane_id = _owner_to_lanes[lane_owner];
    auto& buffer = _per_lane_buffers[lane_id];
    buffer->append_chunk(chunk);

    if (_should_passthrough(buffer->num_rows, buffer->num_bytes)) {
        _lane_arbiter->enable_passthrough_mode();
        for (const auto& [_, lane_id] : _owner_to_lanes) {
            _per_lane_buffers[lane_id]->set_passthrough();
        }
        return Status::OK();
    }

    if (buffer->should_populate_cache()) {
        populate_cache(lane_owner);
    }
    return Status::OK();
}

ChunkPtr CacheOperator::_pull_chunk_from_per_lane_buffer(PerLaneBufferPtr& buffer) {
    if (buffer->can_release()) {
        _lane_arbiter->release_lane(buffer->lane_owner);
        buffer->reset();
    } else if (buffer->has_chunks()) {
        auto chunk = buffer->get_next_chunk();
        if (buffer->can_release()) {
            _lane_arbiter->release_lane(buffer->lane_owner);
            buffer->reset();
        }
        return chunk;
    }
    return nullptr;
}

StatusOr<ChunkPtr> CacheOperator::pull_chunk(RuntimeState* state) {
    auto passthrough_mode = _lane_arbiter->in_passthrough_mode();
    auto update_metrics = [this, passthrough_mode](ChunkPtr& chunk) {
        if (!passthrough_mode || chunk == nullptr) {
            return;
        }
        _cache_passthrough_bytes_counter->update(chunk->bytes_usage());
        _cache_passthrough_chunks_counter->update(1);
        _cache_passthrough_rows_counter->update(chunk->num_rows());
    };
    auto opt_lane = _lane_arbiter->preferred_lane();
    if (opt_lane.has_value()) {
        auto lane = opt_lane.value();
        auto& buffer = _per_lane_buffers[lane];
        auto chunk = _pull_chunk_from_per_lane_buffer(buffer);
        if (chunk != nullptr) {
            update_metrics(chunk);
            return chunk;
        }
    }

    for (const auto& [_, lane_id] : _owner_to_lanes) {
        auto& buffer = _per_lane_buffers[lane_id];
        auto chunk = _pull_chunk_from_per_lane_buffer(buffer);
        if (chunk != nullptr) {
            update_metrics(chunk);
            return chunk;
        }
    }
    update_metrics(_passthrough_chunk);
    return std::move(_passthrough_chunk);
}

CacheOperatorFactory::CacheOperatorFactory(int32_t id, int32_t plan_node_id, CacheManagerRawPtr cache_mgr,
                                           const CacheParam& cache_param)

        : pipeline::OperatorFactory(id, "cache", plan_node_id), _cache_mgr(cache_mgr), _cache_param(cache_param) {}

Status CacheOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(pipeline::OperatorFactory::prepare(state));
    return Status::OK();
}

void CacheOperatorFactory::close(RuntimeState* state) {
    pipeline::OperatorFactory::close(state);
}

pipeline::OperatorPtr CacheOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    return std::make_shared<CacheOperator>(this, driver_sequence, _cache_mgr, _cache_param);
}

} // namespace starrocks::query_cache
