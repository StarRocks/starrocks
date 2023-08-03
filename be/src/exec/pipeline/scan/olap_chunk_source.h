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

#include <utility>

#include "exec/olap_common.h"
#include "exec/olap_scan_prepare.h"
#include "exec/olap_utils.h"
#include "exec/pipeline/scan/chunk_source.h"
#include "exec/workgroup/work_group_fwd.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "gen_cpp/InternalService_types.h"
#include "runtime/runtime_state.h"
#include "storage/conjunctive_predicates.h"
#include "storage/tablet.h"
#include "storage/tablet_reader.h"

namespace starrocks {

class SlotDescriptor;

namespace pipeline {

class ScanOperator;
class OlapScanContext;

class OlapChunkSource final : public ChunkSource {
public:
    OlapChunkSource(ScanOperator* op, RuntimeProfile* runtime_profile, MorselPtr&& morsel, OlapScanNode* scan_node,
                    OlapScanContext* scan_ctx);

    ~OlapChunkSource() override;

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

private:
    Status _read_chunk(RuntimeState* state, ChunkPtr* chunk) override;

    const workgroup::WorkGroupScanSchedEntity* _scan_sched_entity(const workgroup::WorkGroup* wg) const override;

    Status _get_tablet(const TInternalScanRange* scan_range);
    Status _init_reader_params(const std::vector<std::unique_ptr<OlapScanRange>>& key_ranges,
                               const std::vector<uint32_t>& scanner_columns, std::vector<uint32_t>& reader_columns);
    Status _init_scanner_columns(std::vector<uint32_t>& scanner_columns);
    Status _init_unused_output_columns(const std::vector<std::string>& unused_output_columns);
    Status _init_olap_reader(RuntimeState* state);
    void _init_counter(RuntimeState* state);
    Status _init_global_dicts(TabletReaderParams* params);
    Status _read_chunk_from_storage([[maybe_unused]] RuntimeState* state, Chunk* chunk);
    void _update_counter();
    void _update_realtime_counter(Chunk* chunk);
    void _decide_chunk_size(bool has_predicate);

private:
    TabletReaderParams _params{};
    OlapScanNode* _scan_node;
    OlapScanContext* _scan_ctx;

    const int64_t _limit; // -1: no limit
    TInternalScanRange* _scan_range;

    ConjunctivePredicates _not_push_down_predicates;
    std::vector<uint8_t> _selection;

    ObjectPool _obj_pool;
    TabletSharedPtr _tablet;
    int64_t _version = 0;

    RuntimeState* _runtime_state = nullptr;
    const std::vector<SlotDescriptor*>* _slots = nullptr;

    // For release memory.
    using PredicatePtr = std::unique_ptr<ColumnPredicate>;
    std::vector<PredicatePtr> _predicate_free_pool;

    // NOTE: _reader may reference the _predicate_free_pool, it should be released before the _predicate_free_pool
    std::shared_ptr<TabletReader> _reader;
    // projection iterator, doing the job of choosing |_scanner_columns| from |_reader_columns|.
    std::shared_ptr<ChunkIterator> _prj_iter;

    std::unordered_set<uint32_t> _unused_output_column_ids;

    // slot descriptors for each one of |output_columns|.
    std::vector<SlotDescriptor*> _query_slots;

    // The following are profile meatures
    int64_t _num_rows_read = 0;

    RuntimeProfile::Counter* _bytes_read_counter = nullptr;
    RuntimeProfile::Counter* _rows_read_counter = nullptr;

    RuntimeProfile::Counter* _expr_filter_timer = nullptr;
    RuntimeProfile::Counter* _create_seg_iter_timer = nullptr;
    RuntimeProfile::Counter* _io_timer = nullptr;
    RuntimeProfile::Counter* _read_compressed_counter = nullptr;
    RuntimeProfile::Counter* _decompress_timer = nullptr;
    RuntimeProfile::Counter* _read_uncompressed_counter = nullptr;
    RuntimeProfile::Counter* _raw_rows_counter = nullptr;
    RuntimeProfile::Counter* _pred_filter_counter = nullptr;
    RuntimeProfile::Counter* _del_vec_filter_counter = nullptr;
    RuntimeProfile::Counter* _pred_filter_timer = nullptr;
    RuntimeProfile::Counter* _chunk_copy_timer = nullptr;
    RuntimeProfile::Counter* _get_rowsets_timer = nullptr;
    RuntimeProfile::Counter* _get_delvec_timer = nullptr;
    RuntimeProfile::Counter* _seg_init_timer = nullptr;
    RuntimeProfile::Counter* _zm_filtered_counter = nullptr;
    RuntimeProfile::Counter* _bf_filtered_counter = nullptr;
    RuntimeProfile::Counter* _seg_zm_filtered_counter = nullptr;
    RuntimeProfile::Counter* _seg_rt_filtered_counter = nullptr;
    RuntimeProfile::Counter* _sk_filtered_counter = nullptr;
    RuntimeProfile::Counter* _block_seek_timer = nullptr;
    RuntimeProfile::Counter* _block_seek_counter = nullptr;
    RuntimeProfile::Counter* _block_load_timer = nullptr;
    RuntimeProfile::Counter* _block_load_counter = nullptr;
    RuntimeProfile::Counter* _block_fetch_timer = nullptr;
    RuntimeProfile::Counter* _read_pages_num_counter = nullptr;
    RuntimeProfile::Counter* _cached_pages_num_counter = nullptr;
    RuntimeProfile::Counter* _bi_filtered_counter = nullptr;
    RuntimeProfile::Counter* _bi_filter_timer = nullptr;
    RuntimeProfile::Counter* _pushdown_predicates_counter = nullptr;
    RuntimeProfile::Counter* _rowsets_read_count = nullptr;
    RuntimeProfile::Counter* _segments_read_count = nullptr;
    RuntimeProfile::Counter* _total_columns_data_page_count = nullptr;
    RuntimeProfile::Counter* _read_pk_index_timer = nullptr;
};
} // namespace pipeline
} // namespace starrocks
