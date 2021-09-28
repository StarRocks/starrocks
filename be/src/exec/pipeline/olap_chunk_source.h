// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <utility>

#include "exec/olap_common.h"
#include "exec/olap_utils.h"
#include "exec/pipeline/chunk_source.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "gen_cpp/InternalService_types.h"
#include "runtime/runtime_state.h"
#include "storage/tablet.h"
#include "storage/vectorized/conjunctive_predicates.h"
#include "storage/vectorized/reader.h"
#include "storage/vectorized/reader_params.h"

namespace starrocks {
class SlotDescriptor;
namespace vectorized {
class RuntimeFilterProbeCollector;
}
namespace pipeline {

class OlapChunkSource final : public ChunkSource {
public:
    OlapChunkSource(MorselPtr&& morsel, int32_t tuple_id, std::vector<ExprContext*> conjunct_ctxs,
                    const vectorized::RuntimeFilterProbeCollector& runtime_filters,
                    std::vector<std::string> key_column_names, bool skip_aggregation)
            : ChunkSource(std::move(morsel)),
              _tuple_id(tuple_id),
              _conjunct_ctxs(std::move(conjunct_ctxs)),
              _runtime_filters(runtime_filters),
              _key_column_names(std::move(key_column_names)),
              _skip_aggregation(skip_aggregation) {
        OlapMorsel* olap_morsel = (OlapMorsel*)_morsel.get();
        _scan_range = olap_morsel->get_scan_range();
    }

    ~OlapChunkSource() override = default;

    Status prepare(RuntimeState* state) override;

    Status close(RuntimeState* state) override;

    bool has_next_chunk() const override;

    StatusOr<vectorized::ChunkUniquePtr> get_next_chunk() override;
    void cache_next_chunk_blocking() override;
    StatusOr<vectorized::ChunkUniquePtr> get_next_chunk_nonblocking() override;

private:
    Status _get_tablet(const TInternalScanRange* scan_range);
    Status _init_reader_params(const std::vector<OlapScanRange*>& key_ranges,
                               const std::vector<uint32_t>& scanner_columns, std::vector<uint32_t>& reader_columns,
                               vectorized::ReaderParams* params);
    Status _init_scanner_columns(std::vector<uint32_t>& scanner_columns);
    Status _init_olap_reader(RuntimeState* state);
    Status _build_scan_range(RuntimeState* state);
    Status _read_chunk_from_storage([[maybe_unused]] RuntimeState* state, vectorized::Chunk* chunk);

    int32_t _tuple_id;
    std::vector<ExprContext*> _conjunct_ctxs;

    const vectorized::RuntimeFilterProbeCollector& _runtime_filters;
    std::vector<std::string> _key_column_names;
    bool _skip_aggregation;
    TInternalScanRange* _scan_range;

    Status _status = Status::OK();
    StatusOr<vectorized::ChunkUniquePtr> _chunk;
    // Same size with |_conjunct_ctxs|, indicate which element has been normalized.
    std::vector<bool> _normalized_conjuncts;
    // The conjuncts couldn't push down to storage engine
    std::vector<ExprContext*> _un_push_down_conjuncts;
    vectorized::ConjunctivePredicates _un_push_down_predicates;
    std::vector<uint8_t> _selection;

    ObjectPool _obj_pool;
    TabletSharedPtr _tablet;
    int64_t _version = 0;

    // Constructed from params
    RuntimeState* _runtime_state = nullptr;
    const std::vector<SlotDescriptor*>* _slots = nullptr;
    std::vector<OlapScanRange*> _scanner_ranges;
    std::map<std::string, ColumnValueRangeType> _column_value_ranges;
    OlapScanKeys _scan_keys;
    std::vector<std::unique_ptr<OlapScanRange>> _cond_ranges;
    std::vector<TCondition> _olap_filter;
    std::vector<TCondition> _is_null_vector;

    std::shared_ptr<vectorized::Reader> _reader;
    // projection iterator, doing the job of choosing |_scanner_columns| from |_reader_columns|.
    std::shared_ptr<vectorized::ChunkIterator> _prj_iter;

    // For release memory.
    using PredicatePtr = std::unique_ptr<vectorized::ColumnPredicate>;
    std::vector<PredicatePtr> _predicate_free_pool;

    // The following are profile meatures
    int64_t _num_rows_read = 0;
    int64_t _raw_rows_read = 0;
    int64_t _compressed_bytes_read = 0;

    RuntimeProfile* _scan_profile = nullptr;
    // Non-pushed-down predicates filter time.
    RuntimeProfile::Counter* _expr_filter_timer = nullptr;
    RuntimeProfile::Counter* _scan_timer = nullptr;
    RuntimeProfile::Counter* _capture_rowset_timer = nullptr;
    RuntimeProfile::Counter* _tablet_counter = nullptr;
    RuntimeProfile::Counter* _reader_init_timer = nullptr;
    RuntimeProfile::Counter* _io_timer = nullptr;
    RuntimeProfile::Counter* _read_compressed_counter = nullptr;
    RuntimeProfile::Counter* _decompress_timer = nullptr;
    RuntimeProfile::Counter* _read_uncompressed_counter = nullptr;
    RuntimeProfile::Counter* _raw_rows_counter = nullptr;
    RuntimeProfile::Counter* _pred_filter_counter = nullptr;
    RuntimeProfile::Counter* _del_vec_filter_counter = nullptr;
    RuntimeProfile::Counter* _pred_filter_timer = nullptr;
    RuntimeProfile::Counter* _chunk_copy_timer = nullptr;
    RuntimeProfile::Counter* _seg_init_timer = nullptr;
    RuntimeProfile::Counter* _zm_filtered_counter = nullptr;
    RuntimeProfile::Counter* _bf_filtered_counter = nullptr;
    RuntimeProfile::Counter* _sk_filtered_counter = nullptr;
    RuntimeProfile::Counter* _block_seek_timer = nullptr;
    RuntimeProfile::Counter* _block_seek_counter = nullptr;
    RuntimeProfile::Counter* _block_load_timer = nullptr;
    RuntimeProfile::Counter* _block_load_counter = nullptr;
    RuntimeProfile::Counter* _block_fetch_timer = nullptr;
    RuntimeProfile::Counter* _index_load_timer = nullptr;
    RuntimeProfile::Counter* _total_pages_num_counter = nullptr;
    RuntimeProfile::Counter* _cached_pages_num_counter = nullptr;
    RuntimeProfile::Counter* _bi_filtered_counter = nullptr;
    RuntimeProfile::Counter* _bi_filter_timer = nullptr;
    RuntimeProfile::Counter* _pushdown_predicates_counter = nullptr;
};
} // namespace pipeline
} // namespace starrocks
