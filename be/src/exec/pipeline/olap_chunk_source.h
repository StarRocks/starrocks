// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <utility>

#include "exec/olap_common.h"
#include "exec/olap_utils.h"
#include "exec/pipeline/chunk_source.h"
#include "exec/vectorized/olap_scan_prepare.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "gen_cpp/InternalService_types.h"
#include "runtime/runtime_state.h"
#include "storage/tablet.h"
#include "storage/vectorized/conjunctive_predicates.h"
#include "storage/vectorized/tablet_reader.h"

namespace starrocks {
class SlotDescriptor;
namespace vectorized {
class RuntimeFilterProbeCollector;
}
namespace pipeline {

class OlapChunkSource final : public ChunkSource {
public:
    OlapChunkSource(MorselPtr&& morsel, int32_t tuple_id, int64_t limit, bool enable_column_expr_predicate,
                    std::vector<ExprContext*> conjunct_ctxs, std::vector<ExprContext*>& runtime_in_filters,
                    vectorized::RuntimeFilterProbeCollector* runtime_bloom_filters,
                    std::vector<std::string> key_column_names, bool skip_aggregation,
                    std::vector<std::string>* unused_output_columns, RuntimeProfile* runtime_profile)
            : ChunkSource(std::move(morsel)),
              _tuple_id(tuple_id),
              _limit(limit),
              _enable_column_expr_predicate(enable_column_expr_predicate),
              _conjunct_ctxs(std::move(conjunct_ctxs)),
              _runtime_in_filters(runtime_in_filters),
              _runtime_bloom_filters(*runtime_bloom_filters),
              _key_column_names(std::move(key_column_names)),
              _skip_aggregation(skip_aggregation),
              _unused_output_columns(unused_output_columns),
              _runtime_profile(runtime_profile) {
        _conjunct_ctxs.insert(_conjunct_ctxs.end(), _runtime_in_filters.begin(), _runtime_in_filters.end());
        OlapMorsel* olap_morsel = (OlapMorsel*)_morsel.get();
        _scan_range = olap_morsel->get_scan_range();
    }

    ~OlapChunkSource() override = default;

    Status prepare(RuntimeState* state) override;

    Status close(RuntimeState* state) override;

    bool has_next_chunk() const override;

    bool has_output() const override;

    virtual size_t get_buffer_size() const override;

    StatusOr<vectorized::ChunkPtr> get_next_chunk_from_buffer() override;

    Status buffer_next_batch_chunks_blocking(size_t chunk_size, bool& can_finish, size_t* read_size) override;

private:
    Status _get_tablet(const TInternalScanRange* scan_range);
    Status _init_reader_params(const std::vector<OlapScanRange*>& key_ranges,
                               const std::vector<uint32_t>& scanner_columns, std::vector<uint32_t>& reader_columns);
    Status _init_scanner_columns(std::vector<uint32_t>& scanner_columns);
    Status _init_unused_output_columns(const std::vector<std::string>& unused_output_columns);
    Status _init_olap_reader(RuntimeState* state);
    void _init_counter(RuntimeState* state);
    Status _init_global_dicts(vectorized::TabletReaderParams* params);
    Status _build_scan_range(RuntimeState* state);
    Status _read_chunk_from_storage([[maybe_unused]] RuntimeState* state, vectorized::Chunk* chunk);
    void _update_counter();
    void _update_realtime_counter(vectorized::Chunk* chunk);

    vectorized::TabletReaderParams _params = {};

    const int32_t _tuple_id;
    const int64_t _limit; // -1: no limit
    const bool _enable_column_expr_predicate;
    std::vector<ExprContext*> _conjunct_ctxs;
    const std::vector<ExprContext*>& _runtime_in_filters;
    const vectorized::RuntimeFilterProbeCollector& _runtime_bloom_filters;
    std::vector<std::string> _key_column_names;
    bool _skip_aggregation;
    TInternalScanRange* _scan_range;

    Status _status = Status::OK();
    UnboundedBlockingQueue<vectorized::ChunkPtr> _chunk_buffer;
    // The conjuncts couldn't push down to storage engine
    std::vector<ExprContext*> _not_push_down_conjuncts;
    vectorized::ConjunctivePredicates _not_push_down_predicates;
    std::vector<uint8_t> _selection;

    ObjectPool _obj_pool;
    TabletSharedPtr _tablet;
    int64_t _version = 0;

    RuntimeState* _runtime_state = nullptr;
    const std::vector<SlotDescriptor*>* _slots = nullptr;
    std::vector<std::unique_ptr<OlapScanRange>> _key_ranges;
    std::vector<OlapScanRange*> _scanner_ranges;
    vectorized::OlapScanConjunctsManager _conjuncts_manager;
    vectorized::DictOptimizeParser _dict_optimize_parser;

    std::shared_ptr<vectorized::TabletReader> _reader;
    // projection iterator, doing the job of choosing |_scanner_columns| from |_reader_columns|.
    std::shared_ptr<vectorized::ChunkIterator> _prj_iter;

    const std::vector<std::string>* _unused_output_columns = nullptr;
    std::unordered_set<uint32_t> _unused_output_column_ids;
    // For release memory.
    using PredicatePtr = std::unique_ptr<vectorized::ColumnPredicate>;
    std::vector<PredicatePtr> _predicate_free_pool;

    // slot descriptors for each one of |output_columns|.
    std::vector<SlotDescriptor*> _query_slots;

    // The following are profile meatures
    int64_t _num_rows_read = 0;
    int64_t _raw_rows_read = 0;
    int64_t _compressed_bytes_read = 0;

    RuntimeProfile* _runtime_profile;
    RuntimeProfile::Counter* _bytes_read_counter = nullptr;
    RuntimeProfile::Counter* _rows_read_counter = nullptr;

    RuntimeProfile* _scan_profile = nullptr;
    RuntimeProfile::Counter* _expr_filter_timer = nullptr;
    RuntimeProfile::Counter* _scan_timer = nullptr;
    RuntimeProfile::Counter* _create_seg_iter_timer = nullptr;
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
    RuntimeProfile::Counter* _read_pages_num_counter = nullptr;
    RuntimeProfile::Counter* _cached_pages_num_counter = nullptr;
    RuntimeProfile::Counter* _bi_filtered_counter = nullptr;
    RuntimeProfile::Counter* _bi_filter_timer = nullptr;
    RuntimeProfile::Counter* _pushdown_predicates_counter = nullptr;
    RuntimeProfile::Counter* _rowsets_read_count = nullptr;
    RuntimeProfile::Counter* _segments_read_count = nullptr;
    RuntimeProfile::Counter* _total_columns_data_page_count = nullptr;
};
} // namespace pipeline
} // namespace starrocks
