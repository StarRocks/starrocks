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

#include "connector/lake_connector.h"

#include "common/constexpr.h"
#include "exec/connector_scan_node.h"
#include "exec/olap_scan_prepare.h"
#include "gen_cpp/InternalService_types.h"
#include "runtime/global_dict/parser.h"
#include "storage/column_predicate_rewriter.h"
#include "storage/conjunctive_predicates.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_reader.h"
#include "storage/olap_runtime_range_pruner.hpp"
#include "storage/predicate_parser.h"
#include "storage/projection_iterator.h"
#include "util/starrocks_metrics.h"

namespace starrocks::connector {

// ================================

class LakeDataSourceProvider;

// TODO: support parallel scan within a single tablet
class LakeDataSource final : public DataSource {
public:
    explicit LakeDataSource(const LakeDataSourceProvider* provider, const TScanRange& scan_range);
    ~LakeDataSource() override;

    Status open(RuntimeState* state) override;
    void close(RuntimeState* state) override;
    Status get_next(RuntimeState* state, ChunkPtr* chunk) override;

    int64_t raw_rows_read() const override { return _raw_rows_read; }
    int64_t num_rows_read() const override { return _num_rows_read; }
    int64_t num_bytes_read() const override { return _bytes_read; }
    int64_t cpu_time_spent() const override { return _cpu_time_spent_ns; }

private:
    Status get_tablet(const TInternalScanRange& scan_range);
    Status init_global_dicts(TabletReaderParams* params);
    Status init_unused_output_columns(const std::vector<std::string>& unused_output_columns);
    Status init_scanner_columns(std::vector<uint32_t>& scanner_columns);
    void decide_chunk_size(bool has_predicate);
    Status init_reader_params(const std::vector<OlapScanRange*>& key_ranges,
                              const std::vector<uint32_t>& scanner_columns, std::vector<uint32_t>& reader_columns);
    Status init_tablet_reader(RuntimeState* state);
    Status build_scan_range(RuntimeState* state);
    void init_counter(RuntimeState* state);
    void update_realtime_counter(Chunk* chunk);
    void update_counter();

private:
    const LakeDataSourceProvider* _provider;
    const TInternalScanRange _scan_range;

    Status _status = Status::OK();
    // The conjuncts couldn't push down to storage engine
    std::vector<ExprContext*> _not_push_down_conjuncts;
    ConjunctivePredicates _not_push_down_predicates;
    std::vector<uint8_t> _selection;

    ObjectPool _obj_pool;

    RuntimeState* _runtime_state = nullptr;
    const std::vector<SlotDescriptor*>* _slots = nullptr;
    std::vector<std::unique_ptr<OlapScanRange>> _key_ranges;
    std::vector<OlapScanRange*> _scanner_ranges;
    OlapScanConjunctsManager _conjuncts_manager;
    DictOptimizeParser _dict_optimize_parser;

    std::shared_ptr<const TabletSchema> _tablet_schema;
    int64_t _version = 0;
    TabletReaderParams _params{};
    std::shared_ptr<lake::TabletReader> _reader;
    // projection iterator, doing the job of choosing |_scanner_columns| from |_reader_columns|.
    std::shared_ptr<ChunkIterator> _prj_iter;

    std::unordered_set<uint32_t> _unused_output_column_ids;
    // For release memory.
    using PredicatePtr = std::unique_ptr<ColumnPredicate>;
    std::vector<PredicatePtr> _predicate_free_pool;

    // slot descriptors for each one of |output_columns|.
    std::vector<SlotDescriptor*> _query_slots;

    // The following are profile meatures
    int64_t _num_rows_read = 0;
    int64_t _raw_rows_read = 0;
    int64_t _bytes_read = 0;
    int64_t _cpu_time_spent_ns = 0;

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
    RuntimeProfile::Counter* _get_delvec_timer = nullptr;
    RuntimeProfile::Counter* _get_delta_column_group_timer = nullptr;
    RuntimeProfile::Counter* _seg_init_timer = nullptr;
    RuntimeProfile::Counter* _column_iterator_init_timer = nullptr;
    RuntimeProfile::Counter* _bitmap_index_iterator_init_timer = nullptr;
    RuntimeProfile::Counter* _zone_map_filter_timer = nullptr;
    RuntimeProfile::Counter* _rows_key_range_filter_timer = nullptr;
    RuntimeProfile::Counter* _bf_filter_timer = nullptr;
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
    RuntimeProfile::Counter* _bi_filtered_counter = nullptr;
    RuntimeProfile::Counter* _bi_filter_timer = nullptr;
    RuntimeProfile::Counter* _pushdown_predicates_counter = nullptr;
    RuntimeProfile::Counter* _rowsets_read_count = nullptr;
    RuntimeProfile::Counter* _segments_read_count = nullptr;
    RuntimeProfile::Counter* _total_columns_data_page_count = nullptr;
    RuntimeProfile::Counter* _read_pk_index_timer = nullptr;

    // IO statistics
    RuntimeProfile::Counter* _io_statistics_timer = nullptr;
    // Page count
    RuntimeProfile::Counter* _pages_count_memory_counter = nullptr;
    RuntimeProfile::Counter* _pages_count_local_disk_counter = nullptr;
    RuntimeProfile::Counter* _pages_count_remote_counter = nullptr;
    RuntimeProfile::Counter* _pages_count_total_counter = nullptr;
    // Compressed bytes read
    RuntimeProfile::Counter* _compressed_bytes_read_local_disk_counter = nullptr;
    RuntimeProfile::Counter* _compressed_bytes_read_remote_counter = nullptr;
    RuntimeProfile::Counter* _compressed_bytes_read_total_counter = nullptr;
    RuntimeProfile::Counter* _compressed_bytes_read_request_counter = nullptr;
    // IO count
    RuntimeProfile::Counter* _io_count_local_disk_counter = nullptr;
    RuntimeProfile::Counter* _io_count_remote_counter = nullptr;
    RuntimeProfile::Counter* _io_count_total_counter = nullptr;
    RuntimeProfile::Counter* _io_count_request_counter = nullptr;
    // IO time
    RuntimeProfile::Counter* _io_ns_local_disk_timer = nullptr;
    RuntimeProfile::Counter* _io_ns_remote_timer = nullptr;
    RuntimeProfile::Counter* _io_ns_total_timer = nullptr;
};

// ================================

class LakeDataSourceProvider final : public DataSourceProvider {
public:
    friend class LakeDataSource;
    LakeDataSourceProvider(ConnectorScanNode* scan_node, const TPlanNode& plan_node);
    ~LakeDataSourceProvider() override = default;

    DataSourcePtr create_data_source(const TScanRange& scan_range) override;

    Status init(ObjectPool* pool, RuntimeState* state) override;
    const TupleDescriptor* tuple_descriptor(RuntimeState* state) const override;

    // Make cloud native table behavior same as olap table
    bool always_shared_scan() const override { return false; }

protected:
    ConnectorScanNode* _scan_node;
    const TLakeScanNode _t_lake_scan_node;
};

// ================================

LakeDataSource::LakeDataSource(const LakeDataSourceProvider* provider, const TScanRange& scan_range)
        : _provider(provider), _scan_range(scan_range.internal_scan_range) {}

LakeDataSource::~LakeDataSource() {
    _reader.reset();
    _predicate_free_pool.clear();
}

Status LakeDataSource::open(RuntimeState* state) {
    _runtime_state = state;
    const TLakeScanNode& thrift_lake_scan_node = _provider->_t_lake_scan_node;
    TupleDescriptor* tuple_desc = state->desc_tbl().get_tuple_descriptor(thrift_lake_scan_node.tuple_id);
    _slots = &tuple_desc->slots();

    _runtime_profile->add_info_string("Table", tuple_desc->table_desc()->name());
    if (thrift_lake_scan_node.__isset.rollup_name) {
        _runtime_profile->add_info_string("Rollup", thrift_lake_scan_node.rollup_name);
    }
    if (thrift_lake_scan_node.__isset.sql_predicates) {
        _runtime_profile->add_info_string("Predicates", thrift_lake_scan_node.sql_predicates);
    }

    init_counter(state);

    // eval const conjuncts
    RETURN_IF_ERROR(OlapScanConjunctsManager::eval_const_conjuncts(_conjunct_ctxs, &_status));

    _dict_optimize_parser.set_mutable_dict_maps(state, state->mutable_query_global_dict_map());
    DictOptimizeParser::rewrite_descriptor(state, _conjunct_ctxs, thrift_lake_scan_node.dict_string_id_to_int_ids,
                                           &(tuple_desc->decoded_slots()));

    // Init _conjuncts_manager.
    OlapScanConjunctsManager& cm = _conjuncts_manager;
    cm.conjunct_ctxs_ptr = &_conjunct_ctxs;
    cm.tuple_desc = tuple_desc;
    cm.obj_pool = &_obj_pool;
    cm.key_column_names = &thrift_lake_scan_node.sort_key_column_names;
    cm.runtime_filters = _runtime_filters;
    cm.runtime_state = state;

    const TQueryOptions& query_options = state->query_options();
    int32_t max_scan_key_num;
    if (query_options.__isset.max_scan_key_num && query_options.max_scan_key_num > 0) {
        max_scan_key_num = query_options.max_scan_key_num;
    } else {
        max_scan_key_num = config::max_scan_key_num;
    }
    bool enable_column_expr_predicate = false;
    if (thrift_lake_scan_node.__isset.enable_column_expr_predicate) {
        enable_column_expr_predicate = thrift_lake_scan_node.enable_column_expr_predicate;
    }

    // Parse conjuncts via _conjuncts_manager.
    RETURN_IF_ERROR(cm.parse_conjuncts(true, max_scan_key_num, enable_column_expr_predicate));

    RETURN_IF_ERROR(build_scan_range(_runtime_state));

    RETURN_IF_ERROR(init_tablet_reader(_runtime_state));
    return Status::OK();
}

void LakeDataSource::close(RuntimeState* state) {
    if (_reader) {
        // close reader to update statistics before update counters
        _reader->close();
        update_counter();
    }
    if (_prj_iter) {
        _prj_iter->close();
    }
    if (_reader) {
        _reader.reset();
    }
    _predicate_free_pool.clear();
    _dict_optimize_parser.close(state);
}

Status LakeDataSource::get_next(RuntimeState* state, ChunkPtr* chunk) {
    chunk->reset(ChunkHelper::new_chunk_pooled(_prj_iter->output_schema(), _runtime_state->chunk_size(), true));
    auto* chunk_ptr = chunk->get();

    do {
        RETURN_IF_ERROR(state->check_mem_limit("read chunk from storage"));
        RETURN_IF_ERROR(_prj_iter->get_next(chunk_ptr));

        TRY_CATCH_ALLOC_SCOPE_START()

        for (auto slot : _query_slots) {
            size_t column_index = chunk_ptr->schema()->get_field_index_by_name(slot->col_name());
            chunk_ptr->set_slot_id_to_index(slot->id(), column_index);
        }

        if (!_not_push_down_predicates.empty()) {
            SCOPED_TIMER(_expr_filter_timer);
            size_t nrows = chunk_ptr->num_rows();
            _selection.resize(nrows);
            _not_push_down_predicates.evaluate(chunk_ptr, _selection.data(), 0, nrows);
            chunk_ptr->filter(_selection);
            DCHECK_CHUNK(chunk_ptr);
        }
        if (!_not_push_down_conjuncts.empty()) {
            SCOPED_TIMER(_expr_filter_timer);
            RETURN_IF_ERROR(ExecNode::eval_conjuncts(_not_push_down_conjuncts, chunk_ptr));
            DCHECK_CHUNK(chunk_ptr);
        }
        TRY_CATCH_ALLOC_SCOPE_END()
    } while (chunk_ptr->num_rows() == 0);
    update_realtime_counter(chunk_ptr);
    return Status::OK();
}

Status LakeDataSource::get_tablet(const TInternalScanRange& scan_range) {
    _version = strtoul(scan_range.version.c_str(), nullptr, 10);
    ASSIGN_OR_RETURN(auto tablet, ExecEnv::GetInstance()->lake_tablet_manager()->get_tablet(scan_range.tablet_id));
    ASSIGN_OR_RETURN(_tablet_schema, tablet.get_schema());
    return Status::OK();
}

// mapping a slot-column-id to schema-columnid
Status LakeDataSource::init_global_dicts(TabletReaderParams* params) {
    const TLakeScanNode& thrift_lake_scan_node = _provider->_t_lake_scan_node;
    const auto& global_dict_map = _runtime_state->get_query_global_dict_map();
    auto global_dict = _obj_pool.add(new ColumnIdToGlobalDictMap());
    // mapping column id to storage column ids
    const TupleDescriptor* tuple_desc = _runtime_state->desc_tbl().get_tuple_descriptor(thrift_lake_scan_node.tuple_id);
    for (auto slot : tuple_desc->slots()) {
        if (!slot->is_materialized()) {
            continue;
        }
        auto iter = global_dict_map.find(slot->id());
        if (iter != global_dict_map.end()) {
            auto& dict_map = iter->second.first;
            int32_t index = _tablet_schema->field_index(slot->col_name());
            DCHECK(index >= 0);
            global_dict->emplace(index, const_cast<GlobalDictMap*>(&dict_map));
        }
    }
    params->global_dictmaps = global_dict;
    return Status::OK();
}

Status LakeDataSource::init_unused_output_columns(const std::vector<std::string>& unused_output_columns) {
    for (const auto& col_name : unused_output_columns) {
        int32_t index = _tablet_schema->field_index(col_name);
        if (index < 0) {
            std::stringstream ss;
            ss << "invalid field name: " << col_name;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }
        _unused_output_column_ids.insert(index);
    }
    _params.unused_output_column_ids = &_unused_output_column_ids;
    return Status::OK();
}

Status LakeDataSource::init_scanner_columns(std::vector<uint32_t>& scanner_columns) {
    for (auto slot : *_slots) {
        DCHECK(slot->is_materialized());
        int32_t index = _tablet_schema->field_index(slot->col_name());
        if (index < 0) {
            std::stringstream ss;
            ss << "invalid field name: " << slot->col_name();
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }
        scanner_columns.push_back(index);
        if (!_unused_output_column_ids.count(index)) {
            _query_slots.push_back(slot);
        }
    }
    // Put key columns before non-key columns, as the `MergeIterator` and `AggregateIterator`
    // required.
    std::sort(scanner_columns.begin(), scanner_columns.end());
    if (scanner_columns.empty()) {
        return Status::InternalError("failed to build storage scanner, no materialized slot!");
    }
    return Status::OK();
}

void LakeDataSource::decide_chunk_size(bool has_predicate) {
    if (!has_predicate && _read_limit != -1 && _read_limit < _runtime_state->chunk_size()) {
        // Improve for select * from table limit x, x is small
        _params.chunk_size = _read_limit;
    } else {
        _params.chunk_size = _runtime_state->chunk_size();
    }
}

Status LakeDataSource::init_reader_params(const std::vector<OlapScanRange*>& key_ranges,
                                          const std::vector<uint32_t>& scanner_columns,
                                          std::vector<uint32_t>& reader_columns) {
    const TLakeScanNode& thrift_lake_scan_node = _provider->_t_lake_scan_node;
    bool skip_aggregation = thrift_lake_scan_node.is_preaggregation;
    auto parser = _obj_pool.add(new PredicateParser(*_tablet_schema));
    _params.is_pipeline = true;
    _params.reader_type = READER_QUERY;
    _params.skip_aggregation = skip_aggregation;
    _params.profile = _runtime_profile;
    _params.runtime_state = _runtime_state;
    _params.use_page_cache = !config::disable_storage_page_cache && _scan_range.fill_data_cache;
    _params.fill_data_cache = _scan_range.fill_data_cache;
    _params.runtime_range_pruner = OlapRuntimeScanRangePruner(parser, _conjuncts_manager.unarrived_runtime_filters());

    std::vector<PredicatePtr> preds;
    RETURN_IF_ERROR(_conjuncts_manager.get_column_predicates(parser, &preds));
    decide_chunk_size(!preds.empty());
    _has_any_predicate = (!preds.empty());
    for (auto& p : preds) {
        if (parser->can_pushdown(p.get())) {
            _params.predicates.push_back(p.get());
        } else {
            _not_push_down_predicates.add(p.get());
        }
        _predicate_free_pool.emplace_back(std::move(p));
    }

    {
        GlobalDictPredicatesRewriter not_pushdown_predicate_rewriter(_not_push_down_predicates,
                                                                     *_params.global_dictmaps);
        not_pushdown_predicate_rewriter.rewrite_predicate(&_obj_pool);
    }

    // Range
    for (const auto& key_range : key_ranges) {
        if (key_range->begin_scan_range.size() == 1 && key_range->begin_scan_range.get_value(0) == NEGATIVE_INFINITY) {
            continue;
        }

        _params.range = key_range->begin_include ? TabletReaderParams::RangeStartOperation::GE
                                                 : TabletReaderParams::RangeStartOperation::GT;
        _params.end_range = key_range->end_include ? TabletReaderParams::RangeEndOperation::LE
                                                   : TabletReaderParams::RangeEndOperation::LT;

        _params.start_key.push_back(key_range->begin_scan_range);
        _params.end_key.push_back(key_range->end_scan_range);
    }

    // Return columns
    if (skip_aggregation) {
        reader_columns = scanner_columns;
    } else {
        for (size_t i = 0; i < _tablet_schema->num_key_columns(); i++) {
            reader_columns.push_back(i);
        }
        for (auto index : scanner_columns) {
            if (!_tablet_schema->column(index).is_key()) {
                reader_columns.push_back(index);
            }
        }
    }
    // Actually only the key columns need to be sorted by id, here we check all
    // for simplicity.
    DCHECK(std::is_sorted(reader_columns.begin(), reader_columns.end()));
    return Status::OK();
}

Status LakeDataSource::init_tablet_reader(RuntimeState* runtime_state) {
    const TLakeScanNode& thrift_lake_scan_node = _provider->_t_lake_scan_node;
    // output columns of `this` OlapScanner, i.e, the final output columns of `get_chunk`.
    std::vector<uint32_t> scanner_columns;
    // columns fetched from |_reader|.
    std::vector<uint32_t> reader_columns;

    RETURN_IF_ERROR(get_tablet(_scan_range));
    RETURN_IF_ERROR(init_global_dicts(&_params));
    RETURN_IF_ERROR(init_unused_output_columns(thrift_lake_scan_node.unused_output_column_name));
    RETURN_IF_ERROR(init_scanner_columns(scanner_columns));
    RETURN_IF_ERROR(init_reader_params(_scanner_ranges, scanner_columns, reader_columns));
    starrocks::Schema child_schema = ChunkHelper::convert_schema(*_tablet_schema, reader_columns);

    ASSIGN_OR_RETURN(auto tablet, ExecEnv::GetInstance()->lake_tablet_manager()->get_tablet(_scan_range.tablet_id));
    ASSIGN_OR_RETURN(_reader, tablet.new_reader(_version, std::move(child_schema)));
    if (reader_columns.size() == scanner_columns.size()) {
        _prj_iter = _reader;
    } else {
        starrocks::Schema output_schema = ChunkHelper::convert_schema(*_tablet_schema, scanner_columns);
        _prj_iter = new_projection_iterator(output_schema, _reader);
    }

    if (!_not_push_down_conjuncts.empty() || !_not_push_down_predicates.empty()) {
        _expr_filter_timer = ADD_TIMER(_runtime_profile, "ExprFilterTime");
    }

    DCHECK(_params.global_dictmaps != nullptr);
    RETURN_IF_ERROR(_prj_iter->init_encoded_schema(*_params.global_dictmaps));
    RETURN_IF_ERROR(_prj_iter->init_output_schema(*_params.unused_output_column_ids));

    RETURN_IF_ERROR(_reader->prepare());
    RETURN_IF_ERROR(_reader->open(_params));
    return Status::OK();
}

Status LakeDataSource::build_scan_range(RuntimeState* state) {
    // Get key_ranges and not_push_down_conjuncts from _conjuncts_manager.
    RETURN_IF_ERROR(_conjuncts_manager.get_key_ranges(&_key_ranges));
    _conjuncts_manager.get_not_push_down_conjuncts(&_not_push_down_conjuncts);
    RETURN_IF_ERROR(_dict_optimize_parser.rewrite_conjuncts(&_not_push_down_conjuncts, state));

    int scanners_per_tablet = 64;
    int num_ranges = _key_ranges.size();
    int ranges_per_scanner = std::max(1, num_ranges / scanners_per_tablet);
    for (int i = 0; i < num_ranges;) {
        _scanner_ranges.push_back(_key_ranges[i].get());
        i++;
        for (int j = 1;
             i < num_ranges && j < ranges_per_scanner && _key_ranges[i]->end_include == _key_ranges[i - 1]->end_include;
             ++j, ++i) {
            _scanner_ranges.push_back(_key_ranges[i].get());
        }
    }
    return Status::OK();
}

void LakeDataSource::init_counter(RuntimeState* state) {
    _bytes_read_counter = ADD_COUNTER(_runtime_profile, "BytesRead", TUnit::BYTES);
    _rows_read_counter = ADD_COUNTER(_runtime_profile, "RowsRead", TUnit::UNIT);

    _create_seg_iter_timer = ADD_TIMER(_runtime_profile, "CreateSegmentIter");

    _read_compressed_counter = ADD_COUNTER(_runtime_profile, "CompressedBytesRead", TUnit::BYTES);
    _read_uncompressed_counter = ADD_COUNTER(_runtime_profile, "UncompressedBytesRead", TUnit::BYTES);

    _raw_rows_counter = ADD_COUNTER(_runtime_profile, "RawRowsRead", TUnit::UNIT);
    _pushdown_predicates_counter =
            ADD_COUNTER_SKIP_MERGE(_runtime_profile, "PushdownPredicates", TUnit::UNIT, TCounterMergeType::SKIP_ALL);

    _get_delvec_timer = ADD_TIMER(_runtime_profile, "GetDelVec");
    _get_delta_column_group_timer = ADD_TIMER(_runtime_profile, "GetDeltaColumnGroup");
    _read_pk_index_timer = ADD_TIMER(_runtime_profile, "ReadPKIndex");

    // SegmentInit
    _seg_init_timer = ADD_TIMER(_runtime_profile, "SegmentInit");
    _bi_filter_timer = ADD_CHILD_TIMER(_runtime_profile, "BitmapIndexFilter", "SegmentInit");
    _bi_filtered_counter = ADD_CHILD_COUNTER(_runtime_profile, "BitmapIndexFilterRows", TUnit::UNIT, "SegmentInit");
    _bf_filtered_counter = ADD_CHILD_COUNTER(_runtime_profile, "BloomFilterFilterRows", TUnit::UNIT, "SegmentInit");
    _seg_zm_filtered_counter =
            ADD_CHILD_COUNTER(_runtime_profile, "SegmentZoneMapFilterRows", TUnit::UNIT, "SegmentInit");
    _seg_rt_filtered_counter =
            ADD_CHILD_COUNTER(_runtime_profile, "SegmentRuntimeZoneMapFilterRows", TUnit::UNIT, "SegmentInit");
    _zm_filtered_counter = ADD_CHILD_COUNTER(_runtime_profile, "ZoneMapIndexFilterRows", TUnit::UNIT, "SegmentInit");
    _sk_filtered_counter = ADD_CHILD_COUNTER(_runtime_profile, "ShortKeyFilterRows", TUnit::UNIT, "SegmentInit");
    _column_iterator_init_timer = ADD_CHILD_TIMER(_runtime_profile, "ColumnIteratorInit", "SegmentInit");
    _bitmap_index_iterator_init_timer = ADD_CHILD_TIMER(_runtime_profile, "BitmapIndexIteratorInit", "SegmentInit");
    _zone_map_filter_timer = ADD_CHILD_TIMER(_runtime_profile, "ZoneMapIndexFiter", "SegmentInit");
    _rows_key_range_filter_timer = ADD_CHILD_TIMER(_runtime_profile, "ShortKeyFilter", "SegmentInit");
    _bf_filter_timer = ADD_CHILD_TIMER(_runtime_profile, "BloomFilterFilter", "SegmentInit");

    // SegmentRead
    _block_load_timer = ADD_TIMER(_runtime_profile, "SegmentRead");
    _block_fetch_timer = ADD_CHILD_TIMER(_runtime_profile, "BlockFetch", "SegmentRead");
    _block_load_counter = ADD_CHILD_COUNTER(_runtime_profile, "BlockFetchCount", TUnit::UNIT, "SegmentRead");
    _block_seek_timer = ADD_CHILD_TIMER(_runtime_profile, "BlockSeek", "SegmentRead");
    _block_seek_counter = ADD_CHILD_COUNTER(_runtime_profile, "BlockSeekCount", TUnit::UNIT, "SegmentRead");
    _pred_filter_timer = ADD_CHILD_TIMER(_runtime_profile, "PredFilter", "SegmentRead");
    _pred_filter_counter = ADD_CHILD_COUNTER(_runtime_profile, "PredFilterRows", TUnit::UNIT, "SegmentRead");
    _del_vec_filter_counter = ADD_CHILD_COUNTER(_runtime_profile, "DelVecFilterRows", TUnit::UNIT, "SegmentRead");
    _chunk_copy_timer = ADD_CHILD_TIMER(_runtime_profile, "ChunkCopy", "SegmentRead");
    _decompress_timer = ADD_CHILD_TIMER(_runtime_profile, "DecompressT", "SegmentRead");
    _rowsets_read_count = ADD_CHILD_COUNTER(_runtime_profile, "RowsetsReadCount", TUnit::UNIT, "SegmentRead");
    _segments_read_count = ADD_CHILD_COUNTER(_runtime_profile, "SegmentsReadCount", TUnit::UNIT, "SegmentRead");
    _total_columns_data_page_count =
            ADD_CHILD_COUNTER(_runtime_profile, "TotalColumnsDataPageCount", TUnit::UNIT, "SegmentRead");

    // IO statistics
    // IOTime
    _io_timer = ADD_TIMER(_runtime_profile, "IOTime");
    _io_statistics_timer = ADD_TIMER(_runtime_profile, "IOStatistics");
    // Page count
    _pages_count_memory_counter = ADD_CHILD_COUNTER(_runtime_profile, "PagesCountMemory", TUnit::UNIT, "IOStatistics");
    _pages_count_local_disk_counter =
            ADD_CHILD_COUNTER(_runtime_profile, "PagesCountLocalDisk", TUnit::UNIT, "IOStatistics");
    _pages_count_remote_counter = ADD_CHILD_COUNTER(_runtime_profile, "PagesCountRemote", TUnit::UNIT, "IOStatistics");
    _pages_count_total_counter = ADD_CHILD_COUNTER(_runtime_profile, "PagesCountTotal", TUnit::UNIT, "IOStatistics");
    // Compressed bytes read
    _compressed_bytes_read_local_disk_counter =
            ADD_CHILD_COUNTER(_runtime_profile, "CompressedBytesReadLocalDisk", TUnit::BYTES, "IOStatistics");
    _compressed_bytes_read_remote_counter =
            ADD_CHILD_COUNTER(_runtime_profile, "CompressedBytesReadRemote", TUnit::BYTES, "IOStatistics");
    _compressed_bytes_read_total_counter =
            ADD_CHILD_COUNTER(_runtime_profile, "CompressedBytesReadTotal", TUnit::BYTES, "IOStatistics");
    _compressed_bytes_read_request_counter =
            ADD_CHILD_COUNTER(_runtime_profile, "CompressedBytesReadRequest", TUnit::BYTES, "IOStatistics");
    // IO count
    _io_count_local_disk_counter = ADD_CHILD_COUNTER(_runtime_profile, "IOCountLocalDisk", TUnit::UNIT, "IOStatistics");
    _io_count_remote_counter = ADD_CHILD_COUNTER(_runtime_profile, "IOCountRemote", TUnit::UNIT, "IOStatistics");
    _io_count_total_counter = ADD_CHILD_COUNTER(_runtime_profile, "IOCountTotal", TUnit::UNIT, "IOStatistics");
    _io_count_request_counter = ADD_CHILD_COUNTER(_runtime_profile, "IOCountRequest", TUnit::UNIT, "IOStatistics");
    // IO time
    _io_ns_local_disk_timer = ADD_CHILD_TIMER(_runtime_profile, "IOTimeLocalDisk", "IOStatistics");
    _io_ns_remote_timer = ADD_CHILD_TIMER(_runtime_profile, "IOTimeRemote", "IOStatistics");
    _io_ns_total_timer = ADD_CHILD_TIMER(_runtime_profile, "IOTimeTotal", "IOStatistics");
}

void LakeDataSource::update_realtime_counter(Chunk* chunk) {
    _num_rows_read += chunk->num_rows();
    auto& stats = _reader->stats();
    _raw_rows_read = stats.raw_rows_read;
    _bytes_read = stats.bytes_read;
    _cpu_time_spent_ns = stats.decompress_ns + stats.vec_cond_ns + stats.del_filter_ns;
}

void LakeDataSource::update_counter() {
    COUNTER_UPDATE(_create_seg_iter_timer, _reader->stats().create_segment_iter_ns);
    COUNTER_UPDATE(_rows_read_counter, _num_rows_read);

    COUNTER_UPDATE(_io_timer, _reader->stats().io_ns);
    COUNTER_UPDATE(_read_compressed_counter, _reader->stats().compressed_bytes_read);
    COUNTER_UPDATE(_decompress_timer, _reader->stats().decompress_ns);
    COUNTER_UPDATE(_read_uncompressed_counter, _reader->stats().uncompressed_bytes_read);
    COUNTER_UPDATE(_bytes_read_counter, _reader->stats().bytes_read);

    COUNTER_UPDATE(_block_load_timer, _reader->stats().block_load_ns);
    COUNTER_UPDATE(_block_load_counter, _reader->stats().blocks_load);
    COUNTER_UPDATE(_block_fetch_timer, _reader->stats().block_fetch_ns);
    COUNTER_UPDATE(_block_seek_timer, _reader->stats().block_seek_ns);

    COUNTER_UPDATE(_chunk_copy_timer, _reader->stats().vec_cond_chunk_copy_ns);
    COUNTER_UPDATE(_get_delvec_timer, _reader->stats().get_delvec_ns);
    COUNTER_UPDATE(_get_delta_column_group_timer, _reader->stats().get_delta_column_group_ns);
    COUNTER_UPDATE(_seg_init_timer, _reader->stats().segment_init_ns);
    COUNTER_UPDATE(_column_iterator_init_timer, _reader->stats().column_iterator_init_ns);
    COUNTER_UPDATE(_bitmap_index_iterator_init_timer, _reader->stats().bitmap_index_iterator_init_ns);
    COUNTER_UPDATE(_zone_map_filter_timer, _reader->stats().zone_map_filter_ns);
    COUNTER_UPDATE(_rows_key_range_filter_timer, _reader->stats().rows_key_range_filter_ns);
    COUNTER_UPDATE(_bf_filter_timer, _reader->stats().bf_filter_ns);
    COUNTER_UPDATE(_read_pk_index_timer, _reader->stats().read_pk_index_ns);

    COUNTER_UPDATE(_raw_rows_counter, _reader->stats().raw_rows_read);

    int64_t cond_evaluate_ns = 0;
    cond_evaluate_ns += _reader->stats().vec_cond_evaluate_ns;
    cond_evaluate_ns += _reader->stats().branchless_cond_evaluate_ns;
    cond_evaluate_ns += _reader->stats().expr_cond_evaluate_ns;
    // In order to avoid exposing too detailed metrics, we still record these infos on `_pred_filter_timer`
    // When we support metric classification, we can disassemble it again.
    COUNTER_UPDATE(_pred_filter_timer, cond_evaluate_ns);
    COUNTER_UPDATE(_pred_filter_counter, _reader->stats().rows_vec_cond_filtered);
    COUNTER_UPDATE(_del_vec_filter_counter, _reader->stats().rows_del_vec_filtered);

    COUNTER_UPDATE(_seg_zm_filtered_counter, _reader->stats().segment_stats_filtered);
    COUNTER_UPDATE(_seg_rt_filtered_counter, _reader->stats().runtime_stats_filtered);
    COUNTER_UPDATE(_zm_filtered_counter, _reader->stats().rows_stats_filtered);
    COUNTER_UPDATE(_bf_filtered_counter, _reader->stats().rows_bf_filtered);
    COUNTER_UPDATE(_sk_filtered_counter, _reader->stats().rows_key_range_filtered);

    COUNTER_UPDATE(_bi_filtered_counter, _reader->stats().rows_bitmap_index_filtered);
    COUNTER_UPDATE(_bi_filter_timer, _reader->stats().bitmap_index_filter_timer);
    COUNTER_UPDATE(_block_seek_counter, _reader->stats().block_seek_num);

    COUNTER_UPDATE(_rowsets_read_count, _reader->stats().rowsets_read_count);
    COUNTER_UPDATE(_segments_read_count, _reader->stats().segments_read_count);
    COUNTER_UPDATE(_total_columns_data_page_count, _reader->stats().total_columns_data_page_count);

    COUNTER_SET(_pushdown_predicates_counter, (int64_t)_params.predicates.size());

    StarRocksMetrics::instance()->query_scan_bytes.increment(_bytes_read);
    StarRocksMetrics::instance()->query_scan_rows.increment(_raw_rows_read);

    if (_reader->stats().decode_dict_ns > 0) {
        RuntimeProfile::Counter* c = ADD_TIMER(_runtime_profile, "DictDecode");
        COUNTER_UPDATE(c, _reader->stats().decode_dict_ns);
    }
    if (_reader->stats().late_materialize_ns > 0) {
        RuntimeProfile::Counter* c = ADD_TIMER(_runtime_profile, "LateMaterialize");
        COUNTER_UPDATE(c, _reader->stats().late_materialize_ns);
    }
    if (_reader->stats().del_filter_ns > 0) {
        RuntimeProfile::Counter* c1 = ADD_TIMER(_runtime_profile, "DeleteFilter");
        RuntimeProfile::Counter* c2 = ADD_COUNTER(_runtime_profile, "DeleteFilterRows", TUnit::UNIT);
        COUNTER_UPDATE(c1, _reader->stats().del_filter_ns);
        COUNTER_UPDATE(c2, _reader->stats().rows_del_filtered);
    }

    int64_t pages_total = _reader->stats().total_pages_num;
    int64_t pages_from_memory = _reader->stats().cached_pages_num;
    int64_t pages_from_local_disk = _reader->stats().pages_from_local_disk;
    COUNTER_UPDATE(_pages_count_memory_counter, pages_from_memory);
    COUNTER_UPDATE(_pages_count_local_disk_counter, pages_from_local_disk);
    COUNTER_UPDATE(_pages_count_remote_counter, pages_total - pages_from_memory - pages_from_local_disk);
    COUNTER_UPDATE(_pages_count_total_counter, pages_total);

    COUNTER_UPDATE(_compressed_bytes_read_local_disk_counter, _reader->stats().compressed_bytes_read_local_disk);
    COUNTER_UPDATE(_compressed_bytes_read_remote_counter, _reader->stats().compressed_bytes_read_remote);
    COUNTER_UPDATE(_compressed_bytes_read_total_counter, _reader->stats().compressed_bytes_read);
    COUNTER_UPDATE(_compressed_bytes_read_request_counter, _reader->stats().compressed_bytes_read_request);

    COUNTER_UPDATE(_io_count_local_disk_counter, _reader->stats().io_count_local_disk);
    COUNTER_UPDATE(_io_count_remote_counter, _reader->stats().io_count_remote);
    COUNTER_UPDATE(_io_count_total_counter, _reader->stats().io_count);
    COUNTER_UPDATE(_io_count_request_counter, _reader->stats().io_count_request);

    COUNTER_UPDATE(_io_ns_local_disk_timer, _reader->stats().io_ns_local_disk);
    COUNTER_UPDATE(_io_ns_remote_timer, _reader->stats().io_ns_remote);
    COUNTER_UPDATE(_io_ns_total_timer, _reader->stats().io_ns);
    COUNTER_UPDATE(_io_statistics_timer, _reader->stats().io_ns);
}

// ================================

LakeDataSourceProvider::LakeDataSourceProvider(ConnectorScanNode* scan_node, const TPlanNode& plan_node)
        : _scan_node(scan_node), _t_lake_scan_node(plan_node.lake_scan_node) {}

DataSourcePtr LakeDataSourceProvider::create_data_source(const TScanRange& scan_range) {
    return std::make_unique<LakeDataSource>(this, scan_range);
}

Status LakeDataSourceProvider::init(ObjectPool* pool, RuntimeState* state) {
    if (_t_lake_scan_node.__isset.bucket_exprs) {
        const auto& bucket_exprs = _t_lake_scan_node.bucket_exprs;
        _partition_exprs.resize(bucket_exprs.size());
        for (int i = 0; i < bucket_exprs.size(); ++i) {
            RETURN_IF_ERROR(Expr::create_expr_tree(pool, bucket_exprs[i], &_partition_exprs[i], state));
        }
    }
    return Status::OK();
}

const TupleDescriptor* LakeDataSourceProvider::tuple_descriptor(RuntimeState* state) const {
    return state->desc_tbl().get_tuple_descriptor(_t_lake_scan_node.tuple_id);
}

// ================================

DataSourceProviderPtr LakeConnector::create_data_source_provider(ConnectorScanNode* scan_node,
                                                                 const TPlanNode& plan_node) const {
    return std::make_unique<LakeDataSourceProvider>(scan_node, plan_node);
}

} // namespace starrocks::connector
