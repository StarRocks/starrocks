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

#include "exec/pipeline/scan/olap_chunk_source.h"

#include <cstdint>
#include <string_view>
#include <unordered_map>

#include "column/column.h"
#include "column/column_access_path.h"
#include "column/field.h"
#include "common/status.h"
#include "exec/olap_scan_node.h"
#include "exec/olap_scan_prepare.h"
#include "exec/pipeline/scan/olap_scan_context.h"
#include "exec/pipeline/scan/scan_operator.h"
#include "exec/workgroup/work_group.h"
#include "gen_cpp/Metrics_types.h"
#include "gen_cpp/RuntimeProfile_types.h"
#include "gutil/map_util.h"
#include "io/io_profiler.h"
#include "runtime/current_thread.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "storage/chunk_helper.h"
#include "storage/column_predicate_rewriter.h"
#include "storage/index/vector/vector_search_option.h"
#include "storage/predicate_parser.h"
#include "storage/projection_iterator.h"
#include "storage/runtime_range_pruner.hpp"
#include "storage/storage_engine.h"
#include "storage/tablet_index.h"
#include "types/logical_type.h"
#include "util/runtime_profile.h"
#include "util/table_metrics.h"

namespace starrocks::pipeline {

OlapChunkSource::OlapChunkSource(ScanOperator* op, RuntimeProfile* runtime_profile, MorselPtr&& morsel,
                                 OlapScanNode* scan_node, OlapScanContext* scan_ctx)
        : ChunkSource(op, runtime_profile, std::move(morsel), scan_ctx->get_chunk_buffer()),
          _scan_node(scan_node),
          _scan_ctx(scan_ctx),
          _limit(scan_node->limit()),
          _scan_range(down_cast<ScanMorsel*>(_morsel.get())->get_olap_scan_range()) {}

OlapChunkSource::~OlapChunkSource() {
    _reader.reset();
    _predicate_free_pool.clear();
}

void OlapChunkSource::close(RuntimeState* state) {
    if (_reader) {
        _update_counter();
    }
    if (_prj_iter) {
        _prj_iter->close();
    }
    if (_reader) {
        _reader.reset();
    }
    _predicate_free_pool.clear();
}

Status OlapChunkSource::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ChunkSource::prepare(state));
    _runtime_state = state;
    const TOlapScanNode& thrift_olap_scan_node = _scan_node->thrift_olap_scan_node();
    const TVectorSearchOptions& vector_search_options = thrift_olap_scan_node.vector_search_options;
    _use_vector_index = thrift_olap_scan_node.__isset.vector_search_options && vector_search_options.enable_use_ann;
    if (_use_vector_index) {
        _use_ivfpq = vector_search_options.use_ivfpq;
        _vector_distance_column_name = vector_search_options.vector_distance_column_name;
        _vector_slot_id = vector_search_options.vector_slot_id;
        _params.vector_search_option = std::make_shared<VectorSearchOption>();
    }
    const TupleDescriptor* tuple_desc = state->desc_tbl().get_tuple_descriptor(thrift_olap_scan_node.tuple_id);
    _slots = &tuple_desc->slots();

    _runtime_profile->add_info_string("Table", tuple_desc->table_desc()->name());
    if (thrift_olap_scan_node.__isset.rollup_name) {
        _runtime_profile->add_info_string("Rollup", thrift_olap_scan_node.rollup_name);
    }
    if (thrift_olap_scan_node.__isset.sql_predicates) {
        _runtime_profile->add_info_string("Predicates", thrift_olap_scan_node.sql_predicates);
    }

    _init_counter(state);

    RETURN_IF_ERROR(_init_olap_reader(_runtime_state));

    return Status::OK();
}

void OlapChunkSource::update_chunk_exec_stats(RuntimeState* state) {
    if (state->query_ctx()) {
        auto* ctx = _runtime_state->query_ctx();
        int32_t node_id = _scan_op->get_plan_node_id();
        int64_t total_index_filter = _reader->stats().rows_bf_filtered + _reader->stats().rows_bitmap_index_filtered +
                                     _reader->stats().segment_stats_filtered +
                                     _reader->stats().rows_key_range_filtered + _reader->stats().rows_stats_filtered;
        ctx->update_index_filter_stats(node_id, total_index_filter);
        ctx->update_rf_filter_stats(node_id, _reader->stats().runtime_stats_filtered);
        ctx->update_pred_filter_stats(node_id, _reader->stats().rows_vec_cond_filtered);
        ctx->update_push_rows_stats(node_id, _reader->stats().raw_rows_read + total_index_filter);
    }
}

TCounterMinMaxType::type OlapChunkSource::_get_counter_min_max_type(const std::string& metric_name) {
    const auto& skip_min_max_metrics = _morsel->skip_min_max_metrics();
    if (skip_min_max_metrics.find(metric_name) != skip_min_max_metrics.end()) {
        return TCounterMinMaxType::SKIP_ALL;
    }

    return TCounterMinMaxType::MIN_MAX_ALL;
}

void OlapChunkSource::_init_counter(RuntimeState* state) {
    _bytes_read_counter = ADD_COUNTER(_runtime_profile, "BytesRead", TUnit::BYTES);
    _rows_read_counter = ADD_COUNTER(_runtime_profile, "RowsRead", TUnit::UNIT);

    _create_seg_iter_timer = ADD_CHILD_TIMER(_runtime_profile, "CreateSegmentIter", IO_TASK_EXEC_TIMER_NAME);

    _read_compressed_counter = ADD_COUNTER(_runtime_profile, "CompressedBytesRead", TUnit::BYTES);
    _read_uncompressed_counter = ADD_COUNTER(_runtime_profile, "UncompressedBytesRead", TUnit::BYTES);

    _raw_rows_counter = ADD_COUNTER(_runtime_profile, "RawRowsRead", TUnit::UNIT);
    _read_pages_num_counter = ADD_COUNTER(_runtime_profile, "ReadPagesNum", TUnit::UNIT);
    _cached_pages_num_counter = ADD_COUNTER(_runtime_profile, "CachedPagesNum", TUnit::UNIT);
    _pushdown_predicates_counter =
            ADD_COUNTER_SKIP_MERGE(_runtime_profile, "PushdownPredicates", TUnit::UNIT, TCounterMergeType::SKIP_ALL);
    _pushdown_access_paths_counter =
            ADD_COUNTER_SKIP_MERGE(_runtime_profile, "PushdownAccessPaths", TUnit::UNIT, TCounterMergeType::SKIP_ALL);

    _get_rowsets_timer = ADD_CHILD_TIMER(_runtime_profile, "GetRowsets", IO_TASK_EXEC_TIMER_NAME);
    _get_delvec_timer = ADD_CHILD_TIMER(_runtime_profile, "GetDelVec", IO_TASK_EXEC_TIMER_NAME);
    _get_delta_column_group_timer = ADD_CHILD_TIMER(_runtime_profile, "GetDeltaColumnGroup", IO_TASK_EXEC_TIMER_NAME);
    _read_pk_index_timer = ADD_CHILD_TIMER(_runtime_profile, "ReadPKIndex", IO_TASK_EXEC_TIMER_NAME);

    // SegmentInit
    const std::string segment_init_name = "SegmentInit";
    _seg_init_timer = ADD_CHILD_TIMER(_runtime_profile, segment_init_name, IO_TASK_EXEC_TIMER_NAME);
    _bi_filter_timer = ADD_CHILD_TIMER(_runtime_profile, "BitmapIndexFilter", segment_init_name);
    _get_row_ranges_by_vector_index_timer =
            ADD_CHILD_TIMER(_runtime_profile, "GetVectorRowRangesTime", segment_init_name);
    _vector_search_timer = ADD_CHILD_TIMER(_runtime_profile, "VectorSearchTime", segment_init_name);
    _process_vector_distance_and_id_timer =
            ADD_CHILD_TIMER(_runtime_profile, "ProcessVectorDistanceAndIdTime", segment_init_name);
    _bi_filtered_counter = ADD_CHILD_COUNTER(_runtime_profile, "BitmapIndexFilterRows", TUnit::UNIT, segment_init_name);
    _bf_filtered_counter = ADD_CHILD_COUNTER(_runtime_profile, "BloomFilterFilterRows", TUnit::UNIT, segment_init_name);
    _gin_filtered_counter = ADD_CHILD_COUNTER(_runtime_profile, "GinFilterRows", TUnit::UNIT, segment_init_name);
    _gin_filtered_timer = ADD_CHILD_TIMER(_runtime_profile, "GinFilter", segment_init_name);
    _seg_zm_filtered_counter =
            ADD_CHILD_COUNTER_SKIP_MIN_MAX(_runtime_profile, "SegmentZoneMapFilterRows", TUnit::UNIT,
                                           _get_counter_min_max_type("SegmentZoneMapFilterRows"), segment_init_name);
    _seg_rt_filtered_counter =
            ADD_CHILD_COUNTER(_runtime_profile, "SegmentRuntimeZoneMapFilterRows", TUnit::UNIT, segment_init_name);
    _zm_filtered_counter =
            ADD_CHILD_COUNTER(_runtime_profile, "ZoneMapIndexFilterRows", TUnit::UNIT, segment_init_name);
    _vector_index_filtered_counter =
            ADD_CHILD_COUNTER(_runtime_profile, "VectorIndexFilterRows", TUnit::UNIT, segment_init_name);
    _sk_filtered_counter =
            ADD_CHILD_COUNTER_SKIP_MIN_MAX(_runtime_profile, "ShortKeyFilterRows", TUnit::UNIT,
                                           _get_counter_min_max_type("ShortKeyFilterRows"), segment_init_name);
    _rows_after_sk_filtered_counter =
            ADD_CHILD_COUNTER(_runtime_profile, "RemainingRowsAfterShortKeyFilter", TUnit::UNIT, segment_init_name);
    _column_iterator_init_timer = ADD_CHILD_TIMER(_runtime_profile, "ColumnIteratorInit", segment_init_name);
    _bitmap_index_iterator_init_timer = ADD_CHILD_TIMER(_runtime_profile, "BitmapIndexIteratorInit", segment_init_name);
    _zone_map_filter_timer = ADD_CHILD_TIMER(_runtime_profile, "ZoneMapIndexFiter", segment_init_name);
    _rows_key_range_filter_timer = ADD_CHILD_TIMER(_runtime_profile, "ShortKeyFilter", segment_init_name);
    _rows_key_range_counter =
            ADD_CHILD_COUNTER(_runtime_profile, "ShortKeyRangeNumber", TUnit::UNIT, segment_init_name);
    _bf_filter_timer = ADD_CHILD_TIMER(_runtime_profile, "BloomFilterFilter", segment_init_name);

    // SegmentRead
    const std::string segment_read_name = "SegmentRead";
    _block_load_timer = ADD_CHILD_TIMER(_runtime_profile, segment_read_name, IO_TASK_EXEC_TIMER_NAME);
    _block_fetch_timer = ADD_CHILD_TIMER(_runtime_profile, "BlockFetch", segment_read_name);
    _block_load_counter = ADD_CHILD_COUNTER(_runtime_profile, "BlockFetchCount", TUnit::UNIT, segment_read_name);
    _block_seek_timer = ADD_CHILD_TIMER(_runtime_profile, "BlockSeek", segment_read_name);
    _block_seek_counter = ADD_CHILD_COUNTER(_runtime_profile, "BlockSeekCount", TUnit::UNIT, segment_read_name);
    _pred_filter_timer = ADD_CHILD_TIMER(_runtime_profile, "PredFilter", segment_read_name);
    _pred_filter_counter = ADD_CHILD_COUNTER(_runtime_profile, "PredFilterRows", TUnit::UNIT, segment_read_name);
    _del_vec_filter_counter = ADD_CHILD_COUNTER(_runtime_profile, "DelVecFilterRows", TUnit::UNIT, segment_read_name);
    _chunk_copy_timer = ADD_CHILD_TIMER(_runtime_profile, "ChunkCopy", segment_read_name);
    _decompress_timer = ADD_CHILD_TIMER(_runtime_profile, "DecompressT", segment_read_name);
    _rowsets_read_count = ADD_CHILD_COUNTER(_runtime_profile, "RowsetsReadCount", TUnit::UNIT, segment_read_name);
    _segments_read_count = ADD_CHILD_COUNTER(_runtime_profile, "SegmentsReadCount", TUnit::UNIT, segment_read_name);
    _total_columns_data_page_count =
            ADD_CHILD_COUNTER(_runtime_profile, "TotalColumnsDataPageCount", TUnit::UNIT, segment_read_name);

    // IOTime
    _io_timer = ADD_CHILD_TIMER(_runtime_profile, "IOTime", IO_TASK_EXEC_TIMER_NAME);

    _access_path_hits_counter = ADD_COUNTER(_runtime_profile, "AccessPathHits", TUnit::UNIT);
    _access_path_unhits_counter = ADD_COUNTER(_runtime_profile, "AccessPathUnhits", TUnit::UNIT);
}

Status OlapChunkSource::_get_tablet(const TInternalScanRange* scan_range) {
    _version = strtoul(scan_range->version.c_str(), nullptr, 10);

    ASSIGN_OR_RETURN(_tablet, OlapScanNode::get_tablet(scan_range));

    return Status::OK();
}

void OlapChunkSource::_decide_chunk_size(bool has_predicate) {
    if (!has_predicate && _limit != -1 && _limit < _runtime_state->chunk_size()) {
        // Improve for select * from table limit x, x is small
        _params.chunk_size = _limit;
    } else {
        _params.chunk_size = _runtime_state->chunk_size();
    }
}

Status OlapChunkSource::_init_reader_params(const std::vector<std::unique_ptr<OlapScanRange>>& key_ranges,
                                            const std::vector<uint32_t>& scanner_columns,
                                            std::vector<uint32_t>& reader_columns) {
    const TOlapScanNode& thrift_olap_scan_node = _scan_node->thrift_olap_scan_node();
    bool skip_aggregation = thrift_olap_scan_node.is_preaggregation;
    auto parser = _obj_pool.add(new OlapPredicateParser(_tablet_schema));
    _params.is_pipeline = true;
    _params.reader_type = READER_QUERY;
    _params.skip_aggregation = skip_aggregation;
    _params.profile = _runtime_profile;
    _params.runtime_state = _runtime_state;
    _params.use_page_cache = _runtime_state->use_page_cache();
    _params.use_pk_index = thrift_olap_scan_node.use_pk_index;
    _params.sample_options = thrift_olap_scan_node.sample_options;
    if (thrift_olap_scan_node.__isset.enable_prune_column_after_index_filter) {
        _params.prune_column_after_index_filter = thrift_olap_scan_node.enable_prune_column_after_index_filter;
    }
    if (thrift_olap_scan_node.__isset.enable_gin_filter) {
        _params.enable_gin_filter = thrift_olap_scan_node.enable_gin_filter;
    }
    _params.use_vector_index = _use_vector_index;
    if (_use_vector_index) {
        const TVectorSearchOptions& vector_options = thrift_olap_scan_node.vector_search_options;

        _params.vector_search_option->vector_distance_column_name = _vector_distance_column_name;
        _params.vector_search_option->k = vector_options.vector_limit_k;
        for (const std::string& str : vector_options.query_vector) {
            _params.vector_search_option->query_vector.push_back(std::stof(str));
        }
        if (_runtime_state->query_options().__isset.ann_params) {
            _params.vector_search_option->query_params = _runtime_state->query_options().ann_params;
        }
        _params.vector_search_option->vector_range = vector_options.vector_range;
        _params.vector_search_option->result_order = vector_options.result_order;
        _params.vector_search_option->use_ivfpq = _use_ivfpq;
        _params.vector_search_option->k_factor = _runtime_state->query_options().k_factor;
        _params.vector_search_option->pq_refine_factor = _runtime_state->query_options().pq_refine_factor;
    }
    if (thrift_olap_scan_node.__isset.sorted_by_keys_per_tablet) {
        _params.sorted_by_keys_per_tablet = thrift_olap_scan_node.sorted_by_keys_per_tablet;
    }
    _params.runtime_range_pruner =
            RuntimeScanRangePruner(parser, _scan_ctx->conjuncts_manager().unarrived_runtime_filters());
    _morsel->init_tablet_reader_params(&_params);

    ASSIGN_OR_RETURN(auto pred_tree, _scan_ctx->conjuncts_manager().get_predicate_tree(parser, _predicate_free_pool));
    _decide_chunk_size(!pred_tree.empty());
    PredicateAndNode pushdown_pred_root;
    PredicateAndNode non_pushdown_pred_root;
    pred_tree.root().partition_copy([parser](const auto& node) { return parser->can_pushdown(node); },
                                    &pushdown_pred_root, &non_pushdown_pred_root);
    _params.pred_tree = PredicateTree::create(std::move(pushdown_pred_root));
    _non_pushdown_pred_tree = PredicateTree::create(std::move(non_pushdown_pred_root));

    {
        GlobalDictPredicatesRewriter not_pushdown_predicate_rewriter(*_params.global_dictmaps);
        RETURN_IF_ERROR(not_pushdown_predicate_rewriter.rewrite_predicate(&_obj_pool, _non_pushdown_pred_tree));
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

Status OlapChunkSource::_init_scanner_columns(std::vector<uint32_t>& scanner_columns) {
    for (auto slot : *_slots) {
        DCHECK(slot->is_materialized());
        int32_t index;
        if (_use_vector_index && !_use_ivfpq && slot->id() == _vector_slot_id) {
            index = _tablet_schema->num_columns();
            _params.vector_search_option->vector_column_id = index;
            _params.vector_search_option->vector_slot_id = slot->id();
        } else {
            index = _tablet_schema->field_index(slot->col_name());
        }
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

Status OlapChunkSource::_init_unused_output_columns(const std::vector<std::string>& unused_output_columns) {
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

Status OlapChunkSource::_init_column_access_paths(Schema* schema) {
    // column access paths
    auto* paths = _scan_ctx->column_access_paths();
    int64_t leaf_size = 0;
    for (const auto& path : *paths) {
        auto& root = path->path();
        int32_t index = _tablet_schema->field_index(root);
        auto field = schema->get_field_by_name(root);
        if (index >= 0 && field != nullptr) {
            auto res = path->convert_by_index(field.get(), index);
            // read whole data, doesn't effect query
            if (LIKELY(res.ok())) {
                _column_access_paths.emplace_back(std::move(res.value()));
                leaf_size += path->leaf_size();
            } else {
                LOG(WARNING) << "failed to convert column access path: " << res.status();
            }
        } else {
            LOG(WARNING) << "failed to find column in schema: " << root;
        }
    }
    _params.column_access_paths = &_column_access_paths;

    // update counter
    COUNTER_SET(_pushdown_access_paths_counter, leaf_size);
    return Status::OK();
}

Status prune_field_by_access_paths(Field* field, ColumnAccessPath* path) {
    if (path->children().size() < 1) {
        return Status::OK();
    }
    if (field->type()->type() == LogicalType::TYPE_ARRAY) {
        DCHECK_EQ(path->children().size(), 1);
        DCHECK_EQ(field->sub_fields().size(), 1);
        RETURN_IF_ERROR(prune_field_by_access_paths(&field->sub_fields()[0], path->children()[0].get()));
    } else if (field->type()->type() == LogicalType::TYPE_MAP) {
        DCHECK_EQ(path->children().size(), 1);
        auto child_path = path->children()[0].get();
        if (child_path->is_index() || child_path->is_all()) {
            DCHECK_EQ(field->sub_fields().size(), 2);
            RETURN_IF_ERROR(prune_field_by_access_paths(&field->sub_fields()[1], child_path));
        } else {
            return Status::OK();
        }
    } else if (field->type()->type() == LogicalType::TYPE_STRUCT) {
        std::unordered_map<std::string_view, ColumnAccessPath*> path_index;
        for (auto& child_path : path->children()) {
            path_index[child_path->path()] = child_path.get();
        }

        std::vector<Field> new_fields;
        for (auto& child_fields : field->sub_fields()) {
            auto iter = path_index.find(child_fields.name());
            if (iter != path_index.end()) {
                auto child_path = iter->second;
                RETURN_IF_ERROR(prune_field_by_access_paths(&child_fields, child_path));
                new_fields.emplace_back(child_fields);
            }
        }

        field->set_sub_fields(std::move(new_fields));
    }
    return Status::OK();
}

Status OlapChunkSource::_prune_schema_by_access_paths(Schema* schema) {
    if (_column_access_paths.empty()) {
        return Status::OK();
    }

    // schema
    for (auto& path : _column_access_paths) {
        if (path->is_from_predicate()) {
            continue;
        }
        auto& root = path->path();
        auto field = schema->get_field_by_name(root);
        if (field == nullptr) {
            LOG(WARNING) << "failed to find column in schema: " << root;
            continue;
        }
        // field maybe modified, so we need to deep copy
        auto new_field = std::make_shared<Field>(*field);
        schema->set_field_by_name(new_field, root);
        RETURN_IF_ERROR(prune_field_by_access_paths(new_field.get(), path.get()));
    }

    return Status::OK();
}

Status OlapChunkSource::_init_olap_reader(RuntimeState* runtime_state) {
    const TOlapScanNode& thrift_olap_scan_node = _scan_node->thrift_olap_scan_node();
    // output columns of `this` OlapScanner, i.e, the final output columns of `get_chunk`.
    std::vector<uint32_t> scanner_columns;
    // columns fetched from |_reader|.
    std::vector<uint32_t> reader_columns;

    RETURN_IF_ERROR(_get_tablet(_scan_range));
    _table_metrics =
            StarRocksMetrics::instance()->table_metrics_mgr()->get_table_metrics(_tablet->tablet_meta()->table_id());

    auto scope = IOProfiler::scope(IOProfiler::TAG_QUERY, _scan_range->tablet_id);

    // schema_id that not greater than 0 is invalid
    if (_scan_node->thrift_olap_scan_node().__isset.schema_id && _scan_node->thrift_olap_scan_node().schema_id > 0 &&
        _scan_node->thrift_olap_scan_node().schema_id == _tablet->tablet_schema()->id()) {
        _tablet_schema = _tablet->tablet_schema();
    }

    if (_tablet_schema == nullptr) {
        // if column_desc come from fe, reset tablet schema
        if (_scan_node->thrift_olap_scan_node().__isset.columns_desc &&
            !_scan_node->thrift_olap_scan_node().columns_desc.empty() &&
            _scan_node->thrift_olap_scan_node().columns_desc[0].col_unique_id >= 0) {
            _tablet_schema =
                    TabletSchema::copy(*_tablet->tablet_schema(), _scan_node->thrift_olap_scan_node().columns_desc);
        } else {
            _tablet_schema = _tablet->tablet_schema();
        }
    }

    RETURN_IF_ERROR(_init_global_dicts(&_params));
    RETURN_IF_ERROR(_init_unused_output_columns(thrift_olap_scan_node.unused_output_column_name));
    RETURN_IF_ERROR(_init_scanner_columns(scanner_columns));
    RETURN_IF_ERROR(_init_reader_params(_scan_ctx->key_ranges(), scanner_columns, reader_columns));

    // schema is new object, but fields not
    starrocks::Schema child_schema = ChunkHelper::convert_schema(_tablet_schema, reader_columns);
    RETURN_IF_ERROR(_init_column_access_paths(&child_schema));
    // will modify schema field, need to copy schema
    RETURN_IF_ERROR(_prune_schema_by_access_paths(&child_schema));

    std::vector<RowsetSharedPtr> rowsets;
    for (auto& rowset : _morsel->rowsets()) {
        rowsets.emplace_back(std::dynamic_pointer_cast<Rowset>(rowset));
    }

    _reader = std::make_shared<TabletReader>(_tablet, Version(_morsel->from_version(), _version),
                                             std::move(child_schema), std::move(rowsets), &_tablet_schema);
    _reader->set_use_gtid(_morsel->get_olap_scan_range()->__isset.gtid);
    if (reader_columns.size() == scanner_columns.size()) {
        _prj_iter = _reader;
    } else {
        starrocks::Schema output_schema = ChunkHelper::convert_schema(_tablet_schema, scanner_columns);
        _prj_iter = new_projection_iterator(output_schema, _reader);
    }

    if (!_scan_ctx->not_push_down_conjuncts().empty() || !_non_pushdown_pred_tree.empty()) {
        _expr_filter_timer = ADD_CHILD_TIMER(_runtime_profile, "ExprFilterTime", IO_TASK_EXEC_TIMER_NAME);

        _non_pushdown_predicates_counter = ADD_COUNTER_SKIP_MERGE(_runtime_profile, "NonPushdownPredicates",
                                                                  TUnit::UNIT, TCounterMergeType::SKIP_ALL);
        COUNTER_SET(_non_pushdown_predicates_counter,
                    static_cast<int64_t>(_scan_ctx->not_push_down_conjuncts().size() + _non_pushdown_pred_tree.size()));
        if (runtime_state->fragment_ctx()->pred_tree_params().enable_show_in_profile) {
            _runtime_profile->add_info_string(
                    "NonPushdownPredicateTree",
                    _non_pushdown_pred_tree.visit([](const auto& node) { return node.debug_string(); }));
        }
    }

    DCHECK(_params.global_dictmaps != nullptr);
    RETURN_IF_ERROR(_prj_iter->init_encoded_schema(*_params.global_dictmaps));
    RETURN_IF_ERROR(_prj_iter->init_output_schema(*_params.unused_output_column_ids));
    _reader->set_is_asc_hint(_scan_op->is_asc());

    RETURN_IF_ERROR(_reader->prepare());
    RETURN_IF_ERROR(_reader->open(_params));

    return Status::OK();
}

Status OlapChunkSource::_read_chunk(RuntimeState* state, ChunkPtr* chunk) {
    chunk->reset(ChunkHelper::new_chunk_pooled(_prj_iter->output_schema(), _runtime_state->chunk_size()));
    auto scope = IOProfiler::scope(IOProfiler::TAG_QUERY, _tablet->tablet_id());
    return _read_chunk_from_storage(_runtime_state, (*chunk).get());
}

// mapping a slot-column-id to schema-columnid
Status OlapChunkSource::_init_global_dicts(TabletReaderParams* params) {
    const TOlapScanNode& thrift_olap_scan_node = _scan_node->thrift_olap_scan_node();
    const auto& global_dict_map = _runtime_state->get_query_global_dict_map();
    auto global_dict = _obj_pool.add(new ColumnIdToGlobalDictMap());
    // mapping column id to storage column ids
    const TupleDescriptor* tuple_desc = _runtime_state->desc_tbl().get_tuple_descriptor(thrift_olap_scan_node.tuple_id);
    for (auto slot : tuple_desc->slots()) {
        if (!slot->is_materialized()) {
            continue;
        }
        auto iter = global_dict_map.find(slot->id());
        if (iter != global_dict_map.end()) {
            auto& dict_map = iter->second.first;
            int32_t index;
            if (_use_vector_index && !_use_ivfpq && slot->id() == _vector_slot_id) {
                index = _tablet_schema->num_columns();
            } else {
                index = _tablet_schema->field_index(slot->col_name());
            }
            DCHECK(index >= 0);
            global_dict->emplace(index, const_cast<GlobalDictMap*>(&dict_map));
        }
    }
    params->global_dictmaps = global_dict;

    return Status::OK();
}

Status OlapChunkSource::_read_chunk_from_storage(RuntimeState* state, Chunk* chunk) {
    if (state->is_cancelled()) {
        return Status::Cancelled("canceled state");
    }

    do {
        RETURN_IF_ERROR(state->check_mem_limit("read chunk from storage"));
        RETURN_IF_ERROR(_prj_iter->get_next(chunk));

        TRY_CATCH_ALLOC_SCOPE_START()

        for (auto slot : _query_slots) {
            size_t column_index = chunk->schema()->get_field_index_by_name(slot->col_name());
            chunk->set_slot_id_to_index(slot->id(), column_index);
        }

        if (!_non_pushdown_pred_tree.empty()) {
            SCOPED_TIMER(_expr_filter_timer);
            size_t nrows = chunk->num_rows();
            _selection.resize(nrows);
            RETURN_IF_ERROR(_non_pushdown_pred_tree.evaluate(chunk, _selection.data(), 0, nrows));
            chunk->filter(_selection);
            DCHECK_CHUNK(chunk);
        }
        if (!_scan_ctx->not_push_down_conjuncts().empty()) {
            SCOPED_TIMER(_expr_filter_timer);
            RETURN_IF_ERROR(ExecNode::eval_conjuncts(_scan_ctx->not_push_down_conjuncts(), chunk));
            DCHECK_CHUNK(chunk);
        }
        TRY_CATCH_ALLOC_SCOPE_END()

    } while (chunk->num_rows() == 0);
    _update_realtime_counter(chunk);
    // Improve for select * from table limit x, x is small
    if (_limit != -1 && _num_rows_read >= _limit) {
        return Status::EndOfFile("limit reach");
    }
    return Status::OK();
}

void OlapChunkSource::_update_realtime_counter(Chunk* chunk) {
    auto& stats = _reader->stats();
    size_t num_rows = chunk->num_rows();
    _num_rows_read += num_rows;
    _scan_rows_num = stats.raw_rows_read;
    _scan_bytes = stats.bytes_read;
    _cpu_time_spent_ns = stats.decompress_ns + stats.vec_cond_ns + stats.del_filter_ns;

    const TQueryOptions& query_options = _runtime_state->query_options();
    if (query_options.__isset.load_job_type && query_options.load_job_type == TLoadJobType::INSERT_QUERY) {
        size_t bytes_usage = chunk->bytes_usage();
        _runtime_state->update_num_rows_load_from_source(num_rows);
        _runtime_state->update_num_bytes_load_from_source(bytes_usage);
    }

    _chunk_buffer.update_limiter(chunk);
}

void OlapChunkSource::_update_counter() {
    COUNTER_UPDATE(_create_seg_iter_timer, _reader->stats().create_segment_iter_ns);
    COUNTER_UPDATE(_rows_read_counter, _num_rows_read);

    COUNTER_UPDATE(_io_timer, _reader->stats().io_ns);
    COUNTER_UPDATE(_read_compressed_counter, _reader->stats().compressed_bytes_read_request);
    COUNTER_UPDATE(_decompress_timer, _reader->stats().decompress_ns);
    COUNTER_UPDATE(_read_uncompressed_counter, _reader->stats().uncompressed_bytes_read);
    COUNTER_UPDATE(_bytes_read_counter, _reader->stats().bytes_read);

    COUNTER_UPDATE(_block_load_timer, _reader->stats().block_load_ns);
    COUNTER_UPDATE(_block_load_counter, _reader->stats().blocks_load);
    COUNTER_UPDATE(_block_fetch_timer, _reader->stats().block_fetch_ns);
    COUNTER_UPDATE(_block_seek_timer, _reader->stats().block_seek_ns);

    COUNTER_UPDATE(_chunk_copy_timer, _reader->stats().vec_cond_chunk_copy_ns);
    COUNTER_UPDATE(_get_rowsets_timer, _reader->stats().get_rowsets_ns);
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
    COUNTER_UPDATE(_vector_index_filtered_counter, _reader->stats().rows_vector_index_filtered);
    COUNTER_UPDATE(_bf_filtered_counter, _reader->stats().rows_bf_filtered);
    COUNTER_UPDATE(_sk_filtered_counter, _reader->stats().rows_key_range_filtered);
    COUNTER_UPDATE(_rows_after_sk_filtered_counter, _reader->stats().rows_after_key_range);
    COUNTER_UPDATE(_rows_key_range_counter, _reader->stats().rows_key_range_num);

    COUNTER_UPDATE(_read_pages_num_counter, _reader->stats().total_pages_num);
    COUNTER_UPDATE(_cached_pages_num_counter, _reader->stats().cached_pages_num);

    COUNTER_UPDATE(_bi_filtered_counter, _reader->stats().rows_bitmap_index_filtered);
    COUNTER_UPDATE(_bi_filter_timer, _reader->stats().bitmap_index_filter_timer);
    COUNTER_UPDATE(_gin_filtered_counter, _reader->stats().rows_gin_filtered);
    COUNTER_UPDATE(_gin_filtered_timer, _reader->stats().gin_index_filter_ns);
    COUNTER_UPDATE(_get_row_ranges_by_vector_index_timer, _reader->stats().get_row_ranges_by_vector_index_timer);
    COUNTER_UPDATE(_vector_search_timer, _reader->stats().vector_search_timer);
    COUNTER_UPDATE(_process_vector_distance_and_id_timer, _reader->stats().process_vector_distance_and_id_timer);
    COUNTER_UPDATE(_block_seek_counter, _reader->stats().block_seek_num);

    COUNTER_UPDATE(_rowsets_read_count, _reader->stats().rowsets_read_count);
    COUNTER_UPDATE(_segments_read_count, _reader->stats().segments_read_count);
    COUNTER_UPDATE(_total_columns_data_page_count, _reader->stats().total_columns_data_page_count);

    COUNTER_SET(_pushdown_predicates_counter, (int64_t)_params.pred_tree.size());

    if (_runtime_state->fragment_ctx()->pred_tree_params().enable_show_in_profile) {
        _runtime_profile->add_info_string(
                "PushdownPredicateTree", _params.pred_tree.visit([](const auto& node) { return node.debug_string(); }));
    }

    StarRocksMetrics::instance()->query_scan_bytes.increment(_scan_bytes);
    StarRocksMetrics::instance()->query_scan_rows.increment(_scan_rows_num);
    _table_metrics->scan_read_bytes.increment(_scan_bytes);
    _table_metrics->scan_read_rows.increment(_scan_rows_num);

    if (_reader->stats().decode_dict_ns > 0) {
        RuntimeProfile::Counter* c = ADD_CHILD_TIMER(_runtime_profile, "DictDecode", IO_TASK_EXEC_TIMER_NAME);
        COUNTER_UPDATE(c, _reader->stats().decode_dict_ns);
    }
    if (_reader->stats().late_materialize_ns > 0) {
        RuntimeProfile::Counter* c = ADD_CHILD_TIMER(_runtime_profile, "LateMaterialize", IO_TASK_EXEC_TIMER_NAME);
        COUNTER_UPDATE(c, _reader->stats().late_materialize_ns);
    }
    if (_reader->stats().del_filter_ns > 0) {
        RuntimeProfile::Counter* c1 = ADD_CHILD_TIMER(_runtime_profile, "DeleteFilter", IO_TASK_EXEC_TIMER_NAME);
        RuntimeProfile::Counter* c2 = ADD_COUNTER(_runtime_profile, "DeleteFilterRows", TUnit::UNIT);
        COUNTER_UPDATE(c1, _reader->stats().del_filter_ns);
        COUNTER_UPDATE(c2, _reader->stats().rows_del_filtered);
    }

    if (_reader->stats().flat_json_hits.size() > 0 || _reader->stats().merge_json_hits.size() > 0) {
        std::string access_path_hits = "AccessPathHits";
        int64_t total = 0;
        for (auto& [k, v] : _reader->stats().flat_json_hits) {
            std::string path = fmt::format("[Hit]{}", k);
            auto* path_counter = _runtime_profile->get_counter(path);
            if (path_counter == nullptr) {
                path_counter = ADD_CHILD_COUNTER(_runtime_profile, path, TUnit::UNIT, access_path_hits);
            }
            total += v;
            COUNTER_UPDATE(path_counter, v);
        }
        for (auto& [k, v] : _reader->stats().merge_json_hits) {
            std::string merge_path = fmt::format("[HitMerge]{}", k);
            auto* path_counter = _runtime_profile->get_counter(merge_path);
            if (path_counter == nullptr) {
                path_counter = ADD_CHILD_COUNTER(_runtime_profile, merge_path, TUnit::UNIT, access_path_hits);
            }
            total += v;
            COUNTER_UPDATE(path_counter, v);
        }
        COUNTER_UPDATE(_access_path_hits_counter, total);
    }
    if (_reader->stats().dynamic_json_hits.size() > 0) {
        std::string access_path_unhits = "AccessPathUnhits";
        int64_t total = 0;
        for (auto& [k, v] : _reader->stats().dynamic_json_hits) {
            std::string path = fmt::format("[Unhit]{}", k);
            auto* path_counter = _runtime_profile->get_counter(path);
            if (path_counter == nullptr) {
                path_counter = ADD_CHILD_COUNTER(_runtime_profile, path, TUnit::UNIT, access_path_unhits);
            }
            total += v;
            COUNTER_UPDATE(path_counter, v);
        }
        COUNTER_UPDATE(_access_path_unhits_counter, total);
    }

    std::string parent_name = "SegmentRead";
    if (_reader->stats().json_init_ns > 0) {
        RuntimeProfile::Counter* c = ADD_CHILD_TIMER(_runtime_profile, "FlatJsonInit", parent_name);
        COUNTER_UPDATE(c, _reader->stats().json_init_ns);
    }
    if (_reader->stats().json_cast_ns > 0) {
        RuntimeProfile::Counter* c = ADD_CHILD_TIMER(_runtime_profile, "FlatJsonCast", parent_name);
        COUNTER_UPDATE(c, _reader->stats().json_cast_ns);
    }
    if (_reader->stats().json_merge_ns > 0) {
        RuntimeProfile::Counter* c = ADD_CHILD_TIMER(_runtime_profile, "FlatJsonMerge", parent_name);
        COUNTER_UPDATE(c, _reader->stats().json_merge_ns);
    }
    if (_reader->stats().json_flatten_ns > 0) {
        RuntimeProfile::Counter* c = ADD_CHILD_TIMER(_runtime_profile, "FlatJsonFlatten", parent_name);
        COUNTER_UPDATE(c, _reader->stats().json_flatten_ns);
    }

    // Data sampling
    if (_params.sample_options.enable_sampling) {
        _runtime_profile->add_info_string("SampleMethod", to_string(_params.sample_options.sample_method));
        _runtime_profile->add_info_string("SamplePercent",
                                          std::to_string(_params.sample_options.probability_percent) + "%");
        COUNTER_UPDATE(ADD_CHILD_TIMER(_runtime_profile, "SampleTime", parent_name),
                       _reader->stats().sample_population_size);
        COUNTER_UPDATE(ADD_CHILD_TIMER(_runtime_profile, "SampleBuildHistogramTime", parent_name),
                       _reader->stats().sample_build_histogram_time_ns);
        COUNTER_UPDATE(ADD_CHILD_COUNTER(_runtime_profile, "SampleSize", TUnit::UNIT, parent_name),
                       _reader->stats().sample_size);
        COUNTER_UPDATE(ADD_CHILD_COUNTER(_runtime_profile, "SamplePopulationSize", TUnit::UNIT, parent_name),
                       _reader->stats().sample_population_size);
        COUNTER_UPDATE(ADD_CHILD_COUNTER(_runtime_profile, "SampleBuildHistogramCount", TUnit::UNIT, parent_name),
                       _reader->stats().sample_build_histogram_count);
    }
}

} // namespace starrocks::pipeline
