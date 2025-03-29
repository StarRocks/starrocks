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

#include <vector>

#include "column/column_access_path.h"
#include "exec/connector_scan_node.h"
#include "exec/olap_scan_prepare.h"
#include "exec/pipeline/fragment_context.h"
#include "runtime/global_dict/parser.h"
#include "storage/column_predicate_rewriter.h"
#include "storage/lake/tablet.h"
#include "storage/predicate_parser.h"
#include "storage/predicate_tree/predicate_tree.hpp"
#include "storage/projection_iterator.h"
#include "storage/rowset/short_key_range_option.h"
#include "storage/runtime_range_pruner.hpp"
#include "util/starrocks_metrics.h"

namespace starrocks::connector {

LakeDataSource::LakeDataSource(const LakeDataSourceProvider* provider, const TScanRange& scan_range)
        : _provider(provider), _scan_range(scan_range.internal_scan_range) {}

LakeDataSource::~LakeDataSource() {
    _reader.reset();
    _predicate_free_pool.clear();
}

std::string LakeDataSource::name() const {
    return "LakeDataSource";
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

    // init column access paths
    if (thrift_lake_scan_node.__isset.column_access_paths) {
        for (int i = 0; i < thrift_lake_scan_node.column_access_paths.size(); ++i) {
            auto st = ColumnAccessPath::create(thrift_lake_scan_node.column_access_paths[i], state, state->obj_pool());
            if (LIKELY(st.ok())) {
                _column_access_paths.emplace_back(std::move(st.value()));
            } else {
                LOG(WARNING) << "Failed to create column access path: "
                             << thrift_lake_scan_node.column_access_paths[i].type << "index: " << i
                             << ", error: " << st.status();
            }
        }
    }

    // eval const conjuncts
    RETURN_IF_ERROR(ScanConjunctsManager::eval_const_conjuncts(_conjunct_ctxs, &_status));
    DictOptimizeParser::rewrite_descriptor(state, _conjunct_ctxs, thrift_lake_scan_node.dict_string_id_to_int_ids,
                                           &(tuple_desc->decoded_slots()));

    // Init _conjuncts_manager.
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

    ScanConjunctsManagerOptions opts;
    opts.conjunct_ctxs_ptr = &_conjunct_ctxs;
    opts.tuple_desc = tuple_desc;
    opts.obj_pool = &_obj_pool;
    opts.key_column_names = &thrift_lake_scan_node.sort_key_column_names;
    opts.runtime_filters = _runtime_filters;
    opts.runtime_state = state;
    opts.scan_keys_unlimited = true;
    opts.max_scan_key_num = max_scan_key_num;
    opts.enable_column_expr_predicate = enable_column_expr_predicate;
    opts.pred_tree_params = state->fragment_ctx()->pred_tree_params();
    opts.driver_sequence = runtime_membership_filter_eval_context.driver_sequence;

    _conjuncts_manager = std::make_unique<ScanConjunctsManager>(std::move(opts));
    ScanConjunctsManager& cm = *_conjuncts_manager;

    // Parse conjuncts via _conjuncts_manager.
    RETURN_IF_ERROR(cm.parse_conjuncts());

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
}

Status LakeDataSource::get_next(RuntimeState* state, ChunkPtr* chunk) {
    chunk->reset(ChunkHelper::new_chunk_pooled(_prj_iter->output_schema(), _runtime_state->chunk_size()));
    auto* chunk_ptr = chunk->get();

    do {
        RETURN_IF_ERROR(state->check_mem_limit("read chunk from storage"));
        RETURN_IF_ERROR(_prj_iter->get_next(chunk_ptr));

        TRY_CATCH_ALLOC_SCOPE_START()

        for (auto slot : _query_slots) {
            size_t column_index = chunk_ptr->schema()->get_field_index_by_name(slot->col_name());
            chunk_ptr->set_slot_id_to_index(slot->id(), column_index);
        }

        if (!_non_pushdown_pred_tree.empty()) {
            SCOPED_TIMER(_expr_filter_timer);
            size_t nrows = chunk_ptr->num_rows();
            _selection.resize(nrows);
            RETURN_IF_ERROR(_non_pushdown_pred_tree.evaluate(chunk_ptr, _selection.data(), 0, nrows));
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
    int64_t tablet_id = scan_range.tablet_id;
    int64_t version = strtoul(scan_range.version.c_str(), nullptr, 10);
    ASSIGN_OR_RETURN(_tablet, ExecEnv::GetInstance()->lake_tablet_manager()->get_tablet(tablet_id, version));
    _tablet_schema = _tablet.get_schema();
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
    auto parser = _obj_pool.add(new OlapPredicateParser(_tablet_schema));
    _params.is_pipeline = true;
    _params.reader_type = READER_QUERY;
    _params.skip_aggregation = skip_aggregation;
    _params.profile = _runtime_profile;
    _params.runtime_state = _runtime_state;
    _params.use_page_cache =
            !config::disable_storage_page_cache && _scan_range.fill_data_cache && !_scan_range.skip_page_cache;
    _params.lake_io_opts.fill_data_cache = _scan_range.fill_data_cache;
    _params.lake_io_opts.skip_disk_cache = _scan_range.skip_disk_cache;
    _params.runtime_range_pruner = RuntimeScanRangePruner(parser, _conjuncts_manager->unarrived_runtime_filters());
    _params.lake_io_opts.cache_file_only = _runtime_state->query_options().__isset.enable_cache_select &&
                                           _runtime_state->query_options().enable_cache_select &&
                                           config::lake_cache_select_in_physical_way;
    _params.splitted_scan_rows = _provider->get_splitted_scan_rows();
    _params.scan_dop = _provider->get_scan_dop();

    ASSIGN_OR_RETURN(auto pred_tree, _conjuncts_manager->get_predicate_tree(parser, _predicate_free_pool));
    decide_chunk_size(!pred_tree.empty());
    _has_any_predicate = !pred_tree.empty();

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

    if (_split_context != nullptr) {
        auto split_context = down_cast<const pipeline::LakeSplitContext*>(_split_context);
        if (_provider->could_split_physically()) {
            // physical
            _params.rowid_range_option = split_context->rowid_range;
        } else {
            // logical
            _params.short_key_ranges_option = split_context->short_key_range;
        }
    }
    starrocks::Schema child_schema = ChunkHelper::convert_schema(_tablet_schema, reader_columns);
    RETURN_IF_ERROR(init_column_access_paths(&child_schema));
    // will modify schema field, need to copy schema
    RETURN_IF_ERROR(prune_schema_by_access_paths(&child_schema));

    // need to split
    bool need_split = _provider->could_split() && _split_context == nullptr;
    if (need_split) {
        // used to construct a new morsel
        _params.plan_node_id = _morsel->get_plan_node_id();
        _params.scan_range = _morsel->get_scan_range();
    }
    ASSIGN_OR_RETURN(_reader, _tablet.new_reader(std::move(child_schema), need_split,
                                                 _provider->could_split_physically(), _morsel->rowsets()));
    if (reader_columns.size() == scanner_columns.size()) {
        _prj_iter = _reader;
    } else {
        starrocks::Schema output_schema = ChunkHelper::convert_schema(_tablet_schema, scanner_columns);
        _prj_iter = new_projection_iterator(output_schema, _reader);
    }

    if (!_not_push_down_conjuncts.empty() || !_non_pushdown_pred_tree.empty()) {
        _expr_filter_timer = ADD_TIMER(_runtime_profile, "ExprFilterTime");

        _non_pushdown_predicates_counter = ADD_COUNTER_SKIP_MERGE(_runtime_profile, "NonPushdownPredicates",
                                                                  TUnit::UNIT, TCounterMergeType::SKIP_ALL);
        COUNTER_SET(_non_pushdown_predicates_counter,
                    static_cast<int64_t>(_not_push_down_conjuncts.size() + _non_pushdown_pred_tree.size()));
        if (runtime_state->fragment_ctx()->pred_tree_params().enable_show_in_profile) {
            _runtime_profile->add_info_string(
                    "NonPushdownPredicateTree",
                    _non_pushdown_pred_tree.visit([](const auto& node) { return node.debug_string(); }));
        }
    }

    DCHECK(_params.global_dictmaps != nullptr);
    RETURN_IF_ERROR(_prj_iter->init_encoded_schema(*_params.global_dictmaps));
    RETURN_IF_ERROR(_prj_iter->init_output_schema(*_params.unused_output_column_ids));
    _reader->set_is_asc_hint(_provider->is_asc_hint());

    RETURN_IF_ERROR(_reader->prepare());
    RETURN_IF_ERROR(_reader->open(_params));
    return Status::OK();
}

Status LakeDataSource::init_column_access_paths(Schema* schema) {
    // column access paths
    int64_t leaf_size = 0;
    std::vector<ColumnAccessPathPtr> new_one;
    for (const auto& path : _column_access_paths) {
        auto& root = path->path();
        int32_t index = _tablet_schema->field_index(root);
        auto field = schema->get_field_by_name(root);
        if (index >= 0 && field != nullptr) {
            auto res = path->convert_by_index(field.get(), index);
            // read whole data, doesn't effect query
            if (LIKELY(res.ok())) {
                new_one.emplace_back(std::move(res.value()));
                leaf_size += path->leaf_size();
            } else {
                LOG(WARNING) << "failed to convert column access path: " << res.status();
            }
        } else {
            LOG(WARNING) << "failed to find column in schema: " << root;
        }
    }
    _column_access_paths = std::move(new_one);
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

Status LakeDataSource::prune_schema_by_access_paths(Schema* schema) {
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

Status LakeDataSource::build_scan_range(RuntimeState* state) {
    // Get key_ranges and not_push_down_conjuncts from _conjuncts_manager.
    RETURN_IF_ERROR(_conjuncts_manager->get_key_ranges(&_key_ranges));
    _conjuncts_manager->get_not_push_down_conjuncts(&_not_push_down_conjuncts);
    RETURN_IF_ERROR(state->mutable_dict_optimize_parser()->rewrite_conjuncts(&_not_push_down_conjuncts));

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
    _access_path_hits_counter = ADD_COUNTER(_runtime_profile, "AccessPathHits", TUnit::UNIT);
    _access_path_unhits_counter = ADD_COUNTER(_runtime_profile, "AccessPathUnhits", TUnit::UNIT);

    _bytes_read_counter = ADD_COUNTER(_runtime_profile, "BytesRead", TUnit::BYTES);
    _rows_read_counter = ADD_COUNTER(_runtime_profile, "RowsRead", TUnit::UNIT);

    _create_seg_iter_timer = ADD_TIMER(_runtime_profile, "CreateSegmentIter");

    _read_compressed_counter = ADD_COUNTER(_runtime_profile, "CompressedBytesRead", TUnit::BYTES);
    _read_uncompressed_counter = ADD_COUNTER(_runtime_profile, "UncompressedBytesRead", TUnit::BYTES);

    _raw_rows_counter = ADD_COUNTER(_runtime_profile, "RawRowsRead", TUnit::UNIT);
    _pushdown_predicates_counter =
            ADD_COUNTER_SKIP_MERGE(_runtime_profile, "PushdownPredicates", TUnit::UNIT, TCounterMergeType::SKIP_ALL);
    _pushdown_access_paths_counter =
            ADD_COUNTER_SKIP_MERGE(_runtime_profile, "PushdownAccessPaths", TUnit::UNIT, TCounterMergeType::SKIP_ALL);

    _get_delvec_timer = ADD_TIMER(_runtime_profile, "GetDelVec");
    _get_delta_column_group_timer = ADD_TIMER(_runtime_profile, "GetDeltaColumnGroup");
    _read_pk_index_timer = ADD_TIMER(_runtime_profile, "ReadPKIndex");

    // SegmentInit
    const std::string segment_init_name = "SegmentInit";
    _seg_init_timer = ADD_TIMER(_runtime_profile, segment_init_name);
    _bi_filter_timer = ADD_CHILD_TIMER(_runtime_profile, "BitmapIndexFilter", segment_init_name);
    _bi_filtered_counter = ADD_CHILD_COUNTER(_runtime_profile, "BitmapIndexFilterRows", TUnit::UNIT, segment_init_name);
    _bf_filtered_counter = ADD_CHILD_COUNTER(_runtime_profile, "BloomFilterFilterRows", TUnit::UNIT, segment_init_name);
    _seg_zm_filtered_counter =
            ADD_CHILD_COUNTER(_runtime_profile, "SegmentZoneMapFilterRows", TUnit::UNIT, segment_init_name);
    _seg_rt_filtered_counter =
            ADD_CHILD_COUNTER(_runtime_profile, "SegmentRuntimeZoneMapFilterRows", TUnit::UNIT, segment_init_name);
    _zm_filtered_counter =
            ADD_CHILD_COUNTER(_runtime_profile, "ZoneMapIndexFilterRows", TUnit::UNIT, segment_init_name);
    _sk_filtered_counter = ADD_CHILD_COUNTER(_runtime_profile, "ShortKeyFilterRows", TUnit::UNIT, segment_init_name);
    _rows_after_sk_filtered_counter =
            ADD_CHILD_COUNTER(_runtime_profile, "RemainingRowsAfterShortKeyFilter", TUnit::UNIT, segment_init_name);
    _rows_key_range_counter =
            ADD_CHILD_COUNTER(_runtime_profile, "ShortKeyRangeNumber", TUnit::UNIT, segment_init_name);
    _column_iterator_init_timer = ADD_CHILD_TIMER(_runtime_profile, "ColumnIteratorInit", segment_init_name);
    _bitmap_index_iterator_init_timer = ADD_CHILD_TIMER(_runtime_profile, "BitmapIndexIteratorInit", segment_init_name);
    _zone_map_filter_timer = ADD_CHILD_TIMER(_runtime_profile, "ZoneMapIndexFiter", segment_init_name);
    _rows_key_range_filter_timer = ADD_CHILD_TIMER(_runtime_profile, "ShortKeyFilter", segment_init_name);
    _bf_filter_timer = ADD_CHILD_TIMER(_runtime_profile, "BloomFilterFilter", segment_init_name);

    // SegmentRead
    const std::string segment_read_name = "SegmentRead";
    _block_load_timer = ADD_TIMER(_runtime_profile, segment_read_name);
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

    // IO statistics
    // IOTime
    _io_timer = ADD_TIMER(_runtime_profile, "IOTime");
    const std::string io_statistics_name = "IOStatistics";
    ADD_COUNTER(_runtime_profile, io_statistics_name, TUnit::NONE);
    // Page count
    _pages_count_memory_counter =
            ADD_CHILD_COUNTER(_runtime_profile, "PagesCountMemory", TUnit::UNIT, io_statistics_name);
    _pages_count_local_disk_counter =
            ADD_CHILD_COUNTER(_runtime_profile, "PagesCountLocalDisk", TUnit::UNIT, io_statistics_name);
    _pages_count_remote_counter =
            ADD_CHILD_COUNTER(_runtime_profile, "PagesCountRemote", TUnit::UNIT, io_statistics_name);
    _pages_count_total_counter =
            ADD_CHILD_COUNTER(_runtime_profile, "PagesCountTotal", TUnit::UNIT, io_statistics_name);
    // Compressed bytes read
    _compressed_bytes_read_local_disk_counter =
            ADD_CHILD_COUNTER(_runtime_profile, "CompressedBytesReadLocalDisk", TUnit::BYTES, io_statistics_name);
    _compressed_bytes_read_remote_counter =
            ADD_CHILD_COUNTER(_runtime_profile, "CompressedBytesReadRemote", TUnit::BYTES, io_statistics_name);
    _compressed_bytes_read_total_counter =
            ADD_CHILD_COUNTER(_runtime_profile, "CompressedBytesReadTotal", TUnit::BYTES, io_statistics_name);
    _compressed_bytes_read_request_counter =
            ADD_CHILD_COUNTER(_runtime_profile, "CompressedBytesReadRequest", TUnit::BYTES, io_statistics_name);
    // IO count
    _io_count_local_disk_counter =
            ADD_CHILD_COUNTER(_runtime_profile, "IOCountLocalDisk", TUnit::UNIT, io_statistics_name);
    _io_count_remote_counter = ADD_CHILD_COUNTER(_runtime_profile, "IOCountRemote", TUnit::UNIT, io_statistics_name);
    _io_count_total_counter = ADD_CHILD_COUNTER(_runtime_profile, "IOCountTotal", TUnit::UNIT, io_statistics_name);
    _io_count_request_counter = ADD_CHILD_COUNTER(_runtime_profile, "IOCountRequest", TUnit::UNIT, io_statistics_name);
    // IO time
    _io_ns_local_disk_timer = ADD_CHILD_TIMER(_runtime_profile, "IOTimeLocalDisk", io_statistics_name);
    _io_ns_remote_timer = ADD_CHILD_TIMER(_runtime_profile, "IOTimeRemote", io_statistics_name);
    _io_ns_total_timer = ADD_CHILD_TIMER(_runtime_profile, "IOTimeTotal", io_statistics_name);
    // Prefetch
    _prefetch_hit_counter = ADD_CHILD_COUNTER(_runtime_profile, "PrefetchHitCount", TUnit::UNIT, io_statistics_name);
    _prefetch_wait_finish_timer = ADD_CHILD_TIMER(_runtime_profile, "PrefetchWaitFinishTime", io_statistics_name);
    _prefetch_pending_timer = ADD_CHILD_TIMER(_runtime_profile, "PrefetchPendingTime", io_statistics_name);
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
    COUNTER_UPDATE(_rows_after_sk_filtered_counter, _reader->stats().rows_after_key_range);
    COUNTER_UPDATE(_rows_key_range_counter, _reader->stats().rows_key_range_num);

    COUNTER_UPDATE(_bi_filtered_counter, _reader->stats().rows_bitmap_index_filtered);
    COUNTER_UPDATE(_bi_filter_timer, _reader->stats().bitmap_index_filter_timer);
    COUNTER_UPDATE(_block_seek_counter, _reader->stats().block_seek_num);

    COUNTER_UPDATE(_rowsets_read_count, _reader->stats().rowsets_read_count);
    COUNTER_UPDATE(_segments_read_count, _reader->stats().segments_read_count);
    COUNTER_UPDATE(_total_columns_data_page_count, _reader->stats().total_columns_data_page_count);

    COUNTER_SET(_pushdown_predicates_counter, (int64_t)_params.pred_tree.size());

    if (_runtime_state->fragment_ctx()->pred_tree_params().enable_show_in_profile) {
        _runtime_profile->add_info_string(
                "PushdownPredicateTree", _params.pred_tree.visit([](const auto& node) { return node.debug_string(); }));
    }

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

    COUNTER_UPDATE(_io_ns_local_disk_timer, _reader->stats().io_ns_read_local_disk);
    COUNTER_UPDATE(_io_ns_remote_timer, _reader->stats().io_ns_remote);
    COUNTER_UPDATE(_io_ns_total_timer, _reader->stats().io_ns);

    COUNTER_UPDATE(_prefetch_hit_counter, _reader->stats().prefetch_hit_count);
    COUNTER_UPDATE(_prefetch_wait_finish_timer, _reader->stats().prefetch_wait_finish_ns);
    COUNTER_UPDATE(_prefetch_pending_timer, _reader->stats().prefetch_pending_ns);

    // update cache related info for CACHE SELECT
    if (_runtime_state->query_options().__isset.query_type &&
        _runtime_state->query_options().query_type == TQueryType::LOAD) {
        _runtime_state->update_num_datacache_read_bytes(_reader->stats().compressed_bytes_read_local_disk);
        _runtime_state->update_num_datacache_read_time_ns(_reader->stats().io_ns_read_local_disk);
        _runtime_state->update_num_datacache_write_bytes(_reader->stats().compressed_bytes_write_local_disk);
        _runtime_state->update_num_datacache_write_time_ns(_reader->stats().io_ns_write_local_disk);
        _runtime_state->update_num_datacache_count(1);
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

StatusOr<pipeline::MorselQueuePtr> LakeDataSourceProvider::convert_scan_range_to_morsel_queue(
        const std::vector<TScanRangeParams>& scan_ranges, int node_id, int32_t pipeline_dop,
        bool enable_tablet_internal_parallel, TTabletInternalParallelMode::type tablet_internal_parallel_mode,
        size_t num_total_scan_ranges, size_t scan_parallelism) {
    int64_t lake_scan_parallelism = 0;
    if (!scan_ranges.empty() && enable_tablet_internal_parallel) {
        ASSIGN_OR_RETURN(_could_split, _could_tablet_internal_parallel(scan_ranges, pipeline_dop, num_total_scan_ranges,
                                                                       tablet_internal_parallel_mode,
                                                                       &lake_scan_parallelism, &splitted_scan_rows));
        if (_could_split) {
            ASSIGN_OR_RETURN(_could_split_physically, _could_split_tablet_physically(scan_ranges));
        }
    }

    return DataSourceProvider::convert_scan_range_to_morsel_queue(
            scan_ranges, node_id, pipeline_dop, enable_tablet_internal_parallel, tablet_internal_parallel_mode,
            num_total_scan_ranges, (size_t)lake_scan_parallelism);
}

StatusOr<bool> LakeDataSourceProvider::_could_tablet_internal_parallel(
        const std::vector<TScanRangeParams>& scan_ranges, int32_t pipeline_dop, size_t num_total_scan_ranges,
        TTabletInternalParallelMode::type tablet_internal_parallel_mode, int64_t* scan_parallelism,
        int64_t* splitted_scan_rows) const {
    //if (_t_lake_scan_node.use_pk_index) {
    //    return false;
    //}

    bool force_split = tablet_internal_parallel_mode == TTabletInternalParallelMode::type::FORCE_SPLIT;
    // The enough number of tablets shouldn't use tablet internal parallel.
    if (!force_split && num_total_scan_ranges >= pipeline_dop) {
        return false;
    }

    int64_t num_table_rows = 0;
    for (const auto& tablet_scan_range : scan_ranges) {
        int64_t version = std::stoll(tablet_scan_range.scan_range.internal_scan_range.version);
#ifdef BE_TEST
        ASSIGN_OR_RETURN(auto tablet_num_rows,
                         _tablet_manager->get_tablet_num_rows(
                                 tablet_scan_range.scan_range.internal_scan_range.tablet_id, version));
        num_table_rows += static_cast<int64_t>(tablet_num_rows);
#else
        ASSIGN_OR_RETURN(auto tablet_num_rows,
                         ExecEnv::GetInstance()->lake_tablet_manager()->get_tablet_num_rows(
                                 tablet_scan_range.scan_range.internal_scan_range.tablet_id, version));
        num_table_rows += static_cast<int64_t>(tablet_num_rows);
#endif
    }

    // splitted_scan_rows is restricted in the range [min_splitted_scan_rows, max_splitted_scan_rows].
    *splitted_scan_rows =
            config::tablet_internal_parallel_max_splitted_scan_bytes / _scan_node->estimated_scan_row_bytes();
    *splitted_scan_rows =
            std::max(config::tablet_internal_parallel_min_splitted_scan_rows,
                     std::min(*splitted_scan_rows, config::tablet_internal_parallel_max_splitted_scan_rows));
    *scan_parallelism = num_table_rows / *splitted_scan_rows;

    if (force_split) {
        return true;
    }

    bool could =
            *scan_parallelism >= pipeline_dop || *scan_parallelism >= config::tablet_internal_parallel_min_scan_dop;
    return could;
}

StatusOr<bool> LakeDataSourceProvider::_could_split_tablet_physically(
        const std::vector<TScanRangeParams>& scan_ranges) const {
    // Keys type needn't merge or aggregate.
    int64_t version = std::stoll(scan_ranges[0].scan_range.internal_scan_range.version);
    KeysType keys_type;
#ifdef BE_TEST
    ASSIGN_OR_RETURN(
            auto first_tablet_schema,
            _tablet_manager->get_tablet_schema(scan_ranges[0].scan_range.internal_scan_range.tablet_id, &version));
    keys_type = first_tablet_schema->keys_type();
#else
    ASSIGN_OR_RETURN(auto first_tablet_schema,
                     ExecEnv::GetInstance()->lake_tablet_manager()->get_tablet_schema(
                             scan_ranges[0].scan_range.internal_scan_range.tablet_id, &version));
    keys_type = first_tablet_schema->keys_type();
#endif

    const auto skip_aggr = _t_lake_scan_node.is_preaggregation;
    bool is_keys_type_matched = keys_type == PRIMARY_KEYS || keys_type == DUP_KEYS ||
                                ((keys_type == UNIQUE_KEYS || keys_type == AGG_KEYS) && skip_aggr);

    return is_keys_type_matched;
}

} // namespace starrocks::connector
