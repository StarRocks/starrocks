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
#define FMT_HEADER_ONLY

#include <arrow/array/builder_primitive.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type_fwd.h>
#include <arrow/util/key_value_metadata.h>

#include <utility>

#include "fmt/format.h"

// project dependencies
#include "convert/starrocks_arrow_converter.h"
#include "starrocks_format_reader.h"

// starrocks dependencies
#include "column/chunk.h"
#include "common/status.h"
#include "exec/olap_scan_prepare.h"
#include "exec/pipeline/runtime_filter_types.h"
#include "format_utils.h"
#include "gen_cpp/QueryPlanExtra_types.h"
#include "options.h"
#include "runtime/descriptors.h"
#include "starrocks_format/starrocks_lib.h"
#include "storage/chunk_helper.h"
#include "storage/column_predicate.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/versioned_tablet.h"
#include "storage/olap_common.h"
#include "storage/predicate_parser.h"
#include "storage/tablet_schema.h"
#include "util/thrift_util.h"
#include "util/url_coding.h"

namespace starrocks::lake::format {

class StarRocksFormatReaderImpl : public StarRocksFormatReader {
public:
    StarRocksFormatReaderImpl(int64_t tablet_id, std::string tablet_root_path, int64_t version,
                              std::shared_ptr<arrow::Schema> required_schema,
                              std::shared_ptr<arrow::Schema> output_schema,
                              std::unordered_map<std::string, std::string> options)
            : _tablet_id(tablet_id),
              _version(version),
              _required_schema(std::move(required_schema)),
              _output_schema(std::move(output_schema)),
              _tablet_root_path(std::move(tablet_root_path)),
              _options(std::move(options)) {
        auto itr = _options.find(SR_FORMAT_CHUNK_SIZE);
        if (itr != _options.end() && !itr->second.empty()) {
            _chunk_size = stoi(itr->second);
        } else {
            _chunk_size = config::vector_chunk_size;
        }
    }

    arrow::Status open() override {
        /*
         * support the below file system options, same as hadoop aws fs options
         * fs.s3a.path.style.access default false
         * fs.s3a.access.key
         * fs.s3a.secret.key
         * fs.s3a.endpoint
         * fs.s3a.endpoint.region
         * fs.s3a.connection.ssl.enabled
         * fs.s3a.retry.limit
         * fs.s3a.retry.interval
         */
        auto fs_options = filter_map_by_key_prefix(_options, "fs.");
        auto provider = std::make_shared<FixedLocationProvider>(_tablet_root_path);
        auto metadata_location = provider->tablet_metadata_location(_tablet_id, _version);
        FORMAT_ASSIGN_OR_RAISE_ARROW_STATUS(auto fs, FileSystem::Create(metadata_location, FSOptions(fs_options)));
        FORMAT_ASSIGN_OR_RAISE_ARROW_STATUS(auto metadata,
                                            _lake_tablet_manager->get_tablet_metadata(metadata_location, true, 0, fs));

        // get tablet schema;
        _tablet_schema = std::make_shared<TabletSchema>(metadata->schema());
        bool using_column_uid = false;
        auto itr = _options.find(SR_FORMAT_USING_COLUMN_UID);
        if (itr != _options.end() && !itr->second.empty()) {
            using_column_uid = itr->second.compare("true");
        }

        // get scan column index from tablet schema.
        std::vector<uint32_t> required_column_indexes;
        ARROW_RETURN_NOT_OK(schema_to_column_index(_required_schema, required_column_indexes, using_column_uid));
        // append key columns first.
        for (size_t i = 0; i < _tablet_schema->num_key_columns(); i++) {
            _scan_column_indexes.push_back(i);
        }
        for (auto index : required_column_indexes) {
            if (!_tablet_schema->column(index).is_key()) {
                _scan_column_indexes.push_back(index);
            }
        }
        std::sort(_scan_column_indexes.begin(), _scan_column_indexes.end());
        _scan_schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema, _scan_column_indexes));
        // create tablet reader
        _tablet = std::make_unique<VersionedTablet>(_lake_tablet_manager, metadata);
        FORMAT_ASSIGN_OR_RAISE_ARROW_STATUS(_tablet_reader, _tablet->new_reader(*_scan_schema));

        // get output column index from tablet schema
        std::vector<uint32_t> output_column_indexes;
        ARROW_RETURN_NOT_OK(schema_to_column_index(_output_schema, output_column_indexes, using_column_uid));
        // if scan columns not same as output column. we need project again after filter
        if (std::equal(_scan_column_indexes.begin(), _scan_column_indexes.end(), output_column_indexes.begin(),
                       output_column_indexes.end())) {
            _output_tablet_schema = _scan_schema;
        } else {
            _need_project = true;
            _output_tablet_schema =
                    std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema, output_column_indexes));
            ARROW_RETURN_NOT_OK(build_output_index_map(_output_tablet_schema, _scan_schema));
        }

        ARROW_ASSIGN_OR_RAISE(_arrow_converter,
                              ChunkToRecordBatchConverter::create(_output_tablet_schema, _output_schema,
                                                                  arrow::default_memory_pool()));
        ARROW_RETURN_NOT_OK(_arrow_converter->init());

        TabletReaderParams read_params;
        read_params.reader_type = READER_BYPASS_QUERY;
        read_params.is_pipeline = false;
        read_params.skip_aggregation = false;
        read_params.chunk_size = _chunk_size;
        read_params.use_page_cache = false;
        read_params.lake_io_opts.fill_data_cache = false;
        read_params.lake_io_opts.fs = fs;
        read_params.lake_io_opts.location_provider = provider;

        // parse query plan
        auto query_plan_iter = _options.find(SR_FORMAT_QUERY_PLAN);
        if (query_plan_iter != _options.end() && !query_plan_iter->second.empty()) {
            _state = std::make_shared<RuntimeState>(TQueryGlobals());
            ARROW_RETURN_NOT_OK(to_arrow_status(_state->init_query_global_dict(std::vector<TGlobalDict>())));
            ARROW_RETURN_NOT_OK(to_arrow_status(parse_query_plan(query_plan_iter->second)));
            ARROW_RETURN_NOT_OK(to_arrow_status(init_reader_params(read_params)));
        }

        ARROW_RETURN_NOT_OK(to_arrow_status(_tablet_reader->prepare()));
        ARROW_RETURN_NOT_OK(to_arrow_status(_tablet_reader->open(read_params)));

        return arrow::Status::OK();
    }

    arrow::Status get_next(ArrowArray* c_arrow_array) override {
        auto chunk = ChunkHelper::new_chunk(*_output_tablet_schema, _chunk_size);
        Status status = do_get_next(chunk);
        std::shared_ptr<arrow::RecordBatch> record_batch;
        if (status.ok()) {
            ARROW_ASSIGN_OR_RAISE(record_batch, _arrow_converter->convert(chunk.get()))
        } else if (status.is_end_of_file()) {
            ARROW_ASSIGN_OR_RAISE(record_batch, arrow::RecordBatch::MakeEmpty(_output_schema));
        } else {
            return to_arrow_status(status);
        }

        if (record_batch) {
            return ExportRecordBatch(*record_batch, c_arrow_array);
        }

        return arrow::Status::OK();
    }

    void close() override {
        if (_tablet_reader) {
            _tablet_reader->close();
        }

        if (_tablet_reader) {
            _tablet_reader.reset();
        }

        _predicate_free_pool.clear();
    }

private:
    arrow::Status build_output_index_map(const std::shared_ptr<Schema>& output, const std::shared_ptr<Schema>& input) {
        DCHECK(output);
        DCHECK(input);
        std::unordered_map<ColumnId, size_t> input_indexes;
        for (size_t i = 0; i < input->num_fields(); i++) {
            input_indexes[input->field(i)->id()] = i;
        }

        _index_map.resize(output->num_fields());
        for (size_t i = 0; i < output->num_fields(); i++) {
            if (input_indexes.count(output->field(i)->id()) == 0) {
                return arrow::Status::Invalid("Output column ", output->field(i)->name(),
                                              " isn't in scan column list.");
            }
            _index_map[i] = input_indexes[output->field(i)->id()];
        }
        return arrow::Status::OK();
    }

    arrow::Result<int32_t> get_column_id(const std::shared_ptr<arrow::Field>& field) {
        auto metadata = field->metadata();
        if (!field->HasMetadata() || !metadata->Contains(SR_FORMAT_COLUMN_ID)) {
            return arrow::Status::Invalid("Missing arrow field metadata ", SR_FORMAT_COLUMN_ID);
        }

        try {
            ARROW_ASSIGN_OR_RAISE(string str_unique_id, metadata->Get(SR_FORMAT_COLUMN_ID));
            return std::stoi(str_unique_id);
        } catch (const std::invalid_argument& e) {
            return arrow::Status::Invalid("Invalid argument: ", e.what());
        } catch (const std::out_of_range& e) {
            return arrow::Status::Invalid("Out of range: ", e.what());
        }
    }

    arrow::Status schema_to_column_index(const std::shared_ptr<arrow::Schema>& schema,
                                         std::vector<uint32_t>& column_indexes, bool using_column_uid) {
        std::stringstream ss;
        for (int col_idx = 0; col_idx < schema->num_fields(); col_idx++) {
            int32_t index = -1;
            if (using_column_uid) {
                ARROW_ASSIGN_OR_RAISE(int column_id, get_column_id(schema->field(col_idx)));
                index = _tablet_schema->field_index(column_id);
                if (index < 0) {
                    ss << "Invalid field(" << col_idx << ") unique id: " << column_id;
                }
            } else {
                index = _tablet_schema->field_index(schema->field(col_idx)->name());
                if (index < 0) {
                    ss << "Invalid field(" << col_idx << ") name: " << schema->field(col_idx)->name();
                }
            }
            if (index < 0) {
                return arrow::Status::Invalid(ss.str());
            }
            column_indexes.push_back(index);
        }
        return arrow::Status::OK();
    }

    Status parse_query_plan(std::string& encoded_query_plan) {
        std::string query_plan_info;
        if (!base64_decode(encoded_query_plan, &query_plan_info)) {
            return Status::InvalidArgument("Decode query plan error: " + encoded_query_plan);
        }
        const auto* buf = (const uint8_t*)query_plan_info.data();
        uint32_t len = query_plan_info.size();

        // deserialize TQueryPlanInfo
        TQueryPlanInfo t_query_plan_info;
        RETURN_IF_ERROR(deserialize_thrift_msg(buf, &len, TProtocolType::BINARY, &t_query_plan_info));
        TPlanNode* plan_node = nullptr;
        for (auto& node : t_query_plan_info.plan_fragment.plan.nodes) {
            if (node.node_type == TPlanNodeType::LAKE_SCAN_NODE) {
                if (!plan_node) {
                    plan_node = &node;
                } else {
                    return Status::InvalidArgument("There should be only one lake scan node in query plan!");
                }
            }
        }

        // There should be a lake scan plan node, because only one table in query plan.
        if (!plan_node) {
            return Status::InvalidArgument("There is no lake scan node in query plan!");
        }

        // get tuple descriptor
        RETURN_IF_ERROR(DescriptorTbl::create(_state.get(), &_obj_pool, t_query_plan_info.desc_tbl, &_desc_tbl, 4096));
        auto tuple_id = plan_node->lake_scan_node.tuple_id;
        _tuple_desc = _desc_tbl->get_tuple_descriptor(tuple_id);
        for (auto slot : _tuple_desc->slots()) {
            DCHECK(slot->is_materialized());
            int32_t index = _tablet_schema->field_index(slot->col_name());
            if (index < 0) {
                return Status::InternalError("Invalid field name: " + slot->col_name());
            }
            // set query slots for pushdown filter
            auto itr = std::find(_scan_column_indexes.begin(), _scan_column_indexes.end(), index);
            if (itr != _scan_column_indexes.end()) {
                _query_slots.push_back(slot);
            }
        }

        // get conjuncts
        if (plan_node->__isset.conjuncts && plan_node->conjuncts.size() > 0) {
            RETURN_IF_ERROR(Expr::create_expr_trees(&_obj_pool, plan_node->conjuncts, &_conjunct_ctxs, _state.get()));
            RETURN_IF_ERROR(Expr::prepare(_conjunct_ctxs, _state.get()));
            for (auto ctx : _conjunct_ctxs) {
                Status status = ctx->open(_state.get());
            }
            RETURN_IF_ERROR(Expr::open(_conjunct_ctxs, _state.get()));
        }

        return Status::OK();
    }

    Status init_reader_params(TabletReaderParams& params) {
        auto key_column_names = std::make_shared<std::vector<std::string>>();
        if (PRIMARY_KEYS == _tablet_schema->keys_type() && _tablet_schema->sort_key_idxes().size() > 0) {
            for (auto sort_key_index : _tablet_schema->sort_key_idxes()) {
                TabletColumn col = _tablet_schema->column(sort_key_index);
                key_column_names->emplace_back(col.name());
            }
        } else {
            for (const auto& col : _tablet_schema->columns()) {
                if (col.is_key()) {
                    key_column_names->emplace_back(col.name());
                }
            }
        }

        ScanConjunctsManagerOptions conjuncts_manager_opts = ScanConjunctsManagerOptions{
                .conjunct_ctxs_ptr = &_conjunct_ctxs,
                .tuple_desc = _tuple_desc,
                .obj_pool = &_obj_pool,
                .key_column_names = key_column_names.get(),
                .runtime_state = _state.get(),
        };

        _conjuncts_manager = std::make_unique<ScanConjunctsManager>(std::move(conjuncts_manager_opts));

        RETURN_IF_ERROR(_conjuncts_manager->parse_conjuncts());

        auto parser = _obj_pool.add(new OlapPredicateParser(_tablet_schema));
        std::vector<PredicatePtr> preds;
        RETURN_IF_ERROR(_conjuncts_manager->get_predicate_tree(parser, preds));

        PredicateAndNode and_node;
        for (auto& p : preds) {
            if (parser->can_pushdown(p.get())) {
                and_node.add_child(PredicateColumnNode{p.get()});
            } else {
                _not_push_down_predicates.add(p.get());
            }
            _predicate_free_pool.emplace_back(std::move(p));
        }
        params.pred_tree = PredicateTree::create(std::move(and_node));

        _conjuncts_manager->get_not_push_down_conjuncts(&_not_push_down_conjuncts);

        std::vector<std::unique_ptr<OlapScanRange>> key_ranges;
        RETURN_IF_ERROR(_conjuncts_manager->get_key_ranges(&key_ranges));

        std::vector<OlapScanRange*> scanner_ranges;
        int scanners_per_tablet = 64;
        int num_ranges = key_ranges.size();
        int ranges_per_scanner = std::max(1, num_ranges / scanners_per_tablet);
        for (int i = 0; i < num_ranges;) {
            scanner_ranges.push_back(key_ranges[i].get());
            i++;
            for (int j = 1; i < num_ranges && j < ranges_per_scanner &&
                            key_ranges[i]->end_include == key_ranges[i - 1]->end_include;
                 ++j, ++i) {
                scanner_ranges.push_back(key_ranges[i].get());
            }
        }

        for (const auto& key_range : scanner_ranges) {
            if (key_range->begin_scan_range.size() == 1 &&
                key_range->begin_scan_range.get_value(0) == NEGATIVE_INFINITY) {
                continue;
            }

            params.range = key_range->begin_include ? TabletReaderParams::RangeStartOperation::GE
                                                    : TabletReaderParams::RangeStartOperation::GT;
            params.end_range = key_range->end_include ? TabletReaderParams::RangeEndOperation::LE
                                                      : TabletReaderParams::RangeEndOperation::LT;

            params.start_key.push_back(key_range->begin_scan_range);
            params.end_key.push_back(key_range->end_scan_range);
        }

        return Status::OK();
    }

    Status do_get_next(ChunkUniquePtr& chunk_ptr) {
        auto* output_chunk = chunk_ptr.get();
        if (!_scan_chunk) {
            _scan_chunk = ChunkHelper::new_chunk(_tablet_reader->output_schema(), _chunk_size);
        }
        _scan_chunk->reset();

        do {
            RETURN_IF_ERROR(_tablet_reader->get_next(_scan_chunk.get()));
            // If there is no filter, _query_slots will be empty.
            for (auto slot : _query_slots) {
                size_t column_index = _scan_chunk->schema()->get_field_index_by_name(slot->col_name());
                _scan_chunk->set_slot_id_to_index(slot->id(), column_index);
            }

            if (!_not_push_down_predicates.empty()) {
                // SCOPED_TIMER(_expr_filter_timer);
                size_t row_num = _scan_chunk->num_rows();
                _selection.clear();
                _selection.resize(row_num);
                auto status = _not_push_down_predicates.evaluate(_scan_chunk.get(), _selection.data(), 0, row_num);
                _scan_chunk->filter(_selection);
                DCHECK_CHUNK(_scan_chunk);
            }

            if (!_not_push_down_conjuncts.empty()) {
                // SCOPED_TIMER(_expr_filter_timer);
                auto status = ExecNode::eval_conjuncts(_not_push_down_conjuncts, _scan_chunk.get());
                DCHECK_CHUNK(_scan_chunk.get());
            }

            if (_need_project) {
                Columns& input_columns = _scan_chunk->columns();
                for (size_t i = 0; i < _index_map.size(); i++) {
                    output_chunk->get_column_by_index(i).swap(input_columns[_index_map[i]]);
                }
            } else {
                auto scan_chunk = _scan_chunk.get();
                output_chunk->swap_chunk(*(scan_chunk));
            }
        } while (output_chunk->num_rows() == 0);

        return Status::OK();
    }

private:
    int64_t _tablet_id;
    int64_t _version;
    std::shared_ptr<arrow::Schema> _required_schema;
    std::shared_ptr<arrow::Schema> _output_schema;
    std::string _tablet_root_path;
    std::unordered_map<std::string, std::string> _options;

    int32_t _chunk_size;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::unique_ptr<VersionedTablet> _tablet;
    std::shared_ptr<TabletReader> _tablet_reader;

    // internal tablet reader schema
    std::vector<uint32_t> _scan_column_indexes;
    std::unordered_set<uint32_t> _unused_output_column_ids;
    std::shared_ptr<Schema> _scan_schema;
    std::shared_ptr<Chunk> _scan_chunk;
    // format reader output schema
    std::shared_ptr<Schema> _output_tablet_schema;
    // mapping from index of column in output chunk to index of column in input chunk.
    std::vector<size_t> _index_map;
    // need choose select columns when scan schema are not same as output schema
    bool _need_project = false;

    // filter pushdown use the vars
    std::shared_ptr<RuntimeState> _state;
    ObjectPool _obj_pool;
    // _desc_tbl, tuple_desc,  _query_slots, _conjunct_ctxs memory are maintained by _obj_pool
    DescriptorTbl* _desc_tbl = nullptr;
    TupleDescriptor* _tuple_desc = nullptr;
    // slot descriptors for each one of |output_columns|. used by _not_push_down_conjuncts.
    std::vector<SlotDescriptor*> _query_slots;
    std::vector<ExprContext*> _conjunct_ctxs;

    std::unique_ptr<ScanConjunctsManager> _conjuncts_manager = nullptr;
    using PredicatePtr = std::unique_ptr<ColumnPredicate>;
    // The conjuncts couldn't push down to storage engine
    std::vector<ExprContext*> _not_push_down_conjuncts;
    ConjunctivePredicates _not_push_down_predicates;
    std::vector<PredicatePtr> _predicate_free_pool;
    Buffer<uint8_t> _selection;

    std::shared_ptr<ChunkToRecordBatchConverter> _arrow_converter;
};

arrow::Result<StarRocksFormatReader*> StarRocksFormatReader::create(
        int64_t tablet_id, std::string tablet_root_path, int64_t version, ArrowSchema* required_arrow_schema,
        ArrowSchema* output_arrow_schema, std::unordered_map<std::string, std::string> options) {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> required_schema, arrow::ImportSchema(required_arrow_schema));
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> output_schema, arrow::ImportSchema(output_arrow_schema));
    return create(tablet_id, std::move(tablet_root_path), version, required_schema, output_schema, std::move(options));
}

arrow::Result<StarRocksFormatReader*> StarRocksFormatReader::create(
        int64_t tablet_id, std::string tablet_root_path, int64_t version,
        std::shared_ptr<arrow::Schema> required_schema, std::shared_ptr<arrow::Schema> output_schema,
        std::unordered_map<std::string, std::string> options) {
    StarRocksFormatReaderImpl* format_Reader = new StarRocksFormatReaderImpl(
            tablet_id, std::move(tablet_root_path), version, required_schema, output_schema, std::move(options));

    return format_Reader;
}

} // namespace starrocks::lake::format
