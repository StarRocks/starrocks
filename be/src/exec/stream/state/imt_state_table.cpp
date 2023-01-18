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

#include "exec/stream/state/imt_state_table.h"

#include <fmt/format.h>

#include "exec/pipeline/query_context.h"
#include "exec/pipeline/stream_epoch_manager.h"

namespace starrocks::stream {

IMTStateTable::IMTStateTable(const TIMTDescriptor& imt) : _imt(imt) {
    DCHECK_EQ(_imt.imt_type, TIMTType::OLAP_TABLE);
    DCHECK(_imt.__isset.olap_table);

    _db_name = _imt.olap_table.db_name;
    _table_name = _imt.olap_table.table_name;
    _table_id = _imt.olap_table.schema.table_id;
}

Status IMTStateTable::prepare(RuntimeState* state) {
    // prepare read
    _table_reader = std::make_unique<TableReader>(_convert_to_reader_params(_imt.olap_table.schema.version));
    _schema = _table_reader->tablet_schema();

    // Construct a default non_keys(values)'s field names array by default.
    // TODO: construct non-key values from user's scope.
    auto field_names = _schema.field_names();
    DCHECK_LT(0, _schema.num_key_fields());
    DCHECK_LT(_schema.num_key_fields(), _schema.num_fields());
    _non_keys_field_names.resize(_schema.num_fields() - _schema.num_key_fields());
    std::copy_backward(field_names.begin() + _schema.num_key_fields(), field_names.end(), _non_keys_field_names.end());

    // prepare write
    Status status;
    _olap_table_sink = std::make_unique<stream_load::OlapTableSink>(&_obj_pool, _output_exprs, &status, state);
    RETURN_IF_ERROR(status);
    RETURN_IF_ERROR(_olap_table_sink->init(_convert_to_sink_params(), state));
    return _olap_table_sink->prepare(state);
}

Status IMTStateTable::open(RuntimeState* state) {
    return _olap_table_sink->open(state);
}

TableReaderParams IMTStateTable::_convert_to_reader_params(int64_t version) {
    return TableReaderParams{
            .schema = _imt.olap_table.schema,
            .version = version,
            .partition_param = _imt.olap_table.partition,
            .location_param = _imt.olap_table.location,
            .nodes_info = _imt.olap_table.nodes_info,
    };
}

stream_load::OlapTableSinkParams IMTStateTable::_convert_to_sink_params() {
    // TODO: All these parameters should be assigned in FE and cherrypick to BE later.
    return stream_load::OlapTableSinkParams{
            .load_id = _imt.load_id,

            .txn_id = _imt.txn_id,
            .load_channel_timeout_s = _imt.olap_table.load_channel_timeout_s,
            .num_replicas = _imt.olap_table.num_replicas,
            .tuple_id = _imt.olap_table.schema.tuple_desc.id,

            .keys_type = _imt.olap_table.keys_type,
            .schema = _imt.olap_table.schema,
            .partition = _imt.olap_table.partition,
            .location = _imt.olap_table.location,
            .nodes_info = _imt.olap_table.nodes_info,

            .write_quorum_type = _imt.olap_table.write_quorum_type,
            .merge_condition = _imt.olap_table.merge_condition,
            .txn_trace_parent = _imt.olap_table.txn_trace_parent,

            .is_lake_table = false,
            .need_gen_rollup = false,
            .enable_replicated_storage = _imt.olap_table.enable_replicated_storage,
            .is_output_tuple_desc_same_with_input = true,
    };
}

Status IMTStateTable::seek(const Columns& keys, StateTableResult& state_result) const {
    return seek(keys, _non_keys_field_names, state_result);
}

Status IMTStateTable::seek(const Columns& keys, const std::vector<uint8_t>& selection,
                           StateTableResult& state_result) const {
    return _table_reader->multi_get(keys, selection, _non_keys_field_names, state_result.found,
                                    &state_result.result_chunk);
}

Status IMTStateTable::seek(const Columns& keys, const std::vector<std::string>& projection_columns,
                           StateTableResult& state_result) const {
    return _table_reader->multi_get(keys, projection_columns, state_result.found, &state_result.result_chunk);
}

ChunkIteratorPtrOr IMTStateTable::prefix_scan(const Columns& keys, size_t row_idx) const {
    return prefix_scan(_non_keys_field_names, keys, row_idx);
}

ChunkIteratorPtrOr IMTStateTable::prefix_scan(const std::vector<std::string>& projection_columns, const Columns& keys,
                                              size_t row_idx) const {
    std::vector<const ColumnPredicate*> predicates;
    predicates.reserve(keys.size());
    auto datum_tuple = _convert_to_datum_tuple(keys, row_idx);
    ObjectPool obj_pool;
    _table_reader->build_eq_predicates(datum_tuple, &predicates, obj_pool);
    return _table_reader->scan(projection_columns, predicates);
}

DatumTuple IMTStateTable::_convert_to_datum_tuple(const Columns& keys, size_t row_idx) const {
    DatumTuple res;
    res.reserve(keys.size());
    for (const auto& column : keys) {
        res.append(column->get(row_idx));
    }
    return res;
}

Status IMTStateTable::write(RuntimeState* state, const StreamChunkPtr& chunk) {
    DCHECK(chunk);
    DCHECK(_olap_table_sink);
    if (StreamChunkConverter::has_ops_column(chunk)) {
        auto new_chunk = StreamChunkConverter::to_chunk(chunk);
        return _olap_table_sink->send_chunk(state, new_chunk.get());
    } else {
        return _olap_table_sink->send_chunk(state, chunk.get());
    }
}

Status IMTStateTable::commit(RuntimeState* state) {
    return _olap_table_sink->close(state, Status::OK());
}

Status IMTStateTable::reset_epoch(RuntimeState* state) {
    // reset reader
    pipeline::StreamEpochManager* stream_epoch_manager = state->query_ctx()->stream_epoch_manager();
    auto& imt_version_map = stream_epoch_manager->imt_version_map();
    auto iter = imt_version_map.find(_table_id);
    if (iter == imt_version_map.end()) {
        return Status::InternalError(fmt::format("Cannot find newest version in epoch: {}" + _table_id));
    }
    _table_reader = std::make_unique<TableReader>(_convert_to_reader_params(iter->second));

    // reset writer
    if (!_olap_table_sink->is_close_done()) {
        // TODO: ignore error for now.
        _olap_table_sink->close(state, Status::OK());
    }
    RETURN_IF_ERROR(_olap_table_sink->reset_epoch(state));
    RETURN_IF_ERROR(_olap_table_sink->prepare(state));
    RETURN_IF_ERROR(_olap_table_sink->open(state));
    return Status::OK();
}

} // namespace starrocks::stream