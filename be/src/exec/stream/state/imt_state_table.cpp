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

namespace starrocks::stream {

IMTStateTable::IMTStateTable(const TIMTDescriptor& imt) : _imt(imt) {
    DCHECK_EQ(_imt.imt_type, TIMTType::OLAP_TABLE);
    DCHECK(_imt.__isset.olap_table);

    _db_name = _imt.olap_table.db_name;
    _table_name = _imt.olap_table.table_name;
}

Status IMTStateTable::prepare(RuntimeState* state) {
    // prepare read
    _table_reader = std::make_unique<TableReader>(_convert_to_reader_params());
    _schema = _table_reader->tablet_schema();
    auto field_names = _schema.field_names();
    // TODO: Construct a default non_keys(values)'s field names array by default.
    std::copy_backward(field_names.begin() + _schema.num_key_fields(), field_names.end(),
                       _non_keys_field_names.begin());

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

TableReaderParams IMTStateTable::_convert_to_reader_params() {
    return TableReaderParams{
            .schema = _imt.olap_table.schema,
            .version = _imt.olap_table.schema.version,
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
            .load_channel_timeout_s = 0,
            .num_replicas = 1,
            .tuple_id = _imt.olap_table.schema.tuple_desc.id,

            .keys_type = TKeysType::PRIMARY_KEYS,
            .schema = _imt.olap_table.schema,
            .partition = _imt.olap_table.partition,
            .location = _imt.olap_table.location,
            .nodes_info = _imt.olap_table.nodes_info,

            .write_quorum_type = TWriteQuorumType::MAJORITY,
            .merge_condition = "",
            .txn_trace_parent = "",

            .is_lake_table = false,
            .need_gen_rollup = false,
            .enable_replicated_storage = false,
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

Status IMTStateTable::write(RuntimeState* state, StreamChunk* chunk) {
    DCHECK(chunk);
    DCHECK(_olap_table_sink);
    // TODO: `ops` column in the StreamChunk should be considered later.
    return _olap_table_sink->send_chunk(state, chunk);
}

Status IMTStateTable::commit(RuntimeState* state) {
    Status status;
    return _olap_table_sink->close(state, status);
}

} // namespace starrocks::stream