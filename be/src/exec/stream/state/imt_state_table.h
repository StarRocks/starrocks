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

#include "column/datum_tuple.h"
#include "column/vectorized_field.h"
#include "column/vectorized_schema.h"
#include "exec/stream/state/state_table.h"
#include "exec/tablet_sink.h"
#include "gen_cpp/Descriptors_types.h"
#include "storage/chunk_iterator.h"
#include "storage/table_reader.h"

namespace starrocks::stream {

/**
 * `IMTStateTable` represents a physical and persistent `StateTable` which is used for
 * statefull operators. It uses a starrocks internal (olap) table as its persistent 
 * layer(maybe other storage mediums can be used later).
 * 
 * `IMTStateTable` uses `TableReader` and `OlapTableSink` to interact(read/write) with 
 * the base storage layer.
 */
class IMTStateTable : public StateTable {
public:
    IMTStateTable(const TIMTDescriptor& imt);
    ~IMTStateTable() override = default;

    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;

    Status seek(const Columns& keys, StateTableResult& values) const override;
    Status seek(const Columns& keys, const std::vector<uint8_t>& selection, StateTableResult& values) const override;
    Status seek(const Columns& keys, const std::vector<std::string>& projection_columns,
                StateTableResult& values) const override;

    ChunkIteratorPtrOr prefix_scan(const Columns& keys, size_t row_idx) const override;
    ChunkIteratorPtrOr prefix_scan(const std::vector<std::string>& projection_columns, const Columns& keys,
                                   size_t row_idx) const override;

    Status write(RuntimeState* state, const StreamChunkPtr& chunk) override;
    Status commit(RuntimeState* state) override;
    Status reset_epoch(RuntimeState* state) override;

private:
    TableReaderParams _convert_to_reader_params(int64_t version);
    stream_load::OlapTableSinkParams _convert_to_sink_params();
    DatumTuple _convert_to_datum_tuple(const Columns& keys, size_t row_idx) const;

private:
    const TIMTDescriptor& _imt;

    std::string _db_name;
    std::string _table_name;
    int64_t _table_id;

    VectorizedSchema _schema;
    std::vector<std::string> _non_keys_field_names;
    Version _version;

    // read api
    std::unique_ptr<TableReader> _table_reader;

    // write api
    ObjectPool _obj_pool;
    std::vector<TExpr> _output_exprs;
    std::unique_ptr<stream_load::OlapTableSink> _olap_table_sink;
};

} // namespace starrocks::stream
