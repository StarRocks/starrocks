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

#include "storage/table_reader.h"

#include "column/datum_convert.h"
#include "column/datum_tuple.h"
#include "storage/chunk_helper.h"
#include "storage/projection_iterator.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_reader.h"
#include "storage/union_iterator.h"

namespace starrocks {

TableReader::TableReader(const TableReaderParams& params) : _params(params) {
    for (auto& partition : _params.partition_param.partitions) {
        for (auto& index : partition.indexes) {
            for (int64_t tablet_id : index.tablets) {
                TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, true);
                if (tablet != nullptr) {
                    _local_tablets.push_back(tablet);
                }
            }
        }
    }

    _tablet_schema = ChunkHelper::convert_schema_to_format_v2(_local_tablets[0]->tablet_schema());
    CHECK_EQ(_params.schema.num_fields(), _tablet_schema.num_fields());
    for (int i = 0; i < _params.schema.num_fields(); i++) {
        CHECK_EQ(_params.schema.field(i)->name(), _tablet_schema.field(i)->name());
    }
}

TableReader::~TableReader() {}

Status TableReader::multi_get(const Chunk& keys, const std::vector<std::string>& value_columns,
                              std::vector<bool>& found, Chunk& values) {
    DeferOp defer([&] { _obj_pool.clear(); });
    VectorizedSchema value_schema = _build_value_schema(value_columns);
    ChunkPtr read_chunk = ChunkHelper::new_chunk(value_schema, 1);
    std::vector<const ColumnPredicate*> predicates;
    Status status;
    found.clear();
    values.reset();
    for (int i = 0; i < keys.num_rows(); i++) {
        DatumTuple tuple = keys.get(i);
        predicates.clear();
        _build_get_predicates(tuple, &predicates);
        StatusOr<ChunkIteratorPtr> status_or = _base_scan(value_schema, predicates);
        if (!status_or.ok()) {
            status = status_or.status();
            break;
        }
        ChunkIteratorPtr iterator = status_or.value();
        read_chunk->reset();
        status = iterator->get_next(read_chunk.get());
        if (!status.ok() && !status.is_end_of_file()) {
            return status;
        }

        if (status.is_end_of_file()) {
            found.push_back(false);
            continue;
        }

        found.push_back(true);
        CHECK_EQ(read_chunk->num_rows(), 1);
        values.append(*read_chunk);
    }
    return Status::OK();
}

StatusOr<ChunkIteratorPtr> TableReader::scan(const std::vector<std::string>& value_columns,
                                             const std::vector<const ColumnPredicate*>& predicates) {
    VectorizedSchema value_schema = _build_value_schema(value_columns);
    return _base_scan(value_schema, predicates);
}

StatusOr<ChunkIteratorPtr> TableReader::_base_scan(VectorizedSchema& value_schema,
                                                   const std::vector<const ColumnPredicate*>& predicates) {
    TabletReaderParams tablet_reader_params;
    tablet_reader_params.predicates = predicates;
    std::vector<ChunkIteratorPtr> tablet_readers;
    for (const TabletSharedPtr& tablet : _local_tablets) {
        std::shared_ptr<TabletReader> reader =
                std::make_shared<TabletReader>(tablet, Version(0, _params.version), _params.schema);
        Status status = reader->prepare();
        if (!status.ok()) {
            return status;
        }

        status = reader->open(tablet_reader_params);
        if (!status.ok()) {
            return status;
        }
        tablet_readers.push_back(reader);
    }

    return new_projection_iterator(value_schema, new_union_iterator(std::move(tablet_readers)));
}

void TableReader::_build_get_predicates(DatumTuple& tuple, std::vector<const ColumnPredicate*>* predicates) {
    for (size_t i = 0; i < tuple.size(); i++) {
        const Datum& datum = tuple.get(i);
        const VectorizedFieldPtr& field = _tablet_schema.field(i);
        ColumnPredicate* predicate =
                new_column_eq_predicate(field->type(), field->id(), datum_to_string(field->type().get(), datum));
        _obj_pool.add(predicate);
        predicates->push_back(predicate);
    }
}

VectorizedSchema TableReader::_build_value_schema(const std::vector<std::string>& value_columns) {
    VectorizedSchema schema;
    for (auto& name : value_columns) {
        VectorizedFieldPtr field = _tablet_schema.get_field_by_name(name);
        CHECK(field != nullptr);
        schema.append(field);
    }

    return schema;
}

} // namespace starrocks