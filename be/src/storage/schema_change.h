// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/schema_change.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <deque>
#include <queue>
#include <vector>

#include "column/datum.h"
#include "column/datum_convert.h"
#include "gen_cpp/AgentService_types.h"
#include "storage/chunk_helper.h"
#include "storage/column_mapping.h"
#include "storage/convert_helper.h"
#include "storage/delete_handler.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/tablet.h"
#include "storage/tablet_reader.h"
#include "storage/tablet_reader_params.h"

namespace starrocks {
class Field;
class Tablet;

namespace vectorized {

class ChunkChanger {
public:
    ChunkChanger(const TabletSchema& tablet_schema);

    virtual ~ChunkChanger();

    ColumnMapping* get_mutable_column_mapping(size_t column_index);

    SchemaMapping get_schema_mapping() const { return _schema_mapping; }

    std::vector<ColumnId>* get_mutable_selected_column_indexs() { return &_selected_column_indexs; }

    bool change_chunk(ChunkPtr& base_chunk, ChunkPtr& new_chunk, const TabletMetaSharedPtr& base_tablet_meta,
                      const TabletMetaSharedPtr& new_tablet_meta, MemPool* mem_pool);

    bool change_chunkV2(ChunkPtr& base_chunk, ChunkPtr& new_chunk, const Schema& base_schema, const Schema& new_schema,
                        MemPool* mem_pool);

    static const MaterializeTypeConverter* _get_materialize_type_converter(std::string materialized_function,
                                                                           FieldType type);

private:
    // @brief column-mapping specification of new schema
    SchemaMapping _schema_mapping;

    std::vector<ColumnId> _selected_column_indexs;

    DISALLOW_COPY(ChunkChanger);
};

class ChunkAllocator {
public:
    ChunkAllocator(const TabletSchema& tablet_schema, size_t memory_limitation);
    virtual ~ChunkAllocator() = default;

    static Status allocate(ChunkPtr& chunk, size_t num_rows, Schema& schema);
    bool is_memory_enough_to_sort(size_t num_rows) const;
    void set_cur_mem_usage(size_t mem_usage) { _memory_allocated = mem_usage; }
    void set_row_len(size_t row_len) { _row_len = row_len; }

private:
    const TabletSchema& _tablet_schema;
    size_t _memory_allocated = 0;
    size_t _row_len;
    size_t _memory_limitation;
};

class SchemaChange {
public:
    SchemaChange() = default;
    virtual ~SchemaChange() = default;

    virtual bool process(TabletReader* reader, RowsetWriter* new_rowset_writer, TabletSharedPtr tablet,
                         TabletSharedPtr base_tablet, RowsetSharedPtr rowset) = 0;

    virtual Status processV2(TabletReader* reader, RowsetWriter* new_rowset_writer, TabletSharedPtr tablet,
                             TabletSharedPtr base_tablet, RowsetSharedPtr rowset) = 0;
};

class LinkedSchemaChange : public SchemaChange {
public:
    explicit LinkedSchemaChange(ChunkChanger* chunk_changer) : SchemaChange(), _chunk_changer(chunk_changer) {}
    ~LinkedSchemaChange() override = default;

    bool process(TabletReader* reader, RowsetWriter* new_rowset_writer, TabletSharedPtr new_tablet,
                 TabletSharedPtr base_tablet, RowsetSharedPtr rowset) override;

    Status processV2(TabletReader* reader, RowsetWriter* new_rowset_writer, TabletSharedPtr new_tablet,
                     TabletSharedPtr base_tablet, RowsetSharedPtr rowset) override;

private:
    ChunkChanger* _chunk_changer = nullptr;
    DISALLOW_COPY(LinkedSchemaChange);
};

// @brief schema change without sorting.
class SchemaChangeDirectly final : public SchemaChange {
public:
    explicit SchemaChangeDirectly(ChunkChanger* chunk_changer) : SchemaChange(), _chunk_changer(chunk_changer) {}
    ~SchemaChangeDirectly() override = default;

    bool process(TabletReader* reader, RowsetWriter* new_rowset_writer, TabletSharedPtr new_tablet,
                 TabletSharedPtr base_tablet, RowsetSharedPtr rowset) override;

    Status processV2(TabletReader* reader, RowsetWriter* new_rowset_writer, TabletSharedPtr new_tablet,
                     TabletSharedPtr base_tablet, RowsetSharedPtr rowset) override;

private:
    ChunkChanger* _chunk_changer = nullptr;
    DISALLOW_COPY(SchemaChangeDirectly);
};

// @breif schema change with sorting
class SchemaChangeWithSorting : public SchemaChange {
public:
    explicit SchemaChangeWithSorting(ChunkChanger* chunk_changer, size_t memory_limitation);
    ~SchemaChangeWithSorting() override;

    bool process(TabletReader* reader, RowsetWriter* new_rowset_writer, TabletSharedPtr new_tablet,
                 TabletSharedPtr base_tablet, RowsetSharedPtr rowset) override;

    Status processV2(TabletReader* reader, RowsetWriter* new_rowset_writer, TabletSharedPtr new_tablet,
                     TabletSharedPtr base_tablet, RowsetSharedPtr rowset);

private:
    static bool _internal_sorting(std::vector<ChunkPtr>& chunk_arr, RowsetWriter* new_rowset_writer,
                                  TabletSharedPtr tablet);

    ChunkChanger* _chunk_changer = nullptr;
    size_t _memory_limitation;
    ChunkAllocator* _chunk_allocator = nullptr;
    DISALLOW_COPY(SchemaChangeWithSorting);
};

class SchemaChangeHandler {
public:
    SchemaChangeHandler() = default;
    virtual ~SchemaChangeHandler() = default;

    // schema change v2, it will not set alter task in base tablet
    Status process_alter_tablet_v2(const TAlterTabletReqV2& request);

    struct AlterMaterializedViewParam {
        std::string column_name;
        std::string origin_column_name;
        std::string mv_expr;
    };

    struct SchemaChangeParams {
        AlterTabletType alter_tablet_type;
        TabletSharedPtr base_tablet;
        TabletSharedPtr new_tablet;
        std::vector<std::unique_ptr<TabletReader>> rowset_readers;
        Version version;
        std::unordered_map<std::string, AlterMaterializedViewParam> materialized_params_map;
        std::vector<RowsetSharedPtr> rowsets_to_change;
        bool sc_sorting = false;
        bool sc_directly = false;
        std::unique_ptr<ChunkChanger> chunk_changer = nullptr;
    };

private:
    static Status _get_versions_to_be_changed(TabletSharedPtr base_tablet,
                                              std::vector<Version>* versions_to_be_changed);

    Status _do_process_alter_tablet_v2(const TAlterTabletReqV2& request);

    Status _do_process_alter_tablet_v2_normal(const TAlterTabletReqV2& request, SchemaChangeParams& sc_params,
                                              const TabletSharedPtr& base_tablet, const TabletSharedPtr& new_tablet);

    Status _validate_alter_result(TabletSharedPtr new_tablet, const TAlterTabletReqV2& request);

    static Status _convert_historical_rowsets(SchemaChangeParams& sc_params);

    static Status _parse_request(
            const std::shared_ptr<Tablet>& base_tablet, const std::shared_ptr<Tablet>& new_tablet,
            ChunkChanger* chunk_changer, bool* sc_sorting, bool* sc_directly,
            const std::unordered_map<std::string, AlterMaterializedViewParam>& materialized_function_map);

    // default_value for new column is needed
    static Status _init_column_mapping(ColumnMapping* column_mapping, const TabletColumn& column_schema,
                                       const std::string& value);

private:
    DISALLOW_COPY(SchemaChangeHandler);
};

} // namespace vectorized
} // namespace starrocks
