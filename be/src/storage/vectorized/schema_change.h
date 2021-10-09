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
#include "storage/column_mapping.h"
#include "storage/delete_handler.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/tablet.h"
#include "storage/vectorized/chunk_helper.h"
#include "storage/vectorized/tablet_reader.h"
#include "storage/vectorized/tablet_reader_params.h"

namespace starrocks {
// defined in 'field.h'
class Field;
class FieldInfo;
// defined in 'tablet.h'
class Tablet;

namespace vectorized {
using ReaderSharedPtr = std::shared_ptr<TabletReader>;

class ChunkChanger {
public:
    ChunkChanger(const TabletSchema& tablet_schema);

    virtual ~ChunkChanger();

    ColumnMapping* get_mutable_column_mapping(size_t column_index);

    SchemaMapping get_schema_mapping() const { return _schema_mapping; }

    bool change_chunk(ChunkPtr& base_chunk, ChunkPtr& new_chunk, TabletSharedPtr& base_tablet,
                      TabletSharedPtr& new_tablet, MemPool* mem_pool);

private:
    // @brief column-mapping specification of new schema
    SchemaMapping _schema_mapping;

    DISALLOW_COPY_AND_ASSIGN(ChunkChanger);
};

class ChunkAllocator {
public:
    ChunkAllocator(const TabletSchema& tablet_schema, size_t memory_limitation);
    virtual ~ChunkAllocator();

    Status allocate(ChunkPtr& chunk, size_t num_rows, Schema& schema);
    void release(ChunkPtr& chunk, size_t num_rows);
    bool is_memory_enough_to_sort(size_t num_rows, size_t allocated_rows);

private:
    const TabletSchema& _tablet_schema;
    size_t _memory_allocated;
    size_t _row_len;
    size_t _memory_limitation;
};

class SchemaChange {
public:
    SchemaChange(MemTracker* mem_tracker) { _mem_tracker = std::make_unique<MemTracker>(-1, "", mem_tracker, true); }
    virtual ~SchemaChange() {}

    virtual bool process(vectorized::TabletReader* reader, RowsetWriter* new_rowset_writer, TabletSharedPtr tablet,
                         TabletSharedPtr base_tablet, RowsetSharedPtr rowset) = 0;

protected:
    std::unique_ptr<MemTracker> _mem_tracker = nullptr;
};

class LinkedSchemaChange : public SchemaChange {
public:
    explicit LinkedSchemaChange(MemTracker* mem_tracker, ChunkChanger& chunk_changer)
            : SchemaChange(mem_tracker), _chunk_changer(chunk_changer) {}
    ~LinkedSchemaChange() { _mem_tracker->release(_mem_tracker->consumption()); }

    bool process(vectorized::TabletReader* reader, RowsetWriter* new_rowset_writer, TabletSharedPtr new_tablet,
                 TabletSharedPtr base_tablet, RowsetSharedPtr rowset) override;

private:
    ChunkChanger& _chunk_changer;
    DISALLOW_COPY_AND_ASSIGN(LinkedSchemaChange);
};

// @brief schema change without sorting.
class SchemaChangeDirectly : public SchemaChange {
public:
    explicit SchemaChangeDirectly(MemTracker* mem_tracker, ChunkChanger& chunk_changer)
            : SchemaChange(mem_tracker), _chunk_changer(chunk_changer), _chunk_allocator(nullptr) {}
    virtual ~SchemaChangeDirectly() {
        SAFE_DELETE(_chunk_allocator);
        _mem_tracker->release(_mem_tracker->consumption());
    }

    bool process(vectorized::TabletReader* reader, RowsetWriter* new_rowset_writer, TabletSharedPtr new_tablet,
                 TabletSharedPtr base_tablet, RowsetSharedPtr rowset) override;

private:
    ChunkChanger& _chunk_changer;
    ChunkAllocator* _chunk_allocator;
    DISALLOW_COPY_AND_ASSIGN(SchemaChangeDirectly);
};

// @breif schema change with sorting
class SchemaChangeWithSorting : public SchemaChange {
public:
    explicit SchemaChangeWithSorting(MemTracker* mem_tracker, ChunkChanger& chunk_changer, size_t memory_limitation);
    virtual ~SchemaChangeWithSorting();

    bool process(vectorized::TabletReader* reader, RowsetWriter* new_rowset_writer, TabletSharedPtr new_tablet,
                 TabletSharedPtr base_tablet, RowsetSharedPtr rowset) override;

private:
    bool _internal_sorting(std::vector<ChunkPtr>& chunk_arr, RowsetWriter* new_rowset_writer, TabletSharedPtr tablet);

    ChunkChanger& _chunk_changer;
    size_t _memory_limitation;
    ChunkAllocator* _chunk_allocator;
    DISALLOW_COPY_AND_ASSIGN(SchemaChangeWithSorting);
};

class SchemaChangeHandler {
public:
    SchemaChangeHandler() {}
    virtual ~SchemaChangeHandler() {}

    // schema change v2, it will not set alter task in base tablet
    Status process_alter_tablet_v2(const TAlterTabletReqV2& request);

private:
    Status _get_versions_to_be_changed(TabletSharedPtr base_tablet, std::vector<Version>* versions_to_be_changed);

    struct AlterMaterializedViewParam {
        std::string column_name;
        std::string origin_column_name;
        std::string mv_expr;
    };

public:
    struct SchemaChangeParams {
        AlterTabletType alter_tablet_type;
        TabletSharedPtr base_tablet;
        TabletSharedPtr new_tablet;
        std::vector<vectorized::TabletReader*> rowset_readers;
        Version version;
        std::unordered_map<std::string, AlterMaterializedViewParam> materialized_params_map;
        std::vector<RowsetSharedPtr> rowsets_to_change;

        SchemaChangeParams() {}
    };

private:
    Status _do_process_alter_tablet_v2(const TAlterTabletReqV2& request);

    Status _do_process_alter_tablet_v2_normal(const TAlterTabletReqV2& request, const TabletSharedPtr& base_tablet,
                                              const TabletSharedPtr& new_tablet);

    Status _validate_alter_result(TabletSharedPtr new_tablet, const TAlterTabletReqV2& request);

    static Status _convert_historical_rowsets(SchemaChangeParams& sc_params);

    static Status _parse_request(
            TabletSharedPtr base_tablet, TabletSharedPtr new_tablet, ChunkChanger* chunk_changer, bool* sc_sorting,
            bool* sc_directly,
            const std::unordered_map<std::string, AlterMaterializedViewParam>& materialized_function_map);

    // default_value for new column is needed
    static Status _init_column_mapping(ColumnMapping* column_mapping, const TabletColumn& column_schema,
                                       const std::string& value);

private:
    RowsetReaderContext _reader_context;

    DISALLOW_COPY_AND_ASSIGN(SchemaChangeHandler);
};

} // namespace vectorized
} // namespace starrocks