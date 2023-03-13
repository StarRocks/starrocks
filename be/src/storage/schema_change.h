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
#include "storage/delete_handler.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/schema_change_utils.h"
#include "storage/tablet.h"
#include "storage/tablet_reader.h"
#include "storage/tablet_reader_params.h"

namespace starrocks {
class Field;
class Tablet;

class ChunkSorter {
public:
    ChunkSorter() = default;
    virtual ~ChunkSorter() = default;

    bool sort(ChunkPtr& chunk, const TabletSharedPtr& new_tablet);

private:
    ChunkPtr _swap_chunk = nullptr;
    size_t _max_allocated_rows = 0;
};

class SchemaChange {
public:
    SchemaChange() = default;
    virtual ~SchemaChange() = default;

    virtual Status process(TabletReader* reader, RowsetWriter* new_rowset_writer, TabletSharedPtr tablet,
                           TabletSharedPtr base_tablet, RowsetSharedPtr rowset) = 0;
};

class LinkedSchemaChange : public SchemaChange {
public:
    explicit LinkedSchemaChange(ChunkChanger* chunk_changer) : SchemaChange(), _chunk_changer(chunk_changer) {}
    ~LinkedSchemaChange() override = default;

    Status process(TabletReader* reader, RowsetWriter* new_rowset_writer, TabletSharedPtr new_tablet,
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

    Status process(TabletReader* reader, RowsetWriter* new_rowset_writer, TabletSharedPtr new_tablet,
                   TabletSharedPtr base_tablet, RowsetSharedPtr rowset) override;

private:
    ChunkChanger* _chunk_changer = nullptr;
    DISALLOW_COPY(SchemaChangeDirectly);
};

// @breif schema change with sorting
class SchemaChangeWithSorting : public SchemaChange {
public:
    explicit SchemaChangeWithSorting(ChunkChanger* chunk_changer, size_t memory_limitation);
    ~SchemaChangeWithSorting() override = default;

    Status process(TabletReader* reader, RowsetWriter* new_rowset_writer, TabletSharedPtr new_tablet,
                   TabletSharedPtr base_tablet, RowsetSharedPtr rowset) override;

    static bool _internal_sorting(std::vector<ChunkPtr>& chunk_arr, RowsetWriter* new_rowset_writer,
                                  TabletSharedPtr tablet);

private:
    ChunkChanger* _chunk_changer = nullptr;
    size_t _memory_limitation;
    DISALLOW_COPY(SchemaChangeWithSorting);
};

class SchemaChangeHandler {
public:
    SchemaChangeHandler() = default;
    ~SchemaChangeHandler() = default;

    // schema change v2, it will not set alter task in base tablet
    Status process_alter_tablet_v2(const TAlterTabletReqV2& request);

private:
    struct SchemaChangeParams {
        TabletSharedPtr base_tablet;
        TabletSharedPtr new_tablet;
        std::vector<std::unique_ptr<TabletReader>> rowset_readers;
        Version version;
        MaterializedViewParamMap materialized_params_map;
        std::vector<RowsetSharedPtr> rowsets_to_change;
        bool sc_sorting = false;
        bool sc_directly = false;
        std::unique_ptr<ChunkChanger> chunk_changer = nullptr;
    };

    static Status _get_versions_to_be_changed(const TabletSharedPtr& base_tablet,
                                              std::vector<Version>* versions_to_be_changed);

    Status _do_process_alter_tablet_v2(const TAlterTabletReqV2& request);

    Status _do_process_alter_tablet_v2_normal(const TAlterTabletReqV2& request, SchemaChangeParams& sc_params,
                                              const TabletSharedPtr& base_tablet, const TabletSharedPtr& new_tablet);

    Status _validate_alter_result(const TabletSharedPtr& new_tablet, const TAlterTabletReqV2& request);

    static Status _convert_historical_rowsets(SchemaChangeParams& sc_params);

    DISALLOW_COPY(SchemaChangeHandler);
};

} // namespace starrocks
