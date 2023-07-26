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

#include "common/statusor.h"
#include "exprs/expr.h"
#include "storage/column_mapping.h"
#include "storage/convert_helper.h"
#include "storage/tablet_meta.h"
#include "storage/tablet_schema.h"

namespace starrocks {

struct AlterMaterializedViewParam {
    std::string column_name;
    std::string origin_column_name;
    std::unique_ptr<TExpr> mv_expr;
};
using MaterializedViewParamMap = std::unordered_map<std::string, AlterMaterializedViewParam>;

class ChunkChanger {
public:
    ChunkChanger(const TabletSchema& tablet_schema);
    ~ChunkChanger();

    ColumnMapping* get_mutable_column_mapping(size_t column_index);

    const SchemaMapping& get_schema_mapping() const { return _schema_mapping; }

    const std::unordered_map<int32_t, int32_t>& get_slot_id_to_index_map() const { return _slot_id_to_index_map; }
    std::unordered_map<int32_t, int32_t>* get_mutable_slot_id_to_index_map() { return &_slot_id_to_index_map; }

    ObjectPool* get_object_pool() { return &_obj_pool; }

    RuntimeState* get_runtime_state() { return _state; }

    std::unordered_map<int, ExprContext*>* get_mc_exprs() { return &_mc_exprs; }

    bool change_chunk(ChunkPtr& base_chunk, ChunkPtr& new_chunk, const TabletMetaSharedPtr& base_tablet_meta,
                      const TabletMetaSharedPtr& new_tablet_meta, MemPool* mem_pool);

    bool change_chunk_v2(ChunkPtr& base_chunk, ChunkPtr& new_chunk, const Schema& base_schema, const Schema& new_schema,
                         MemPool* mem_pool);

    Status fill_materialized_columns(ChunkPtr& new_chunk);

    void init_runtime_state(TQueryOptions query_options, TQueryGlobals query_globals);

    Status append_materialized_columns(ChunkPtr& read_chunk, ChunkPtr& new_chunk,
                                       const std::vector<uint32_t>& all_ref_columns_ids, int base_schema_columns);

    const std::vector<ColumnId>& get_selected_column_indexes() const { return _selected_column_indexes; }
    std::vector<ColumnId>* get_mutable_selected_column_indexes() { return &_selected_column_indexes; }

    void set_has_mv_expr_context(bool has_mv_expr_context) { this->_has_mv_expr_context = has_mv_expr_context; }

    Status prepare();

private:
    // @brief column-mapping specification of new schema
    SchemaMapping _schema_mapping;

    std::vector<ColumnId> _selected_column_indexes;

    ObjectPool _obj_pool;
    RuntimeState* _state = nullptr;
    // columnId -> expr
    std::unordered_map<int, ExprContext*> _mc_exprs;

    bool _has_mv_expr_context{false};
    // base table's slot_id to index mapping
    std::unordered_map<int32_t, int32_t> _slot_id_to_index_map;

    DISALLOW_COPY(ChunkChanger);
};

class SchemaChangeUtils {
public:
    static void init_materialized_params(const TAlterTabletReqV2& request,
                                         MaterializedViewParamMap* materialized_view_param_map);

    static Status parse_request(const TabletSchema& base_schema, const TabletSchema& new_schema,
                                ChunkChanger* chunk_changer,
                                const MaterializedViewParamMap& materialized_view_param_map, bool has_delete_predicates,
                                bool* sc_sorting, bool* sc_directly, std::unordered_set<int>* materialized_column_idxs);

private:
    // default_value for new column is needed
    static Status init_column_mapping(ColumnMapping* column_mapping, const TabletColumn& column_schema,
                                      const std::string& value);
};

} // namespace starrocks
