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
#include "storage/tablet.h"
#include "storage/tablet_meta.h"
#include "storage/tablet_reader.h"
#include "storage/tablet_reader_params.h"
#include "storage/tablet_schema.h"

namespace starrocks {

class ChunkChanger;

struct AlterMaterializedViewParam {
    std::string column_name;
    std::string origin_column_name;
    std::unique_ptr<TExpr> mv_expr;
};
using MaterializedViewParamMap = std::unordered_map<std::string, AlterMaterializedViewParam>;

struct SchemaChangeParams {
    TabletSharedPtr base_tablet;
    TabletSharedPtr new_tablet;
    std::vector<std::unique_ptr<TabletReader>> rowset_readers;
    Version version;
    TabletSchemaCSPtr base_tablet_schema = nullptr;
    std::vector<RowsetSharedPtr> rowsets_to_change;
    bool sc_sorting = false;
    bool sc_directly = false;
    std::unique_ptr<ChunkChanger> chunk_changer = nullptr;

    TAlterJobType::type alter_job_type;

    // materialzied view parameters
    DescriptorTbl* desc_tbl = nullptr;
    std::unique_ptr<TExpr> where_expr;
    std::vector<std::string> base_table_column_names;
    MaterializedViewParamMap materialized_params_map;
};

class ChunkChanger {
public:
    ChunkChanger(const TabletSchemaCSPtr& base_schema, const TabletSchemaCSPtr& new_schema,
                 std::vector<std::string>& base_table_column_names, TAlterJobType::type alter_job_type);
    ChunkChanger(const TabletSchemaCSPtr& new_schema);
    ~ChunkChanger();

    ColumnMapping* get_mutable_column_mapping(size_t column_index);

    Status prepare_where_expr(const TExpr& where_expr) {
        VLOG(2) << "parse contain where expr";
        RETURN_IF_ERROR(Expr::create_expr_tree(&_obj_pool, where_expr, &_where_expr, _state));
        RETURN_IF_ERROR(_where_expr->prepare(_state));
        RETURN_IF_ERROR(_where_expr->open(_state));
        return Status::OK();
    }
    ExprContext* get_where_expr() { return _where_expr; }

    const SchemaMapping& get_schema_mapping() const { return _schema_mapping; }

    ObjectPool* get_object_pool() { return &_obj_pool; }

    RuntimeState* get_runtime_state() { return _state; }

    std::unordered_map<int, ExprContext*>* get_gc_exprs() { return &_gc_exprs; }

    bool change_chunk_v2(ChunkPtr& base_chunk, ChunkPtr& new_chunk, const Schema& base_schema, const Schema& new_schema,
                         MemPool* mem_pool);

    Status fill_generated_columns(ChunkPtr& new_chunk);

    void init_runtime_state(const TQueryOptions& query_options, const TQueryGlobals& query_globals);

    Status append_generated_columns(ChunkPtr& read_chunk, ChunkPtr& new_chunk,
                                    const std::vector<uint32_t>& all_ref_columns_ids, int base_schema_columns);

    const std::vector<ColumnId>& get_selected_column_indexes() const { return _selected_column_indexes; }
    std::vector<ColumnId>* get_mutable_selected_column_indexes() { return &_selected_column_indexes; }

    Status prepare();

    void set_query_slots(DescriptorTbl* desc_tbl) {
        std::vector<TupleDescriptor*> tuples;
        desc_tbl->get_tuple_descs(&tuples);
        DCHECK_LE(0, tuples.size());
        _query_slots = tuples[0]->slots();
    }

private:
    Buffer<uint8_t> _execute_where_expr(ChunkPtr& chunk);

private:
    TabletSchemaCSPtr _base_schema;
    std::vector<std::string> _base_table_column_names;
    TAlterJobType::type _alter_job_type = TAlterJobType::SCHEMA_CHANGE;

    // @brief column-mapping specification of new schema
    SchemaMapping _schema_mapping;

    std::vector<ColumnId> _selected_column_indexes;

    ObjectPool _obj_pool;
    RuntimeState* _state = nullptr;
    // columnId -> expr
    std::unordered_map<int, ExprContext*> _gc_exprs;

    // slot descriptors for each one of |_scanner_columns|.
    std::vector<SlotDescriptor*> _query_slots;
    ExprContext* _where_expr = nullptr;
    // column_ref based on base schema -> real column idx based on pruned columns
    // eg: base table schema        : a, b, c, d, e
    //     select column indexes    : 1, 0, 2
    // then this column ref mapping:
    //                              0 -> 1 (a is in 1th slot of fetched chunk)
    //                              1 -> 0 (b is in 0th slot of fetched chunk)
    //                              2 -> 2 (c is in 2th slot of fetched chunk)
    std::unordered_map<int, int> _column_ref_mapping;

    DISALLOW_COPY(ChunkChanger);
};

class SchemaChangeUtils {
public:
    static void init_materialized_params(const TAlterTabletReqV2& request,
                                         MaterializedViewParamMap* materialized_view_param_map,
                                         std::unique_ptr<TExpr>& where_expr);

    static Status parse_request(const TabletSchemaCSPtr& base_schema, const TabletSchemaCSPtr& new_schema,
                                ChunkChanger* chunk_changer,
                                const MaterializedViewParamMap& materialized_view_param_map,
                                const std::unique_ptr<TExpr>& where_expr, bool has_delete_predicates, bool* sc_sorting,
                                bool* sc_directly, std::unordered_set<int>* materialized_column_idxs);

private:
    // default_value for new column is needed
    static Status init_column_mapping(ColumnMapping* column_mapping, const TabletColumn& column_schema,
                                      const std::string& value);

    static Status parse_request_normal(const TabletSchemaCSPtr& base_schema, const TabletSchemaCSPtr& new_schema,
                                       ChunkChanger* chunk_changer,
                                       const MaterializedViewParamMap& materialized_view_param_map,
                                       const std::unique_ptr<TExpr>& where_expr, bool has_delete_predicates,
                                       bool* sc_sorting, bool* sc_directly,
                                       std::unordered_set<int>* materialized_column_idxs);

    static Status parse_request_for_sort_key(const TabletSchemaCSPtr& base_schema, const TabletSchemaCSPtr& new_schema,
                                             bool* sc_sorting, bool* sc_directly);
};

} // namespace starrocks
