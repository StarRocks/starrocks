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

// This file contains code originally based on Apache Doris'
// be/src/util/arrow/row_batch.cpp and starrocks_column_to_arrow.cpp under the Apache License 2.0.

#include "exec/arrow/result_to_arrow_converter.h"

#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <fmt/format.h>

#include "column/arrow/column_to_arrow_converter.h"
#include "column/arrow/type_to_arrow_converter.h"
#include "column/chunk.h"
#include "common/logging.h"
#include "common/statusor.h"
#include "exprs/column_ref.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "gutil/casts.h"
#include "runtime/descriptors.h"

namespace starrocks {

namespace {

ColumnRef* find_first_column_ref(Expr* expr) {
    if (expr->is_slotref()) {
        return down_cast<ColumnRef*>(expr);
    }
    for (Expr* child : expr->children()) {
        if (ColumnRef* ref = find_first_column_ref(child); ref != nullptr) {
            return ref;
        }
    }
    return nullptr;
}

} // namespace

Status convert_to_arrow_schema(const RowDescriptor& row_desc,
                               const std::unordered_map<int64_t, std::string>& id_to_col_name,
                               std::shared_ptr<arrow::Schema>* result,
                               const std::vector<ExprContext*>& output_expr_ctxs,
                               const std::vector<std::string>* output_column_names, int32_t flight_sql_version) {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (size_t i = 0; i < output_expr_ctxs.size(); ++i) {
        const auto& expr_context = output_expr_ctxs[i];

        Expr* expr = expr_context->root();
        std::shared_ptr<arrow::Field> field;
        std::string col_name;
        ColumnRef* col_ref = find_first_column_ref(expr);
        DCHECK(col_ref != nullptr);
        int64_t slot_id = col_ref->slot_id();
        int64_t tuple_id = col_ref->tuple_id();
        int64_t id = tuple_id << 32 | slot_id;

        if (output_column_names != nullptr) {
            col_name = (*output_column_names)[i];
        } else if (auto it = id_to_col_name.find(id); it != id_to_col_name.end()) {
            col_name = it->second;
        } else {
            LOG(WARNING) << "Can't find the RefSlot in the row_desc.";
        }

        if (flight_sql_version <= 0) {
            RETURN_IF_ERROR(convert_to_arrow_field(expr->type(), col_name, expr->is_nullable(), &field));
        } else {
            RETURN_IF_ERROR(convert_to_arrow_field_for_flight_sql(expr->type(), col_name, expr->is_nullable(), &field,
                                                                  flight_sql_version));
        }

        fields.push_back(field);
    }
    *result = arrow::schema(std::move(fields));
    return Status::OK();
}

Status convert_chunk_to_arrow_batch(Chunk* chunk, std::vector<ExprContext*>& output_expr_ctxs,
                                    const std::shared_ptr<arrow::Schema>& schema, arrow::MemoryPool* pool,
                                    std::shared_ptr<arrow::RecordBatch>* result) {
    if (output_expr_ctxs.size() != schema->num_fields()) {
        return Status::InvalidArgument(fmt::format("arrow schema fields({}) do not match output expressions({})",
                                                   schema->num_fields(), output_expr_ctxs.size()));
    }

    const size_t num_result_cols = output_expr_ctxs.size();
    std::vector<std::shared_ptr<arrow::Array>> result_columns(num_result_cols);

    const size_t num_rows = chunk->num_rows();
    for (auto i = 0; i < num_result_cols; ++i) {
        ASSIGN_OR_RETURN(ColumnPtr column, output_expr_ctxs[i]->evaluate(chunk))

        const Expr* expr = output_expr_ctxs[i]->root();
        RETURN_IF_ERROR(convert_column_to_arrow_array(column, expr->type(), schema->field(i)->type(), pool,
                                                      &result_columns[i]));
    }

    *result = arrow::RecordBatch::Make(schema, num_rows, std::move(result_columns));

    return Status::OK();
}

Status convert_chunk_to_arrow_batch(Chunk* chunk, const std::vector<const TypeDescriptor*>& slot_types,
                                    const std::vector<SlotId>& slot_ids, const std::shared_ptr<arrow::Schema>& schema,
                                    arrow::MemoryPool* pool, std::shared_ptr<arrow::RecordBatch>* result) {
    if (chunk->num_columns() != schema->num_fields()) {
        return Status::InvalidArgument("number fields not match");
    }

    std::vector<std::shared_ptr<arrow::Array>> arrays(slot_types.size());

    size_t num_rows = chunk->num_rows();
    for (auto i = 0; i < slot_types.size(); ++i) {
        auto column = chunk->get_column_by_slot_id(slot_ids[i]);
        RETURN_IF_ERROR(
                convert_column_to_arrow_array(column, *slot_types[i], schema->field(i)->type(), pool, &arrays[i]));
    }
    *result = arrow::RecordBatch::Make(schema, num_rows, std::move(arrays));
    return Status::OK();
}

} // namespace starrocks
