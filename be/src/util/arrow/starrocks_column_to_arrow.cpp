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

#include "util/arrow/starrocks_column_to_arrow.h"

#include <fmt/format.h>

#include "column/arrow/column_to_arrow_converter.h"
#include "column/chunk.h"
#include "common/statusor.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "types/type_descriptor.h"

namespace starrocks {

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

Status convert_columns_to_arrow_batch(size_t num_rows, const Columns& columns, arrow::MemoryPool* pool,
                                      const TypeDescriptor* type_descs, const std::shared_ptr<arrow::Schema>& schema,
                                      std::shared_ptr<arrow::RecordBatch>* result) {
    size_t num_columns = columns.size();
    std::vector<std::shared_ptr<arrow::Array>> arrays(num_columns);

    for (size_t i = 0; i < num_columns; ++i) {
        RETURN_IF_ERROR(
                convert_column_to_arrow_array(columns[i], type_descs[i], schema->field(i)->type(), pool, &arrays[i]));
    }
    *result = arrow::RecordBatch::Make(schema, num_rows, std::move(arrays));
    return Status::OK();
}

// only used for UT test
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
