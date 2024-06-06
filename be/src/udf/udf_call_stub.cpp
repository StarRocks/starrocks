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

#include "udf/udf_call_stub.h"

#include <fmt/format.h>

#include <memory>
#include <string>
#include <utility>

#include "column/nullable_column.h"
#include "common/compiler_util.h"
#include "common/status.h"
#include "exec/arrow_to_starrocks_converter.h"
#include "gutil/casts.h"
#include "util/arrow/row_batch.h"
#include "util/arrow/starrocks_column_to_arrow.h"
#include "util/arrow/utils.h"

namespace starrocks {

struct SchemaNameGenerator {
    size_t id = 0;
    std::string generate() { return std::to_string(id); }
};

StatusOr<ColumnPtr> AbstractArrowFuncCallStub::evaluate(const Columns& columns, size_t row_num) {
    // convert to columns to arrow record batch
    DCHECK_EQ(_func_ctx->get_num_args(), columns.size());

    // build input schema
    SchemaNameGenerator name_generator;
    arrow::SchemaBuilder schema_builder;

    size_t num_columns = columns.size();
    const auto& arg_types = _func_ctx->get_arg_types();
    DCHECK_EQ(num_columns, arg_types.size());
    for (size_t i = 0; i < num_columns; ++i) {
        std::shared_ptr<arrow::Field> field;
        RETURN_IF_ERROR(
                convert_to_arrow_field(arg_types[i], name_generator.generate(), columns[i]->is_nullable(), &field));
        RETURN_IF_ERROR(to_status(schema_builder.AddField(field)));
    }

    std::shared_ptr<arrow::Schema> schema;
    auto result = schema_builder.Finish();
    RETURN_IF_ERROR(to_status(std::move(result).Value(&schema)));

    // convert column to arrow array
    std::shared_ptr<arrow::RecordBatch> record_batch;
    RETURN_IF_ERROR(convert_columns_to_arrow_batch(row_num, columns, arrow::default_memory_pool(),
                                                   _func_ctx->get_arg_types().data(), schema, &record_batch));

    ASSIGN_OR_RETURN(auto result_batch, do_evaluate(std::move(*record_batch)));

    // convert arrow result batch to starrocks column
    if (result_batch->num_columns() != 1) {
        return Status::InternalError(
                fmt::format("Invalid result batch expect 1 but found {}", result_batch->num_columns()));
    }
    auto awColArray = result_batch->column(0);
    const auto& f = result_batch->schema()->field(0);
    return _convert_arrow_to_native(*awColArray, *f);
}

StatusOr<ColumnPtr> AbstractArrowFuncCallStub::_convert_arrow_to_native(const arrowArray& result_ary,
                                                                        const arrowField& field_type) {
    size_t result_num_rows = result_ary.length();

    ConvertFunc converter = get_arrow_converter(field_type.type()->id(), _func_ctx->get_return_type().type,
                                                field_type.nullable(), true);
    if (UNLIKELY(converter == nullptr)) {
        return Status::NotSupported(fmt::format("unsupported arrow type {} to starrocks type {}",
                                                field_type.type()->ToString(), _func_ctx->get_return_type().type));
    }
    // UDF return result is always nullable
    auto native_column = _func_ctx->create_column(_func_ctx->get_return_type(), true);
    auto nullable_column = down_cast<NullableColumn*>(native_column.get());
    auto null_column = nullable_column->mutable_null_column();
    auto null_data = null_column->get_data().data();

    auto data_column = nullable_column->data_column().get();

    native_column->reserve(result_num_rows);

    size_t null_count = fill_null_column(&result_ary, 0, result_num_rows, null_column, 0);
    nullable_column->set_has_null(null_count != 0);

    Filter filter;
    filter.resize(result_num_rows);
    auto status = converter(&result_ary, 0, result_num_rows, data_column, 0, null_data, &filter, nullptr, nullptr);
    RETURN_IF_ERROR(status);
    nullable_column->update_has_null();

    return native_column;
}

class ErrorArrowFuncCallStub : public UDFCallStub {
public:
    ErrorArrowFuncCallStub(Status error_status) : _status(std::move(error_status)) {}
    StatusOr<ColumnPtr> evaluate(const Columns& columns, size_t row_num) override { return _status; }

private:
    Status _status;
};

std::unique_ptr<UDFCallStub> create_error_call_stub(Status error_status) {
    DCHECK(!error_status.ok());
    return std::make_unique<ErrorArrowFuncCallStub>(std::move(error_status));
}

} // namespace starrocks