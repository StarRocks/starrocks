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

#include "base/utility/arrow_utils.h"
#include "column/nullable_column.h"
#include "common/compiler_util.h"
#include "common/status.h"
#include "exec/arrow_to_starrocks_converter.h"
#include "exprs/function_helper.h"
#include "gutil/casts.h"
#include "util/arrow/row_batch.h"
#include "util/arrow/starrocks_column_to_arrow.h"

namespace starrocks {

struct SchemaNameGenerator {
    size_t id = 0;
    std::string generate() { return std::to_string(id); }
};

// Recursively build a ConvertFuncTree mirroring the Arrow result type.
// The Arrow schema returned by the Python worker is constructed from the same
// StarRocks return type via convert_to_arrow_type, so nested shapes match by
// construction — but we still validate for safety.
static Status build_arrow_to_sr_convert_tree(const arrow::DataType& arrow_type, const TypeDescriptor& sr_type,
                                             bool is_nullable, ConvertFuncTree* tree) {
    auto at = arrow_type.id();
    auto lt = sr_type.type;
    tree->func = get_arrow_converter(at, lt, is_nullable, /*is_strict=*/true);
    if (tree->func == nullptr) {
        return Status::NotSupported(fmt::format("unsupported arrow type {} to starrocks type {}", arrow_type.ToString(),
                                                type_to_string(lt)));
    }
    tree->children.clear();
    tree->field_names.clear();

    switch (lt) {
    case TYPE_ARRAY: {
        if (at != ArrowTypeId::LIST && at != ArrowTypeId::LARGE_LIST && at != ArrowTypeId::FIXED_SIZE_LIST) {
            return Status::InternalError(
                    fmt::format("Arrow type {} does not match StarRocks ARRAY", arrow_type.ToString()));
        }
        auto child = std::make_unique<ConvertFuncTree>();
        const auto& element_arrow_type = *arrow_type.field(0)->type();
        RETURN_IF_ERROR(build_arrow_to_sr_convert_tree(element_arrow_type, sr_type.children[0],
                                                       /*is_nullable=*/true, child.get()));
        tree->children.emplace_back(std::move(child));
        break;
    }
    case TYPE_MAP: {
        if (at != ArrowTypeId::MAP) {
            return Status::InternalError(
                    fmt::format("Arrow type {} does not match StarRocks MAP", arrow_type.ToString()));
        }
        const auto* map_type = down_cast<const arrow::MapType*>(&arrow_type);
        const arrow::DataType* sub_types[2] = {map_type->key_type().get(), map_type->item_type().get()};
        for (int i = 0; i < 2; ++i) {
            auto child = std::make_unique<ConvertFuncTree>();
            RETURN_IF_ERROR(build_arrow_to_sr_convert_tree(*sub_types[i], sr_type.children[i],
                                                           /*is_nullable=*/true, child.get()));
            tree->children.emplace_back(std::move(child));
        }
        break;
    }
    case TYPE_STRUCT: {
        if (at != ArrowTypeId::STRUCT) {
            return Status::InternalError(
                    fmt::format("Arrow type {} does not match StarRocks STRUCT", arrow_type.ToString()));
        }
        tree->field_names = sr_type.field_names;
        for (size_t i = 0; i < sr_type.children.size(); ++i) {
            auto child = std::make_unique<ConvertFuncTree>();
            // Arrow field order may not match StarRocks struct field order — look up by name.
            int idx = -1;
            for (int j = 0; j < arrow_type.num_fields(); ++j) {
                if (arrow_type.field(j)->name() == sr_type.field_names[i]) {
                    idx = j;
                    break;
                }
            }
            if (idx >= 0) {
                const auto& child_arrow_type = *arrow_type.field(idx)->type();
                RETURN_IF_ERROR(build_arrow_to_sr_convert_tree(child_arrow_type, sr_type.children[i],
                                                               /*is_nullable=*/true, child.get()));
            }
            // If the field is missing, leave the child tree empty — the StructGuard converter
            // appends nulls for that column.
            tree->children.emplace_back(std::move(child));
        }
        break;
    }
    default:
        break;
    }
    return Status::OK();
}

StatusOr<ColumnPtr> AbstractArrowFuncCallStub::evaluate(const Columns& columns, size_t row_num) {
    // convert to columns to arrow record batch
    RETURN_IF_DCHECK_EQ_FAILED(_func_ctx->get_num_args(), columns.size());

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
    const TypeDescriptor& return_type = _func_ctx->get_return_type();

    // Build a recursive converter tree so that ARRAY / MAP / STRUCT (and arbitrarily
    // nested combinations) dispatch into the correct child converters.
    ConvertFuncTree tree;
    RETURN_IF_ERROR(build_arrow_to_sr_convert_tree(*field_type.type(), return_type, field_type.nullable(), &tree));

    // UDF return result is always nullable
    auto native_column = FunctionHelper::create_column(return_type, true);
    auto nullable_column = down_cast<NullableColumn*>(native_column.get());
    auto null_column = nullable_column->null_column_raw_ptr();
    auto null_data = null_column->get_data().data();

    auto data_column = nullable_column->data_column_raw_ptr();

    native_column->reserve(result_num_rows);

    size_t null_count = fill_null_column(&result_ary, 0, result_num_rows, null_column, 0);
    nullable_column->set_has_null(null_count != 0);

    Filter filter;
    filter.resize(result_num_rows);
    // Pass nullptr ctx to match the legacy primitive path. Some primitive
    // converters (e.g. the BINARY/STRING specialization) check `ctx != nullptr`
    // but then unconditionally deref `ctx->current_slot`, so a stub context
    // with a null slot would crash. UDF flow has no SlotDescriptor and never
    // hits the timestamp / non-nullable branches that need a real ctx.
    auto status = tree.func(&result_ary, 0, result_num_rows, data_column, 0, null_data, &filter, nullptr, &tree);
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
