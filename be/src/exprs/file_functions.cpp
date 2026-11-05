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

#include "exprs/file_functions.h"

#include <algorithm>
#include <array>
#include <string>

#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/const_column.h"
#include "column/file_column.h"
#include "column/nullable_column.h"
#include "column/struct_column.h"
#include "exprs/function_context.h"
#include "gutil/strings/ascii_ctype.h"

namespace starrocks {

namespace {

// Logical type of a FileColumn sub-column.
LogicalType file_field_type(size_t field_index) {
    switch (field_index) {
    case FileColumn::URI:
    case FileColumn::CONTENT_TYPE:
    case FileColumn::CHECKSUM:
        return TYPE_VARCHAR;
    case FileColumn::OFFSET:
    case FileColumn::SIZE:
        return TYPE_BIGINT;
    case FileColumn::INLINE:
        return TYPE_VARBINARY;
    default:
        DCHECK(false) << "unknown FILE field " << field_index;
        return TYPE_UNKNOWN;
    }
}

ColumnPtr normalize_file_field(const ColumnPtr& field, LogicalType type, size_t num_rows) {
    if (field == nullptr || field->only_null()) {
        auto result = ColumnHelper::create_column(TypeDescriptor(type), true);
        result->append_nulls(num_rows);
        return result;
    }
    if (field->is_constant()) {
        auto result = ColumnHelper::create_column(TypeDescriptor(type), true);
        const auto& values = down_cast<const ConstColumn*>(field.get())->data_column();
        result->append_value_multiple_times(*values, 0, num_rows);
        return result;
    }
    // The const-reference overload shares the data instead of acquiring a mutable copy.
    return NullableColumn::wrap_if_necessary(field);
}

// Skip outer NULL rows during validation, then merge them with the invalid-row bitmap.
template <bool NullOnInvalid>
StatusOr<NullColumnPtr> validate_file_fields(const std::array<ColumnPtr, FileColumn::NUM_FIELDS>& fields,
                                           const NullableColumn* outer_nullable, size_t num_rows) {
    const ColumnViewer<TYPE_BIGINT> offset(fields[FileColumn::OFFSET]);
    const ColumnViewer<TYPE_BIGINT> size(fields[FileColumn::SIZE]);
    NullColumn::MutablePtr invalid_nulls;
    for (size_t row = 0; row < num_rows; ++row) {
        if (outer_nullable != nullptr && outer_nullable->is_null(row)) {
            continue;
        }
        const char* error = nullptr;
        if (fields[FileColumn::URI]->is_null(row) == fields[FileColumn::INLINE]->is_null(row)) {
            error = "TO_FILE requires exactly one of 'uri' and 'inline' to be non-NULL";
        } else if (!offset.is_null(row) && offset.value(row) < 0) {
            error = "TO_FILE field 'offset' must be >= 0";
        } else if (!size.is_null(row) && size.value(row) < 0) {
            error = "TO_FILE field 'size' must be >= 0";
        }
        if (error == nullptr) {
            continue;
        }
        if constexpr (!NullOnInvalid) {
            return Status::InvalidArgument(error);
        } else {
            if (invalid_nulls == nullptr) {
                invalid_nulls = NullColumn::create(num_rows, 0);
            }
            invalid_nulls->get_data()[row] = 1;
        }
    }
    NullColumnPtr nulls = std::move(invalid_nulls);
    if (outer_nullable != nullptr) {
        if (nulls != nullptr) {
            nulls = FunctionHelper::union_null_column(nulls, outer_nullable->null_column());
        } else {
            nulls = outer_nullable->null_column();
        }
    }
    return nulls;
}

template <bool NullOnInvalid>
StatusOr<ColumnPtr> to_file_impl(FunctionContext* context, const Columns& columns) {
    const std::string function_name = NullOnInvalid ? "TRY_TO_FILE" : "TO_FILE";
    const auto& input = columns[0];
    if (input->only_null()) {
        return ColumnHelper::create_const_null_column(input->size());
    }

    const Column* input_data = input.get();
    if (input_data->is_constant()) {
        input_data = down_cast<const ConstColumn*>(input_data)->data_column().get();
    }
    const NullableColumn* nullable = nullptr;
    if (input_data->is_nullable()) {
        nullable = down_cast<const NullableColumn*>(input_data);
        input_data = nullable->data_column_raw_ptr();
    }
    const auto* type = context->get_arg_type(0);
    if (!input_data->is_struct() || type->type != TYPE_STRUCT) {
        return Status::InvalidArgument(function_name + " requires a STRUCT argument");
    }
    const auto* source = down_cast<const StructColumn*>(input_data);
    const auto source_fields = source->fields();
    if (type->field_names.size() != source_fields.size() || type->children.size() != type->field_names.size()) {
        return Status::InvalidArgument(function_name + " requires matching STRUCT fields and types");
    }
    std::array<int, FileColumn::NUM_FIELDS> mapping;
    mapping.fill(-1);
    for (size_t i = 0; i < type->field_names.size(); ++i) {
        std::string name = type->field_names[i];
        std::ranges::transform(name, name.begin(), [](unsigned char c) { return ascii_tolower(c); });
        const auto it = std::ranges::find(FileColumn::kFieldNames, name);
        if (it == FileColumn::kFieldNames.end()) {
            return Status::InvalidArgument(function_name + " has unknown field '" + name + "'");
        }
        const size_t index = it - FileColumn::kFieldNames.begin();
        if (mapping[index] != -1) {
            return Status::InvalidArgument(function_name + " has duplicate field '" + name + "'");
        }
        mapping[index] = i;
    }

    std::array<ColumnPtr, FileColumn::NUM_FIELDS> fields;
    for (size_t i = 0; i < fields.size(); ++i) {
        const LogicalType field_type = file_field_type(i);
        if (mapping[i] == -1) {
            fields[i] = normalize_file_field(nullptr, field_type, input_data->size());
        } else {
            // CHAR and VARCHAR match in FE without an explicit cast and share BinaryColumn storage.
            const auto source_type = type->children[mapping[i]].type;
            if (source_type != field_type && !(field_type == TYPE_VARCHAR && source_type == TYPE_CHAR)) {
                return Status::InvalidArgument(function_name + " field '" + std::string(FileColumn::kFieldNames[i]) +
                                               "' has an unexpected type");
            }
            fields[i] = normalize_file_field(source_fields[mapping[i]], field_type, input_data->size());
        }
    }

    ASSIGN_OR_RETURN(auto nulls, validate_file_fields<NullOnInvalid>(fields, nullable, input_data->size()));
    ColumnPtr result = FileColumn::create(std::move(fields));
    if (nulls != nullptr) {
        result = NullableColumn::create(result, nulls);
    }
    if (input->is_constant()) {
        result = ConstColumn::create(std::move(result), input->size());
    }
    return result;
}

// Project the field through the FILE's outer NULLs without copying its data.
template <FileColumn::FileField Field>
StatusOr<ColumnPtr> get_file_field(const Columns& columns) {
    const auto& input = columns[0];
    if (input->only_null()) {
        return ColumnHelper::create_const_null_column(input->size());
    }
    const auto physical = FunctionHelper::get_data_column_of_const(input);
    const auto* data = ColumnHelper::get_data_column(physical);
    if (!data->is_file()) {
        return Status::InvalidArgument("FILE getter requires a FILE argument");
    }
    ColumnPtr result = down_cast<const FileColumn*>(data)->fields()[Field];
    if (physical->has_null()) {
        auto nulls = FunctionHelper::union_nullable_column(physical, result);
        result = NullableColumn::create(FunctionHelper::get_data_column_of_nullable(result), std::move(nulls));
    }
    if (input->is_constant()) {
        result = ConstColumn::create(std::move(result), input->size());
    }
    return result;
}

} // namespace

StatusOr<ColumnPtr> FileFunctions::to_file(FunctionContext* context, const Columns& columns) {
    return to_file_impl<false>(context, columns);
}

StatusOr<ColumnPtr> FileFunctions::try_to_file(FunctionContext* context, const Columns& columns) {
    return to_file_impl<true>(context, columns);
}

StatusOr<ColumnPtr> FileFunctions::fl_get_uri(FunctionContext* context, const Columns& columns) {
    return get_file_field<FileColumn::URI>(columns);
}

StatusOr<ColumnPtr> FileFunctions::fl_get_offset(FunctionContext* context, const Columns& columns) {
    return get_file_field<FileColumn::OFFSET>(columns);
}

StatusOr<ColumnPtr> FileFunctions::fl_get_size(FunctionContext* context, const Columns& columns) {
    return get_file_field<FileColumn::SIZE>(columns);
}

StatusOr<ColumnPtr> FileFunctions::fl_get_content_type(FunctionContext* context, const Columns& columns) {
    return get_file_field<FileColumn::CONTENT_TYPE>(columns);
}

StatusOr<ColumnPtr> FileFunctions::fl_get_checksum(FunctionContext* context, const Columns& columns) {
    return get_file_field<FileColumn::CHECKSUM>(columns);
}

StatusOr<ColumnPtr> FileFunctions::fl_get_inline(FunctionContext* context, const Columns& columns) {
    return get_file_field<FileColumn::INLINE>(columns);
}

} // namespace starrocks

#include "gen_cpp/opcode/FileFunctions.inc"
