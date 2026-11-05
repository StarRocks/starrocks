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

#include <gtest/gtest.h>

#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/file_column.h"
#include "column/nullable_column.h"
#include "column/struct_column.h"
#include "exprs/function_context.h"

namespace starrocks {
namespace {

std::unique_ptr<FunctionContext> make_context(const TypeDescriptor& argument) {
    return std::unique_ptr<FunctionContext>(
            FunctionContext::create_context(nullptr, nullptr, TypeDescriptor(TYPE_FILE), {argument}));
}

TypeDescriptor struct_type(std::vector<std::string> names, std::vector<TypeDescriptor> types) {
    TypeDescriptor type(TYPE_STRUCT);
    type.field_names = std::move(names);
    type.children = std::move(types);
    return type;
}

} // namespace

TEST(FileFunctionsTest, ReorderedFieldsAndBinaryPayload) {
    auto type = struct_type({"INLINE", "size", "URI", "checksum", "offset", "content_type"},
                            {TypeDescriptor(TYPE_VARBINARY), TypeDescriptor(TYPE_BIGINT), TypeDescriptor(TYPE_VARCHAR),
                             TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_BIGINT), TypeDescriptor(TYPE_VARCHAR)});
    auto input = ColumnHelper::create_column(type, false);
    std::string bytes("a\0b", 3);
    input->append_datum(Datum(DatumStruct{Datum(Slice(bytes)), Datum(int64_t(3)), Datum(), Datum(Slice("opaque")),
                                         Datum(int64_t(0)), Datum(Slice("arbitrary"))}));
    input->append_datum(Datum(DatumStruct{Datum(), Datum(std::numeric_limits<int64_t>::max()), Datum(Slice("not a URI")),
                                         Datum(), Datum(), Datum()}));
    auto context = make_context(type);
    ColumnPtr column = std::move(input);
    auto result = FileFunctions::to_file(context.get(), {column});
    ASSERT_TRUE(result.ok()) << result.status();
    ASSERT_TRUE(result.value()->is_file());
    auto actual = result.value()->get(0).get_struct();
    EXPECT_TRUE(actual[FileColumn::URI].is_null());
    EXPECT_EQ(0, actual[FileColumn::OFFSET].get_int64());
    EXPECT_EQ(3, actual[FileColumn::SIZE].get_int64());
    EXPECT_EQ("arbitrary", actual[FileColumn::CONTENT_TYPE].get_slice().to_string());
    EXPECT_EQ("opaque", actual[FileColumn::CHECKSUM].get_slice().to_string());
    EXPECT_EQ(bytes, actual[FileColumn::INLINE].get_slice().to_string());
    auto reference = result.value()->get(1).get_struct();
    EXPECT_EQ("not a URI", reference[FileColumn::URI].get_slice().to_string());
    EXPECT_EQ(std::numeric_limits<int64_t>::max(), reference[FileColumn::SIZE].get_int64());
    EXPECT_TRUE(reference[FileColumn::OFFSET].is_null());
    EXPECT_TRUE(reference[FileColumn::INLINE].is_null());

    const auto source_fields = down_cast<const StructColumn*>(column.get())->fields();
    const auto file_fields = down_cast<const FileColumn*>(result.value().get())->fields();
    EXPECT_EQ(source_fields[0].get(), file_fields[FileColumn::INLINE].get());
    EXPECT_EQ(source_fields[2].get(), file_fields[FileColumn::URI].get());
}

TEST(FileFunctionsTest, MissingFieldsAndOuterNulls) {
    auto type = struct_type({"uri", "inline", "offset"},
                            {TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_VARBINARY), TypeDescriptor(TYPE_BIGINT)});
    auto input = ColumnHelper::create_column(type, false);
    input->append_datum(Datum(DatumStruct{Datum(Slice("")), Datum(), Datum()}));
    // Invalid payloads beneath an outer NULL must not be validated.
    input->append_datum(Datum(DatumStruct{Datum(Slice("a")), Datum(Slice("bytes")), Datum(int64_t(-1))}));
    input->append_datum(Datum(DatumStruct{Datum(), Datum(), Datum()}));
    auto nulls = NullColumn::create();
    nulls->append(0);
    nulls->append(1);
    nulls->append(1);
    ColumnPtr column = NullableColumn::create(std::move(input), std::move(nulls));
    auto context = make_context(type);
    auto result = FileFunctions::to_file(context.get(), {column});
    ASSERT_TRUE(result.ok()) << result.status();
    ASSERT_EQ(3, result.value()->size());
    EXPECT_FALSE(result.value()->is_null(0));
    EXPECT_TRUE(result.value()->is_null(1));
    EXPECT_TRUE(result.value()->is_null(2));
    auto first = result.value()->get(0).get_struct();
    EXPECT_EQ("", first[FileColumn::URI].get_slice().to_string());
    for (size_t i = 1; i < FileColumn::NUM_FIELDS; ++i) {
        EXPECT_TRUE(first[i].is_null());
    }
    EXPECT_EQ(down_cast<const NullableColumn*>(column.get())->null_column().get(),
              down_cast<const NullableColumn*>(result.value().get())->null_column().get());
}

TEST(FileFunctionsTest, ConstantAndNullInputs) {
    auto type = struct_type({"inline"}, {TypeDescriptor(TYPE_VARBINARY)});
    auto input = ColumnHelper::create_column(type, false);
    input->append_datum(Datum(DatumStruct{Datum(Slice(""))}));
    ColumnPtr constant = ConstColumn::create(std::move(input), 7);
    auto context = make_context(type);
    auto result = FileFunctions::to_file(context.get(), {constant});
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_TRUE(result.value()->is_constant());
    EXPECT_EQ(7, result.value()->size());
    EXPECT_EQ(1, down_cast<const ConstColumn*>(result.value().get())->data_column()->size());
    EXPECT_FALSE(result.value()->get(6).get_struct()[FileColumn::INLINE].is_null());
    EXPECT_EQ("", result.value()->get(6).get_struct()[FileColumn::INLINE].get_slice().to_string());

    auto null_result = FileFunctions::to_file(context.get(), {ColumnHelper::create_const_null_column(7)});
    ASSERT_TRUE(null_result.ok()) << null_result.status();
    EXPECT_TRUE(null_result.value()->only_null());
    EXPECT_EQ(7, null_result.value()->size());

    auto nullable = ColumnHelper::create_column(type, true);
    nullable->append_nulls(3);
    auto typed_null_result = FileFunctions::to_file(context.get(), {std::move(nullable)});
    ASSERT_TRUE(typed_null_result.ok()) << typed_null_result.status();
    EXPECT_EQ(3, typed_null_result.value()->size());
    EXPECT_EQ(3, ColumnHelper::count_nulls(typed_null_result.value()));
}

TEST(FileFunctionsTest, NonNullableAndConstantFields) {
    auto type = struct_type({"uri", "inline", "offset"},
                            {TypeDescriptor(TYPE_CHAR), TypeDescriptor(TYPE_VARBINARY), TypeDescriptor(TYPE_BIGINT)});
    MutableColumns fields;
    fields.emplace_back(ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("a"), 3)->clone());
    fields.emplace_back(ColumnHelper::create_const_null_column(3));
    auto offsets = Int64Column::create();
    offsets->append(0);
    offsets->append(1);
    offsets->append(2);
    fields.emplace_back(std::move(offsets));
    ColumnPtr input = StructColumn::create(std::move(fields), type.field_names);
    const auto source_fields = down_cast<const StructColumn*>(input.get())->fields();
    auto context = make_context(type);
    auto result = FileFunctions::to_file(context.get(), {input});
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(3, result.value()->size());
    for (size_t row = 0; row < 3; ++row) {
        auto actual = result.value()->get(row).get_struct();
        EXPECT_EQ("a", actual[FileColumn::URI].get_slice().to_string());
        EXPECT_EQ(row, actual[FileColumn::OFFSET].get_int64());
        EXPECT_TRUE(actual[FileColumn::INLINE].is_null());
    }
    const auto file_fields = down_cast<const FileColumn*>(result.value().get())->fields();
    EXPECT_EQ(source_fields[2].get(),
              down_cast<const NullableColumn*>(file_fields[FileColumn::OFFSET].get())->data_column().get());
    EXPECT_TRUE(source_fields[0]->is_constant());
    EXPECT_EQ(3, source_fields[0]->size());
    EXPECT_EQ(1, down_cast<const ConstColumn*>(source_fields[0].get())->data_column()->size());
}

TEST(FileFunctionsTest, RejectInvalidValues) {
    auto type = struct_type({"uri", "inline", "offset", "size"},
                            {TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_VARBINARY), TypeDescriptor(TYPE_BIGINT),
                             TypeDescriptor(TYPE_BIGINT)});
    struct Case {
        DatumStruct row;
        std::string message;
    };
    const std::vector<Case> cases = {
            {{Datum(), Datum(), Datum(), Datum()}, "exactly one of 'uri' and 'inline'"},
            {{Datum(Slice("")), Datum(Slice("")), Datum(), Datum()}, "exactly one of 'uri' and 'inline'"},
            {{Datum(Slice("private-uri")), Datum(), Datum(int64_t(-1)), Datum()}, "'offset' must be >= 0"},
            {{Datum(), Datum(Slice("private-bytes")), Datum(), Datum(std::numeric_limits<int64_t>::min())},
             "'size' must be >= 0"}};
    auto context = make_context(type);
    for (const auto& test : cases) {
        for (bool constant : {false, true}) {
            auto input = ColumnHelper::create_column(type, false);
            if (!constant) {
                input->append_datum(Datum(DatumStruct{Datum(Slice("a")), Datum(), Datum(int64_t(0)), Datum(int64_t(0))}));
            }
            input->append_datum(Datum(test.row));
            ColumnPtr column = std::move(input);
            if (constant) {
                column = ConstColumn::create(std::move(column), 7);
            }
            auto result = FileFunctions::to_file(context.get(), {column});
            ASSERT_FALSE(result.ok());
            EXPECT_TRUE(result.status().is_invalid_argument());
            EXPECT_NE(std::string::npos, result.status().message().find(test.message));
            EXPECT_EQ(std::string::npos, result.status().message().find("private-"));
        }
    }

    auto metadata_type = struct_type({"content_type"}, {TypeDescriptor(TYPE_VARCHAR)});
    auto input = ColumnHelper::create_column(metadata_type, false);
    input->append_datum(Datum(DatumStruct{Datum(Slice("image/jpeg"))}));
    auto metadata_context = make_context(metadata_type);
    auto result = FileFunctions::to_file(metadata_context.get(), {std::move(input)});
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_invalid_argument());
}

TEST(FileFunctionsTest, CopyOnWriteIsolation) {
    auto type = struct_type({"uri", "size"}, {TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_BIGINT)});
    auto context = make_context(type);
    for (bool nullable : {false, true}) {
        for (bool mutate_input : {false, true}) {
            auto source = ColumnHelper::create_column(type, nullable);
            source->append_datum(Datum(DatumStruct{Datum(Slice("a")), Datum(int64_t(1))}));
            source->append_datum(Datum(DatumStruct{Datum(Slice("b")), Datum(int64_t(2))}));
            ColumnPtr input = std::move(source);
            auto result = FileFunctions::to_file(context.get(), {input});
            ASSERT_TRUE(result.ok()) << result.status();
            ColumnPtr output = std::move(result).value();
            const auto* input_data = down_cast<const StructColumn*>(ColumnHelper::get_data_column(input.get()));
            const auto* output_data = down_cast<const FileColumn*>(ColumnHelper::get_data_column(output.get()));
            EXPECT_EQ(input_data->fields()[0].get(), output_data->fields()[FileColumn::URI].get());
            EXPECT_EQ(input_data->fields()[1].get(), output_data->fields()[FileColumn::SIZE].get());
            if (nullable) {
                EXPECT_EQ(down_cast<const NullableColumn*>(input.get())->null_column().get(),
                          down_cast<const NullableColumn*>(output.get())->null_column().get());
            }
            if (mutate_input) {
                auto writable = Column::mutate(std::move(input));
                writable->append_datum(Datum(DatumStruct{Datum(Slice("c")), Datum(int64_t(3))}));
                if (nullable) {
                    auto* value = down_cast<NullableColumn*>(writable.get());
                    value->null_column_data()[0] = 1;
                    value->update_has_null();
                }
                EXPECT_EQ(2, output->size());
                EXPECT_FALSE(output->is_null(0));
                EXPECT_EQ("a", output->get(0).get_struct()[FileColumn::URI].get_slice().to_string());
                EXPECT_EQ(2, output->get(1).get_struct()[FileColumn::SIZE].get_int64());
            } else {
                auto writable = Column::mutate(std::move(output));
                writable->append_datum(Datum(DatumStruct{Datum(Slice("c")), Datum(), Datum(int64_t(3)),
                                                       Datum(), Datum(), Datum()}));
                if (nullable) {
                    auto* value = down_cast<NullableColumn*>(writable.get());
                    value->null_column_data()[0] = 1;
                    value->update_has_null();
                }
                EXPECT_EQ(2, input->size());
                EXPECT_FALSE(input->is_null(0));
                EXPECT_EQ("a", input->get(0).get_struct()[0].get_slice().to_string());
                EXPECT_EQ(2, input->get(1).get_struct()[1].get_int64());
            }
        }
    }
}

TEST(FileFunctionsTest, EmptyBatch) {
    auto type = struct_type({"uri"}, {TypeDescriptor(TYPE_VARCHAR)});
    auto context = make_context(type);
    auto result = FileFunctions::to_file(context.get(), {ColumnHelper::create_column(type, true)});
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(0, result.value()->size());
}

} // namespace starrocks
