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

#include <aws/core/Aws.h>
#include <gtest/gtest.h>

#include <array>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/file_column.h"
#include "column/nullable_column.h"
#include "column/struct_column.h"
#include "exprs/function_context.h"
#include "fs/fs_s3.h"
#include "gen_cpp/CloudConfiguration_types.h"

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

using FileFunction = StatusOr<ColumnPtr> (*)(FunctionContext*, const Columns&);
constexpr std::array<FileFunction, FileColumn::NUM_FIELDS> kGetters = {
        FileFunctions::fl_get_uri,          FileFunctions::fl_get_offset,   FileFunctions::fl_get_size,
        FileFunctions::fl_get_content_type, FileFunctions::fl_get_checksum, FileFunctions::fl_get_inline};
constexpr std::array<LogicalType, FileColumn::NUM_FIELDS> kFieldTypes = {TYPE_VARCHAR, TYPE_BIGINT,  TYPE_BIGINT,
                                                                         TYPE_VARCHAR, TYPE_VARCHAR, TYPE_VARBINARY};

} // namespace

TEST(FileFunctionsTest, ReorderedFieldsAndBinaryPayload) {
    auto type = struct_type({"INLINE", "size", "URI", "checksum", "offset", "content_type"},
                            {TypeDescriptor(TYPE_VARBINARY), TypeDescriptor(TYPE_BIGINT), TypeDescriptor(TYPE_VARCHAR),
                             TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_BIGINT), TypeDescriptor(TYPE_VARCHAR)});
    auto input = ColumnHelper::create_column(type, false);
    std::string bytes("a\0b", 3);
    input->append_datum(Datum(DatumStruct{Datum(Slice(bytes)), Datum(), Datum(), Datum(Slice("opaque")), Datum(),
                                          Datum(Slice("arbitrary"))}));
    input->append_datum(Datum(DatumStruct{Datum(), Datum(std::numeric_limits<int64_t>::max()),
                                          Datum(Slice("not a URI")), Datum(), Datum(), Datum()}));
    auto context = make_context(type);
    ColumnPtr column = std::move(input);
    auto result = FileFunctions::to_file(context.get(), {column});
    ASSERT_TRUE(result.ok()) << result.status();
    ASSERT_TRUE(result.value()->is_file());
    auto actual = result.value()->get(0).get_struct();
    EXPECT_TRUE(actual[FileColumn::URI].is_null());
    EXPECT_TRUE(actual[FileColumn::OFFSET].is_null());
    EXPECT_TRUE(actual[FileColumn::SIZE].is_null());
    EXPECT_EQ("arbitrary", actual[FileColumn::CONTENT_TYPE].get_slice().to_string());
    EXPECT_EQ("opaque", actual[FileColumn::CHECKSUM].get_slice().to_string());
    EXPECT_EQ(bytes, actual[FileColumn::INLINE].get_slice().to_string());
    auto reference = result.value()->get(1).get_struct();
    EXPECT_EQ("not a URI", reference[FileColumn::URI].get_slice().to_string());
    EXPECT_EQ(std::numeric_limits<int64_t>::max(), reference[FileColumn::SIZE].get_int64());
    EXPECT_TRUE(reference[FileColumn::OFFSET].is_null());
    EXPECT_TRUE(reference[FileColumn::INLINE].is_null());
}

TEST(FileFunctionsTest, MissingFieldsAndOuterNulls) {
    auto type = struct_type({"uri", "inline", "offset"}, {TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_VARBINARY),
                                                          TypeDescriptor(TYPE_BIGINT)});
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
            {{Datum(Slice("private-uri")), Datum(), Datum(), Datum(std::numeric_limits<int64_t>::min())},
             "'size' must be >= 0"},
            {{Datum(), Datum(Slice("private-bytes")), Datum(int64_t(0)), Datum()},
             "'offset' and 'size' to be NULL for inline content"},
            {{Datum(), Datum(Slice("private-bytes")), Datum(), Datum(int64_t(3))},
             "'offset' and 'size' to be NULL for inline content"}};
    auto context = make_context(type);
    for (const auto& test : cases) {
        for (bool constant : {false, true}) {
            auto input = ColumnHelper::create_column(type, false);
            if (!constant) {
                input->append_datum(
                        Datum(DatumStruct{Datum(Slice("a")), Datum(), Datum(int64_t(0)), Datum(int64_t(0))}));
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
            auto tolerant = FileFunctions::try_to_file(context.get(), {column});
            ASSERT_TRUE(tolerant.ok()) << tolerant.status();
            EXPECT_EQ(column->size(), tolerant.value()->size());
            if (constant) {
                EXPECT_TRUE(tolerant.value()->is_constant());
                EXPECT_TRUE(tolerant.value()->is_null(6));
                EXPECT_EQ(1, down_cast<const ConstColumn*>(tolerant.value().get())->data_column()->size());
            } else {
                EXPECT_FALSE(tolerant.value()->is_null(0));
                EXPECT_TRUE(tolerant.value()->is_null(1));
                EXPECT_EQ("a", tolerant.value()->get(0).get_struct()[FileColumn::URI].get_slice().to_string());
            }
        }
    }
}

class FileConstructorIsolationTest : public testing::TestWithParam<FileFunction> {};

TEST_P(FileConstructorIsolationTest, FilterSourceAndResultTogether) {
    auto type = struct_type({"uri", "size"}, {TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_BIGINT)});
    auto context = make_context(type);
    for (bool nullable : {false, true}) {
        for (bool output_first : {false, true}) {
            auto source = ColumnHelper::create_column(type, nullable);
            source->append_datum(Datum(DatumStruct{Datum(Slice("a")), Datum(int64_t(1))}));
            if (nullable) {
                source->append_nulls(1);
            } else {
                source->append_datum(Datum(DatumStruct{Datum(Slice("b")), Datum(int64_t(2))}));
            }
            source->append_datum(Datum(DatumStruct{Datum(Slice("c")), Datum(int64_t(3))}));
            source->append_datum(Datum(DatumStruct{Datum(Slice("d")), Datum(int64_t(4))}));
            ColumnPtr input = std::move(source);
            auto result = GetParam()(context.get(), {input});
            ASSERT_TRUE(result.ok()) << result.status();
            ColumnPtr output = std::move(result).value();
            const auto* input_data = down_cast<const StructColumn*>(ColumnHelper::get_data_column(input.get()));
            const auto* output_data = down_cast<const FileColumn*>(ColumnHelper::get_data_column(output.get()));
            EXPECT_NE(input_data->fields()[0].get(), output_data->fields()[FileColumn::URI].get());
            EXPECT_NE(input_data->fields()[1].get(), output_data->fields()[FileColumn::SIZE].get());
            if (nullable) {
                EXPECT_NE(down_cast<const NullableColumn*>(input.get())->null_column().get(),
                          down_cast<const NullableColumn*>(output.get())->null_column().get());
            }
            const std::array<std::string, 3> expected_input = {input->debug_item(1), input->debug_item(2),
                                                               input->debug_item(3)};
            const std::array<std::string, 3> expected_output = {output->debug_item(1), output->debug_item(2),
                                                                output->debug_item(3)};
            Chunk chunk;
            chunk.append_column(output_first ? output : input, 0);
            chunk.append_column(output_first ? input : output, 1);
            // Exercise the actual raw-mutation consumer, without calling Column::mutate() in the test.
            Filter filter = {0, 1, 1, 1};
            ASSERT_EQ(3, chunk.filter(filter));
            ASSERT_EQ(3, input->size());
            ASSERT_EQ(3, output->size());
            input->check_or_die();
            output->check_or_die();
            for (size_t row = 0; row < 3; ++row) {
                EXPECT_EQ(expected_input[row], input->debug_item(row));
                EXPECT_EQ(expected_output[row], output->debug_item(row));
            }
        }
    }
}

INSTANTIATE_TEST_SUITE_P(FileFunctions, FileConstructorIsolationTest,
                         testing::Values(FileFunctions::to_file, FileFunctions::try_to_file));

TEST(FileFunctionsTest, EmptyBatch) {
    auto type = struct_type({"uri"}, {TypeDescriptor(TYPE_VARCHAR)});
    auto context = make_context(type);
    auto result = FileFunctions::to_file(context.get(), {ColumnHelper::create_column(type, true)});
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(0, result.value()->size());
}

TEST(FileFunctionsTest, GettersIsolateFieldsAndMergeOuterNulls) {
    const std::string bytes("a\0b", 3);
    for (bool outer_null : {false, true}) {
        auto file = FileColumn::create();
        // Populate every field to test raw projection independently of constructor validation.
        file->append_datum(Datum(DatumStruct{Datum(Slice("uri")), Datum(int64_t(7)), Datum(int64_t(3)),
                                             Datum(Slice("image/jpeg")), Datum(Slice("opaque")), Datum(Slice(bytes))}));
        file->append_default();
        file->append_datum(Datum(DatumStruct{Datum(Slice("")), Datum(int64_t(0)), Datum(int64_t(0)), Datum(Slice("")),
                                             Datum(Slice("")), Datum(Slice(""))}));
        const auto fields = file->fields();
        ColumnPtr input = std::move(file);
        if (outer_null) {
            auto nulls = NullColumn::create(3, 0);
            nulls->get_data()[0] = 1;
            input = NullableColumn::create(input, std::move(nulls));
        }
        Chunk chunk;
        chunk.append_column(input, 0);
        for (size_t i = 0; i < kGetters.size(); ++i) {
            SCOPED_TRACE(i);
            auto context = std::unique_ptr<FunctionContext>(FunctionContext::create_context(
                    nullptr, nullptr, TypeDescriptor(kFieldTypes[i]), {TypeDescriptor(TYPE_FILE)}));
            auto result = kGetters[i](context.get(), {input});
            ASSERT_TRUE(result.ok()) << result.status();
            EXPECT_EQ(3, result.value()->size());
            const auto* projected = down_cast<const NullableColumn*>(result.value().get());
            const auto* field = down_cast<const NullableColumn*>(fields[i].get());
            EXPECT_NE(field->data_column().get(), projected->data_column().get());
            if (outer_null) {
                EXPECT_TRUE(result.value()->is_null(0));
                EXPECT_FALSE(fields[i]->is_null(0));
                EXPECT_NE(field->null_column().get(), projected->null_column().get());
            } else {
                EXPECT_NE(fields[i].get(), result.value().get());
                if (kFieldTypes[i] == TYPE_BIGINT) {
                    EXPECT_EQ(i == FileColumn::OFFSET ? 7 : 3, result.value()->get(0).get_int64());
                } else {
                    EXPECT_EQ(fields[i]->get(0).get_slice().to_string(),
                              result.value()->get(0).get_slice().to_string());
                }
            }
            EXPECT_TRUE(result.value()->is_null(1));
            EXPECT_FALSE(result.value()->is_null(2));
            if (kFieldTypes[i] == TYPE_BIGINT) {
                EXPECT_EQ(0, result.value()->get(2).get_int64());
            } else {
                EXPECT_EQ("", result.value()->get(2).get_slice().to_string());
            }
            chunk.append_column(result.value(), static_cast<SlotId>(i + 1));
        }
        std::vector<std::array<std::string, 2>> expected;
        for (const auto& column : chunk.columns()) {
            expected.push_back({column->debug_item(0), column->debug_item(2)});
        }
        Filter filter = {1, 0, 1};
        ASSERT_EQ(2, chunk.filter(filter));
        for (size_t i = 0; i < chunk.columns().size(); ++i) {
            const auto& column = chunk.columns()[i];
            ASSERT_EQ(2, column->size());
            column->check_or_die();
            EXPECT_EQ(expected[i][0], column->debug_item(0));
            EXPECT_EQ(expected[i][1], column->debug_item(1));
        }
    }
}

TEST(FileFunctionsTest, GettersConstantNullAndEmptyInputs) {
    for (size_t i = 0; i < kGetters.size(); ++i) {
        auto context = std::unique_ptr<FunctionContext>(FunctionContext::create_context(
                nullptr, nullptr, TypeDescriptor(kFieldTypes[i]), {TypeDescriptor(TYPE_FILE)}));
        auto file = FileColumn::create();
        file->append_datum(Datum(DatumStruct{Datum(Slice("a")), Datum(int64_t(0)), Datum(int64_t(2)), Datum(), Datum(),
                                             Datum(Slice("ab"))}));
        ColumnPtr constant = ConstColumn::create(std::move(file), 7);
        auto result = kGetters[i](context.get(), {constant});
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value()->is_constant());
        EXPECT_EQ(7, result.value()->size());
        EXPECT_EQ(1, down_cast<const ConstColumn*>(result.value().get())->data_column()->size());
        EXPECT_EQ(i == FileColumn::CONTENT_TYPE || i == FileColumn::CHECKSUM, result.value()->is_null(6));

        auto null_result = kGetters[i](context.get(), {ColumnHelper::create_const_null_column(7)});
        ASSERT_TRUE(null_result.ok()) << null_result.status();
        EXPECT_EQ(7, null_result.value()->size());
        EXPECT_EQ(7, ColumnHelper::count_nulls(null_result.value()));
        for (bool nullable : {false, true}) {
            auto empty = kGetters[i](context.get(), {ColumnHelper::create_column(TypeDescriptor(TYPE_FILE), nullable)});
            ASSERT_TRUE(empty.ok()) << empty.status();
            EXPECT_EQ(0, empty.value()->size());
        }
        auto typed_null = ColumnHelper::create_column(TypeDescriptor(TYPE_FILE), true);
        typed_null->append_nulls(3);
        auto all_null = kGetters[i](context.get(), {std::move(typed_null)});
        ASSERT_TRUE(all_null.ok()) << all_null.status();
        EXPECT_EQ(3, ColumnHelper::count_nulls(all_null.value()));
    }
}

TEST(FileFunctionsTest, TryToFileMixedRowsAndOuterNulls) {
    auto type = struct_type({"SIZE", "inline", "uri", "offset"},
                            {TypeDescriptor(TYPE_BIGINT), TypeDescriptor(TYPE_VARBINARY), TypeDescriptor(TYPE_VARCHAR),
                             TypeDescriptor(TYPE_BIGINT)});
    auto source = ColumnHelper::create_column(type, false);
    const std::vector<DatumStruct> rows = {{Datum(int64_t(0)), Datum(), Datum(Slice("")), Datum(int64_t(0))},
                                           {Datum(), Datum(Slice("")), Datum(), Datum()},
                                           {Datum(), Datum(), Datum(), Datum()},
                                           {Datum(), Datum(Slice("bytes")), Datum(Slice("uri")), Datum()},
                                           {Datum(int64_t(-1)), Datum(), Datum(Slice("uri")), Datum()},
                                           {Datum(), Datum(), Datum(Slice("uri")), Datum(int64_t(-1))},
                                           {Datum(), Datum(), Datum(Slice("hidden")), Datum()}};
    for (const auto& row : rows) {
        source->append_datum(Datum(row));
    }
    auto nulls = NullColumn::create(rows.size(), 0);
    nulls->get_data()[6] = 1;
    ColumnPtr input = NullableColumn::create(std::move(source), std::move(nulls));
    auto context = make_context(type);
    auto result = FileFunctions::try_to_file(context.get(), {input});
    ASSERT_TRUE(result.ok()) << result.status();
    ASSERT_EQ(rows.size(), result.value()->size());
    const auto* output = down_cast<const NullableColumn*>(result.value().get());
    const auto* original = down_cast<const NullableColumn*>(input.get());
    // Invalid rows get a new bitmap instead of writing into the input's.
    EXPECT_NE(original->null_column().get(), output->null_column().get());
    for (size_t row = 0; row < rows.size(); ++row) {
        EXPECT_EQ(row >= 2, result.value()->is_null(row));
        EXPECT_EQ(row == 6, input->is_null(row));
        if (row < 2) {
            auto valid = ColumnHelper::create_column(type, false);
            valid->append_datum(Datum(rows[row]));
            auto strict = FileFunctions::to_file(context.get(), {std::move(valid)});
            ASSERT_TRUE(strict.ok()) << strict.status();
            EXPECT_EQ(strict.value()->debug_item(0), result.value()->debug_item(row));
        }
    }
    // Getter must respect the NULL introduced by TRY, even when the field itself has a value.
    auto uris = FileFunctions::fl_get_uri(nullptr, {result.value()});
    ASSERT_TRUE(uris.ok()) << uris.status();
    EXPECT_TRUE(uris.value()->is_null(4));
    EXPECT_TRUE(uris.value()->is_null(5));
    EXPECT_TRUE(uris.value()->is_null(6));
}

TEST(FileFunctionsTest, TryToFileNullConstantAndEmptyInputs) {
    auto type = struct_type({"uri"}, {TypeDescriptor(TYPE_VARCHAR)});
    auto source = ColumnHelper::create_column(type, true);
    source->append_datum(Datum(DatumStruct{Datum(Slice("a"))}));
    source->append_nulls(1);
    ColumnPtr input = std::move(source);
    auto context = make_context(type);
    auto result = FileFunctions::try_to_file(context.get(), {input});
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_FALSE(result.value()->is_null(0));
    EXPECT_TRUE(result.value()->is_null(1));
    auto null_result = FileFunctions::try_to_file(context.get(), {ColumnHelper::create_const_null_column(7)});
    ASSERT_TRUE(null_result.ok()) << null_result.status();
    EXPECT_EQ(7, ColumnHelper::count_nulls(null_result.value()));
    auto one_row = ColumnHelper::create_column(type, false);
    one_row->append_datum(Datum(DatumStruct{Datum(Slice("a"))}));
    ColumnPtr constant = ConstColumn::create(std::move(one_row), 7);
    auto constant_result = FileFunctions::try_to_file(context.get(), {constant});
    ASSERT_TRUE(constant_result.ok()) << constant_result.status();
    EXPECT_TRUE(constant_result.value()->is_constant());
    EXPECT_EQ(7, constant_result.value()->size());
    EXPECT_EQ("a", constant_result.value()->get(6).get_struct()[FileColumn::URI].get_slice().to_string());
    auto typed_null = ColumnHelper::create_column(type, true);
    typed_null->append_nulls(3);
    auto all_null = FileFunctions::try_to_file(context.get(), {std::move(typed_null)});
    ASSERT_TRUE(all_null.ok()) << all_null.status();
    EXPECT_EQ(3, ColumnHelper::count_nulls(all_null.value()));
    auto empty = FileFunctions::try_to_file(context.get(), {ColumnHelper::create_column(type, true)});
    ASSERT_TRUE(empty.ok()) << empty.status();
    EXPECT_EQ(0, empty.value()->size());
}

TEST(FileFunctionsTest, TryToFileDoesNotHideStructuralErrors) {
    auto type = struct_type({"path"}, {TypeDescriptor(TYPE_VARCHAR)});
    auto source = ColumnHelper::create_column(type, false);
    source->append_datum(Datum(DatumStruct{Datum(Slice("a"))}));
    auto context = make_context(type);
    auto result = FileFunctions::try_to_file(context.get(), {std::move(source)});
    ASSERT_FALSE(result.ok());
    EXPECT_NE(std::string::npos, result.status().message().find("TRY_TO_FILE has unknown field"));
}

// ---------------------------------------------------------------------------------------------------------------------
// GET_PRESIGNED_URL / GET_PRESIGNED_FILE. Signing is offline: fixed credentials and an explicit endpoint.

namespace {

using PresignFunction = StatusOr<ColumnPtr> (*)(FunctionContext*, const Columns&);

const std::string kStoredUri = "s3://my-bucket/dir/a.png";
const std::string kStoredPath = "/my-bucket/dir/a.png?";

TCloudConfiguration presign_configuration() {
    std::map<std::string, std::string> properties = {
            {"aws.s3.access_key", "AKIDFILEFUNCTIONS"},
            {"aws.s3.secret_key", "file-functions-secret-key"},
            {"aws.s3.region", "us-east-1"},
            {"aws.s3.endpoint", "https://s3.us-east-1.amazonaws.com"},
            {"aws.s3.use_aws_sdk_default_behavior", "false"},
            {"aws.s3.use_instance_profile", "false"},
            {"aws.s3.enable_path_style_access", "true"},
    };
    TCloudConfiguration configuration;
    configuration.__set_cloud_type(TCloudType::AWS);
    configuration.__set_cloud_properties(properties);
    return configuration;
}

ColumnPtr volume_column(size_t rows) {
    return ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("media_volume"), rows);
}

ColumnPtr string_column(const std::vector<std::optional<std::string>>& values) {
    auto column = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), true);
    for (const auto& value : values) {
        if (value.has_value()) {
            column->append_datum(Datum(Slice(*value)));
        } else {
            (void)column->append_nulls(1);
        }
    }
    return column;
}

ColumnPtr expiration_column(const std::vector<std::optional<int64_t>>& values) {
    auto column = ColumnHelper::create_column(TypeDescriptor(TYPE_BIGINT), true);
    for (const auto& value : values) {
        if (value.has_value()) {
            column->append_datum(Datum(*value));
        } else {
            (void)column->append_nulls(1);
        }
    }
    return column;
}

// A FILE that references |uri| and populates every other reference field.
Datum reference_file(const std::string& uri) {
    return Datum(DatumStruct{Datum(Slice(uri)), Datum(int64_t(5)), Datum(int64_t(10)), Datum(Slice("image/png")),
                             Datum(Slice("checksum")), Datum()});
}

std::string url_at(const ColumnPtr& urls, size_t row) {
    return urls->get(row).get_slice().to_string();
}

std::string uri_of(const ColumnPtr& files, size_t row) {
    return files->get(row).get_struct()[FileColumn::URI].get_slice().to_string();
}

bool is_signed_for(const std::string& url, const std::string& path, int64_t expiration) {
    return url.starts_with("https://") && url.find(path) != std::string::npos &&
           url.find("X-Amz-Signature=") != std::string::npos &&
           (url.find("X-Amz-Expires=" + std::to_string(expiration) + "&") != std::string::npos ||
            url.ends_with("X-Amz-Expires=" + std::to_string(expiration)));
}

} // namespace

class PresignedFileFunctionsTest : public testing::Test {
public:
    static void SetUpTestSuite() { Aws::InitAPI(_s_options); }

    static void TearDownTestSuite() {
        close_s3_clients();
        Aws::ShutdownAPI(_s_options);
    }

protected:
    void SetUp() override {
        _context = make_context(TypeDescriptor(TYPE_VARCHAR));
        _context->set_cloud_configuration(std::make_shared<const TCloudConfiguration>(presign_configuration()));
    }

    ColumnPtr call(PresignFunction function, const Columns& columns) {
        auto result = function(_context.get(), columns);
        EXPECT_TRUE(result.ok()) << result.status();
        return result.ok() ? std::move(result).value() : nullptr;
    }

    std::unique_ptr<FunctionContext> _context;
    static inline Aws::SDKOptions _s_options;
};

TEST_F(PresignedFileFunctionsTest, UrlSignsEachRow) {
    auto uris = string_column({kStoredUri, kStoredUri, std::nullopt, "s3a://my.dotted.bucket/图片/b c.png", ""});
    // A NULL expiration selects the default; NULL and empty URIs skip expiration validation.
    auto signed_urls = call(FileFunctions::get_presigned_url_with_expiration,
                            {volume_column(5), uris, expiration_column({60, std::nullopt, 0, 1, 0})});
    ASSERT_NE(nullptr, signed_urls);
    ASSERT_EQ(5, signed_urls->size());
    EXPECT_FALSE(signed_urls->is_constant());
    EXPECT_TRUE(is_signed_for(url_at(signed_urls, 0), kStoredPath, 60));
    EXPECT_TRUE(is_signed_for(url_at(signed_urls, 1), kStoredPath, 3600));
    EXPECT_TRUE(signed_urls->is_null(2));
    // A dotted bucket stays whole under the s3 schemes, and the key is percent-encoded.
    EXPECT_TRUE(is_signed_for(url_at(signed_urls, 3), "/my.dotted.bucket/%E5%9B%BE%E7%89%87/b%20c.png?", 1));
    EXPECT_TRUE(signed_urls->is_null(4));

    // The planner appends a constant to non-deterministic calls. Without an expiration argument it lands where one
    // would be, and must not be taken for one.
    auto defaulted = call(FileFunctions::get_presigned_url, {volume_column(1), string_column({kStoredUri}),
                                                             ColumnHelper::create_const_column<TYPE_INT>(7, 1)});
    ASSERT_NE(nullptr, defaulted);
    EXPECT_TRUE(is_signed_for(url_at(defaulted, 0), kStoredPath, 3600));
}

TEST_F(PresignedFileFunctionsTest, UrlConstantAndEmptyInputs) {
    // Constant inputs yield a constant result that keeps their logical row count.
    auto constant = call(FileFunctions::get_presigned_url_with_expiration,
                         {volume_column(5), ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice(kStoredUri), 5),
                          ColumnHelper::create_const_column<TYPE_BIGINT>(120, 5)});
    ASSERT_NE(nullptr, constant);
    EXPECT_TRUE(constant->is_constant());
    EXPECT_EQ(5, constant->size());
    EXPECT_EQ(1, down_cast<const ConstColumn*>(constant.get())->data_column()->size());
    EXPECT_TRUE(is_signed_for(url_at(constant, 4), kStoredPath, 120));

    auto null_uri =
            call(FileFunctions::get_presigned_url, {volume_column(4), ColumnHelper::create_const_null_column(4)});
    ASSERT_NE(nullptr, null_uri);
    EXPECT_EQ(4, null_uri->size());
    EXPECT_EQ(4, ColumnHelper::count_nulls(null_uri));

    // An empty batch signs nothing, so an invalid constant cannot fail it.
    auto empty = call(FileFunctions::get_presigned_url_with_expiration,
                      {volume_column(0), ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("dir/a.png"), 0),
                       ColumnHelper::create_const_column<TYPE_BIGINT>(0, 0)});
    ASSERT_NE(nullptr, empty);
    EXPECT_EQ(0, empty->size());
}

// One representative of each error. A valid row precedes the invalid one, so the batch fails on a single row.
TEST_F(PresignedFileFunctionsTest, RejectsInvalidRows) {
    struct Case {
        std::string uri;
        int64_t expiration;
        std::string message;
        // GET_PRESIGNED_FILE passes http(s) URIs through instead of rejecting them.
        bool url_only = false;
    };
    const std::string not_s3 = "not a URI of an S3-compatible object storage";
    const std::vector<Case> cases = {
            // Relative paths and http(s) URLs are not object URIs, and the scheme is case-sensitive.
            {"dir/a.png", 60, not_s3},
            {"https://example.com/a.png", 60, not_s3, true},
            {"S3://my-bucket/a.png", 60, not_s3},
            {"s3://my-bucket", 60, "without an object key"},
            {"s3:///a.png", 60, "Invalid S3 URI"},
            // The URI parser would cut the key at '?' and sign another object.
            {"s3://my-bucket/a?b.png", 60, "query string"},
            {kStoredUri, 0, "between 1 and 3600 seconds"},
            {kStoredUri, 3601, "between 1 and 3600 seconds"}};
    for (const auto& test : cases) {
        SCOPED_TRACE(test.uri + " " + std::to_string(test.expiration));
        auto file = FileColumn::create();
        file->append_datum(reference_file(kStoredUri));
        file->append_datum(reference_file(test.uri));
        const ColumnPtr files = std::move(file);
        const std::array<std::pair<PresignFunction, ColumnPtr>, 2> calls = {
                std::make_pair(PresignFunction(FileFunctions::get_presigned_url_with_expiration),
                               string_column({kStoredUri, test.uri})),
                std::make_pair(PresignFunction(FileFunctions::get_presigned_file_with_expiration), files)};
        const std::array<std::string, 2> names = {"GET_PRESIGNED_URL", "GET_PRESIGNED_FILE"};
        for (size_t i = 0; i < (test.url_only ? 1 : calls.size()); ++i) {
            const Status status = calls[i].first(_context.get(), {volume_column(2), calls[i].second,
                                                                  expiration_column({60, test.expiration})})
                                          .status();
            ASSERT_FALSE(status.ok()) << names[i];
            EXPECT_TRUE(status.is_invalid_argument()) << status;
            EXPECT_NE(std::string::npos, status.message().find(names[i])) << status;
            EXPECT_NE(std::string::npos, status.message().find(test.message)) << status;
        }
    }
}

TEST_F(PresignedFileFunctionsTest, FileReplacesOnlyTheUri) {
    const std::string bytes("a\0b", 3);
    for (bool outer_null : {false, true}) {
        auto file = FileColumn::create();
        file->append_datum(reference_file(kStoredUri));
        // Inline content, an http(s) URI and a FILE without a URI have nothing to sign.
        file->append_datum(
                Datum(DatumStruct{Datum(), Datum(), Datum(), Datum(Slice("image/png")), Datum(), Datum(Slice(bytes))}));
        file->append_datum(reference_file("HTTPS://example.com/a.png?X-Amz-Signature=old"));
        file->append_default();
        file->append_datum(reference_file("oss://my-bucket/b.png"));
        file->append_datum(reference_file(""));
        ColumnPtr input = std::move(file);
        if (outer_null) {
            auto nulls = NullColumn::create(6, 0);
            nulls->get_data()[4] = 1;
            input = NullableColumn::create(input, std::move(nulls));
        }
        const std::array<std::string, 6> before = {input->debug_item(0), input->debug_item(1), input->debug_item(2),
                                                   input->debug_item(3), input->debug_item(4), input->debug_item(5)};
        // Rows that pass through unchanged do not validate their expiration time either.
        auto result = call(FileFunctions::get_presigned_file_with_expiration,
                           {volume_column(6), input, expiration_column({60, 0, -1, 3601, outer_null ? 0 : 120, 0})});
        ASSERT_NE(nullptr, result);
        ASSERT_EQ(6, result->size());
        result->check_or_die();

        auto signed_file = result->get(0).get_struct();
        EXPECT_TRUE(is_signed_for(signed_file[FileColumn::URI].get_slice().to_string(), kStoredPath, 60));
        EXPECT_EQ(5, signed_file[FileColumn::OFFSET].get_int64());
        EXPECT_EQ(10, signed_file[FileColumn::SIZE].get_int64());
        EXPECT_EQ("image/png", signed_file[FileColumn::CONTENT_TYPE].get_slice().to_string());
        EXPECT_EQ("checksum", signed_file[FileColumn::CHECKSUM].get_slice().to_string());
        EXPECT_TRUE(signed_file[FileColumn::INLINE].is_null());

        for (size_t row : {1, 2, 3, 5}) {
            EXPECT_EQ(before[row], result->debug_item(row)) << row;
        }
        if (outer_null) {
            EXPECT_TRUE(result->is_null(4));
        } else {
            EXPECT_TRUE(is_signed_for(uri_of(result, 4), "/my-bucket/b.png?", 120));
        }
        // The input is left as it was.
        for (size_t row = 0; row < before.size(); ++row) {
            EXPECT_EQ(before[row], input->debug_item(row)) << row;
        }
    }
}

TEST_F(PresignedFileFunctionsTest, FileConstantNullAndEmptyInputs) {
    auto one_row = FileColumn::create();
    one_row->append_datum(reference_file(kStoredUri));
    ColumnPtr constant = ConstColumn::create(std::move(one_row), 3);

    // A constant FILE is expanded into one FILE per row, each signed with its own expiration time.
    auto expanded = call(FileFunctions::get_presigned_file_with_expiration,
                         {volume_column(3), constant, expiration_column({60, std::nullopt, 120})});
    ASSERT_NE(nullptr, expanded);
    EXPECT_FALSE(expanded->is_constant());
    ASSERT_EQ(3, expanded->size());
    expanded->check_or_die();
    EXPECT_TRUE(is_signed_for(uri_of(expanded, 0), kStoredPath, 60));
    EXPECT_TRUE(is_signed_for(uri_of(expanded, 1), kStoredPath, 3600));
    EXPECT_TRUE(is_signed_for(uri_of(expanded, 2), kStoredPath, 120));
    EXPECT_EQ(10, expanded->get(2).get_struct()[FileColumn::SIZE].get_int64());
    // The constant input still holds the FILE it was given.
    EXPECT_TRUE(constant->is_constant());
    EXPECT_EQ(3, constant->size());
    EXPECT_EQ(kStoredUri, uri_of(constant, 0));

    auto null_file =
            call(FileFunctions::get_presigned_file, {volume_column(4), ColumnHelper::create_const_null_column(4)});
    ASSERT_NE(nullptr, null_file);
    EXPECT_EQ(4, null_file->size());
    EXPECT_EQ(4, ColumnHelper::count_nulls(null_file));

    auto empty = call(FileFunctions::get_presigned_file,
                      {volume_column(0), ColumnHelper::create_column(TypeDescriptor(TYPE_FILE), true)});
    ASSERT_NE(nullptr, empty);
    EXPECT_EQ(0, empty->size());
}

// The result shares the fields it does not change with its input. Filtering both in one Chunk, which mutates
// columns in place without acquiring COW ownership, must leave each of them intact.
TEST_F(PresignedFileFunctionsTest, FilterSourceAndResultTogether) {
    for (bool nullable : {false, true}) {
        for (bool output_first : {false, true}) {
            auto source = ColumnHelper::create_column(TypeDescriptor(TYPE_FILE), nullable);
            source->append_datum(reference_file("s3://my-bucket/a.png"));
            if (nullable) {
                (void)source->append_nulls(1);
            } else {
                source->append_datum(reference_file("https://example.com/b.png"));
            }
            source->append_datum(reference_file("s3://my-bucket/c.png"));
            source->append_datum(reference_file("s3://my-bucket/d.png"));
            ColumnPtr input = std::move(source);
            ColumnPtr output = call(FileFunctions::get_presigned_file, {volume_column(4), input});
            ASSERT_NE(nullptr, output);
            const auto* input_data = down_cast<const FileColumn*>(ColumnHelper::get_data_column(input.get()));
            const auto* output_data = down_cast<const FileColumn*>(ColumnHelper::get_data_column(output.get()));
            for (size_t field = 0; field < FileColumn::NUM_FIELDS; ++field) {
                EXPECT_NE(input_data->fields()[field].get(), output_data->fields()[field].get()) << field;
            }
            if (nullable) {
                EXPECT_NE(down_cast<const NullableColumn*>(input.get())->null_column().get(),
                          down_cast<const NullableColumn*>(output.get())->null_column().get());
            }
            const std::array<std::string, 3> expected_input = {input->debug_item(1), input->debug_item(2),
                                                               input->debug_item(3)};
            const std::array<std::string, 3> expected_output = {output->debug_item(1), output->debug_item(2),
                                                                output->debug_item(3)};
            Chunk chunk;
            chunk.append_column(output_first ? output : input, 0);
            chunk.append_column(output_first ? input : output, 1);
            Filter filter = {0, 1, 1, 1};
            ASSERT_EQ(3, chunk.filter(filter));
            ASSERT_EQ(3, input->size());
            ASSERT_EQ(3, output->size());
            input->check_or_die();
            output->check_or_die();
            for (size_t row = 0; row < 3; ++row) {
                EXPECT_EQ(expected_input[row], input->debug_item(row));
                EXPECT_EQ(expected_output[row], output->debug_item(row));
            }
            EXPECT_TRUE(is_signed_for(uri_of(output, 1), "/my-bucket/c.png?", 3600));
        }
    }
}

TEST_F(PresignedFileFunctionsTest, CloudConfiguration) {
    // Execution threads clone the context, and every clone signs with the configuration bound to the call.
    std::unique_ptr<FunctionContext> clone(_context->clone());
    EXPECT_EQ(_context->cloud_configuration().get(), clone->cloud_configuration().get());
    auto cloned = FileFunctions::get_presigned_url(clone.get(), {volume_column(1), string_column({kStoredUri})});
    ASSERT_TRUE(cloned.ok()) << cloned.status();
    EXPECT_TRUE(is_signed_for(url_at(cloned.value(), 0), kStoredPath, 3600));

    auto unbound = make_context(TypeDescriptor(TYPE_VARCHAR));
    auto missing = FileFunctions::get_presigned_url(unbound.get(), {volume_column(1), string_column({kStoredUri})});
    ASSERT_FALSE(missing.ok());
    EXPECT_NE(std::string::npos, missing.status().message().find("cloud configuration")) << missing.status();

    TCloudConfiguration azure = presign_configuration();
    azure.__set_cloud_type(TCloudType::AZURE);
    unbound->set_cloud_configuration(std::make_shared<const TCloudConfiguration>(azure));
    auto not_s3 = FileFunctions::get_presigned_url(unbound.get(), {volume_column(1), string_column({kStoredUri})});
    ASSERT_FALSE(not_s3.ok());
    EXPECT_NE(std::string::npos, not_s3.status().message().find("S3-compatible cloud configuration"))
            << not_s3.status();
}

} // namespace starrocks
