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

#include <array>
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
    input->append_datum(Datum(DatumStruct{Datum(Slice(bytes)), Datum(int64_t(3)), Datum(), Datum(Slice("opaque")),
                                          Datum(int64_t(0)), Datum(Slice("arbitrary"))}));
    input->append_datum(Datum(DatumStruct{Datum(), Datum(std::numeric_limits<int64_t>::max()),
                                          Datum(Slice("not a URI")), Datum(), Datum(), Datum()}));
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
            {{Datum(), Datum(Slice("private-bytes")), Datum(), Datum(std::numeric_limits<int64_t>::min())},
             "'size' must be >= 0"}};
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

class FileConstructorCowTest : public testing::TestWithParam<FileFunction> {};

TEST_P(FileConstructorCowTest, CopyOnWriteIsolation) {
    auto type = struct_type({"uri", "size"}, {TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_BIGINT)});
    auto context = make_context(type);
    for (bool nullable : {false, true}) {
        for (bool mutate_input : {false, true}) {
            auto source = ColumnHelper::create_column(type, nullable);
            source->append_datum(Datum(DatumStruct{Datum(Slice("a")), Datum(int64_t(1))}));
            source->append_datum(Datum(DatumStruct{Datum(Slice("b")), Datum(int64_t(2))}));
            ColumnPtr input = std::move(source);
            auto result = GetParam()(context.get(), {input});
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
                writable->append_datum(
                        Datum(DatumStruct{Datum(Slice("c")), Datum(), Datum(int64_t(3)), Datum(), Datum(), Datum()}));
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

INSTANTIATE_TEST_SUITE_P(FileFunctions, FileConstructorCowTest,
                         testing::Values(FileFunctions::to_file, FileFunctions::try_to_file));

TEST(FileFunctionsTest, EmptyBatch) {
    auto type = struct_type({"uri"}, {TypeDescriptor(TYPE_VARCHAR)});
    auto context = make_context(type);
    auto result = FileFunctions::to_file(context.get(), {ColumnHelper::create_column(type, true)});
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(0, result.value()->size());
}

TEST(FileFunctionsTest, GettersShareFieldsAndMergeOuterNulls) {
    const std::string bytes("a\0b", 3);
    for (bool outer_null : {false, true}) {
        auto file = FileColumn::create();
        // A FILE from another producer need not obey TO_FILE's construction rules.
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
        for (size_t i = 0; i < kGetters.size(); ++i) {
            SCOPED_TRACE(i);
            auto context = std::unique_ptr<FunctionContext>(FunctionContext::create_context(
                    nullptr, nullptr, TypeDescriptor(kFieldTypes[i]), {TypeDescriptor(TYPE_FILE)}));
            auto result = kGetters[i](context.get(), {input});
            ASSERT_TRUE(result.ok()) << result.status();
            EXPECT_EQ(3, result.value()->size());
            const auto* projected = down_cast<const NullableColumn*>(result.value().get());
            const auto* field = down_cast<const NullableColumn*>(fields[i].get());
            EXPECT_EQ(field->data_column().get(), projected->data_column().get());
            if (outer_null) {
                EXPECT_TRUE(result.value()->is_null(0));
                EXPECT_FALSE(fields[i]->is_null(0));
                EXPECT_NE(field->null_column().get(), projected->null_column().get());
            } else {
                EXPECT_EQ(fields[i].get(), result.value().get());
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

} // namespace starrocks
