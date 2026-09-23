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

#include "column/file_column.h"

#include <gtest/gtest.h>

#include <cstring>
#include <optional>
#include <string>
#include <vector>

#include "base/testutil/assert.h"
#include "column/binary_column.h"
#include "column/column_hash/column_hash.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/fixed_length_column.h"
#include "column/mysql_row_buffer.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "gutil/casts.h"
#include "types/logical_type.h"
#include "types/type_descriptor.h"

namespace starrocks {

namespace {

Datum make_reference(const char* uri, int64_t offset, int64_t size) {
    return FileDatumBuilder::make(Slice(uri), offset, size, std::nullopt, std::nullopt, std::nullopt);
}

Datum make_inline(const char* bytes) {
    return FileDatumBuilder::make(std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt, Slice(bytes));
}

Datum make_full(const char* uri, int64_t offset, int64_t size, const char* content_type, const char* checksum) {
    return FileDatumBuilder::make(Slice(uri), offset, size, Slice(content_type), Slice(checksum), std::nullopt);
}

Datum make_all_null() {
    return FileDatumBuilder::make(std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt);
}

// Three rows covering the reference form, the inline form and a fully populated reference.
FileColumn::MutablePtr create_test_column() {
    auto column = FileColumn::create();
    column->append_datum(make_reference("s3://bucket/a.png", 4, 8));
    column->append_datum(make_inline("raw-bytes"));
    column->append_datum(make_full("hdfs://ns/b.jpg", 0, 16, "image/jpeg", "crc32:1"));
    return column;
}

const char* const kRow0 = "{uri:'s3://bucket/a.png',offset:4,size:8,content_type:NULL,checksum:NULL,inline:NULL}";
const char* const kRow1 = "{uri:NULL,offset:NULL,size:NULL,content_type:NULL,checksum:NULL,inline:'raw-bytes'}";
const char* const kRow2 =
        "{uri:'hdfs://ns/b.jpg',offset:0,size:16,content_type:'image/jpeg',checksum:'crc32:1',inline:NULL}";
const char* const kNullRow = "{uri:NULL,offset:NULL,size:NULL,content_type:NULL,checksum:NULL,inline:NULL}";
// resize() grows every nullable sub-column with non-NULL default values, not with NULLs.
const char* const kDefaultRow = "{uri:'',offset:0,size:0,content_type:'',checksum:'',inline:''}";

} // namespace

TEST(FileColumnTest, test_create_and_debug) {
    auto column = create_test_column();

    ASSERT_TRUE(column->is_file());
    ASSERT_FALSE(column->is_nullable());
    ASSERT_EQ("file", column->get_name());
    ASSERT_EQ(3, column->size());
    ASSERT_EQ(sizeof(DatumStruct), column->type_size());
    ASSERT_GE(column->capacity(), column->size());

    ASSERT_EQ(kRow0, column->debug_item(0));
    ASSERT_EQ(kRow1, column->debug_item(1));
    ASSERT_EQ(kRow2, column->debug_item(2));
    ASSERT_EQ(std::string(kRow0) + ", " + kRow1 + ", " + kRow2, column->debug_string());

    auto fields = column->fields();
    ASSERT_EQ(FileColumn::NUM_FIELDS, fields.size());
    for (const auto& field : fields) {
        ASSERT_TRUE(field->is_nullable());
        ASSERT_EQ(3, field->size());
    }
    column->check_or_die();
}

TEST(FileColumnTest, test_sized_constructor) {
    auto column = FileColumn::create(static_cast<size_t>(2));
    ASSERT_EQ(2, column->size());
    ASSERT_EQ(kNullRow, column->debug_item(0));
    ASSERT_EQ(kNullRow, column->debug_item(1));

    auto empty = FileColumn::create(static_cast<size_t>(0));
    ASSERT_EQ(0, empty->size());
    ASSERT_EQ("", empty->debug_string());
}

TEST(FileColumnTest, test_get_datum) {
    auto column = create_test_column();

    const Datum datum0 = column->get(0);
    const DatumStruct& row0 = datum0.get_struct();
    ASSERT_EQ(FileColumn::NUM_FIELDS, row0.size());
    ASSERT_EQ("s3://bucket/a.png", row0[FileColumn::URI].get_slice().to_string());
    ASSERT_EQ(4, row0[FileColumn::OFFSET].get_int64());
    ASSERT_EQ(8, row0[FileColumn::SIZE].get_int64());
    ASSERT_TRUE(row0[FileColumn::CONTENT_TYPE].is_null());
    ASSERT_TRUE(row0[FileColumn::CHECKSUM].is_null());
    ASSERT_TRUE(row0[FileColumn::INLINE].is_null());

    const Datum datum1 = column->get(1);
    const DatumStruct& row1 = datum1.get_struct();
    ASSERT_TRUE(row1[FileColumn::URI].is_null());
    ASSERT_TRUE(row1[FileColumn::OFFSET].is_null());
    ASSERT_TRUE(row1[FileColumn::SIZE].is_null());
    ASSERT_EQ("raw-bytes", row1[FileColumn::INLINE].get_slice().to_string());

    const Datum datum2 = column->get(2);
    const DatumStruct& row2 = datum2.get_struct();
    ASSERT_EQ("image/jpeg", row2[FileColumn::CONTENT_TYPE].get_slice().to_string());
    ASSERT_EQ("crc32:1", row2[FileColumn::CHECKSUM].get_slice().to_string());
}

TEST(FileColumnTest, test_clone) {
    auto column = create_test_column();

    auto cloned = column->clone();
    ASSERT_TRUE(cloned->is_file());
    ASSERT_EQ(column->size(), cloned->size());
    ASSERT_EQ(column->debug_string(), cloned->debug_string());

    auto empty = column->clone_empty();
    ASSERT_TRUE(empty->is_file());
    ASSERT_EQ(0, empty->size());

    // The clone must not alias the source sub-columns.
    cloned->append_datum(make_inline("more"));
    ASSERT_EQ(4, cloned->size());
    ASSERT_EQ(3, column->size());
}

TEST(FileColumnTest, test_append_variants) {
    auto src = create_test_column();

    auto column = FileColumn::create();
    column->append(*src, 1, 2);
    ASSERT_EQ(2, column->size());
    ASSERT_EQ(kRow1, column->debug_item(0));
    ASSERT_EQ(kRow2, column->debug_item(1));

    std::vector<uint32_t> indexes{2, 0};
    column->append_selective(*src, indexes.data(), 0, 2);
    ASSERT_EQ(4, column->size());
    ASSERT_EQ(kRow2, column->debug_item(2));
    ASSERT_EQ(kRow0, column->debug_item(3));

    column->append_value_multiple_times(*src, 0, 2);
    ASSERT_EQ(6, column->size());
    ASSERT_EQ(kRow0, column->debug_item(4));
    ASSERT_EQ(kRow0, column->debug_item(5));

    Datum inline_datum = make_inline("raw-bytes");
    column->append_value_multiple_times(&inline_datum, 2);
    ASSERT_EQ(8, column->size());
    ASSERT_EQ(kRow1, column->debug_item(6));
    ASSERT_EQ(kRow1, column->debug_item(7));

    column->check_or_die();
}

TEST(FileColumnTest, test_append_nulls_and_defaults) {
    auto column = FileColumn::create();

    ASSERT_TRUE(column->append_nulls(2));
    ASSERT_EQ(2, column->size());
    ASSERT_EQ(kNullRow, column->debug_item(0));
    ASSERT_EQ(kNullRow, column->debug_item(1));

    column->append_default();
    ASSERT_EQ(3, column->size());
    ASSERT_EQ(kNullRow, column->debug_item(2));

    column->append_default(2);
    ASSERT_EQ(5, column->size());
    ASSERT_EQ(kNullRow, column->debug_item(4));

    column->append_datum(make_all_null());
    ASSERT_EQ(6, column->size());
    ASSERT_EQ(kNullRow, column->debug_item(5));

    // Raw numeric appends have no meaning for FILE.
    ASSERT_EQ(static_cast<size_t>(-1), column->append_numbers(nullptr, 0));

    auto fields = column->fields();
    for (const auto& field : fields) {
        ASSERT_TRUE(down_cast<const NullableColumn*>(field.get())->has_null());
    }
}

TEST(FileColumnTest, test_reserve_resize_remove) {
    auto column = create_test_column();

    column->reserve(64);
    ASSERT_EQ(3, column->size());
    ASSERT_GE(column->capacity(), 3);

    column->resize(5);
    ASSERT_EQ(5, column->size());
    ASSERT_EQ(kRow2, column->debug_item(2));
    ASSERT_EQ(kDefaultRow, column->debug_item(3));
    ASSERT_EQ(kDefaultRow, column->debug_item(4));

    column->resize(2);
    ASSERT_EQ(2, column->size());
    ASSERT_EQ(kRow1, column->debug_item(1));

    column->remove_first_n_values(1);
    ASSERT_EQ(1, column->size());
    ASSERT_EQ(kRow1, column->debug_item(0));
    column->check_or_die();
}

TEST(FileColumnTest, test_filter_range) {
    auto column = create_test_column();
    column->append_datum(make_inline("tail"));
    ASSERT_EQ(4, column->size());

    Filter filter{1, 0, 1, 0};
    ASSERT_EQ(2, column->filter_range(filter, 0, 4));
    ASSERT_EQ(2, column->size());
    ASSERT_EQ(kRow0, column->debug_item(0));
    ASSERT_EQ(kRow2, column->debug_item(1));
    column->check_or_die();

    Filter drop_all{0, 0};
    ASSERT_EQ(0, column->filter_range(drop_all, 0, 2));
    ASSERT_EQ(0, column->size());
}

TEST(FileColumnTest, test_fill_default_and_update_rows) {
    auto column = create_test_column();

    // Every sub-column is nullable, and NullableColumn::fill_default() is a no-op, so rows stay intact.
    Filter filter{0, 1, 0};
    column->fill_default(filter);
    ASSERT_EQ(3, column->size());
    ASSERT_EQ(kRow1, column->debug_item(1));

    // update_rows() requires ascending indexes; row i of `src` replaces row indexes[i] of this column.
    // Swap rows 1 and 2 through it, which also forces the length-changing rebuild path of BinaryColumn.
    auto src = FileColumn::create();
    src->append_datum(make_full("hdfs://ns/b.jpg", 0, 16, "image/jpeg", "crc32:1"));
    src->append_datum(make_inline("raw-bytes"));
    std::vector<uint32_t> indexes{1, 2};
    column->update_rows(*src, indexes.data());
    ASSERT_EQ(3, column->size());
    ASSERT_EQ(kRow0, column->debug_item(0));
    ASSERT_EQ(kRow2, column->debug_item(1));
    ASSERT_EQ(kRow1, column->debug_item(2));
}

TEST(FileColumnTest, test_assign) {
    auto column = create_test_column();
    column->assign(3, 1);
    ASSERT_EQ(3, column->size());
    ASSERT_EQ(kRow1, column->debug_item(0));
    ASSERT_EQ(kRow1, column->debug_item(1));
    ASSERT_EQ(kRow1, column->debug_item(2));
}

TEST(FileColumnTest, test_swap_and_reset) {
    auto column1 = create_test_column();
    auto column2 = FileColumn::create();
    column2->append_datum(make_inline("other"));

    column1->swap_column(*column2);
    ASSERT_EQ(1, column1->size());
    ASSERT_EQ("{uri:NULL,offset:NULL,size:NULL,content_type:NULL,checksum:NULL,inline:'other'}",
              column1->debug_item(0));
    ASSERT_EQ(3, column2->size());
    ASSERT_EQ(kRow0, column2->debug_item(0));
    ASSERT_EQ(kRow2, column2->debug_item(2));

    column2->reset_column();
    ASSERT_EQ(0, column2->size());
    for (const auto& field : column2->fields()) {
        ASSERT_EQ(0, field->size());
    }
    column2->append_datum(make_inline("after-reset"));
    ASSERT_EQ(1, column2->size());
}

TEST(FileColumnTest, test_byte_size_and_memory_usage) {
    auto column = create_test_column();

    size_t expected_bytes = 0;
    size_t expected_memory = 0;
    size_t expected_container = 0;
    size_t expected_reference = 0;
    for (const auto& field : column->fields()) {
        expected_bytes += field->byte_size();
        expected_memory += field->memory_usage();
        expected_container += field->container_memory_usage();
        expected_reference += field->reference_memory_usage(0, 3);
    }
    ASSERT_GT(expected_bytes, 0);
    ASSERT_EQ(expected_bytes, column->byte_size());
    ASSERT_EQ(expected_bytes, column->byte_size(0, 3));
    ASSERT_EQ(column->byte_size(0) + column->byte_size(1) + column->byte_size(2), column->byte_size(0, 3));
    ASSERT_GT(column->byte_size(0), column->byte_size(1)); // row 0 carries a uri and two bigints
    ASSERT_EQ(expected_memory, column->memory_usage());
    ASSERT_EQ(expected_container, column->container_memory_usage());
    ASSERT_EQ(expected_reference, column->reference_memory_usage(0, 3));
}

TEST(FileColumnTest, test_serialize) {
    auto column = create_test_column();

    uint32_t max_size = column->max_one_element_serialize_size();
    ASSERT_GT(max_size, 0);
    std::vector<uint8_t> buffer(max_size);
    uint32_t total = 0;
    for (size_t i = 0; i < column->size(); ++i) {
        uint32_t expected = column->serialize_size(i);
        ASSERT_LE(expected, max_size);
        ASSERT_EQ(expected, column->serialize(i, buffer.data()));
        total += expected;
    }

    // serialize_batch() lays the rows out at a fixed stride and reports the per-row sizes.
    Buffer<uint32_t> slice_sizes(column->size(), 0);
    std::vector<uint8_t> batch(max_size * column->size());
    column->serialize_batch(batch.data(), slice_sizes, column->size(), max_size);
    uint32_t batch_total = 0;
    for (size_t i = 0; i < column->size(); ++i) {
        ASSERT_EQ(column->serialize_size(i), slice_sizes[i]);
        batch_total += slice_sizes[i];
    }
    ASSERT_EQ(total, batch_total);

    // A default row is all NULLs, so it serializes exactly like a row appended with append_nulls().
    auto null_column = FileColumn::create();
    ASSERT_TRUE(null_column->append_nulls(1));
    std::vector<uint8_t> default_buffer(max_size);
    ASSERT_EQ(null_column->serialize_size(0), column->serialize_default(default_buffer.data()));

    // Rows with identical content serialize to identical bytes.
    auto twin = create_test_column();
    std::vector<uint8_t> twin_buffer(max_size);
    ASSERT_EQ(column->serialize(0, buffer.data()), twin->serialize(0, twin_buffer.data()));
    ASSERT_EQ(0, memcmp(buffer.data(), twin_buffer.data(), column->serialize_size(0)));
}

TEST(FileColumnTest, test_compare_and_equals) {
    // Column::EQUALS_* are `static const int` without an out-of-class definition, so binding them by
    // reference inside ASSERT_EQ would not link; copy them into constexpr locals first.
    constexpr int kEqualsTrue = Column::EQUALS_TRUE;
    constexpr int kEqualsFalse = Column::EQUALS_FALSE;
    constexpr int kEqualsNull = Column::EQUALS_NULL;

    auto column = FileColumn::create();
    column->append_datum(make_reference("s3://bucket/a.png", 4, 8));
    column->append_datum(make_reference("s3://bucket/a.png", 4, 9));
    column->append_datum(make_inline("raw-bytes"));
    column->append_datum(make_reference("s3://bucket/a.png", 4, 8));

    ASSERT_EQ(0, column->compare_at(0, 0, *column, -1));
    ASSERT_EQ(0, column->compare_at(0, 3, *column, -1));
    ASSERT_LT(column->compare_at(0, 1, *column, -1), 0); // size 8 < 9
    ASSERT_GT(column->compare_at(1, 0, *column, -1), 0);
    ASSERT_NE(0, column->compare_at(0, 2, *column, -1)); // uri vs NULL uri

    ASSERT_EQ(kEqualsTrue, column->equals(0, *column, 3));
    ASSERT_EQ(kEqualsFalse, column->equals(0, *column, 1));
    ASSERT_EQ(kEqualsFalse, column->equals(0, *column, 2));
    // Under safe_eq NULL <=> NULL is TRUE, so an all-matching row with NULL fields still compares equal.
    ASSERT_EQ(kEqualsTrue, column->equals(2, *column, 2));

    // Unsafe mode: a mismatching non-NULL field short-circuits to FALSE, otherwise NULL fields make the
    // result NULL.
    ASSERT_EQ(kEqualsFalse, column->equals(0, *column, 1, false));
    ASSERT_EQ(kEqualsNull, column->equals(0, *column, 3, false));
}

TEST(FileColumnTest, test_xor_checksum) {
    auto column = create_test_column();
    auto twin = create_test_column();
    ASSERT_EQ(column->xor_checksum(0, 3), twin->xor_checksum(0, 3));
    ASSERT_EQ(0, column->xor_checksum(0, 0));

    // Rows are checksummed independently, so a prefix must match its own checksum.
    ASSERT_EQ(column->xor_checksum(0, 1), twin->xor_checksum(0, 1));
    ASSERT_NE(column->xor_checksum(0, 1), column->xor_checksum(1, 2));
}

TEST(FileColumnTest, test_put_mysql_row_buffer) {
    auto column = create_test_column();

    {
        MysqlRowBuffer buffer;
        column->put_mysql_row_buffer(&buffer, 0, false);
        const std::string expected =
                R"({"uri":"s3://bucket/a.png","offset":4,"size":8,"content_type":null,"checksum":null,"inline":null})";
        ASSERT_EQ(expected.size() + 1, buffer.data().size());
        ASSERT_EQ(static_cast<char>(expected.size()), buffer.data()[0]);
        ASSERT_EQ(expected, buffer.data().substr(1));
    }
    {
        // Inline bytes render as hex inside the bracket, following the nested VARBINARY rules.
        MysqlRowBuffer buffer;
        column->put_mysql_row_buffer(&buffer, 1, false);
        const std::string expected =
                R"({"uri":null,"offset":null,"size":null,"content_type":null,"checksum":null,"inline":"7261772d6279746573"})";
        ASSERT_EQ(expected, buffer.data().substr(1));
    }
    {
        MysqlRowBuffer buffer;
        column->put_mysql_row_buffer(&buffer, 2, false);
        const std::string expected =
                R"({"uri":"hdfs://ns/b.jpg","offset":0,"size":16,"content_type":"image/jpeg","checksum":"crc32:1","inline":null})";
        ASSERT_EQ(expected, buffer.data().substr(1));
    }
}

TEST(FileColumnTest, test_upgrade_downgrade_and_limits) {
    auto column = create_test_column();

    ASSERT_FALSE(column->has_large_column());
    auto upgraded = column->upgrade_if_overflow();
    ASSERT_TRUE(upgraded.ok());
    ASSERT_EQ(nullptr, upgraded.value());

    auto downgraded = column->downgrade();
    ASSERT_TRUE(downgraded.ok());
    ASSERT_EQ(nullptr, downgraded.value());

    ASSERT_TRUE(column->capacity_limit_reached().ok());
    ASSERT_EQ(3, column->size());
    ASSERT_EQ(kRow0, column->debug_item(0));
}

TEST(FileColumnTest, test_mutate_shared_column) {
    ColumnPtr shared = create_test_column();
    ColumnPtr alias = shared;
    ASSERT_EQ(2, shared->use_count());

    // Column::mutate() copies the column when it is shared and then mutates each sub-column.
    MutableColumnPtr mutated = Column::mutate(std::move(shared));
    ASSERT_TRUE(mutated->is_file());
    mutated->append_datum(make_inline("added"));
    ASSERT_EQ(4, mutated->size());
    ASSERT_EQ(3, alias->size());
    ASSERT_EQ(kRow0, mutated->debug_item(0));
}

TEST(FileColumnTest, test_hash_and_null_info_visitors) {
    auto column = FileColumn::create();
    column->append_datum(make_reference("s3://bucket/a.png", 4, 8));
    column->append_datum(make_reference("s3://bucket/a.png", 4, 8));
    column->append_datum(make_inline("raw-bytes"));

    std::vector<uint32_t> hashes(3, 0);
    fnv_hash_column(*column, hashes.data(), 0, 3);
    ASSERT_EQ(hashes[0], hashes[1]);
    ASSERT_NE(hashes[0], hashes[2]);

    std::vector<uint32_t> crc_hashes(3, 0);
    crc32_hash_column(*column, crc_hashes.data(), 0, 3);
    ASSERT_EQ(crc_hashes[0], crc_hashes[1]);

    // update_nested_has_null() walks into every FILE field and refreshes its has_null flag.
    ASSERT_OK(ColumnHelper::update_nested_has_null(column.get()));
    ASSERT_TRUE(down_cast<const NullableColumn*>(column->fields()[FileColumn::URI].get())->has_null());
    ASSERT_TRUE(down_cast<const NullableColumn*>(column->fields()[FileColumn::INLINE].get())->has_null());
}

TEST(FileColumnTest, test_type_plumbing) {
    ASSERT_EQ(TYPE_FILE, TypeDescriptor::create_file_type().type);
    ASSERT_EQ(TYPE_FILE, TypeDescriptor::from_logical_type(TYPE_FILE).type);
    ASSERT_TRUE(TypeDescriptor::from_logical_type(TYPE_FILE).children.empty());
    ASSERT_STREQ("FILE", logical_type_to_string(TYPE_FILE));

    auto nullable = NullableColumn::create(FileColumn::create(), NullColumn::create());
    ASSERT_TRUE(nullable->is_file());
    auto constant = ConstColumn::create(FileColumn::create(static_cast<size_t>(1)), 4);
    ASSERT_TRUE(constant->is_file());
    ASSERT_FALSE(Int64Column::create()->is_file());
    ASSERT_FALSE(BinaryColumn::create()->is_file());
}

} // namespace starrocks
