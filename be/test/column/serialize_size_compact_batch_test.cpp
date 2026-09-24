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

#include <gtest/gtest.h>

#include <cstring>
#include <string>
#include <vector>

#include "base/testutil/parallel_test.h"
#include "column/adaptive_nullable_column.h"
#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/fixed_length_column.h"
#include "column/json_column.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/struct_column.h"
#include "column/variant_column.h"
#include "column/variant_encoder.h"
#include "types/json_value.h"

namespace starrocks {

namespace {

// serialize_size_compact_batch() is only useful if it predicts serialize_batch() exactly: a
// packed key buffer places row i+1 right where the size pass says row i ends, so any
// disagreement makes adjacent rows overwrite each other. Check it both ways:
//   1. the predicted size of every row equals what serialize_batch() adds to slice_sizes, and
//   2. serializing packed (offsets pre-set, max_one_row_size = 0) produces, row by row, the same
//      bytes as the legacy max-stride layout.
void expect_size_pass_matches_serialize_batch(const Column& col) {
    const size_t n = col.size();
    ASSERT_GT(n, 0u);

    Buffer<uint64_t> predicted(n, 0);
    col.serialize_size_compact_batch(predicted, n);

    const uint32_t stride = col.max_one_element_serialize_size_compact();
    std::vector<uint8_t> strided(static_cast<size_t>(stride) * n + 64, 0);
    Buffer<uint32_t> written(n, 0);
    col.serialize_batch(strided.data(), written, n, stride);
    for (size_t i = 0; i < n; ++i) {
        ASSERT_EQ(predicted[i], written[i]) << col.get_name() << " row " << i;
    }
    // A column that claims a fixed row size lets the key builder skip this size pass entirely.
    if (const uint32_t fixed = col.serialize_batch_fixed_row_size(); fixed != 0) {
        for (size_t i = 0; i < n; ++i) {
            ASSERT_EQ(fixed, written[i]) << col.get_name() << " row " << i << " breaks its fixed row size";
        }
    }

    Buffer<uint32_t> offsets(n, 0);
    size_t total = 0;
    for (size_t i = 0; i < n; ++i) {
        offsets[i] = static_cast<uint32_t>(total);
        total += predicted[i];
    }
    const Buffer<uint32_t> starts = offsets;
    std::vector<uint8_t> packed(total + 64, 0);
    col.serialize_batch(packed.data(), offsets, n, 0);
    for (size_t i = 0; i < n; ++i) {
        ASSERT_EQ(starts[i] + predicted[i], offsets[i]) << col.get_name() << " row " << i;
        ASSERT_EQ(0, std::memcmp(packed.data() + starts[i], strided.data() + i * stride, predicted[i]))
                << col.get_name() << " row " << i;
    }
}

// Lengths on both sides of the one-byte/five-byte compact length header boundary (254 / 255).
MutableColumnPtr make_binary_column() {
    auto col = BinaryColumn::create();
    for (size_t len : {0, 1, 7, 254, 255, 256, 1000, 3}) {
        col->append(Slice(std::string(len, 'x')));
    }
    return col;
}

} // namespace

// NOLINTNEXTLINE
PARALLEL_TEST(SerializeSizeCompactBatchTest, fixed_length) {
    auto col = Int32Column::create();
    for (int32_t v : {1, -2, 3, 4}) col->append(v);
    expect_size_pass_matches_serialize_batch(*col);
}

// NOLINTNEXTLINE
PARALLEL_TEST(SerializeSizeCompactBatchTest, binary_across_compact_header_boundary) {
    expect_size_pass_matches_serialize_batch(*make_binary_column());
}

// NOLINTNEXTLINE
PARALLEL_TEST(SerializeSizeCompactBatchTest, nullable_binary_with_nulls) {
    auto col = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    col->append_datum(Slice("abc"));
    col->append_nulls(1);
    col->append_datum(Slice(std::string(300, 'y')));
    col->append_nulls(1);
    col->append_datum(Slice(""));
    ASSERT_TRUE(col->has_null());
    expect_size_pass_matches_serialize_batch(*col);
}

// NOLINTNEXTLINE
PARALLEL_TEST(SerializeSizeCompactBatchTest, nullable_binary_without_nulls) {
    auto col = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    col->append_datum(Slice("abc"));
    col->append_datum(Slice(std::string(300, 'y')));
    ASSERT_FALSE(col->has_null());
    expect_size_pass_matches_serialize_batch(*col);
}

// NOLINTNEXTLINE
PARALLEL_TEST(SerializeSizeCompactBatchTest, nullable_fixed_length_with_nulls) {
    auto col = NullableColumn::create(Int64Column::create(), NullColumn::create());
    col->append_datum(int64_t{1});
    col->append_nulls(1);
    col->append_datum(int64_t{3});
    expect_size_pass_matches_serialize_batch(*col);
}

// NOLINTNEXTLINE
PARALLEL_TEST(SerializeSizeCompactBatchTest, const_binary) {
    auto data = BinaryColumn::create();
    data->append(Slice(std::string(300, 'z')));
    auto col = ConstColumn::create(std::move(data), 5);
    expect_size_pass_matches_serialize_batch(*col);
}

// NOLINTNEXTLINE
PARALLEL_TEST(SerializeSizeCompactBatchTest, array_of_nullable_binary) {
    // Arrays fall back to the default (per-row serialize_size()); their elements use the
    // persisted encoding, not the compact one.
    auto elements = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    auto col = ArrayColumn::create(std::move(elements), UInt32Column::create());
    col->append_datum(DatumArray{Datum(Slice("a")), Datum(), Datum(Slice(std::string(300, 'q')))});
    col->append_datum(DatumArray{});
    col->append_datum(DatumArray{Datum(Slice("bc"))});
    expect_size_pass_matches_serialize_batch(*col);
}

// NOLINTNEXTLINE
PARALLEL_TEST(SerializeSizeCompactBatchTest, nullable_array_with_nulls) {
    // Exercises the default serialize_size_compact_batch_with_null_masks(), which falls back to the
    // per-row serialize_size() for element types without a batched override.
    auto elements = NullableColumn::create(Int32Column::create(), NullColumn::create());
    auto array = ArrayColumn::create(std::move(elements), UInt32Column::create());
    auto col = NullableColumn::create(std::move(array), NullColumn::create());
    col->append_datum(DatumArray{Datum(int32_t{1}), Datum()});
    col->append_nulls(1);
    col->append_datum(DatumArray{});
    col->append_datum(DatumArray{Datum(int32_t{2}), Datum(int32_t{3}), Datum(int32_t{4})});
    ASSERT_TRUE(col->has_null());
    expect_size_pass_matches_serialize_batch(*col);
}

// NOLINTNEXTLINE
PARALLEL_TEST(SerializeSizeCompactBatchTest, struct_with_binary_field) {
    // StructColumn serializes its fields with the persisted encoding (four-byte string length), so
    // summing the fields' compact sizes would be wrong. The per-row default has to be kept.
    auto ids = Int32Column::create();
    auto names = BinaryColumn::create();
    for (int i = 0; i < 4; ++i) {
        ids->append(i);
        names->append(Slice(std::string(i == 2 ? 300 : i, 'n')));
    }
    auto col = StructColumn::create(Columns{std::move(ids), std::move(names)}, std::vector<std::string>{"id", "name"});
    expect_size_pass_matches_serialize_batch(*col);
}

// NOLINTNEXTLINE
PARALLEL_TEST(SerializeSizeCompactBatchTest, map_of_binary_to_int) {
    auto keys = BinaryColumn::create();
    auto values = Int32Column::create();
    auto offsets = UInt32Column::create();
    offsets->append(0);
    keys->append(Slice("b"));
    values->append(2);
    keys->append(Slice(std::string(300, 'a')));
    values->append(1);
    offsets->append(2);
    offsets->append(2); // an empty map
    keys->append(Slice("c"));
    values->append(3);
    offsets->append(3);
    auto col = MapColumn::create(ColumnHelper::cast_to_nullable_column(std::move(keys)),
                                 ColumnHelper::cast_to_nullable_column(std::move(values)), std::move(offsets));
    expect_size_pass_matches_serialize_batch(*col);
}

// NOLINTNEXTLINE
PARALLEL_TEST(SerializeSizeCompactBatchTest, json) {
    auto col = JsonColumn::create();
    for (const char* text : {R"({"a": 1})", R"([1, 2, 3])", R"("s")", R"({"long": "xxxxxxxxxxxxxxxxxxxxxxxx"})"}) {
        auto json = JsonValue::parse(text);
        ASSERT_TRUE(json.ok());
        col->append(std::move(json.value()));
    }
    expect_size_pass_matches_serialize_batch(*col);
}

// NOLINTNEXTLINE
PARALLEL_TEST(SerializeSizeCompactBatchTest, variant) {
    auto col = VariantColumn::create();
    for (const char* text : {R"({"a": 1})", R"([1, "two", null])", R"(3.5)"}) {
        auto encoded = VariantEncoder::encode_json_text_to_variant(text);
        ASSERT_TRUE(encoded.ok()) << encoded.status();
        col->append(&encoded.value());
    }
    expect_size_pass_matches_serialize_batch(*col);
}

// NOLINTNEXTLINE
PARALLEL_TEST(SerializeSizeCompactBatchTest, adaptive_nullable_with_nulls) {
    auto col = AdaptiveNullableColumn::create(BinaryColumn::create(), NullColumn::create());
    col->append_nulls(2);
    col->append_datum(Datum(Slice(std::string(300, 'v'))));
    col->append_datum(Datum(Slice("w")));
    expect_size_pass_matches_serialize_batch(*col);
}

// _has_null is what serialize() honours; a set bit in the null mask means nothing without it (see
// NullableColumn::is_null()). serialize_size() used to read the mask alone, which made the size
// pass disagree with the bytes written for such a column.
// NOLINTNEXTLINE
PARALLEL_TEST(SerializeSizeCompactBatchTest, nullable_mask_bit_without_has_null) {
    auto col = NullableColumn::create(Int64Column::create(), NullColumn::create());
    for (int64_t v : {1, 2, 3}) col->append_datum(v);
    col->null_column_data()[1] = 1; // bypasses set_has_null(): _has_null stays false
    ASSERT_FALSE(col->has_null());
    expect_size_pass_matches_serialize_batch(*col);
}

// The same state one level down, where the per-row default reaches NullableColumn::serialize_size().
// NOLINTNEXTLINE
PARALLEL_TEST(SerializeSizeCompactBatchTest, array_element_mask_bit_without_has_null) {
    auto elements = NullableColumn::create(Int32Column::create(), NullColumn::create());
    auto* raw_elements = elements.get();
    auto col = ArrayColumn::create(std::move(elements), UInt32Column::create());
    col->append_datum(DatumArray{Datum(int32_t{1}), Datum(int32_t{2})});
    col->append_datum(DatumArray{Datum(int32_t{3})});
    raw_elements->null_column_data()[1] = 1; // bypasses set_has_null()
    ASSERT_FALSE(raw_elements->has_null());
    expect_size_pass_matches_serialize_batch(*col);
}

} // namespace starrocks
