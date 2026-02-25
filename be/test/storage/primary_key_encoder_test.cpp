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

#include "storage/primary_key_encoder.h"

#include <gtest/gtest.h>

#include <limits>
#include <memory>

#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/schema.h"
#include "gutil/stringprintf.h"
#include "storage/chunk_helper.h"
#include "types/date_value.h"
#include "types/datum.h"

using namespace std;

namespace starrocks {

static unique_ptr<Schema> create_key_schema(const vector<LogicalType>& types) {
    Fields fields;
    std::vector<ColumnId> sort_key_idxes(types.size());
    for (int i = 0; i < types.size(); i++) {
        string name = StringPrintf("col%d", i);
        auto fd = new Field(i, name, types[i], false);
        fd->set_is_key(true);
        fd->set_aggregate_method(STORAGE_AGGREGATE_NONE);
        fd->set_uid(i);
        fields.emplace_back(fd);
        sort_key_idxes[i] = i;
    }
    return std::make_unique<Schema>(std::move(fields), PRIMARY_KEYS, sort_key_idxes);
}

TEST(PrimaryKeyEncoderTest, testEncodeInt32) {
    auto sc = create_key_schema({TYPE_INT});
    MutableColumnPtr dest;
    PrimaryKeyEncoder::create_column(*sc, &dest, PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1);
    const int n = 1000;
    auto pchunk = ChunkHelper::new_chunk(*sc, n);
    for (int i = 0; i < n; i++) {
        Datum tmp;
        tmp.set_int32(i * 2343);
        pchunk->mutable_columns()[0]->append_datum(tmp);
    }
    PrimaryKeyEncoder::encode(*sc, *pchunk, 0, n, dest.get(), PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1);
    auto dchunk = pchunk->clone_empty_with_schema();
    PrimaryKeyEncoder::decode(*sc, *dest, 0, n, dchunk.get(), PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1);
    ASSERT_EQ(pchunk->num_rows(), dchunk->num_rows());
    for (int i = 0; i < n; i++) {
        ASSERT_EQ(pchunk->get_column_by_index(0)->get(i).get_int32(),
                  dchunk->get_column_by_index(0)->get(i).get_int32());
    }
}

TEST(PrimaryKeyEncoderTest, testEncodeInt128) {
    auto sc = create_key_schema({TYPE_LARGEINT});
    MutableColumnPtr dest;
    PrimaryKeyEncoder::create_column(*sc, &dest, PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1);
    const int n = 1000;
    auto pchunk = ChunkHelper::new_chunk(*sc, n);
    for (int i = 0; i < n; i++) {
        Datum tmp;
        tmp.set_int128(i * 2343);
        pchunk->mutable_columns()[0]->append_datum(tmp);
    }
    vector<uint32_t> indexes;
    for (int i = 0; i < n; i++) {
        indexes.emplace_back(i);
    }
    PrimaryKeyEncoder::encode_selective(*sc, *pchunk, indexes.data(), n, dest.get(),
                                        PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1);
    auto dchunk = pchunk->clone_empty_with_schema();
    PrimaryKeyEncoder::decode(*sc, *dest, 0, n, dchunk.get(), PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1);
    ASSERT_EQ(pchunk->num_rows(), dchunk->num_rows());
    for (int i = 0; i < n; i++) {
        ASSERT_EQ(pchunk->get_column_by_index(0)->get(i).get_int128(),
                  dchunk->get_column_by_index(0)->get(i).get_int128());
    }
}

TEST(PrimaryKeyEncoderTest, testEncodeComposite) {
    auto sc = create_key_schema({TYPE_INT, TYPE_VARCHAR, TYPE_SMALLINT, TYPE_BOOLEAN});
    MutableColumnPtr dest;
    PrimaryKeyEncoder::create_column(*sc, &dest, PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1);
    const int n = 1;
    auto pchunk = ChunkHelper::new_chunk(*sc, n);
    for (int i = 0; i < n; i++) {
        Datum tmp;
        tmp.set_int32(i * 2343);
        pchunk->mutable_columns()[0]->append_datum(tmp);
        string tmpstr = StringPrintf("slice000%d", i * 17);
        if (i % 5 == 0) {
            // set some '\0'
            tmpstr[rand() % tmpstr.size()] = '\0';
        }
        tmp.set_slice(tmpstr);
        pchunk->mutable_columns()[1]->append_datum(tmp);
        tmp.set_int16(i);
        pchunk->mutable_columns()[2]->append_datum(tmp);
        tmp.set_uint8(i % 2);
        pchunk->mutable_columns()[3]->append_datum(tmp);
    }
    vector<uint32_t> indexes;
    for (int i = 0; i < n; i++) {
        indexes.emplace_back(i);
    }
    PrimaryKeyEncoder::encode_selective(*sc, *pchunk, indexes.data(), n, dest.get(),
                                        PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1);
    auto dchunk = pchunk->clone_empty_with_schema();
    PrimaryKeyEncoder::decode(*sc, *dest, 0, n, dchunk.get(), PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1);
    ASSERT_EQ(pchunk->num_rows(), dchunk->num_rows());
    for (int i = 0; i < n; i++) {
        ASSERT_EQ(pchunk->get_column_by_index(0)->get(i).get_int32(),
                  dchunk->get_column_by_index(0)->get(i).get_int32());
        ASSERT_EQ(pchunk->get_column_by_index(1)->get(i).get_slice(),
                  dchunk->get_column_by_index(1)->get(i).get_slice());
        ASSERT_EQ(pchunk->get_column_by_index(2)->get(i).get_int16(),
                  dchunk->get_column_by_index(2)->get(i).get_int16());
        ASSERT_EQ(pchunk->get_column_by_index(3)->get(i).get<uint8>(),
                  dchunk->get_column_by_index(3)->get(i).get<uint8>());
    }
}

TEST(PrimaryKeyEncoderTest, testEncodeCompositeLimit) {
    {
        auto sc = create_key_schema({TYPE_INT, TYPE_VARCHAR, TYPE_SMALLINT, TYPE_BOOLEAN});
        const int n = 1;
        auto pchunk = ChunkHelper::new_chunk(*sc, n);
        Datum tmp;
        tmp.set_int32(42);
        pchunk->mutable_columns()[0]->append_datum(tmp);
        string tmpstr("slice0000");
        tmpstr[tmpstr.size() - 1] = '\0';
        tmp.set_slice(tmpstr);
        pchunk->mutable_columns()[1]->append_datum(tmp);
        tmp.set_int16(10);
        pchunk->mutable_columns()[2]->append_datum(tmp);
        tmp.set_uint8(1);
        pchunk->mutable_columns()[3]->append_datum(tmp);
        EXPECT_TRUE(PrimaryKeyEncoder::encode_exceed_limit(*sc, *pchunk, 0, n, 10,
                                                           PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1));
        EXPECT_FALSE(PrimaryKeyEncoder::encode_exceed_limit(*sc, *pchunk, 0, n, 128,
                                                            PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1));
    }

    {
        auto sc = create_key_schema({TYPE_INT, TYPE_VARCHAR, TYPE_SMALLINT, TYPE_BOOLEAN});
        const int n = 1;
        auto pchunk = ChunkHelper::new_chunk(*sc, n);
        Datum tmp;
        tmp.set_int32(42);
        pchunk->mutable_columns()[0]->append_datum(tmp);
        string tmpstr(128, 's');
        tmpstr[tmpstr.size() - 1] = '\0';
        tmp.set_slice(tmpstr);
        pchunk->mutable_columns()[1]->append_datum(tmp);
        tmp.set_int16(10);
        pchunk->mutable_columns()[2]->append_datum(tmp);
        tmp.set_uint8(1);
        pchunk->mutable_columns()[3]->append_datum(tmp);
        EXPECT_TRUE(PrimaryKeyEncoder::encode_exceed_limit(*sc, *pchunk, 0, n, 128,
                                                           PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1));
    }
}

TEST(PrimaryKeyEncoderTest, testEncodeVarcharLimit) {
    auto sc = create_key_schema({TYPE_VARCHAR});
    const int n = 2;
    {
        auto pchunk = ChunkHelper::new_chunk(*sc, n);
        Datum tmp;
        string tmpstr("slice00000");
        tmp.set_slice(tmpstr);
        pchunk->mutable_columns()[0]->append_datum(tmp);
        tmpstr = "slice000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
                 "0000"
                 "00000000000000000000000000000000000";
        tmp.set_slice(tmpstr);
        pchunk->mutable_columns()[0]->append_datum(tmp);
        EXPECT_TRUE(PrimaryKeyEncoder::encode_exceed_limit(*sc, *pchunk, 0, n, 128,
                                                           PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1));
    }
    {
        auto pchunk = ChunkHelper::new_chunk(*sc, n);
        Datum tmp;
        string tmpstr("slice00000");
        tmp.set_slice(tmpstr);
        pchunk->mutable_columns()[0]->append_datum(tmp);
        tmpstr = "slice00000000000000000000000000000000000";
        tmp.set_slice(tmpstr);
        pchunk->mutable_columns()[0]->append_datum(tmp);
        EXPECT_FALSE(PrimaryKeyEncoder::encode_exceed_limit(*sc, *pchunk, 0, n, 128,
                                                            PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1));
    }
}

TEST(PrimaryKeyEncoderTest, testSingleIntV2EncodingRoundTripAndColumnType) {
    auto sc = create_key_schema({TYPE_INT});
    MutableColumnPtr v1_dest;
    MutableColumnPtr v2_dest;

    ASSERT_TRUE(PrimaryKeyEncoder::create_column(*sc, &v1_dest, PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1).ok());
    ASSERT_TRUE(PrimaryKeyEncoder::create_column(*sc, &v2_dest, PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2).ok());
    ASSERT_FALSE(v1_dest->is_binary());
    ASSERT_TRUE(v2_dest->is_binary());

    auto pchunk = ChunkHelper::new_chunk(*sc, 5);
    std::vector<int32_t> values = {std::numeric_limits<int32_t>::min(), -1, 0, 1, std::numeric_limits<int32_t>::max()};
    for (int32_t v : values) {
        Datum d;
        d.set_int32(v);
        pchunk->mutable_columns()[0]->append_datum(d);
    }

    PrimaryKeyEncoder::encode(*sc, *pchunk, 0, pchunk->num_rows(), v2_dest.get(),
                              PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2);
    auto decoded = pchunk->clone_empty_with_schema();
    ASSERT_TRUE(PrimaryKeyEncoder::decode(*sc, *v2_dest, 0, v2_dest->size(), decoded.get(),
                                          PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2)
                        .ok());
    ASSERT_EQ(pchunk->num_rows(), decoded->num_rows());
    for (int i = 0; i < pchunk->num_rows(); i++) {
        ASSERT_EQ(pchunk->get_column_by_index(0)->get(i).get_int32(),
                  decoded->get_column_by_index(0)->get(i).get_int32());
    }
}

TEST(PrimaryKeyEncoderTest, testEncodedTypeAndFixedSizeByEncodingType) {
    auto single_sc = create_key_schema({TYPE_INT});
    std::vector<ColumnId> key_idxes = {0};

    ASSERT_EQ(PrimaryKeyEncoder::encoded_primary_key_type(*single_sc, key_idxes,
                                                          PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1),
              TYPE_INT);
    ASSERT_EQ(PrimaryKeyEncoder::encoded_primary_key_type(*single_sc, key_idxes,
                                                          PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2),
              TYPE_VARCHAR);
    ASSERT_EQ(PrimaryKeyEncoder::get_encoded_fixed_size(*single_sc, PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1), 4);
    ASSERT_EQ(PrimaryKeyEncoder::get_encoded_fixed_size(*single_sc, PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2), 0);
}

TEST(PrimaryKeyEncoderTest, testV2EncodingPreservesSortOrderForInt) {
    auto sc = create_key_schema({TYPE_INT});
    // Values spanning negative, zero, and positive range, including INT32 boundaries
    std::vector<int32_t> sorted_values = {std::numeric_limits<int32_t>::min(), -1000, -1, 0, 1, 1000,
                                          std::numeric_limits<int32_t>::max()};

    MutableColumnPtr dest;
    ASSERT_TRUE(PrimaryKeyEncoder::create_column(*sc, &dest, PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2).ok());

    auto pchunk = ChunkHelper::new_chunk(*sc, sorted_values.size());
    for (int32_t v : sorted_values) {
        Datum d;
        d.set_int32(v);
        pchunk->mutable_columns()[0]->append_datum(d);
    }
    PrimaryKeyEncoder::encode(*sc, *pchunk, 0, pchunk->num_rows(), dest.get(),
                              PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2);

    // Verify that encoded keys preserve the same order as original values
    auto& bcol = down_cast<BinaryColumn&>(*dest);
    for (size_t i = 1; i < bcol.size(); i++) {
        Slice prev = bcol.get_slice(i - 1);
        Slice curr = bcol.get_slice(i);
        ASSERT_LT(prev.compare(curr), 0) << "Encoded key order violated at index " << i << ": value["
                                         << sorted_values[i - 1] << "] should be < value[" << sorted_values[i] << "]";
    }
}

TEST(PrimaryKeyEncoderTest, testV2EncodingPreservesSortOrderForBigint) {
    auto sc = create_key_schema({TYPE_BIGINT});
    std::vector<int64_t> sorted_values = {std::numeric_limits<int64_t>::min(), -1, 0, 1,
                                          std::numeric_limits<int64_t>::max()};

    MutableColumnPtr dest;
    ASSERT_TRUE(PrimaryKeyEncoder::create_column(*sc, &dest, PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2).ok());

    auto pchunk = ChunkHelper::new_chunk(*sc, sorted_values.size());
    for (int64_t v : sorted_values) {
        Datum d;
        d.set_int64(v);
        pchunk->mutable_columns()[0]->append_datum(d);
    }
    PrimaryKeyEncoder::encode(*sc, *pchunk, 0, pchunk->num_rows(), dest.get(),
                              PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2);

    auto& bcol = down_cast<BinaryColumn&>(*dest);
    for (size_t i = 1; i < bcol.size(); i++) {
        Slice prev = bcol.get_slice(i - 1);
        Slice curr = bcol.get_slice(i);
        ASSERT_LT(prev.compare(curr), 0) << "Encoded key order violated at index " << i << ": value["
                                         << sorted_values[i - 1] << "] should be < value[" << sorted_values[i] << "]";
    }
}

TEST(PrimaryKeyEncoderTest, testEncodingTypeMappingFallback) {
    auto invalid_pb = static_cast<PrimaryKeyEncodingTypePB>(9999);
    auto invalid_internal = static_cast<PrimaryKeyEncodingType>(9999);

    ASSERT_EQ(PrimaryKeyEncoder::encoding_type_from_pb(invalid_pb), PrimaryKeyEncodingType::PK_ENCODING_TYPE_NONE);
    ASSERT_EQ(PrimaryKeyEncoder::pb_from_encoding_type(invalid_internal),
              PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_NONE);
}

// ======================== V2 encoding tests ========================

// Helper: encode with V2, then verify encoded keys preserve sort order via byte comparison
static void verify_v2_sort_order_preserved(const Schema& schema, const Chunk& chunk, size_t n) {
    MutableColumnPtr dest;
    ASSERT_TRUE(PrimaryKeyEncoder::create_column(schema, &dest, PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2).ok());
    PrimaryKeyEncoder::encode(schema, chunk, 0, n, dest.get(), PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2);
    ASSERT_TRUE(dest->is_binary());
    auto& bcol = down_cast<BinaryColumn&>(*dest);
    ASSERT_EQ(bcol.size(), n);
    for (size_t i = 1; i < bcol.size(); i++) {
        Slice prev = bcol.get_slice(i - 1);
        Slice curr = bcol.get_slice(i);
        ASSERT_LT(prev.compare(curr), 0) << "Encoded key order violated at index " << i;
    }
}

// Helper: encode with V2, decode, and verify round-trip correctness
static void verify_v2_round_trip(const Schema& schema, const Chunk& original) {
    size_t n = original.num_rows();
    MutableColumnPtr dest;
    ASSERT_TRUE(PrimaryKeyEncoder::create_column(schema, &dest, PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2).ok());
    PrimaryKeyEncoder::encode(schema, original, 0, n, dest.get(), PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2);
    auto decoded = original.clone_empty_with_schema();
    ASSERT_TRUE(
            PrimaryKeyEncoder::decode(schema, *dest, 0, n, decoded.get(), PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2)
                    .ok());
    ASSERT_EQ(n, decoded->num_rows());
    for (size_t col = 0; col < schema.num_key_fields(); col++) {
        for (size_t row = 0; row < n; row++) {
            ASSERT_EQ(original.get_column_by_index(col)->get(row), decoded->get_column_by_index(col)->get(row))
                    << "Mismatch at col=" << col << " row=" << row;
        }
    }
}

TEST(PrimaryKeyEncoderTest, testV2EncodingPreservesSortOrderForBoolean) {
    auto sc = create_key_schema({TYPE_BOOLEAN});
    auto pchunk = ChunkHelper::new_chunk(*sc, 2);
    // false(0) < true(1)
    for (uint8_t v : {(uint8_t)0, (uint8_t)1}) {
        Datum d;
        d.set_uint8(v);
        pchunk->mutable_columns()[0]->append_datum(d);
    }
    verify_v2_sort_order_preserved(*sc, *pchunk, 2);
    verify_v2_round_trip(*sc, *pchunk);
}

TEST(PrimaryKeyEncoderTest, testV2EncodingPreservesSortOrderForTinyint) {
    auto sc = create_key_schema({TYPE_TINYINT});
    std::vector<int8_t> sorted_values = {std::numeric_limits<int8_t>::min(), -1, 0, 1,
                                         std::numeric_limits<int8_t>::max()};
    auto pchunk = ChunkHelper::new_chunk(*sc, sorted_values.size());
    for (int8_t v : sorted_values) {
        Datum d;
        d.set_int8(v);
        pchunk->mutable_columns()[0]->append_datum(d);
    }
    verify_v2_sort_order_preserved(*sc, *pchunk, sorted_values.size());
    verify_v2_round_trip(*sc, *pchunk);
}

TEST(PrimaryKeyEncoderTest, testV2EncodingPreservesSortOrderForSmallint) {
    auto sc = create_key_schema({TYPE_SMALLINT});
    std::vector<int16_t> sorted_values = {std::numeric_limits<int16_t>::min(), -1, 0, 1,
                                          std::numeric_limits<int16_t>::max()};
    auto pchunk = ChunkHelper::new_chunk(*sc, sorted_values.size());
    for (int16_t v : sorted_values) {
        Datum d;
        d.set_int16(v);
        pchunk->mutable_columns()[0]->append_datum(d);
    }
    verify_v2_sort_order_preserved(*sc, *pchunk, sorted_values.size());
    verify_v2_round_trip(*sc, *pchunk);
}

TEST(PrimaryKeyEncoderTest, testV2EncodingPreservesSortOrderForLargeint) {
    auto sc = create_key_schema({TYPE_LARGEINT});
    std::vector<int128_t> sorted_values = {std::numeric_limits<int128_t>::min(), static_cast<int128_t>(-1),
                                           static_cast<int128_t>(0), static_cast<int128_t>(1),
                                           std::numeric_limits<int128_t>::max()};
    auto pchunk = ChunkHelper::new_chunk(*sc, sorted_values.size());
    for (int128_t v : sorted_values) {
        Datum d;
        d.set_int128(v);
        pchunk->mutable_columns()[0]->append_datum(d);
    }
    verify_v2_sort_order_preserved(*sc, *pchunk, sorted_values.size());
    verify_v2_round_trip(*sc, *pchunk);
}

TEST(PrimaryKeyEncoderTest, testV2EncodingPreservesSortOrderForDate) {
    auto sc = create_key_schema({TYPE_DATE});
    // DateValue stores julian day as int32_t internally
    auto pchunk = ChunkHelper::new_chunk(*sc, 3);
    // 2000-01-01, 2020-06-15, 2025-12-31
    std::vector<std::string> date_strings = {"2000-01-01", "2020-06-15", "2025-12-31"};
    for (const auto& s : date_strings) {
        DateValue dv;
        dv.from_string(s.c_str(), s.size());
        Datum d;
        d.set_date(dv);
        pchunk->mutable_columns()[0]->append_datum(d);
    }
    verify_v2_sort_order_preserved(*sc, *pchunk, 3);
    verify_v2_round_trip(*sc, *pchunk);
}

TEST(PrimaryKeyEncoderTest, testV2EncodingPreservesSortOrderForDatetime) {
    auto sc = create_key_schema({TYPE_DATETIME});
    auto pchunk = ChunkHelper::new_chunk(*sc, 3);
    // TimestampValue stores microseconds as int64_t internally
    std::vector<std::string> datetime_strings = {"2000-01-01 00:00:00", "2020-06-15 12:30:45", "2025-12-31 23:59:59"};
    for (const auto& s : datetime_strings) {
        TimestampValue tv;
        tv.from_string(s.c_str(), s.size());
        Datum d;
        d.set_timestamp(tv);
        pchunk->mutable_columns()[0]->append_datum(d);
    }
    verify_v2_sort_order_preserved(*sc, *pchunk, 3);
    verify_v2_round_trip(*sc, *pchunk);
}

TEST(PrimaryKeyEncoderTest, testV2EncodingRoundTripForSingleVarchar) {
    auto sc = create_key_schema({TYPE_VARCHAR});
    auto pchunk = ChunkHelper::new_chunk(*sc, 4);
    std::vector<std::string> values = {"", "abc", "hello world", std::string("with\0null", 9)};
    for (const auto& s : values) {
        Datum d;
        d.set_slice(Slice(s));
        pchunk->mutable_columns()[0]->append_datum(d);
    }
    verify_v2_round_trip(*sc, *pchunk);
}

TEST(PrimaryKeyEncoderTest, testV2EncodingPreservesSortOrderForSingleVarchar) {
    auto sc = create_key_schema({TYPE_VARCHAR});
    auto pchunk = ChunkHelper::new_chunk(*sc, 4);
    // Lexicographic order: "" < "aaa" < "aab" < "b"
    std::vector<std::string> sorted_values = {"", "aaa", "aab", "b"};
    for (const auto& s : sorted_values) {
        Datum d;
        d.set_slice(Slice(s));
        pchunk->mutable_columns()[0]->append_datum(d);
    }
    verify_v2_sort_order_preserved(*sc, *pchunk, sorted_values.size());
}

TEST(PrimaryKeyEncoderTest, testV2EncodingRoundTripForCompositeIntVarchar) {
    auto sc = create_key_schema({TYPE_INT, TYPE_VARCHAR});
    const int n = 10;
    auto pchunk = ChunkHelper::new_chunk(*sc, n);
    for (int i = 0; i < n; i++) {
        Datum d_int;
        d_int.set_int32(i * 100 - 500);
        pchunk->mutable_columns()[0]->append_datum(d_int);

        Datum d_str;
        std::string s = StringPrintf("key_%04d", i);
        if (i % 3 == 0) {
            // inject \0 byte to test escape handling
            s[2] = '\0';
        }
        d_str.set_slice(Slice(s));
        pchunk->mutable_columns()[1]->append_datum(d_str);
    }
    verify_v2_round_trip(*sc, *pchunk);
}

TEST(PrimaryKeyEncoderTest, testV2EncodingPreservesSortOrderForCompositeIntInt) {
    auto sc = create_key_schema({TYPE_INT, TYPE_BIGINT});
    // Pairs in ascending order: (int, bigint)
    auto pchunk = ChunkHelper::new_chunk(*sc, 5);
    std::vector<std::pair<int32_t, int64_t>> sorted_pairs = {{-100, -1000}, {-100, 0}, {0, -1}, {0, 0}, {100, 999}};
    for (const auto& [v1, v2] : sorted_pairs) {
        Datum d1, d2;
        d1.set_int32(v1);
        d2.set_int64(v2);
        pchunk->mutable_columns()[0]->append_datum(d1);
        pchunk->mutable_columns()[1]->append_datum(d2);
    }
    verify_v2_sort_order_preserved(*sc, *pchunk, sorted_pairs.size());
    verify_v2_round_trip(*sc, *pchunk);
}

TEST(PrimaryKeyEncoderTest, testV2EncodingPreservesSortOrderForCompositeVarcharInt) {
    auto sc = create_key_schema({TYPE_VARCHAR, TYPE_INT});
    auto pchunk = ChunkHelper::new_chunk(*sc, 4);
    // Composite order: ("aaa", 1) < ("aaa", 2) < ("aab", -1) < ("b", 0)
    std::vector<std::pair<std::string, int32_t>> sorted_pairs = {{"aaa", 1}, {"aaa", 2}, {"aab", -1}, {"b", 0}};
    for (const auto& [s, v] : sorted_pairs) {
        Datum d_str, d_int;
        d_str.set_slice(Slice(s));
        d_int.set_int32(v);
        pchunk->mutable_columns()[0]->append_datum(d_str);
        pchunk->mutable_columns()[1]->append_datum(d_int);
    }
    verify_v2_sort_order_preserved(*sc, *pchunk, sorted_pairs.size());
    verify_v2_round_trip(*sc, *pchunk);
}

TEST(PrimaryKeyEncoderTest, testV2EncodingRoundTripForCompositeAllTypes) {
    auto sc = create_key_schema({TYPE_BOOLEAN, TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT, TYPE_VARCHAR});
    const int n = 5;
    auto pchunk = ChunkHelper::new_chunk(*sc, n);
    for (int i = 0; i < n; i++) {
        Datum d;
        d.set_uint8(i % 2);
        pchunk->mutable_columns()[0]->append_datum(d);

        d.set_int8(static_cast<int8_t>(i * 10 - 20));
        pchunk->mutable_columns()[1]->append_datum(d);

        d.set_int16(static_cast<int16_t>(i * 100 - 200));
        pchunk->mutable_columns()[2]->append_datum(d);

        d.set_int32(i * 1000 - 2000);
        pchunk->mutable_columns()[3]->append_datum(d);

        d.set_int64(static_cast<int64_t>(i) * 10000 - 20000);
        pchunk->mutable_columns()[4]->append_datum(d);

        std::string s = StringPrintf("val_%d", i);
        d.set_slice(Slice(s));
        pchunk->mutable_columns()[5]->append_datum(d);
    }
    verify_v2_round_trip(*sc, *pchunk);
}

TEST(PrimaryKeyEncoderTest, testV2EncodeSelectiveRoundTrip) {
    auto sc = create_key_schema({TYPE_INT, TYPE_VARCHAR});
    const int n = 6;
    auto pchunk = ChunkHelper::new_chunk(*sc, n);
    for (int i = 0; i < n; i++) {
        Datum d_int;
        d_int.set_int32(i * 111);
        pchunk->mutable_columns()[0]->append_datum(d_int);
        Datum d_str;
        std::string s = StringPrintf("s%d", i);
        d_str.set_slice(Slice(s));
        pchunk->mutable_columns()[1]->append_datum(d_str);
    }

    // Select only even indices: 0, 2, 4
    std::vector<uint32_t> indexes = {0, 2, 4};
    MutableColumnPtr dest;
    ASSERT_TRUE(PrimaryKeyEncoder::create_column(*sc, &dest, PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2).ok());
    PrimaryKeyEncoder::encode_selective(*sc, *pchunk, indexes.data(), indexes.size(), dest.get(),
                                        PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2);
    ASSERT_EQ(dest->size(), 3);

    auto decoded = pchunk->clone_empty_with_schema();
    ASSERT_TRUE(PrimaryKeyEncoder::decode(*sc, *dest, 0, 3, decoded.get(), PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2)
                        .ok());
    for (int i = 0; i < 3; i++) {
        int orig_idx = indexes[i];
        ASSERT_EQ(pchunk->get_column_by_index(0)->get(orig_idx).get_int32(),
                  decoded->get_column_by_index(0)->get(i).get_int32());
        ASSERT_EQ(pchunk->get_column_by_index(1)->get(orig_idx).get_slice(),
                  decoded->get_column_by_index(1)->get(i).get_slice());
    }
}

TEST(PrimaryKeyEncoderTest, testV2EncodeExceedLimitForComposite) {
    auto sc = create_key_schema({TYPE_INT, TYPE_VARCHAR, TYPE_SMALLINT});
    const int n = 1;
    auto pchunk = ChunkHelper::new_chunk(*sc, n);
    Datum d;
    d.set_int32(42);
    pchunk->mutable_columns()[0]->append_datum(d);
    // VARCHAR with \0 byte to test escape overhead calculation
    std::string s(100, 'x');
    s[50] = '\0';
    d.set_slice(Slice(s));
    pchunk->mutable_columns()[1]->append_datum(d);
    d.set_int16(10);
    pchunk->mutable_columns()[2]->append_datum(d);

    // 4 (int) + 100 (varchar) + 1 (escape for \0) + 2 (separator) + 2 (smallint) = 109
    EXPECT_FALSE(PrimaryKeyEncoder::encode_exceed_limit(*sc, *pchunk, 0, n, 128,
                                                        PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2));
    EXPECT_TRUE(PrimaryKeyEncoder::encode_exceed_limit(*sc, *pchunk, 0, n, 10,
                                                       PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2));
}

} // namespace starrocks
