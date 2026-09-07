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

#include "storage_primitive/primary_key_encoder.h"

#include <gtest/gtest.h>

#include <limits>
#include <memory>

#include "base/utility/defer_op.h"
#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/chunk_factory.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/schema.h"
#include "common/config_local_io_fwd.h"
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

static BinaryColumn::MutablePtr make_unrepresentable_binary_column() {
    const bool old_zero_copy = config::enable_zero_copy_from_page_cache;
    config::enable_zero_copy_from_page_cache = true;
    DeferOp restore_zero_copy([old_zero_copy] { config::enable_zero_copy_from_page_cache = old_zero_copy; });

    auto owner = std::make_shared<std::string>("x");
    ContainerResource resource(owner, owner->data(), Column::MAX_CAPACITY_LIMIT);
    BinaryColumn::Offsets offsets;
    offsets.emplace_back(0);
    offsets.emplace_back(Column::MAX_CAPACITY_LIMIT);
    return BinaryColumn::create(std::move(resource), std::move(offsets));
}

TEST(PrimaryKeyEncoderTest, testEncodeInt32) {
    auto sc = create_key_schema({TYPE_INT});
    MutableColumnPtr dest;
    PrimaryKeyEncoder::create_column(*sc, &dest, PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1);
    const int n = 1000;
    auto pchunk = ChunkFactory::new_chunk(*sc, n);
    for (int i = 0; i < n; i++) {
        Datum tmp;
        tmp.set_int32(i * 2343);
        pchunk->columns()[0]->as_mutable_ptr()->append_datum(tmp);
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
    auto pchunk = ChunkFactory::new_chunk(*sc, n);
    for (int i = 0; i < n; i++) {
        Datum tmp;
        tmp.set_int128(i * 2343);
        pchunk->columns()[0]->as_mutable_ptr()->append_datum(tmp);
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
    auto pchunk = ChunkFactory::new_chunk(*sc, n);
    for (int i = 0; i < n; i++) {
        Datum tmp;
        tmp.set_int32(i * 2343);
        pchunk->columns()[0]->as_mutable_ptr()->append_datum(tmp);
        string tmpstr = StringPrintf("slice000%d", i * 17);
        if (i % 5 == 0) {
            // set some '\0'
            tmpstr[rand() % tmpstr.size()] = '\0';
        }
        tmp.set_slice(tmpstr);
        pchunk->columns()[1]->as_mutable_ptr()->append_datum(tmp);
        tmp.set_int16(i);
        pchunk->columns()[2]->as_mutable_ptr()->append_datum(tmp);
        tmp.set_uint8(i % 2);
        pchunk->columns()[3]->as_mutable_ptr()->append_datum(tmp);
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

TEST(PrimaryKeyEncoderTest, testDecodeCompositeWithFastFlag) {
    auto sc = create_key_schema({TYPE_INT, TYPE_VARCHAR, TYPE_SMALLINT});
    MutableColumnPtr dest;
    ASSERT_TRUE(PrimaryKeyEncoder::create_column(*sc, &dest, PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1).ok());

    auto pchunk = ChunkFactory::new_chunk(*sc, 1);
    Datum tmp;
    tmp.set_int32(42);
    pchunk->columns()[0]->as_mutable_ptr()->append_datum(tmp);
    tmp.set_slice("plain-string");
    pchunk->columns()[1]->as_mutable_ptr()->append_datum(tmp);
    tmp.set_int16(7);
    pchunk->columns()[2]->as_mutable_ptr()->append_datum(tmp);

    PrimaryKeyEncoder::encode(*sc, *pchunk, 0, 1, dest.get(), PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1);

    std::vector<uint8_t> value_encode_flags(1, PRIMARY_KEY_DECODE_FAST);
    auto dchunk = pchunk->clone_empty_with_schema();
    ASSERT_TRUE(PrimaryKeyEncoder::decode(*sc, *dest, 0, 1, dchunk.get(), PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1,
                                          &value_encode_flags)
                        .ok());

    ASSERT_EQ(1, dchunk->num_rows());
    ASSERT_EQ(42, dchunk->get_column_by_index(0)->get(0).get_int32());
    ASSERT_EQ(Slice("plain-string"), dchunk->get_column_by_index(1)->get(0).get_slice());
    ASSERT_EQ(7, dchunk->get_column_by_index(2)->get(0).get_int16());
}

TEST(PrimaryKeyEncoderTest, testDecodeCompositeWithSkipFlag) {
    auto sc = create_key_schema({TYPE_INT, TYPE_VARCHAR, TYPE_SMALLINT});
    MutableColumnPtr dest;
    ASSERT_TRUE(PrimaryKeyEncoder::create_column(*sc, &dest, PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1).ok());

    auto pchunk = ChunkFactory::new_chunk(*sc, 1);
    Datum tmp;
    tmp.set_int32(42);
    pchunk->columns()[0]->as_mutable_ptr()->append_datum(tmp);
    tmp.set_slice("plain-string");
    pchunk->columns()[1]->as_mutable_ptr()->append_datum(tmp);
    tmp.set_int16(7);
    pchunk->columns()[2]->as_mutable_ptr()->append_datum(tmp);

    PrimaryKeyEncoder::encode(*sc, *pchunk, 0, 1, dest.get(), PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1);

    std::vector<uint8_t> value_encode_flags(1, PRIMARY_KEY_DECODE_SKIP);
    auto dchunk = pchunk->clone_empty_with_schema();
    ASSERT_TRUE(PrimaryKeyEncoder::decode(*sc, *dest, 0, 1, dchunk.get(), PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1,
                                          &value_encode_flags)
                        .ok());

    ASSERT_EQ(1, dchunk->num_rows());
    ASSERT_EQ(0, dchunk->get_column_by_index(0)->get(0).get_int32());
    ASSERT_EQ(0, dchunk->get_column_by_index(1)->get(0).get_slice().size);
    ASSERT_EQ(0, dchunk->get_column_by_index(2)->get(0).get_int16());
}

TEST(PrimaryKeyEncoderTest, testEncodeCompositeLimit) {
    {
        auto sc = create_key_schema({TYPE_INT, TYPE_VARCHAR, TYPE_SMALLINT, TYPE_BOOLEAN});
        const int n = 1;
        auto pchunk = ChunkFactory::new_chunk(*sc, n);
        Datum tmp;
        tmp.set_int32(42);
        pchunk->columns()[0]->as_mutable_ptr()->append_datum(tmp);
        string tmpstr("slice0000");
        tmpstr[tmpstr.size() - 1] = '\0';
        tmp.set_slice(tmpstr);
        pchunk->columns()[1]->as_mutable_ptr()->append_datum(tmp);
        tmp.set_int16(10);
        pchunk->columns()[2]->as_mutable_ptr()->append_datum(tmp);
        tmp.set_uint8(1);
        pchunk->columns()[3]->as_mutable_ptr()->append_datum(tmp);
        EXPECT_TRUE(PrimaryKeyEncoder::encode_exceed_limit(*sc, *pchunk, 0, n, 10,
                                                           PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1));
        EXPECT_FALSE(PrimaryKeyEncoder::encode_exceed_limit(*sc, *pchunk, 0, n, 128,
                                                            PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1));
    }

    {
        auto sc = create_key_schema({TYPE_INT, TYPE_VARCHAR, TYPE_SMALLINT, TYPE_BOOLEAN});
        const int n = 1;
        auto pchunk = ChunkFactory::new_chunk(*sc, n);
        Datum tmp;
        tmp.set_int32(42);
        pchunk->columns()[0]->as_mutable_ptr()->append_datum(tmp);
        string tmpstr(128, 's');
        tmpstr[tmpstr.size() - 1] = '\0';
        tmp.set_slice(tmpstr);
        pchunk->columns()[1]->as_mutable_ptr()->append_datum(tmp);
        tmp.set_int16(10);
        pchunk->columns()[2]->as_mutable_ptr()->append_datum(tmp);
        tmp.set_uint8(1);
        pchunk->columns()[3]->as_mutable_ptr()->append_datum(tmp);
        EXPECT_TRUE(PrimaryKeyEncoder::encode_exceed_limit(*sc, *pchunk, 0, n, 128,
                                                           PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1));
    }
}

TEST(PrimaryKeyEncoderTest, testEncodeVarcharLimit) {
    auto sc = create_key_schema({TYPE_VARCHAR});
    const int n = 2;
    {
        auto pchunk = ChunkFactory::new_chunk(*sc, n);
        Datum tmp;
        string tmpstr("slice00000");
        tmp.set_slice(tmpstr);
        pchunk->columns()[0]->as_mutable_ptr()->append_datum(tmp);
        tmpstr = "slice000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
                 "0000"
                 "00000000000000000000000000000000000";
        tmp.set_slice(tmpstr);
        pchunk->columns()[0]->as_mutable_ptr()->append_datum(tmp);
        EXPECT_TRUE(PrimaryKeyEncoder::encode_exceed_limit(*sc, *pchunk, 0, n, 128,
                                                           PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1));
    }
    {
        auto pchunk = ChunkFactory::new_chunk(*sc, n);
        Datum tmp;
        string tmpstr("slice00000");
        tmp.set_slice(tmpstr);
        pchunk->columns()[0]->as_mutable_ptr()->append_datum(tmp);
        tmpstr = "slice00000000000000000000000000000000000";
        tmp.set_slice(tmpstr);
        pchunk->columns()[0]->as_mutable_ptr()->append_datum(tmp);
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

    auto pchunk = ChunkFactory::new_chunk(*sc, 5);
    std::vector<int32_t> values = {std::numeric_limits<int32_t>::min(), -1, 0, 1, std::numeric_limits<int32_t>::max()};
    for (int32_t v : values) {
        Datum d;
        d.set_int32(v);
        pchunk->columns()[0]->as_mutable_ptr()->append_datum(d);
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

TEST(PrimaryKeyEncoderTest, testDeleteFileBinaryColumnSizeCheck) {
    std::vector<Slice> strings{{"a"}, {"bb"}, {"ccc"}};

    auto binary = BinaryColumn::create();
    binary->append_strings(strings.data(), strings.size());
    ASSERT_TRUE(PrimaryKeyEncoder::check_delete_file_binary_column_size(*binary).ok());

    auto sticky_large = BinaryColumn::create();
    sticky_large->append_strings(strings.data(), strings.size());
    AdaptiveOffsets::Large large_offsets;
    large_offsets.resize(sticky_large->get_offset().size());
    for (size_t i = 0; i < sticky_large->get_offset().size(); ++i) {
        large_offsets[i] = sticky_large->get_offset()[i];
    }
    sticky_large->get_offset().set_large_buffer(std::move(large_offsets));
    ASSERT_TRUE(sticky_large->get_offset().is_large());
    ASSERT_TRUE(PrimaryKeyEncoder::check_delete_file_binary_column_size(*sticky_large).ok());

    auto nullable_data = BinaryColumn::create();
    nullable_data->append_strings(strings.data(), strings.size());
    auto nullable = NullableColumn::create(std::move(nullable_data), NullColumn::create(strings.size(), 0));
    ASSERT_TRUE(PrimaryKeyEncoder::check_delete_file_binary_column_size(*nullable).ok());

    auto large_binary = LargeBinaryColumn::create();
    large_binary->append_strings(strings.data(), strings.size());
    ASSERT_FALSE(PrimaryKeyEncoder::check_delete_file_binary_column_size(*large_binary).ok());

    auto overlimit_binary = make_unrepresentable_binary_column();
    ASSERT_FALSE(overlimit_binary->is_payload_size_representable().ok());
    auto overlimit_status = PrimaryKeyEncoder::check_delete_file_binary_column_size(*overlimit_binary);
    ASSERT_FALSE(overlimit_status.ok());
    ASSERT_TRUE(overlimit_status.is_capacity_limit_exceeded()) << overlimit_status;
    ASSERT_NE(std::string::npos, std::string(overlimit_status.message()).find("byte payload size")) << overlimit_status;

    auto fixed = Int32Column::create();
    int32_t value = 1;
    fixed->append_numbers(&value, sizeof(value));
    ASSERT_TRUE(PrimaryKeyEncoder::check_delete_file_binary_column_size(*fixed).ok());
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

    auto pchunk = ChunkFactory::new_chunk(*sc, sorted_values.size());
    for (int32_t v : sorted_values) {
        Datum d;
        d.set_int32(v);
        pchunk->columns()[0]->as_mutable_ptr()->append_datum(d);
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

    auto pchunk = ChunkFactory::new_chunk(*sc, sorted_values.size());
    for (int64_t v : sorted_values) {
        Datum d;
        d.set_int64(v);
        pchunk->columns()[0]->as_mutable_ptr()->append_datum(d);
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
    auto pchunk = ChunkFactory::new_chunk(*sc, 2);
    // false(0) < true(1)
    for (uint8_t v : {(uint8_t)0, (uint8_t)1}) {
        Datum d;
        d.set_uint8(v);
        pchunk->columns()[0]->as_mutable_ptr()->append_datum(d);
    }
    verify_v2_sort_order_preserved(*sc, *pchunk, 2);
    verify_v2_round_trip(*sc, *pchunk);
}

TEST(PrimaryKeyEncoderTest, testV2EncodingPreservesSortOrderForTinyint) {
    auto sc = create_key_schema({TYPE_TINYINT});
    std::vector<int8_t> sorted_values = {std::numeric_limits<int8_t>::min(), -1, 0, 1,
                                         std::numeric_limits<int8_t>::max()};
    auto pchunk = ChunkFactory::new_chunk(*sc, sorted_values.size());
    for (int8_t v : sorted_values) {
        Datum d;
        d.set_int8(v);
        pchunk->columns()[0]->as_mutable_ptr()->append_datum(d);
    }
    verify_v2_sort_order_preserved(*sc, *pchunk, sorted_values.size());
    verify_v2_round_trip(*sc, *pchunk);
}

TEST(PrimaryKeyEncoderTest, testV2EncodingPreservesSortOrderForSmallint) {
    auto sc = create_key_schema({TYPE_SMALLINT});
    std::vector<int16_t> sorted_values = {std::numeric_limits<int16_t>::min(), -1, 0, 1,
                                          std::numeric_limits<int16_t>::max()};
    auto pchunk = ChunkFactory::new_chunk(*sc, sorted_values.size());
    for (int16_t v : sorted_values) {
        Datum d;
        d.set_int16(v);
        pchunk->columns()[0]->as_mutable_ptr()->append_datum(d);
    }
    verify_v2_sort_order_preserved(*sc, *pchunk, sorted_values.size());
    verify_v2_round_trip(*sc, *pchunk);
}

TEST(PrimaryKeyEncoderTest, testV2EncodingPreservesSortOrderForLargeint) {
    auto sc = create_key_schema({TYPE_LARGEINT});
    std::vector<int128_t> sorted_values = {std::numeric_limits<int128_t>::min(), static_cast<int128_t>(-1),
                                           static_cast<int128_t>(0), static_cast<int128_t>(1),
                                           std::numeric_limits<int128_t>::max()};
    auto pchunk = ChunkFactory::new_chunk(*sc, sorted_values.size());
    for (int128_t v : sorted_values) {
        Datum d;
        d.set_int128(v);
        pchunk->columns()[0]->as_mutable_ptr()->append_datum(d);
    }
    verify_v2_sort_order_preserved(*sc, *pchunk, sorted_values.size());
    verify_v2_round_trip(*sc, *pchunk);
}

TEST(PrimaryKeyEncoderTest, testV2EncodingPreservesSortOrderForDate) {
    auto sc = create_key_schema({TYPE_DATE});
    // DateValue stores julian day as int32_t internally
    auto pchunk = ChunkFactory::new_chunk(*sc, 3);
    // 2000-01-01, 2020-06-15, 2025-12-31
    std::vector<std::string> date_strings = {"2000-01-01", "2020-06-15", "2025-12-31"};
    for (const auto& s : date_strings) {
        DateValue dv;
        dv.from_string(s.c_str(), s.size());
        Datum d;
        d.set_date(dv);
        pchunk->columns()[0]->as_mutable_ptr()->append_datum(d);
    }
    verify_v2_sort_order_preserved(*sc, *pchunk, 3);
    verify_v2_round_trip(*sc, *pchunk);
}

TEST(PrimaryKeyEncoderTest, testV2EncodingPreservesSortOrderForDatetime) {
    auto sc = create_key_schema({TYPE_DATETIME});
    auto pchunk = ChunkFactory::new_chunk(*sc, 3);
    // TimestampValue stores microseconds as int64_t internally
    std::vector<std::string> datetime_strings = {"2000-01-01 00:00:00", "2020-06-15 12:30:45", "2025-12-31 23:59:59"};
    for (const auto& s : datetime_strings) {
        TimestampValue tv;
        tv.from_string(s.c_str(), s.size());
        Datum d;
        d.set_timestamp(tv);
        pchunk->columns()[0]->as_mutable_ptr()->append_datum(d);
    }
    verify_v2_sort_order_preserved(*sc, *pchunk, 3);
    verify_v2_round_trip(*sc, *pchunk);
}

TEST(PrimaryKeyEncoderTest, testV2EncodingRoundTripForSingleVarchar) {
    auto sc = create_key_schema({TYPE_VARCHAR});
    auto pchunk = ChunkFactory::new_chunk(*sc, 4);
    std::vector<std::string> values = {"", "abc", "hello world", std::string("with\0null", 9)};
    for (const auto& s : values) {
        Datum d;
        d.set_slice(Slice(s));
        pchunk->columns()[0]->as_mutable_ptr()->append_datum(d);
    }
    verify_v2_round_trip(*sc, *pchunk);
}

TEST(PrimaryKeyEncoderTest, testV2EncodingPreservesSortOrderForSingleVarchar) {
    auto sc = create_key_schema({TYPE_VARCHAR});
    auto pchunk = ChunkFactory::new_chunk(*sc, 4);
    // Lexicographic order: "" < "aaa" < "aab" < "b"
    std::vector<std::string> sorted_values = {"", "aaa", "aab", "b"};
    for (const auto& s : sorted_values) {
        Datum d;
        d.set_slice(Slice(s));
        pchunk->columns()[0]->as_mutable_ptr()->append_datum(d);
    }
    verify_v2_sort_order_preserved(*sc, *pchunk, sorted_values.size());
}

TEST(PrimaryKeyEncoderTest, testV2EncodingRoundTripForCompositeIntVarchar) {
    auto sc = create_key_schema({TYPE_INT, TYPE_VARCHAR});
    const int n = 10;
    auto pchunk = ChunkFactory::new_chunk(*sc, n);
    for (int i = 0; i < n; i++) {
        Datum d_int;
        d_int.set_int32(i * 100 - 500);
        pchunk->columns()[0]->as_mutable_ptr()->append_datum(d_int);

        Datum d_str;
        std::string s = StringPrintf("key_%04d", i);
        if (i % 3 == 0) {
            // inject \0 byte to test escape handling
            s[2] = '\0';
        }
        d_str.set_slice(Slice(s));
        pchunk->columns()[1]->as_mutable_ptr()->append_datum(d_str);
    }
    verify_v2_round_trip(*sc, *pchunk);
}

TEST(PrimaryKeyEncoderTest, testV2EncodingPreservesSortOrderForCompositeIntInt) {
    auto sc = create_key_schema({TYPE_INT, TYPE_BIGINT});
    // Pairs in ascending order: (int, bigint)
    auto pchunk = ChunkFactory::new_chunk(*sc, 5);
    std::vector<std::pair<int32_t, int64_t>> sorted_pairs = {{-100, -1000}, {-100, 0}, {0, -1}, {0, 0}, {100, 999}};
    for (const auto& [v1, v2] : sorted_pairs) {
        Datum d1, d2;
        d1.set_int32(v1);
        d2.set_int64(v2);
        pchunk->columns()[0]->as_mutable_ptr()->append_datum(d1);
        pchunk->columns()[1]->as_mutable_ptr()->append_datum(d2);
    }
    verify_v2_sort_order_preserved(*sc, *pchunk, sorted_pairs.size());
    verify_v2_round_trip(*sc, *pchunk);
}

TEST(PrimaryKeyEncoderTest, testV2EncodingPreservesSortOrderForCompositeVarcharInt) {
    auto sc = create_key_schema({TYPE_VARCHAR, TYPE_INT});
    auto pchunk = ChunkFactory::new_chunk(*sc, 4);
    // Composite order: ("aaa", 1) < ("aaa", 2) < ("aab", -1) < ("b", 0)
    std::vector<std::pair<std::string, int32_t>> sorted_pairs = {{"aaa", 1}, {"aaa", 2}, {"aab", -1}, {"b", 0}};
    for (const auto& [s, v] : sorted_pairs) {
        Datum d_str, d_int;
        d_str.set_slice(Slice(s));
        d_int.set_int32(v);
        pchunk->columns()[0]->as_mutable_ptr()->append_datum(d_str);
        pchunk->columns()[1]->as_mutable_ptr()->append_datum(d_int);
    }
    verify_v2_sort_order_preserved(*sc, *pchunk, sorted_pairs.size());
    verify_v2_round_trip(*sc, *pchunk);
}

TEST(PrimaryKeyEncoderTest, testV2EncodingRoundTripForCompositeAllTypes) {
    auto sc = create_key_schema({TYPE_BOOLEAN, TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT, TYPE_VARCHAR});
    const int n = 5;
    auto pchunk = ChunkFactory::new_chunk(*sc, n);
    for (int i = 0; i < n; i++) {
        Datum d;
        d.set_uint8(i % 2);
        pchunk->columns()[0]->as_mutable_ptr()->append_datum(d);

        d.set_int8(static_cast<int8_t>(i * 10 - 20));
        pchunk->columns()[1]->as_mutable_ptr()->append_datum(d);

        d.set_int16(static_cast<int16_t>(i * 100 - 200));
        pchunk->columns()[2]->as_mutable_ptr()->append_datum(d);

        d.set_int32(i * 1000 - 2000);
        pchunk->columns()[3]->as_mutable_ptr()->append_datum(d);

        d.set_int64(static_cast<int64_t>(i) * 10000 - 20000);
        pchunk->columns()[4]->as_mutable_ptr()->append_datum(d);

        std::string s = StringPrintf("val_%d", i);
        d.set_slice(Slice(s));
        pchunk->columns()[5]->as_mutable_ptr()->append_datum(d);
    }
    verify_v2_round_trip(*sc, *pchunk);
}

TEST(PrimaryKeyEncoderTest, testV2EncodeSelectiveRoundTrip) {
    auto sc = create_key_schema({TYPE_INT, TYPE_VARCHAR});
    const int n = 6;
    auto pchunk = ChunkFactory::new_chunk(*sc, n);
    for (int i = 0; i < n; i++) {
        Datum d_int;
        d_int.set_int32(i * 111);
        pchunk->columns()[0]->as_mutable_ptr()->append_datum(d_int);
        Datum d_str;
        std::string s = StringPrintf("s%d", i);
        d_str.set_slice(Slice(s));
        pchunk->columns()[1]->as_mutable_ptr()->append_datum(d_str);
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
    auto pchunk = ChunkFactory::new_chunk(*sc, n);
    Datum d;
    d.set_int32(42);
    pchunk->columns()[0]->as_mutable_ptr()->append_datum(d);
    // VARCHAR with \0 byte to test escape overhead calculation
    std::string s(100, 'x');
    s[50] = '\0';
    d.set_slice(Slice(s));
    pchunk->columns()[1]->as_mutable_ptr()->append_datum(d);
    d.set_int16(10);
    pchunk->columns()[2]->as_mutable_ptr()->append_datum(d);

    // 4 (int) + 100 (varchar) + 1 (escape for \0) + 2 (separator) + 2 (smallint) = 109
    EXPECT_FALSE(PrimaryKeyEncoder::encode_exceed_limit(*sc, *pchunk, 0, n, 128,
                                                        PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2));
    EXPECT_TRUE(PrimaryKeyEncoder::encode_exceed_limit(*sc, *pchunk, 0, n, 10,
                                                       PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2));
}

TEST(PrimaryKeyEncoderTest, testSimdSliceEncodingBoundaries) {
    const std::vector<size_t> test_lengths = {0,  1,  2,  7,  8,  9,  15,  16,  17,  23,  24,  31, 32,
                                              33, 47, 48, 63, 64, 65, 127, 128, 129, 255, 256, 512};
    for (size_t len : test_lengths) {
        std::string original(len, 'a');
        for (size_t i = 0; i < len; ++i) {
            original[i] = static_cast<char>('a' + (i % 26));
        }
        Slice src(original);

        // Test is_last = false (exercises SIMD 16-byte and 8-byte chunk loop + rollback + terminator)
        std::string encoded;
        encoding_utils::encode_slice(src, &encoded, false);
        EXPECT_GE(encoded.size(), len + 2);
        EXPECT_EQ(encoded[encoded.size() - 2], '\0');
        EXPECT_EQ(encoded[encoded.size() - 1], '\0');

        // Test normal decode (exercises memchr fast path)
        Slice enc_slice(encoded);
        std::string decoded;
        Status st = encoding_utils::decode_slice(&enc_slice, &decoded, nullptr, false, false);
        ASSERT_TRUE(st.ok()) << "Failed for length " << len;
        EXPECT_EQ(decoded, original) << "Mismatch for length " << len;
        EXPECT_EQ(enc_slice.size, 0);

        // Test fast decode
        Slice enc_slice_fast(encoded);
        Slice decoded_fast;
        st = encoding_utils::decode_slice(&enc_slice_fast, nullptr, &decoded_fast, false, true);
        ASSERT_TRUE(st.ok()) << "Fast decode failed for length " << len;
        EXPECT_EQ(decoded_fast.to_string(), original) << "Fast decode mismatch for length " << len;
        EXPECT_EQ(enc_slice_fast.size, 0);

        // Test is_last = true (terminal column: no escaping, no delimiter appended)
        // Encoding is a plain byte copy; decoding consumes all of src.
        std::string encoded_last;
        encoding_utils::encode_slice(src, &encoded_last, true);
        EXPECT_EQ(encoded_last.size(), len) << "is_last=true encode size mismatch at len=" << len;
        EXPECT_EQ(encoded_last, original) << "is_last=true encode content mismatch at len=" << len;

        // Normal decode for is_last = true
        Slice enc_last_slice(encoded_last);
        std::string decoded_last;
        Status st_last = encoding_utils::decode_slice(&enc_last_slice, &decoded_last, nullptr, true, false);
        ASSERT_TRUE(st_last.ok()) << "is_last=true decode failed at len=" << len;
        EXPECT_EQ(decoded_last, original) << "is_last=true decode mismatch at len=" << len;
        EXPECT_EQ(enc_last_slice.size, 0) << "is_last=true: src not fully consumed at len=" << len;

        // Fast decode for is_last = true
        Slice enc_last_fast(encoded_last);
        Slice decoded_last_fast;
        Status st_last_fast = encoding_utils::decode_slice(&enc_last_fast, nullptr, &decoded_last_fast, true, true);
        ASSERT_TRUE(st_last_fast.ok()) << "is_last=true fast decode failed at len=" << len;
        EXPECT_EQ(decoded_last_fast.to_string(), original) << "is_last=true fast decode mismatch at len=" << len;
        EXPECT_EQ(enc_last_fast.size, 0) << "is_last=true fast: src not fully consumed at len=" << len;
    }
}

TEST(PrimaryKeyEncoderTest, testSimdSliceEncodingNullEscapes) {
    // Test various positions of '\0' across SIMD 8-byte and 16-byte chunk boundaries
    const size_t base_len = 64;
    const std::vector<std::vector<size_t>> null_positions_list = {
            {0},                       // at first byte
            {3},                       // middle of first 8 bytes
            {7},                       // 8-byte boundary
            {8},                       // start of second 8 bytes
            {15},                      // 16-byte boundary
            {16},                      // start of second 16-byte chunk
            {31},                      // 32-byte boundary
            {63},                      // last byte
            {0, 7, 8, 15, 16, 31, 63}, // multiple boundaries
            {10, 11, 12},              // consecutive nulls
    };

    for (const auto& null_positions : null_positions_list) {
        std::string original(base_len, 'x');
        for (size_t pos : null_positions) {
            original[pos] = '\0';
        }
        Slice src(original);

        std::string encoded;
        encoding_utils::encode_slice(src, &encoded, false);

        // Decoding with nulls exercises the escaped scalar fallback in decode_slice
        Slice enc_slice(encoded);
        std::string decoded;
        Status st = encoding_utils::decode_slice(&enc_slice, &decoded, nullptr, false, false);
        ASSERT_TRUE(st.ok());
        EXPECT_EQ(decoded, original);
        EXPECT_EQ(enc_slice.size, 0);
    }

    // Fundamental Invariant: the encoding MUST preserve lexicographical sort order.
    // For any pair of raw strings key_a < key_b, their encoded forms must satisfy
    // encoded(key_a) < encoded(key_b) — otherwise the primary-key index returns
    // wrong results for range scans.

    // Case 1: embedded null < next byte value.
    // Raw: "abc\0def" < "abc\1def"  =>  encoded forms must preserve this order.
    {
        const std::string key_a = {'a', 'b', 'c', '\0', 'd', 'e', 'f'};
        const std::string key_b = {'a', 'b', 'c', '\1', 'd', 'e', 'f'};
        ASSERT_LT(key_a, key_b) << "precondition: raw ordering";
        std::string enc_a, enc_b;
        encoding_utils::encode_slice(Slice(key_a), &enc_a, false);
        encoding_utils::encode_slice(Slice(key_b), &enc_b, false);
        EXPECT_LT(enc_a, enc_b) << "sort order violated: null vs \\x01";
    }

    // Case 2: null-escaped key < higher ASCII value.
    {
        const std::string key_a = {'a', 'b', 'c', '\0', 'd', 'e', 'f'};
        const std::string key_c = {'a', 'b', 'c', '\x02', 'd', 'e', 'f'};
        ASSERT_LT(key_a, key_c) << "precondition: raw ordering";
        std::string enc_a, enc_c;
        encoding_utils::encode_slice(Slice(key_a), &enc_a, false);
        encoding_utils::encode_slice(Slice(key_c), &enc_c, false);
        EXPECT_LT(enc_a, enc_c) << "sort order violated: null vs \\x02";
    }

    // Case 3: shared prefix, shorter < longer.
    {
        const std::string key_a = "abcdef";
        const std::string key_b = "abcdefg";
        ASSERT_LT(key_a, key_b) << "precondition: raw ordering";
        std::string enc_a, enc_b;
        encoding_utils::encode_slice(Slice(key_a), &enc_a, false);
        encoding_utils::encode_slice(Slice(key_b), &enc_b, false);
        EXPECT_LT(enc_a, enc_b) << "sort order violated: prefix ordering";
    }
}

TEST(PrimaryKeyEncoderTest, testDecodeSliceMissingSeparatorReturnsError) {
    // Regression: DCHECK(separator) was fatal in ASAN before the fix.
    // Verify decode_slice returns Status::InvalidArgument for corrupt/truncated
    // inputs instead of aborting, so callers like tablet_splitter can fall back.

    // fast_decode=false path uses memmem searching for "\0\0".
    // Fails (returns InvalidArgument) when no "\0\0" exists in the input.

    // Case A: clean ASCII, no null bytes at all — no "\0\0", no "\0"
    {
        std::string corrupt = "hello_no_terminator";
        Slice s(corrupt);
        std::string dest;
        Status st = encoding_utils::decode_slice(&s, &dest, nullptr, /*is_last=*/false, /*fast_decode=*/false);
        EXPECT_FALSE(st.ok()) << "Expected error for missing \\0\\0 separator (fast_decode=false, no nulls)";
        EXPECT_TRUE(st.is_invalid_argument()) << st.to_string();
    }

    // Case B: single \0 but no \0\0 — memmem still finds no "\0\0"
    {
        const std::string corrupt = {'h', 'e', 'l', '\0', 'l', 'o'};
        Slice s(corrupt);
        std::string dest;
        Status st = encoding_utils::decode_slice(&s, &dest, nullptr, /*is_last=*/false, /*fast_decode=*/false);
        EXPECT_FALSE(st.ok()) << "Expected error for missing \\0\\0 separator (fast_decode=false, single null)";
        EXPECT_TRUE(st.is_invalid_argument()) << st.to_string();
    }

    // fast_decode=true path uses memchr searching for a single '\0'.
    // Fails (returns InvalidArgument) only when there is no '\0' at all.

    // Case C: no null bytes — memchr finds nothing
    {
        std::string corrupt = "hello_no_terminator";
        Slice s(corrupt);
        Slice dest_fast;
        Status st = encoding_utils::decode_slice(&s, nullptr, &dest_fast, /*is_last=*/false, /*fast_decode=*/true);
        EXPECT_FALSE(st.ok()) << "Expected error for missing \\0 separator (fast_decode=true, no nulls)";
        EXPECT_TRUE(st.is_invalid_argument()) << st.to_string();
    }

    // Case D: single '\0' present — fast_decode=true succeeds (documents the asymmetry:
    // fast_decode uses memchr not memmem, so a single null IS a valid boundary marker).
    {
        const std::string valid_for_fast = {'h', 'e', 'l', '\0', 'l', 'o'};
        Slice s(valid_for_fast);
        Slice dest_fast;
        Status st = encoding_utils::decode_slice(&s, nullptr, &dest_fast, /*is_last=*/false, /*fast_decode=*/true);
        EXPECT_TRUE(st.ok()) << "fast_decode=true should succeed when single \\0 present: " << st.to_string();
        EXPECT_EQ(dest_fast.to_string(), "hel") << "Should decode up to the first \\0";
    }
}

TEST(PrimaryKeyEncoderTest, testCompositeWithMiddleVarcharBatch) {
    auto sc = create_key_schema({TYPE_INT, TYPE_VARCHAR, TYPE_BIGINT});
    MutableColumnPtr dest_v1;
    MutableColumnPtr dest_v2;
    ASSERT_TRUE(PrimaryKeyEncoder::create_column(*sc, &dest_v1, PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1).ok());
    ASSERT_TRUE(PrimaryKeyEncoder::create_column(*sc, &dest_v2, PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2).ok());

    const int num_rows = 128;
    auto pchunk = ChunkFactory::new_chunk(*sc, num_rows);

    for (int i = 0; i < num_rows; ++i) {
        Datum d0;
        d0.set_int32(i * 100);
        pchunk->columns()[0]->as_mutable_ptr()->append_datum(d0);

        // Generate strings of varying lengths spanning 0 to 64 bytes
        std::string str(i % 65, 'A' + (i % 26));
        if (i % 7 == 0 && !str.empty()) {
            str[str.size() / 2] = '\0'; // insert null byte
        }
        Datum d1;
        d1.set_slice(Slice(str));
        pchunk->columns()[1]->as_mutable_ptr()->append_datum(d1);

        Datum d2;
        d2.set_int64(i * 1000000LL);
        pchunk->columns()[2]->as_mutable_ptr()->append_datum(d2);
    }

    // Test V1 composite encoding
    PrimaryKeyEncoder::encode(*sc, *pchunk, 0, num_rows, dest_v1.get(), PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1);
    auto decoded_v1 = pchunk->clone_empty_with_schema();
    ASSERT_TRUE(PrimaryKeyEncoder::decode(*sc, *dest_v1, 0, num_rows, decoded_v1.get(),
                                          PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1)
                        .ok());
    for (int i = 0; i < num_rows; ++i) {
        EXPECT_EQ(pchunk->get_column_by_index(0)->get(i).get_int32(),
                  decoded_v1->get_column_by_index(0)->get(i).get_int32());
        EXPECT_EQ(pchunk->get_column_by_index(1)->get(i).get_slice(),
                  decoded_v1->get_column_by_index(1)->get(i).get_slice());
        EXPECT_EQ(pchunk->get_column_by_index(2)->get(i).get_int64(),
                  decoded_v1->get_column_by_index(2)->get(i).get_int64());
    }

    // Test V2 composite encoding
    PrimaryKeyEncoder::encode(*sc, *pchunk, 0, num_rows, dest_v2.get(), PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2);
    auto decoded_v2 = pchunk->clone_empty_with_schema();
    ASSERT_TRUE(PrimaryKeyEncoder::decode(*sc, *dest_v2, 0, num_rows, decoded_v2.get(),
                                          PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2)
                        .ok());
    for (int i = 0; i < num_rows; ++i) {
        EXPECT_EQ(pchunk->get_column_by_index(0)->get(i).get_int32(),
                  decoded_v2->get_column_by_index(0)->get(i).get_int32());
        EXPECT_EQ(pchunk->get_column_by_index(1)->get(i).get_slice(),
                  decoded_v2->get_column_by_index(1)->get(i).get_slice());
        EXPECT_EQ(pchunk->get_column_by_index(2)->get(i).get_int64(),
                  decoded_v2->get_column_by_index(2)->get(i).get_int64());
    }
}

} // namespace starrocks
