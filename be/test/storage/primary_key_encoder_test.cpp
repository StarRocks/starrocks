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

#include <algorithm>
#include <cstring>
#include <limits>
#include <memory>
#include <numeric>
#include <string_view>

#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/datum.h"
#include "column/schema.h"
#include "gutil/casts.h"
#include "gutil/stringprintf.h"
#include "storage/chunk_helper.h"

using namespace std;

namespace starrocks {

template <typename T>
static std::vector<T> sort_values_by_encoded_keys(const BinaryColumn& keys, const std::vector<T>& values) {
    std::vector<size_t> idx(values.size());
    std::iota(idx.begin(), idx.end(), 0);
    std::sort(idx.begin(), idx.end(), [&](size_t a, size_t b) {
        auto sa = keys.get_slice(a);
        auto sb = keys.get_slice(b);
        return std::string_view(sa.data, sa.size) < std::string_view(sb.data, sb.size);
    });
    std::vector<T> res;
    res.reserve(values.size());
    for (auto i : idx) {
        res.push_back(values[i]);
    }
    return res;
}

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

TEST(PrimaryKeyEncoderTest, testCreateColumnAndFixedSizeByEncodingType) {
    auto sc = create_key_schema({TYPE_INT});
    std::vector<ColumnId> key_idxes{0};

    ASSERT_EQ(TYPE_INT, PrimaryKeyEncoder::encoded_primary_key_type(*sc, key_idxes, PrimaryKeyEncodingType::V1));
    ASSERT_EQ(TYPE_VARCHAR, PrimaryKeyEncoder::encoded_primary_key_type(*sc, key_idxes, PrimaryKeyEncodingType::V2));

    ASSERT_EQ(sizeof(int32_t), PrimaryKeyEncoder::get_encoded_fixed_size(*sc, PrimaryKeyEncodingType::V1));
    ASSERT_EQ(0, PrimaryKeyEncoder::get_encoded_fixed_size(*sc, PrimaryKeyEncodingType::V2));

    MutableColumnPtr v1_col;
    MutableColumnPtr v2_col;
    ASSERT_TRUE(PrimaryKeyEncoder::create_column(*sc, &v1_col, PrimaryKeyEncodingType::V1).ok());
    ASSERT_TRUE(PrimaryKeyEncoder::create_column(*sc, &v2_col, PrimaryKeyEncodingType::V2).ok());
    ASSERT_FALSE(v1_col->is_binary() || v1_col->is_large_binary());
    ASSERT_TRUE(v2_col->is_binary());
}

TEST(PrimaryKeyEncoderTest, testEncodeInt32V2OrderPreserving) {
    auto sc = create_key_schema({TYPE_INT});
    MutableColumnPtr dest;
    ASSERT_TRUE(PrimaryKeyEncoder::create_column(*sc, &dest, PrimaryKeyEncodingType::V2).ok());
    ASSERT_TRUE(dest->is_binary());

    const std::vector<int32_t> values = {std::numeric_limits<int32_t>::min(), -1024, -1, 0, 1, 42, 1024,
                                         std::numeric_limits<int32_t>::max()};

    auto pchunk = ChunkHelper::new_chunk(*sc, values.size());
    for (auto v : values) {
        Datum tmp;
        tmp.set_int32(v);
        pchunk->mutable_columns()[0]->append_datum(tmp);
    }

    PrimaryKeyEncoder::encode(*sc, *pchunk, 0, values.size(), dest.get(), PrimaryKeyEncodingType::V2);

    auto dchunk = pchunk->clone_empty_with_schema();
    ASSERT_TRUE(
            PrimaryKeyEncoder::decode(*sc, *dest, 0, values.size(), dchunk.get(), nullptr, PrimaryKeyEncodingType::V2)
                    .ok());
    ASSERT_EQ(pchunk->num_rows(), dchunk->num_rows());
    for (size_t i = 0; i < values.size(); i++) {
        ASSERT_EQ(values[i], dchunk->get_column_by_index(0)->get(i).get_int32());
    }

    auto& bdest = down_cast<BinaryColumn&>(*dest);
    auto sorted_by_encoded = sort_values_by_encoded_keys<int32_t>(bdest, values);
    auto sorted_values = values;
    std::sort(sorted_values.begin(), sorted_values.end());
    ASSERT_EQ(sorted_values, sorted_by_encoded);
}

TEST(PrimaryKeyEncoderTest, testEncodeInt64V2OrderPreserving) {
    auto sc = create_key_schema({TYPE_BIGINT});
    MutableColumnPtr dest;
    ASSERT_TRUE(PrimaryKeyEncoder::create_column(*sc, &dest, PrimaryKeyEncodingType::V2).ok());
    ASSERT_TRUE(dest->is_binary());

    const std::vector<int64_t> values = {std::numeric_limits<int64_t>::min(), -1024, -1, 0, 1, 42, 1024,
                                         std::numeric_limits<int64_t>::max()};

    auto pchunk = ChunkHelper::new_chunk(*sc, values.size());
    for (auto v : values) {
        Datum tmp;
        tmp.set_int64(v);
        pchunk->mutable_columns()[0]->append_datum(tmp);
    }

    PrimaryKeyEncoder::encode(*sc, *pchunk, 0, values.size(), dest.get(), PrimaryKeyEncodingType::V2);

    auto dchunk = pchunk->clone_empty_with_schema();
    ASSERT_TRUE(
            PrimaryKeyEncoder::decode(*sc, *dest, 0, values.size(), dchunk.get(), nullptr, PrimaryKeyEncodingType::V2)
                    .ok());
    ASSERT_EQ(pchunk->num_rows(), dchunk->num_rows());
    for (size_t i = 0; i < values.size(); i++) {
        ASSERT_EQ(values[i], dchunk->get_column_by_index(0)->get(i).get_int64());
    }

    auto& bdest = down_cast<BinaryColumn&>(*dest);
    auto sorted_by_encoded = sort_values_by_encoded_keys<int64_t>(bdest, values);
    auto sorted_values = values;
    std::sort(sorted_values.begin(), sorted_values.end());
    ASSERT_EQ(sorted_values, sorted_by_encoded);
}

TEST(PrimaryKeyEncoderTest, testEncodeInt128V2OrderPreserving) {
    auto sc = create_key_schema({TYPE_LARGEINT});
    MutableColumnPtr dest;
    ASSERT_TRUE(PrimaryKeyEncoder::create_column(*sc, &dest, PrimaryKeyEncodingType::V2).ok());
    ASSERT_TRUE(dest->is_binary());

    const uint128_t sign_bit = (static_cast<uint128_t>(1) << 127);
    const int128_t min_v = static_cast<int128_t>(sign_bit);
    const int128_t max_v = static_cast<int128_t>(sign_bit - 1);
    const std::vector<int128_t> values = {min_v,
                                          min_v + 1,
                                          static_cast<int128_t>(-1024),
                                          static_cast<int128_t>(-1),
                                          static_cast<int128_t>(0),
                                          static_cast<int128_t>(1),
                                          static_cast<int128_t>(42),
                                          max_v - 1,
                                          max_v};

    auto pchunk = ChunkHelper::new_chunk(*sc, values.size());
    for (const auto& v : values) {
        Datum tmp;
        tmp.set_int128(v);
        pchunk->mutable_columns()[0]->append_datum(tmp);
    }

    PrimaryKeyEncoder::encode(*sc, *pchunk, 0, values.size(), dest.get(), PrimaryKeyEncodingType::V2);

    auto dchunk = pchunk->clone_empty_with_schema();
    ASSERT_TRUE(
            PrimaryKeyEncoder::decode(*sc, *dest, 0, values.size(), dchunk.get(), nullptr, PrimaryKeyEncodingType::V2)
                    .ok());
    ASSERT_EQ(pchunk->num_rows(), dchunk->num_rows());
    for (size_t i = 0; i < values.size(); i++) {
        ASSERT_EQ(values[i], dchunk->get_column_by_index(0)->get(i).get_int128());
    }

    auto& bdest = down_cast<BinaryColumn&>(*dest);
    auto sorted_by_encoded = sort_values_by_encoded_keys<int128_t>(bdest, values);
    auto sorted_values = values;
    std::sort(sorted_values.begin(), sorted_values.end());
    ASSERT_EQ(sorted_values, sorted_by_encoded);
}

TEST(PrimaryKeyEncoderTest, testEncodeSelectiveInt32V2) {
    auto sc = create_key_schema({TYPE_INT});
    MutableColumnPtr dest;
    ASSERT_TRUE(PrimaryKeyEncoder::create_column(*sc, &dest, PrimaryKeyEncodingType::V2).ok());
    ASSERT_TRUE(dest->is_binary());

    const std::vector<int32_t> values = {10, 20, 30};
    auto pchunk = ChunkHelper::new_chunk(*sc, values.size());
    for (auto v : values) {
        Datum tmp;
        tmp.set_int32(v);
        pchunk->mutable_columns()[0]->append_datum(tmp);
    }

    const std::vector<uint32_t> idxes = {2, 0};
    PrimaryKeyEncoder::encode_selective(*sc, *pchunk, idxes.data(), idxes.size(), dest.get(),
                                        PrimaryKeyEncodingType::V2);

    auto dchunk = ChunkHelper::new_chunk(*sc, 0);
    ASSERT_TRUE(
            PrimaryKeyEncoder::decode(*sc, *dest, 0, idxes.size(), dchunk.get(), nullptr, PrimaryKeyEncodingType::V2)
                    .ok());
    ASSERT_EQ(idxes.size(), dchunk->num_rows());
    ASSERT_EQ(30, dchunk->get_column_by_index(0)->get(0).get_int32());
    ASSERT_EQ(10, dchunk->get_column_by_index(0)->get(1).get_int32());
}

TEST(PrimaryKeyEncoderTest, testEncodeVarcharV2RawBytes) {
    auto sc = create_key_schema({TYPE_VARCHAR});
    MutableColumnPtr dest;
    ASSERT_TRUE(PrimaryKeyEncoder::create_column(*sc, &dest, PrimaryKeyEncodingType::V2).ok());
    ASSERT_TRUE(dest->is_binary());

    std::vector<std::string> values;
    values.emplace_back(std::string("abc"));
    values.emplace_back(std::string("ab\0cd", 5)); // embedded '\0'

    auto pchunk = ChunkHelper::new_chunk(*sc, values.size());
    for (const auto& v : values) {
        Datum tmp;
        tmp.set_slice(Slice(v.data(), v.size()));
        pchunk->mutable_columns()[0]->append_datum(tmp);
    }

    PrimaryKeyEncoder::encode(*sc, *pchunk, 0, values.size(), dest.get(), PrimaryKeyEncodingType::V2);

    auto& bdest = down_cast<BinaryColumn&>(*dest);
    ASSERT_EQ(values.size(), bdest.size());
    for (size_t i = 0; i < values.size(); i++) {
        auto s = bdest.get_slice(i);
        ASSERT_EQ(values[i].size(), s.size);
        ASSERT_EQ(0, std::memcmp(values[i].data(), s.data, s.size));
    }

    auto dchunk = pchunk->clone_empty_with_schema();
    ASSERT_TRUE(
            PrimaryKeyEncoder::decode(*sc, *dest, 0, values.size(), dchunk.get(), nullptr, PrimaryKeyEncodingType::V2)
                    .ok());
    ASSERT_EQ(values.size(), dchunk->num_rows());
    ASSERT_EQ(Slice(values[0]), dchunk->get_column_by_index(0)->get(0).get_slice());
    ASSERT_EQ(Slice(values[1].data(), values[1].size()), dchunk->get_column_by_index(0)->get(1).get_slice());
}

TEST(PrimaryKeyEncoderTest, testEncodeExceedLimitInt32V2) {
    auto sc = create_key_schema({TYPE_INT});
    const int n = 1;
    auto pchunk = ChunkHelper::new_chunk(*sc, n);
    Datum tmp;
    tmp.set_int32(42);
    pchunk->mutable_columns()[0]->append_datum(tmp);

    EXPECT_TRUE(
            PrimaryKeyEncoder::encode_exceed_limit(*sc, *pchunk, 0, n, /*limit_size=*/3, PrimaryKeyEncodingType::V2));
    EXPECT_FALSE(
            PrimaryKeyEncoder::encode_exceed_limit(*sc, *pchunk, 0, n, /*limit_size=*/4, PrimaryKeyEncodingType::V2));
}

TEST(PrimaryKeyEncoderTest, testCreateLargeBinaryColumnV2) {
    auto sc = create_key_schema({TYPE_INT});
    MutableColumnPtr dest;
    ASSERT_TRUE(PrimaryKeyEncoder::create_column(*sc, &dest, PrimaryKeyEncodingType::V2, true).ok());
    ASSERT_TRUE(dest->is_large_binary());
}

TEST(PrimaryKeyEncoderTest, testEncodeInt32) {
    auto sc = create_key_schema({TYPE_INT});
    MutableColumnPtr dest;
    PrimaryKeyEncoder::create_column(*sc, &dest, PrimaryKeyEncodingType::V1);
    const int n = 1000;
    auto pchunk = ChunkHelper::new_chunk(*sc, n);
    for (int i = 0; i < n; i++) {
        Datum tmp;
        tmp.set_int32(i * 2343);
        pchunk->mutable_columns()[0]->append_datum(tmp);
    }
    PrimaryKeyEncoder::encode(*sc, *pchunk, 0, n, dest.get(), PrimaryKeyEncodingType::V1);
    auto dchunk = pchunk->clone_empty_with_schema();
    PrimaryKeyEncoder::decode(*sc, *dest, 0, n, dchunk.get(), nullptr, PrimaryKeyEncodingType::V1);
    ASSERT_EQ(pchunk->num_rows(), dchunk->num_rows());
    for (int i = 0; i < n; i++) {
        ASSERT_EQ(pchunk->get_column_by_index(0)->get(i).get_int32(),
                  dchunk->get_column_by_index(0)->get(i).get_int32());
    }
}

TEST(PrimaryKeyEncoderTest, testEncodeInt128) {
    auto sc = create_key_schema({TYPE_LARGEINT});
    MutableColumnPtr dest;
    PrimaryKeyEncoder::create_column(*sc, &dest, PrimaryKeyEncodingType::V1);
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
    PrimaryKeyEncoder::encode_selective(*sc, *pchunk, indexes.data(), n, dest.get(), PrimaryKeyEncodingType::V1);
    auto dchunk = pchunk->clone_empty_with_schema();
    PrimaryKeyEncoder::decode(*sc, *dest, 0, n, dchunk.get(), nullptr, PrimaryKeyEncodingType::V1);
    ASSERT_EQ(pchunk->num_rows(), dchunk->num_rows());
    for (int i = 0; i < n; i++) {
        ASSERT_EQ(pchunk->get_column_by_index(0)->get(i).get_int128(),
                  dchunk->get_column_by_index(0)->get(i).get_int128());
    }
}

TEST(PrimaryKeyEncoderTest, testEncodeComposite) {
    auto sc = create_key_schema({TYPE_INT, TYPE_VARCHAR, TYPE_SMALLINT, TYPE_BOOLEAN});
    MutableColumnPtr dest;
    PrimaryKeyEncoder::create_column(*sc, &dest, PrimaryKeyEncodingType::V1);
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
    PrimaryKeyEncoder::encode_selective(*sc, *pchunk, indexes.data(), n, dest.get(), PrimaryKeyEncodingType::V1);
    auto dchunk = pchunk->clone_empty_with_schema();
    PrimaryKeyEncoder::decode(*sc, *dest, 0, n, dchunk.get(), nullptr, PrimaryKeyEncodingType::V1);
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
        EXPECT_TRUE(PrimaryKeyEncoder::encode_exceed_limit(*sc, *pchunk, 0, n, 10, PrimaryKeyEncodingType::V1));
        EXPECT_FALSE(PrimaryKeyEncoder::encode_exceed_limit(*sc, *pchunk, 0, n, 128, PrimaryKeyEncodingType::V1));
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
        EXPECT_TRUE(PrimaryKeyEncoder::encode_exceed_limit(*sc, *pchunk, 0, n, 128, PrimaryKeyEncodingType::V1));
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
        EXPECT_TRUE(PrimaryKeyEncoder::encode_exceed_limit(*sc, *pchunk, 0, n, 128, PrimaryKeyEncodingType::V1));
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
        EXPECT_FALSE(PrimaryKeyEncoder::encode_exceed_limit(*sc, *pchunk, 0, n, 128, PrimaryKeyEncodingType::V1));
    }
}

} // namespace starrocks
