// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/primary_key_encoder.h"

#include <gtest/gtest.h>

#include <memory>

#include "column/chunk.h"
#include "column/datum.h"
#include "column/schema.h"
#include "storage/chunk_helper.h"

using namespace std;

namespace starrocks {

static unique_ptr<vectorized::Schema> create_key_schema(const vector<FieldType>& types) {
    vectorized::Fields fields;
    std::vector<ColumnId> sort_key_idxes(types.size());
    for (int i = 0; i < types.size(); i++) {
        string name = StringPrintf("col%d", i);
        auto fd = new vectorized::Field(i, name, types[i], false);
        fd->set_is_key(true);
        fd->set_aggregate_method(OLAP_FIELD_AGGREGATION_NONE);
        fields.emplace_back(fd);
        sort_key_idxes[i] = i;
    }
    return std::make_unique<vectorized::Schema>(std::move(fields), PRIMARY_KEYS, sort_key_idxes);
}

TEST(PrimaryKeyEncoderTest, testEncodeInt32) {
    auto sc = create_key_schema({OLAP_FIELD_TYPE_INT});
    unique_ptr<vectorized::Column> dest;
    PrimaryKeyEncoder::create_column(*sc, &dest);
    const int n = 1000;
    auto pchunk = ChunkHelper::new_chunk(*sc, n);
    for (int i = 0; i < n; i++) {
        vectorized::Datum tmp;
        tmp.set_int32(i * 2343);
        pchunk->columns()[0]->append_datum(tmp);
    }
    PrimaryKeyEncoder::encode(*sc, *pchunk, 0, n, dest.get());
    auto dchunk = pchunk->clone_empty_with_schema();
    PrimaryKeyEncoder::decode(*sc, *dest, 0, n, dchunk.get());
    ASSERT_EQ(pchunk->num_rows(), dchunk->num_rows());
    for (int i = 0; i < n; i++) {
        ASSERT_EQ(pchunk->get_column_by_index(0)->get(i).get_int32(),
                  dchunk->get_column_by_index(0)->get(i).get_int32());
    }
}

TEST(PrimaryKeyEncoderTest, testEncodeInt128) {
    auto sc = create_key_schema({OLAP_FIELD_TYPE_LARGEINT});
    unique_ptr<vectorized::Column> dest;
    PrimaryKeyEncoder::create_column(*sc, &dest);
    const int n = 1000;
    auto pchunk = ChunkHelper::new_chunk(*sc, n);
    for (int i = 0; i < n; i++) {
        vectorized::Datum tmp;
        tmp.set_int128(i * 2343);
        pchunk->columns()[0]->append_datum(tmp);
    }
    vector<uint32_t> indexes;
    for (int i = 0; i < n; i++) {
        indexes.emplace_back(i);
    }
    PrimaryKeyEncoder::encode_selective(*sc, *pchunk, indexes.data(), n, dest.get());
    auto dchunk = pchunk->clone_empty_with_schema();
    PrimaryKeyEncoder::decode(*sc, *dest, 0, n, dchunk.get());
    ASSERT_EQ(pchunk->num_rows(), dchunk->num_rows());
    for (int i = 0; i < n; i++) {
        ASSERT_EQ(pchunk->get_column_by_index(0)->get(i).get_int128(),
                  dchunk->get_column_by_index(0)->get(i).get_int128());
    }
}

TEST(PrimaryKeyEncoderTest, testEncodeComposite) {
    auto sc = create_key_schema(
            {OLAP_FIELD_TYPE_INT, OLAP_FIELD_TYPE_VARCHAR, OLAP_FIELD_TYPE_SMALLINT, OLAP_FIELD_TYPE_BOOL});
    unique_ptr<vectorized::Column> dest;
    PrimaryKeyEncoder::create_column(*sc, &dest);
    const int n = 1;
    auto pchunk = ChunkHelper::new_chunk(*sc, n);
    for (int i = 0; i < n; i++) {
        vectorized::Datum tmp;
        tmp.set_int32(i * 2343);
        pchunk->columns()[0]->append_datum(tmp);
        string tmpstr = StringPrintf("slice000%d", i * 17);
        if (i % 5 == 0) {
            // set some '\0'
            tmpstr[rand() % tmpstr.size()] = '\0';
        }
        tmp.set_slice(tmpstr);
        pchunk->columns()[1]->append_datum(tmp);
        tmp.set_int16(i);
        pchunk->columns()[2]->append_datum(tmp);
        tmp.set_uint8(i % 2);
        pchunk->columns()[3]->append_datum(tmp);
    }
    vector<uint32_t> indexes;
    for (int i = 0; i < n; i++) {
        indexes.emplace_back(i);
    }
    PrimaryKeyEncoder::encode_selective(*sc, *pchunk, indexes.data(), n, dest.get());
    auto dchunk = pchunk->clone_empty_with_schema();
    PrimaryKeyEncoder::decode(*sc, *dest, 0, n, dchunk.get());
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
        auto sc = create_key_schema(
                {OLAP_FIELD_TYPE_INT, OLAP_FIELD_TYPE_VARCHAR, OLAP_FIELD_TYPE_SMALLINT, OLAP_FIELD_TYPE_BOOL});
        const int n = 1;
        auto pchunk = ChunkHelper::new_chunk(*sc, n);
        vectorized::Datum tmp;
        tmp.set_int32(42);
        pchunk->columns()[0]->append_datum(tmp);
        string tmpstr("slice0000");
        tmpstr[tmpstr.size() - 1] = '\0';
        tmp.set_slice(tmpstr);
        pchunk->columns()[1]->append_datum(tmp);
        tmp.set_int16(10);
        pchunk->columns()[2]->append_datum(tmp);
        tmp.set_uint8(1);
        pchunk->columns()[3]->append_datum(tmp);
        EXPECT_TRUE(PrimaryKeyEncoder::encode_exceed_limit(*sc, *pchunk, 0, n, 10));
        EXPECT_FALSE(PrimaryKeyEncoder::encode_exceed_limit(*sc, *pchunk, 0, n, 128));
    }

    {
        auto sc = create_key_schema(
                {OLAP_FIELD_TYPE_INT, OLAP_FIELD_TYPE_VARCHAR, OLAP_FIELD_TYPE_SMALLINT, OLAP_FIELD_TYPE_BOOL});
        const int n = 1;
        auto pchunk = ChunkHelper::new_chunk(*sc, n);
        vectorized::Datum tmp;
        tmp.set_int32(42);
        pchunk->columns()[0]->append_datum(tmp);
        string tmpstr(128, 's');
        tmpstr[tmpstr.size() - 1] = '\0';
        tmp.set_slice(tmpstr);
        pchunk->columns()[1]->append_datum(tmp);
        tmp.set_int16(10);
        pchunk->columns()[2]->append_datum(tmp);
        tmp.set_uint8(1);
        pchunk->columns()[3]->append_datum(tmp);
        EXPECT_TRUE(PrimaryKeyEncoder::encode_exceed_limit(*sc, *pchunk, 0, n, 128));
    }
}

TEST(PrimaryKeyEncoderTest, testEncodeVarcharLimit) {
    auto sc = create_key_schema({OLAP_FIELD_TYPE_VARCHAR});
    const int n = 2;
    {
        auto pchunk = ChunkHelper::new_chunk(*sc, n);
        vectorized::Datum tmp;
        string tmpstr("slice00000");
        tmp.set_slice(tmpstr);
        pchunk->columns()[0]->append_datum(tmp);
        tmpstr = "slice000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
                 "0000"
                 "00000000000000000000000000000000000";
        tmp.set_slice(tmpstr);
        pchunk->columns()[0]->append_datum(tmp);
        EXPECT_TRUE(PrimaryKeyEncoder::encode_exceed_limit(*sc, *pchunk, 0, n, 128));
    }
    {
        auto pchunk = ChunkHelper::new_chunk(*sc, n);
        vectorized::Datum tmp;
        string tmpstr("slice00000");
        tmp.set_slice(tmpstr);
        pchunk->columns()[0]->append_datum(tmp);
        tmpstr = "slice00000000000000000000000000000000000";
        tmp.set_slice(tmpstr);
        pchunk->columns()[0]->append_datum(tmp);
        EXPECT_FALSE(PrimaryKeyEncoder::encode_exceed_limit(*sc, *pchunk, 0, n, 128));
    }
}

} // namespace starrocks
