// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/olap/primary_key_encoder_test.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "storage/primary_key_encoder.h"

#include <gtest/gtest.h>

#include "column/chunk.h"
#include "column/datum.h"
#include "column/schema.h"
#include "storage/vectorized/chunk_helper.h"

using namespace std;

namespace starrocks {

static unique_ptr<vectorized::Schema> create_key_schema(const vector<FieldType>& types) {
    vectorized::Fields fields;
    for (int i = 0; i < types.size(); i++) {
        string name = StringPrintf("col%d", i);
        auto fd = new vectorized::Field(i, name, types[i], false);
        fd->set_is_key(true);
        fd->set_aggregate_method(OLAP_FIELD_AGGREGATION_NONE);
        fields.emplace_back(fd);
    }
    return unique_ptr<vectorized::Schema>(new vectorized::Schema(std::move(fields)));
}

TEST(PrimaryKeyEncoderTest, testEncodeInt32) {
    auto sc = create_key_schema({OLAP_FIELD_TYPE_INT});
    unique_ptr<vectorized::Column> dest;
    PrimaryKeyEncoder::create_column(*sc, &dest);
    const int n = 1000;
    auto pchunk = vectorized::ChunkHelper::new_chunk(*sc, n);
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
    auto pchunk = vectorized::ChunkHelper::new_chunk(*sc, n);
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
    auto pchunk = vectorized::ChunkHelper::new_chunk(*sc, n);
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
        auto pchunk = vectorized::ChunkHelper::new_chunk(*sc, n);
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
        auto pchunk = vectorized::ChunkHelper::new_chunk(*sc, n);
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

} // namespace starrocks
