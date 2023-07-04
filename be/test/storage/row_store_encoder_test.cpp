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

#include "storage/row_store_encoder.h"

#include <gtest/gtest.h>

#include "column/chunk.h"
#include "column/datum.h"
#include "column/schema.h"
#include "fs/fs_util.h"
#include "gutil/stringprintf.h"
#include "storage/chunk_helper.h"

using namespace std;

namespace starrocks {

static unique_ptr<Schema> create_schema(const vector<pair<LogicalType, bool>>& types) {
    Fields fields;
    for (int i = 0; i < types.size(); i++) {
        string name = StringPrintf("col%d", i);
        auto fd = new Field(i, name, types[i].first, false);
        fd->set_is_key(types[i].second);
        fd->set_aggregate_method(STORAGE_AGGREGATE_NONE);
        fields.emplace_back(fd);
    }
    return unique_ptr<Schema>(new Schema(std::move(fields), PRIMARY_KEYS, {}));
}

TEST(RowStoreEncoderTest, testEncodeInt) {
    auto schema = create_schema({{TYPE_INT, true}, {TYPE_LARGEINT, true}, {TYPE_INT, false}, {TYPE_LARGEINT, false}});
    const int n = 100;
    auto pchunk = ChunkHelper::new_chunk(*schema, n);
    for (int i = 0; i < n; i++) {
        Datum tmp;
        tmp.set_int32(i * 2343);
        Datum tmp2;
        tmp2.set_int128(i * 2343);
        pchunk->columns()[0]->append_datum(tmp);
        pchunk->columns()[1]->append_datum(tmp2);
        pchunk->columns()[2]->append_datum(tmp);
        pchunk->columns()[3]->append_datum(tmp2);
    }
    std::vector<std::string> keys, values;
    RowStoreEncoder::chunk_to_keys(*schema, *pchunk, 0, n, keys);
    RowStoreEncoder::chunk_to_values(*schema, *pchunk, 0, n, values);
    ASSERT_EQ(keys.size(), n);
    ASSERT_EQ(values.size(), n);
    auto dchunk = pchunk->clone_empty_with_schema();
    RowStoreEncoder::kvs_to_chunk(keys, values, *schema, dchunk.get());
    ASSERT_EQ(pchunk->num_rows(), dchunk->num_rows());
    ASSERT_EQ(pchunk->num_rows(), n);
    ASSERT_EQ(pchunk->num_columns(), 4);
    ASSERT_EQ(dchunk->num_columns(), 4);
    for (int i = 0; i < n; i++) {
        ASSERT_EQ(pchunk->get_column_by_index(0)->get(i).get_int32(),
                  dchunk->get_column_by_index(0)->get(i).get_int32());
        ASSERT_EQ(pchunk->get_column_by_index(1)->get(i).get_int128(),
                  dchunk->get_column_by_index(1)->get(i).get_int128());
        ASSERT_EQ(pchunk->get_column_by_index(2)->get(i).get_int32(),
                  dchunk->get_column_by_index(2)->get(i).get_int32());
        ASSERT_EQ(pchunk->get_column_by_index(3)->get(i).get_int128(),
                  dchunk->get_column_by_index(3)->get(i).get_int128());
        ASSERT_EQ(i * 2343, dchunk->get_column_by_index(0)->get(i).get_int32());
        ASSERT_EQ(i * 2343, dchunk->get_column_by_index(1)->get(i).get_int128());
        ASSERT_EQ(i * 2343, dchunk->get_column_by_index(2)->get(i).get_int32());
        ASSERT_EQ(i * 2343, dchunk->get_column_by_index(3)->get(i).get_int128());
    }
}

TEST(RowStoreEncoderTest, testEncodeMix) {
    auto schema = create_schema({{TYPE_INT, true}, {TYPE_VARCHAR, true}, {TYPE_INT, false}, {TYPE_BOOLEAN, false}});
    const int n = 100;
    auto pchunk = ChunkHelper::new_chunk(*schema, n);
    for (int i = 0; i < n; i++) {
        Datum tmp;
        string tmpstr = StringPrintf("slice000%d", i * 17);
        if (i % 5 == 0) {
            // set some '\0'
            tmpstr[rand() % tmpstr.size()] = '\0';
        }
        tmp.set_int32(i * 2343);
        pchunk->columns()[0]->append_datum(tmp);
        tmp.set_slice(tmpstr);
        pchunk->columns()[1]->append_datum(tmp);
        tmp.set_int32(i * 2343);
        pchunk->columns()[2]->append_datum(tmp);
        tmp.set_uint8(i % 2);
        pchunk->columns()[3]->append_datum(tmp);
    }
    std::vector<std::string> keys, values;
    RowStoreEncoder::chunk_to_keys(*schema, *pchunk, 0, n, keys);
    RowStoreEncoder::chunk_to_values(*schema, *pchunk, 0, n, values);
    ASSERT_EQ(keys.size(), n);
    ASSERT_EQ(values.size(), n);
    auto dchunk = pchunk->clone_empty_with_schema();
    RowStoreEncoder::kvs_to_chunk(keys, values, *schema, dchunk.get());
    ASSERT_EQ(pchunk->num_rows(), dchunk->num_rows());
    ASSERT_EQ(pchunk->num_rows(), n);
    ASSERT_EQ(pchunk->num_columns(), 4);
    ASSERT_EQ(dchunk->num_columns(), 4);
    for (int i = 0; i < n; i++) {
        ASSERT_EQ(pchunk->get_column_by_index(0)->get(i).get_int32(),
                  dchunk->get_column_by_index(0)->get(i).get_int32());
        ASSERT_EQ(pchunk->get_column_by_index(1)->get(i).get_slice(),
                  dchunk->get_column_by_index(1)->get(i).get_slice());
        ASSERT_EQ(pchunk->get_column_by_index(2)->get(i).get_int32(),
                  dchunk->get_column_by_index(2)->get(i).get_int32());
        ASSERT_EQ(pchunk->get_column_by_index(3)->get(i).get<uint8>(),
                  dchunk->get_column_by_index(3)->get(i).get<uint8>());
        ASSERT_EQ(i * 2343, dchunk->get_column_by_index(0)->get(i).get_int32());
        ASSERT_EQ(i * 2343, dchunk->get_column_by_index(2)->get(i).get_int32());
        if (i % 5 != 0) {
            string tmpstr = StringPrintf("slice000%d", i * 17);
            ASSERT_EQ(tmpstr, dchunk->get_column_by_index(1)->get(i).get_slice().to_string());
        }
    }
}

TEST(RowStoreEncoderTest, testEncodeVersion) {
    for (int i = 0; i < 1000; i++) {
        string tmpstr = StringPrintf("slice000%d", i * 17);
        int8_t op = i % 2;
        int64_t ver = i * 1000;
        string buf = tmpstr;
        RowStoreEncoder::combine_key_with_ver(buf, op, ver);
        int8_t dop = 0;
        int64_t dver = 0;
        string dstr;
        RowStoreEncoder::split_key_with_ver(buf, dstr, dop, dver);
        ASSERT_EQ(op, dop);
        ASSERT_EQ(ver, dver);
        ASSERT_EQ(tmpstr, dstr);
    }
}

} // namespace starrocks
