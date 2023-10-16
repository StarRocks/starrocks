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
#include "column/column_helper.h"
#include "column/datum.h"
#include "column/schema.h"
#include "fs/fs_util.h"
#include "gutil/stringprintf.h"
#include "storage/chunk_helper.h"
#include "storage/row_store_encoder_factory.h"

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

void common_encode_decode(RowStoreEncoderPtr& row_encoder, std::unique_ptr<Schema>& schema,
                          std::unique_ptr<Schema>& schema_with_row, ChunkUniquePtr& pchunk, const int n) {
    //encode
    auto full_row_col = std::make_unique<BinaryColumn>();
    row_encoder->encode_chunk_to_full_row_column(*schema, *pchunk, full_row_col.get());
    ASSERT_EQ(full_row_col->size(), pchunk->num_rows());

    //decode
    auto read_value_schema = create_schema({{TYPE_VARCHAR, false}, {TYPE_INT, false}, {TYPE_BOOLEAN, false}});
    std::vector<ColumnId> value_column_ids{1, 2, 3};
    std::vector<MutableColumnPtr> read_value_columns(value_column_ids.size());
    read_value_columns.reserve(value_column_ids.size());
    for (uint32_t i = 0; i < value_column_ids.size(); ++i) {
        auto column = ChunkHelper::column_from_field(*read_value_schema->field(i).get());
        read_value_columns[i] = column->clone_empty();
    }
    row_encoder->decode_columns_from_full_row_column(*schema_with_row, *full_row_col, value_column_ids,
                                                     &read_value_columns);

    for (int i = 0; i < n; i++) {
        ASSERT_EQ((*(read_value_columns[0])).get(i).get_slice().to_string(), StringPrintf("slice000%d", i * 17));
        ASSERT_EQ((*(read_value_columns[1])).get(i).get_int32(), i * 2343);
        ASSERT_EQ((*(read_value_columns[2])).get(i).get_uint8(), i % 2);
    }
}

TEST(RowStoreEncoderTest, testBitmap) {
    RowStoreEncoderSimple* row_encoder =
            (RowStoreEncoderSimple*)RowStoreEncoderFactory::instance()->get_or_create_encoder(SIMPLE).get();
    BitmapValue null_bitmap;
    for (int i = 0; i < 100; i++) {
        null_bitmap.add(i % 3);
    }

    // encode bitmap
    std::string dstr;
    row_encoder->encode_null_bitmap(null_bitmap, &dstr);

    // decode bitmap
    Slice s(dstr);
    BitmapValue decode_null_bitmap;
    row_encoder->decode_null_bitmap(&s, decode_null_bitmap);
    ASSERT_EQ(null_bitmap.to_string(), decode_null_bitmap.to_string());
    // for debug
    std::cout << "encode bitmap = " << null_bitmap.to_string() << ", decode bitmap = " << decode_null_bitmap.to_string()
              << std::endl;
}

TEST(RowStoreEncoderTest, testEncodeFullRowColumn) {
    // init schema
    auto schema = create_schema({{TYPE_INT, true}, {TYPE_VARCHAR, false}, {TYPE_INT, false}, {TYPE_BOOLEAN, false}});
    auto schema_with_row = create_schema(
            {{TYPE_INT, true}, {TYPE_VARCHAR, false}, {TYPE_INT, false}, {TYPE_BOOLEAN, false}, {TYPE_VARCHAR, false}});
    // fill chunk
    const int n = 2;
    auto pchunk = ChunkHelper::new_chunk(*schema, n);
    for (int i = 0; i < n; i++) {
        Datum tmp;
        string tmpstr = StringPrintf("slice000%d", i * 17);
        tmp.set_int32(i * 2343);
        pchunk->columns()[0]->append_datum(tmp);
        tmp.set_slice(tmpstr);
        pchunk->columns()[1]->append_datum(tmp);
        tmp.set_int32(i * 2343);
        pchunk->columns()[2]->append_datum(tmp);
        tmp.set_uint8(i % 2);
        pchunk->columns()[3]->append_datum(tmp);
    }

    // simple encoder
    auto simple_row_encoder = RowStoreEncoderFactory::instance()->get_or_create_encoder(SIMPLE);
    common_encode_decode(simple_row_encoder, schema, schema_with_row, pchunk, n);
    std::cout << "---------- simple encode end --------" << std::endl;
}
} // namespace starrocks