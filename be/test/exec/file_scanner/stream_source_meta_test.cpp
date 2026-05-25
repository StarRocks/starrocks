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

#include "exec/file_scanner/stream_source_meta.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "column/column_helper.h"
#include "types/type_descriptor.h"
#include "util/byte_buffer.h"

namespace starrocks {

class StreamSourceMetaTest : public testing::Test {
protected:
    static MutableColumnPtr varchar_col() {
        return ColumnHelper::create_column(TypeDescriptor::create_varchar_type(65535), true);
    }
    static MutableColumnPtr int_col() {
        return ColumnHelper::create_column(TypeDescriptor(LogicalType::TYPE_INT), true);
    }
    static MutableColumnPtr bigint_col() {
        return ColumnHelper::create_column(TypeDescriptor(LogicalType::TYPE_BIGINT), true);
    }
    static MutableColumnPtr map_col() {
        return ColumnHelper::create_column(TypeDescriptor::create_map_type(TypeDescriptor::create_varchar_type(65535),
                                                                           TypeDescriptor::create_varchar_type(65535)),
                                           true);
    }

    static TRoutineLoadMetaColumn desc(int32_t slot_id, TStreamSourceMetaKind::type kind, const std::string& key = "") {
        TRoutineLoadMetaColumn d;
        d.__set_slot_id(slot_id);
        d.__set_kind(kind);
        if (!key.empty()) {
            d.__set_key(key);
        }
        return d;
    }
};

TEST_F(StreamSourceMetaTest, build_descriptor_map) {
    std::vector<TRoutineLoadMetaColumn> descs = {desc(7, TStreamSourceMetaKind::TOPIC),
                                                 desc(9, TStreamSourceMetaKind::HEADER, "trace-id")};
    auto cols = build_stream_source_meta_columns(descs);
    ASSERT_EQ(2, cols.size());
    ASSERT_EQ(TStreamSourceMetaKind::TOPIC, cols.at(7).kind);
    ASSERT_EQ(TStreamSourceMetaKind::HEADER, cols.at(9).kind);
    ASSERT_EQ("trace-id", cols.at(9).key);
}

TEST_F(StreamSourceMetaTest, fill_scalars) {
    StreamMessageMeta meta(ByteBufferMetaType::KAFKA);
    meta.set_topic("orders");
    meta.set_partition(3);
    meta.set_offset(100);
    meta.set_timestamp(1700000000000L);
    meta.set_key("k1");

    auto topic = varchar_col();
    ASSERT_OK(fill_stream_source_meta_column(TStreamSourceMetaKind::TOPIC, "", &meta, topic.get()));
    ASSERT_FALSE(topic->is_null(0));
    ASSERT_EQ("orders", topic->get(0).get_slice().to_string());

    auto part = int_col();
    ASSERT_OK(fill_stream_source_meta_column(TStreamSourceMetaKind::PARTITION, "", &meta, part.get()));
    ASSERT_EQ(3, part->get(0).get_int32());

    auto off = bigint_col();
    ASSERT_OK(fill_stream_source_meta_column(TStreamSourceMetaKind::OFFSET, "", &meta, off.get()));
    ASSERT_EQ(100, off->get(0).get_int64());

    auto ts = bigint_col();
    ASSERT_OK(fill_stream_source_meta_column(TStreamSourceMetaKind::TIMESTAMP, "", &meta, ts.get()));
    ASSERT_EQ(1700000000000L, ts->get(0).get_int64());

    auto key = varchar_col();
    ASSERT_OK(fill_stream_source_meta_column(TStreamSourceMetaKind::KEY, "", &meta, key.get()));
    ASSERT_EQ("k1", key->get(0).get_slice().to_string());
}

TEST_F(StreamSourceMetaTest, fill_null_sentinels) {
    StreamMessageMeta meta(ByteBufferMetaType::KAFKA); // partition/offset/timestamp default -1, no key

    auto part = int_col();
    ASSERT_OK(fill_stream_source_meta_column(TStreamSourceMetaKind::PARTITION, "", &meta, part.get()));
    ASSERT_TRUE(part->is_null(0));

    auto key = varchar_col();
    ASSERT_OK(fill_stream_source_meta_column(TStreamSourceMetaKind::KEY, "", &meta, key.get()));
    ASSERT_TRUE(key->is_null(0));

    // A null meta appends NULL for any kind.
    auto topic = varchar_col();
    ASSERT_OK(fill_stream_source_meta_column(TStreamSourceMetaKind::TOPIC, "", nullptr, topic.get()));
    ASSERT_TRUE(topic->is_null(0));
}

TEST_F(StreamSourceMetaTest, header_last_wins) {
    StreamMessageMeta meta(ByteBufferMetaType::KAFKA);
    meta.add_header("h", "v1");
    meta.add_header("h", "v2"); // duplicate key, last wins
    meta.add_header("x", "y");

    auto h = varchar_col();
    ASSERT_OK(fill_stream_source_meta_column(TStreamSourceMetaKind::HEADER, "h", &meta, h.get()));
    ASSERT_EQ("v2", h->get(0).get_slice().to_string());

    auto missing = varchar_col();
    ASSERT_OK(fill_stream_source_meta_column(TStreamSourceMetaKind::HEADER, "nope", &meta, missing.get()));
    ASSERT_TRUE(missing->is_null(0));

    // HEADERS map collapses the duplicate key (last-wins) -> 2 entries.
    auto m = map_col();
    ASSERT_OK(fill_stream_source_meta_column(TStreamSourceMetaKind::HEADERS, "", &meta, m.get()));
    ASSERT_FALSE(m->is_null(0));
    ASSERT_EQ(2, m->get(0).get_map().size());
}

} // namespace starrocks
