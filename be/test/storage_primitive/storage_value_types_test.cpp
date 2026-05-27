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

#include <sstream>
#include <string>
#include <unordered_map>

#include "gtest/gtest.h"
#include "storage/primitive/storage_enums.h"
#include "storage/primitive/storage_ids.h"
#include "storage/primitive/storage_stats.h"
#include "storage/primitive/storage_version.h"

namespace starrocks {

TEST(StorageValueTypesTest, RowsetIdKeepsOldFormatSemantics) {
    RowsetId id;
    id.init("12345");

    EXPECT_EQ(1, id.version);
    EXPECT_EQ(12345, id.id());
    EXPECT_EQ("12345", id.to_string());
}

TEST(StorageValueTypesTest, RowsetIdKeepsHexFormatSemantics) {
    RowsetId original;
    original.init(2, 0x1234, 0x5678, 0x9abc);

    RowsetId parsed;
    parsed.init(original.to_string());

    EXPECT_EQ(original, parsed);
    EXPECT_EQ(original.version, parsed.version);
    EXPECT_EQ(original.to_string(), parsed.to_string());
}

TEST(StorageValueTypesTest, RowsetIdHashRemainsUsable) {
    RowsetId id;
    id.init(2, 0x1234, 0x5678, 0x9abc);

    std::unordered_map<RowsetId, int, HashOfRowsetId> values;
    values.emplace(id, 10);

    EXPECT_EQ(10, values[id]);
}

TEST(StorageValueTypesTest, TabletInfoAndSegmentIdStringSemantics) {
    TabletUid uid;
    uid.hi = 1;
    uid.lo = 2;

    TabletInfo tablet_info(3, 4, uid);
    EXPECT_EQ("3.4.0000000000000001-0000000000000002", tablet_info.to_string());

    TabletSegmentId segment_id(5, 6);
    EXPECT_EQ("5_6", segment_id.to_string());

    TabletSegmentIdRange range(5, 6, 5, 7);
    EXPECT_TRUE(range.is_valid());
    EXPECT_EQ("[5_6,5_7]", range.to_string());
}

TEST(StorageValueTypesTest, VersionSemantics) {
    Version version(1, 3);
    Version containing(1, 5);
    Version inside(2, 4);
    Version same_end_larger_start(2, 3);

    EXPECT_TRUE(containing.contains(inside));
    EXPECT_FALSE(version.contains(containing));
    EXPECT_LT(same_end_larger_start, version);

    std::stringstream ss;
    ss << version;
    EXPECT_EQ("[1-3]", ss.str());
}

TEST(StorageValueTypesTest, PrimitiveEnumHelpers) {
    EXPECT_EQ("base", to_string(BASE_COMPACTION));
    EXPECT_EQ("cumulative", to_string(CUMULATIVE_COMPACTION));
    EXPECT_EQ("update", to_string(UPDATE_COMPACTION));
    EXPECT_EQ("invalid", to_string(INVALID_COMPACTION));

    EXPECT_TRUE(is_query(READER_QUERY));
    EXPECT_TRUE(is_query(READER_BYPASS_QUERY));
    EXPECT_FALSE(is_query(READER_CHECKSUM));

    EXPECT_TRUE(is_compaction(READER_BASE_COMPACTION));
    EXPECT_TRUE(is_compaction(READER_CUMULATIVE_COMPACTION));
    EXPECT_FALSE(is_compaction(READER_QUERY));

    EXPECT_EQ(OLAP_MATERIALIZE_TYPE_COUNT, 4);
    EXPECT_EQ(PUSH_NORMAL_V2, 4);
}

TEST(StorageValueTypesTest, StatNameConstantsRemainStable) {
    EXPECT_STREQ("bytes_read_local_disk", kBytesReadLocalDisk);
    EXPECT_STREQ("bytes_write_local_disk", kBytesWriteLocalDisk);
    EXPECT_STREQ("bytes_read_remote", kBytesReadRemote);
    EXPECT_STREQ("bytes_write_remote", kBytesWriteRemote);
    EXPECT_STREQ("io_count_local_disk", kIOCountLocalDisk);
    EXPECT_STREQ("io_count_remote", kIOCountRemote);
    EXPECT_STREQ("io_ns_read_local_disk", kIONsReadLocalDisk);
    EXPECT_STREQ("io_ns_write_local_disk", kIONsWriteLocalDisk);
    EXPECT_STREQ("io_ns_read_remote", kIONsReadRemote);
    EXPECT_STREQ("io_ns_write_remote", kIONsWriteRemote);
    EXPECT_STREQ("prefetch_hit_count", kPrefetchHitCount);
    EXPECT_STREQ("prefetch_wait_finish_ns", kPrefetchWaitFinishNs);
    EXPECT_STREQ("prefetch_pending_ns", kPrefetchPendingNs);
}

TEST(StorageValueTypesTest, StatisticsDefaultToZero) {
    OlapReaderStatistics reader_stats;
    OlapWriterStatistics writer_stats;

    EXPECT_EQ(0, reader_stats.bytes_read);
    EXPECT_EQ(0, reader_stats.rowsets_read_count);
    EXPECT_TRUE(reader_stats.flat_json_hits.empty());
    EXPECT_EQ(0, writer_stats.bytes_write_remote);
    EXPECT_EQ(0, writer_stats.segment_count);
}

} // namespace starrocks
