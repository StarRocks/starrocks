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
#include "storage/primitive/aggregate_type.h"
#include "storage/primitive/edit_version.h"
#include "storage/primitive/flat_json_config.h"
#include "storage/primitive/primary_key_encoding_types.h"
#include "storage/primitive/storage_enums.h"
#include "storage/primitive/storage_ids.h"
#include "storage/primitive/storage_stats.h"
#include "storage/primitive/storage_version.h"
#include "storage/primitive/type_utils.h"
#include "storage/primitive/vector_search_option.h"
#include "storage/primitive/zone_map_detail.h"

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

TEST(StorageValueTypesTest, AggregateTypeStringSemantics) {
    EXPECT_EQ(STORAGE_AGGREGATE_SUM, get_aggregation_type_by_string("sum"));
    EXPECT_EQ(STORAGE_AGGREGATE_SUM, get_aggregation_type_by_string("SUM"));
    EXPECT_EQ(STORAGE_AGGREGATE_REPLACE_IF_NOT_NULL, get_aggregation_type_by_string("replace_if_not_null"));
    EXPECT_EQ(STORAGE_AGGREGATE_UNKNOWN, get_aggregation_type_by_string("not_a_method"));

    EXPECT_EQ("min", get_string_by_aggregation_type(STORAGE_AGGREGATE_MIN));
    EXPECT_EQ("replace_if_not_null", get_string_by_aggregation_type(STORAGE_AGGREGATE_REPLACE_IF_NOT_NULL));
    EXPECT_EQ("none", get_string_by_aggregation_type(STORAGE_AGGREGATE_NONE));

    std::stringstream ss;
    ss << STORAGE_AGGREGATE_MAX;
    EXPECT_EQ("max", ss.str());
}

TEST(StorageValueTypesTest, EditVersionStringAndPbSemantics) {
    EditVersion major_only(7, 0);
    EditVersion major_minor(7, 3);

    EXPECT_EQ("7", major_only.to_string());
    EXPECT_EQ("7.3", major_minor.to_string());

    std::stringstream ss;
    ss << major_minor;
    EXPECT_EQ("7.3", ss.str());

    EditVersionPB pb;
    major_minor.to_pb(&pb);
    EditVersion parsed(pb);
    EXPECT_EQ(major_minor, parsed);
    EXPECT_EQ(7, parsed.major_number());
    EXPECT_EQ(3, parsed.minor_number());
}

TEST(StorageValueTypesTest, FlatJsonConfigUpdateAndSerializationSemantics) {
    FlatJsonConfig config(true, 0.25, 0.75, 12);

    EXPECT_TRUE(config.is_flat_json_enabled());
    EXPECT_DOUBLE_EQ(0.25, config.get_flat_json_null_factor());
    EXPECT_DOUBLE_EQ(0.75, config.get_flat_json_sparsity_factor());
    EXPECT_EQ(12, config.get_flat_json_max_column_max());
    EXPECT_EQ(
            "FlatJsonConfig{flat_json_enable=true, flat_json_null_factor=0.25, "
            "flat_json_sparsity_factor=0.75, flat_json_max_column_max=12}",
            config.to_string());

    FlatJsonConfigPB pb;
    config.to_pb(&pb);
    EXPECT_TRUE(pb.flat_json_enable());
    EXPECT_DOUBLE_EQ(0.25, pb.flat_json_null_factor());
    EXPECT_DOUBLE_EQ(0.75, pb.flat_json_sparsity_factor());
    EXPECT_EQ(12, pb.flat_json_max_column_max());

    FlatJsonConfig from_pb;
    from_pb.update(pb);
    EXPECT_TRUE(from_pb.is_flat_json_enabled());
    EXPECT_DOUBLE_EQ(0.25, from_pb.get_flat_json_null_factor());
    EXPECT_DOUBLE_EQ(0.75, from_pb.get_flat_json_sparsity_factor());
    EXPECT_EQ(12, from_pb.get_flat_json_max_column_max());

    TFlatJsonConfig thrift_config;
    thrift_config.__set_flat_json_enable(false);
    thrift_config.__set_flat_json_null_factor(0.5);
    thrift_config.__set_flat_json_sparsity_factor(0.6);
    thrift_config.__set_flat_json_column_max(8);

    FlatJsonConfig from_thrift;
    from_thrift.update(thrift_config);
    EXPECT_FALSE(from_thrift.is_flat_json_enabled());
    EXPECT_DOUBLE_EQ(0.5, from_thrift.get_flat_json_null_factor());
    EXPECT_DOUBLE_EQ(0.6, from_thrift.get_flat_json_sparsity_factor());
    EXPECT_EQ(8, from_thrift.get_flat_json_max_column_max());

    FlatJsonConfig copied;
    copied.update(from_thrift);
    EXPECT_FALSE(copied.is_flat_json_enabled());
    EXPECT_DOUBLE_EQ(0.5, copied.get_flat_json_null_factor());
    EXPECT_DOUBLE_EQ(0.6, copied.get_flat_json_sparsity_factor());
    EXPECT_EQ(8, copied.get_flat_json_max_column_max());
}

TEST(StorageValueTypesTest, PrimaryKeyEncodingTypeValuesRemainStable) {
    EXPECT_EQ(0, static_cast<int>(PrimaryKeyEncodingType::PK_ENCODING_TYPE_NONE));
    EXPECT_EQ(1, static_cast<int>(PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1));
    EXPECT_EQ(2, static_cast<int>(PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2));
}

TEST(StorageValueTypesTest, TypeUtilsStorageFormatSemantics) {
    EXPECT_TRUE(TypeUtils::specific_type_of_format_v1(TYPE_DATE_V1));
    EXPECT_TRUE(TypeUtils::specific_type_of_format_v1(TYPE_DATETIME_V1));
    EXPECT_TRUE(TypeUtils::specific_type_of_format_v1(TYPE_DECIMAL));
    EXPECT_FALSE(TypeUtils::specific_type_of_format_v1(TYPE_DATE));

    EXPECT_EQ(3, TypeUtils::estimate_field_size(TYPE_DATE_V1, 99));
    EXPECT_EQ(8, TypeUtils::estimate_field_size(TYPE_DATETIME, 99));
    EXPECT_EQ(12, TypeUtils::estimate_field_size(TYPE_DECIMAL, 99));
    EXPECT_EQ(99, TypeUtils::estimate_field_size(TYPE_VARCHAR, 99));

    EXPECT_EQ(TYPE_DATE, TypeUtils::to_storage_format_v2(TYPE_DATE_V1));
    EXPECT_EQ(TYPE_DATETIME, TypeUtils::to_storage_format_v2(TYPE_DATETIME_V1));
    EXPECT_EQ(TYPE_DECIMALV2, TypeUtils::to_storage_format_v2(TYPE_DECIMAL));
    EXPECT_EQ(TYPE_INT, TypeUtils::to_storage_format_v2(TYPE_INT));
}

TEST(StorageValueTypesTest, VectorSearchOptionStoresAssignedValues) {
    VectorSearchOption option;
    option.k = 10;
    option.query_vector = {1.0F, 2.0F};
    option.vector_distance_column_name = "distance";
    option.use_vector_index = true;
    option.vector_column_id = 3;
    option.vector_slot_id = 4;
    option.query_params = {{"metric_type", "l2"}};
    option.vector_range = 0.5;
    option.result_order = 1;
    option.use_ivfpq = true;
    option.pq_refine_factor = 2.5;
    option.k_factor = 1.5;

    EXPECT_EQ(10, option.k);
    ASSERT_EQ(2, option.query_vector.size());
    EXPECT_FLOAT_EQ(1.0F, option.query_vector[0]);
    EXPECT_FLOAT_EQ(2.0F, option.query_vector[1]);
    EXPECT_EQ("distance", option.vector_distance_column_name);
    EXPECT_TRUE(option.use_vector_index);
    EXPECT_EQ(3, option.vector_column_id);
    EXPECT_EQ(4, option.vector_slot_id);
    EXPECT_EQ("l2", option.query_params["metric_type"]);
    EXPECT_DOUBLE_EQ(0.5, option.vector_range);
    EXPECT_EQ(1, option.result_order);
    EXPECT_TRUE(option.use_ivfpq);
    EXPECT_DOUBLE_EQ(2.5, option.pq_refine_factor);
    EXPECT_DOUBLE_EQ(1.5, option.k_factor);
}

TEST(StorageValueTypesTest, ZoneMapDetailSemantics) {
    ZoneMapDetail detail(Datum(static_cast<int32_t>(1)), Datum(static_cast<int32_t>(9)), false);

    EXPECT_FALSE(detail.has_null());
    EXPECT_TRUE(detail.has_not_null());
    EXPECT_EQ(1, detail.min_value().get_int32());
    EXPECT_EQ(9, detail.max_value().get_int32());

    detail.set_num_rows(11);
    EXPECT_EQ(11, detail.num_rows());

    ZoneMapDetail null_min(Datum(), Datum(static_cast<int32_t>(9)));
    EXPECT_TRUE(null_min.has_null());
    EXPECT_TRUE(null_min.has_not_null());
    EXPECT_TRUE(null_min.min_or_null_value().is_null());
    EXPECT_EQ(9, null_min.max_value().get_int32());
}

} // namespace starrocks
