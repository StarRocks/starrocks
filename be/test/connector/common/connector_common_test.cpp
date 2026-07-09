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

#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>

#include <string>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "connector/common/connector_sink_commit.h"
#include "connector/common/hive_partition_utils.h"
#include "connector/common/utils.h"
#include "formats/column_evaluator.h"
#include "formats/utils.h"
#include "types/datum.h"

namespace starrocks::connector {

TEST(ConnectorCommonTest, CommitResultWrapsFileResultWithConnectorMetadata) {
    CommitResult result{
            .file_result =
                    {
                            .io_status = Status::OK(),
                            .format = formats::PARQUET,
                            .file_statistics =
                                    {
                                            .record_count = 10,
                                            .file_size = 128,
                                    },
                            .location = "s3://bucket/table/data.parquet",
                    },
    };

    auto& fingerprint_ref = result.set_partition_null_fingerprint("01");
    auto& referenced_file_ref = result.set_referenced_data_file("s3://bucket/table/original.parquet");

    ASSERT_EQ(&result, &fingerprint_ref);
    ASSERT_EQ(&result, &referenced_file_ref);
    ASSERT_TRUE(result.file_result.io_status.ok());
    ASSERT_EQ(formats::PARQUET, result.file_result.format);
    ASSERT_EQ(10, result.file_result.file_statistics.record_count);
    ASSERT_EQ(128, result.file_result.file_statistics.file_size);
    ASSERT_EQ("01", result.partition_null_fingerprint);
    ASSERT_EQ("s3://bucket/table/original.parquet", result.referenced_data_file);
}

struct FileSuffixTestCase {
    std::string format;
    TCompressionType::type compression;
    std::string expected_suffix;
};

class FileSuffixBuilderTest : public ::testing::TestWithParam<FileSuffixTestCase> {};

TEST_P(FileSuffixBuilderTest, BuildCanonicalFileSuffix) {
    auto test_case = GetParam();
    auto normalized = normalize_format_name(test_case.format);
    auto suffix = build_canonical_file_suffix(normalized, test_case.compression);
    ASSERT_TRUE(suffix.ok()) << suffix.status();
    ASSERT_EQ(test_case.expected_suffix, suffix.value());
    ASSERT_EQ(std::string::npos, suffix.value().find(".csv.csv"));
}

INSTANTIATE_TEST_SUITE_P(ConnectorCommonSuffix, FileSuffixBuilderTest,
                         ::testing::Values(FileSuffixTestCase{formats::CSV, TCompressionType::NO_COMPRESSION, "csv"},
                                           FileSuffixTestCase{formats::CSV, TCompressionType::GZIP, "csv.gz"},
                                           FileSuffixTestCase{formats::CSV, TCompressionType::ZSTD, "csv.zst"},
                                           FileSuffixTestCase{formats::CSV, TCompressionType::LZ4, "csv.lz4"},
                                           FileSuffixTestCase{formats::CSV, TCompressionType::LZ4_FRAME, "csv.lz4"},
                                           FileSuffixTestCase{formats::CSV, TCompressionType::SNAPPY, "csv.snappy"},
                                           FileSuffixTestCase{formats::CSV, TCompressionType::DEFLATE, "csv.deflate"},
                                           FileSuffixTestCase{formats::CSV, TCompressionType::ZLIB, "csv.zlib"},
                                           FileSuffixTestCase{formats::CSV, TCompressionType::BZIP2, "csv.bz2"},
                                           FileSuffixTestCase{formats::CSV, TCompressionType::DEFAULT_COMPRESSION,
                                                              "csv"},
                                           FileSuffixTestCase{formats::PARQUET, TCompressionType::GZIP, "parquet"},
                                           FileSuffixTestCase{formats::ORC, TCompressionType::ZSTD, "orc"},
                                           FileSuffixTestCase{"csv.gz.csv", TCompressionType::GZIP, "csv.gz"},
                                           FileSuffixTestCase{"CSV.LZ4.CSV", TCompressionType::LZ4, "csv.lz4"},
                                           FileSuffixTestCase{"  .Csv.zst.csv  ", TCompressionType::ZSTD, "csv.zst"}));

TEST(ConnectorCommonTest, RejectsEmptyFileSuffixFormat) {
    auto normalized = normalize_format_name("   ");
    auto suffix = build_canonical_file_suffix(normalized, TCompressionType::GZIP);
    ASSERT_FALSE(suffix.ok());
}

TEST(ConnectorCommonTest, PathUtilsSplitsPathsAndRemovesTrailingSlash) {
    ASSERT_EQ("s3://bucket/table/part=1", PathUtils::get_parent_path("s3://bucket/table/part=1/data.parquet"));
    ASSERT_EQ("data.parquet", PathUtils::get_filename("s3://bucket/table/part=1/data.parquet"));
    ASSERT_EQ("s3://bucket/table", PathUtils::remove_trailing_slash("s3://bucket/table/"));
    ASSERT_EQ("s3://bucket/table", PathUtils::remove_trailing_slash("s3://bucket/table"));
}

TEST(ConnectorCommonTest, LocationProviderBuildsStablePaths) {
    LocationProvider provider("s3://bucket/table/", "query", 2, 3, "parquet");

    ASSERT_EQ("s3://bucket/table", provider.root_location());
    ASSERT_EQ("s3://bucket/table/dt=2026", provider.root_location("dt=2026/"));
    ASSERT_EQ("s3://bucket/table/query_2_3_0.parquet", provider.get());
    ASSERT_EQ("s3://bucket/table/dt=2026/query_2_3_0.parquet", provider.get("dt=2026/"));
    ASSERT_EQ("s3://bucket/table/dt=2026/query_2_3_1.parquet", provider.get("dt=2026/"));
}

TEST(ConnectorCommonTest, LocationProviderIncludesWriterTag) {
    LocationProvider provider("s3://bucket/table", "query", 2, 3, "parquet", "data");

    ASSERT_EQ("s3://bucket/table/query_2_data_3_0.parquet", provider.get());
}

TEST(ConnectorCommonTest, HivePartitionUtilsFormatsDecimalScale) {
    auto formatted = HivePartitionUtils::format_decimal_value<int32_t>(123, 2);
    ASSERT_TRUE(formatted.ok()) << formatted.status();
    ASSERT_EQ("1.23", formatted.value());

    formatted = HivePartitionUtils::format_decimal_value<int32_t>(123, 4);
    ASSERT_TRUE(formatted.ok()) << formatted.status();
    ASSERT_EQ("0.0123", formatted.value());

    ASSERT_FALSE(HivePartitionUtils::format_decimal_value<int32_t>(123, -1).ok());
}

TEST(ConnectorCommonTest, HivePartitionUtilsBuildsEncodedPartitionName) {
    Chunk chunk;

    auto name_column = ColumnHelper::create_column(TYPE_VARCHAR_DESC, true);
    std::string name_value = "a b/c";
    Datum name;
    name.set_slice(name_value);
    name_column->append_datum(name);
    chunk.append_column(name_column, 0);

    auto day_column = ColumnHelper::create_column(TYPE_INT_DESC, true);
    day_column->append_datum(Datum(int32_t{7}));
    chunk.append_column(day_column, 1);

    auto evaluators = ColumnSlotIdEvaluator::from_types({TYPE_VARCHAR_DESC, TYPE_INT_DESC});
    auto partition_name = HivePartitionUtils::make_partition_name({"name", "day"}, evaluators, &chunk, true);

    ASSERT_TRUE(partition_name.ok()) << partition_name.status();
    ASSERT_EQ("name=a%20b%2Fc/day=7/", partition_name.value());
}

TEST(ConnectorCommonTest, HivePartitionUtilsHandlesNullPartitionPolicy) {
    Chunk chunk;

    auto nullable_column = ColumnHelper::create_column(TYPE_VARCHAR_DESC, true);
    ASSERT_TRUE(nullable_column->append_nulls(1));
    chunk.append_column(nullable_column, 0);

    auto evaluators = ColumnSlotIdEvaluator::from_types({TYPE_VARCHAR_DESC});
    auto rejected = HivePartitionUtils::make_partition_name({"name"}, evaluators, &chunk, false);
    ASSERT_FALSE(rejected.ok());

    auto allowed = HivePartitionUtils::make_partition_name({"name"}, evaluators, &chunk, true);
    ASSERT_TRUE(allowed.ok()) << allowed.status();
    ASSERT_EQ("name=null/", allowed.value());
}

} // namespace starrocks::connector
