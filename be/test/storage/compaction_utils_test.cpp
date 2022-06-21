// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/compaction_utils.h"

#include <gtest/gtest.h>

#include <vector>

namespace starrocks {

TEST(CompactionUtilsTest, test_algorithm_to_string) {
    CompactionAlgorithm ag_1 = HORIZONTAL_COMPACTION;
    const char* ag_1_name = CompactionUtils::compaction_algorithm_to_string(ag_1);
    EXPECT_STREQ("HORIZONTAL_COMPACTION", ag_1_name);

    CompactionAlgorithm ag_2 = VERTICAL_COMPACTION;
    const char* ag_2_name = CompactionUtils::compaction_algorithm_to_string(ag_2);
    EXPECT_STREQ("VERTICAL_COMPACTION", ag_2_name);

    const char* ag_3_name = CompactionUtils::compaction_algorithm_to_string((CompactionAlgorithm)2);
    EXPECT_STREQ("[Unknown CompactionAlgorithm]", ag_3_name);
}

TEST(CompactionUtilsTest, test_get_read_chunk_size) {
    int64_t mem_limit = 2147483648;
    int32_t config_chunk_size = 4096;
    int64_t total_num_rows = 100000;
    int64_t total_mem_footprint = 1024 * 1024 * 100;
    size_t source_num = 10;

    int32_t chunk_size = CompactionUtils::get_read_chunk_size(mem_limit, config_chunk_size, total_num_rows,
                                                              total_mem_footprint, source_num);
    ASSERT_EQ(4096, chunk_size);

    chunk_size = CompactionUtils::get_read_chunk_size(mem_limit, config_chunk_size, 1, total_mem_footprint, source_num);
    ASSERT_EQ(5, chunk_size);

    chunk_size = CompactionUtils::get_read_chunk_size(mem_limit, config_chunk_size, 1, total_mem_footprint, 1);
    ASSERT_EQ(41, chunk_size);

    chunk_size = CompactionUtils::get_read_chunk_size(mem_limit, config_chunk_size, 0, 0, source_num);
    ASSERT_EQ(4096, chunk_size);

    chunk_size = CompactionUtils::get_read_chunk_size(mem_limit, config_chunk_size, 0, 0, 0);
    ASSERT_EQ(4096, chunk_size);
}

TEST(CompactionUtilsTest, test_get_segment_max_rows) {
    int64_t max_segment_file_size = 1024 * 1024 * 1024;
    int64_t input_row_num = 10000;
    int64_t input_size = 100 * 10000;
    uint32_t segment_max_rows = CompactionUtils::get_segment_max_rows(max_segment_file_size, input_row_num, input_size);
    ASSERT_EQ(10737418, segment_max_rows);

    segment_max_rows = CompactionUtils::get_segment_max_rows(max_segment_file_size, 0, 0);
    ASSERT_EQ(1073741824, segment_max_rows);

    max_segment_file_size = -1;
    segment_max_rows = CompactionUtils::get_segment_max_rows(max_segment_file_size, 0, 0);
    ASSERT_EQ(2147483647, segment_max_rows);

    max_segment_file_size = 1LL << 63;
    segment_max_rows = CompactionUtils::get_segment_max_rows(max_segment_file_size, 0, 0);
    ASSERT_EQ(2147483647, segment_max_rows);

    max_segment_file_size = 0;
    segment_max_rows = CompactionUtils::get_segment_max_rows(max_segment_file_size, 0, 0);
    ASSERT_EQ(2147483647, segment_max_rows);
}

TEST(CompactionUtilsTest, test_split_column_into_groups) {
    size_t num_columns = 17;
    size_t num_key_columns = 1;
    int64_t max_columns_per_group = 5;
    std::vector<std::vector<uint32_t>> column_groups;
    CompactionUtils::split_column_into_groups(num_columns, num_key_columns, max_columns_per_group, &column_groups);
    ASSERT_EQ(5, column_groups.size());
    ASSERT_EQ(1, column_groups[0].size());
    ASSERT_EQ(5, column_groups[1].size());
    ASSERT_EQ(5, column_groups[2].size());
    ASSERT_EQ(5, column_groups[3].size());
    ASSERT_EQ(1, column_groups[4].size());
}

TEST(CompactionUtilsTest, test_choose_compaction_algorithm) {
    size_t num_columns = 17;
    int64_t max_columns_per_group = 5;
    size_t source_num = 10;
    CompactionAlgorithm choice =
            CompactionUtils::choose_compaction_algorithm(num_columns, max_columns_per_group, source_num);
    ASSERT_EQ(VERTICAL_COMPACTION, choice);
    choice = CompactionUtils::choose_compaction_algorithm(num_columns, max_columns_per_group, 1);
    ASSERT_EQ(HORIZONTAL_COMPACTION, choice);
    choice = CompactionUtils::choose_compaction_algorithm(5, max_columns_per_group, source_num);
    ASSERT_EQ(HORIZONTAL_COMPACTION, choice);
}

} // namespace starrocks
