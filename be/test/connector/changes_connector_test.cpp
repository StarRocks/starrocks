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

#include "connector/changes_connector.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <climits>
#include <vector>

#include "column/fixed_length_column.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"

namespace starrocks::connector {

class ChangesConnectorTest : public ::testing::Test {
public:
    void SetUp() override { _exec_env = ExecEnv::GetInstance(); }

protected:
    ExecEnv* _exec_env = nullptr;
};

// Test ChangesConnector type
TEST_F(ChangesConnectorTest, test_connector_type) {
    ChangesConnector connector;
    EXPECT_EQ(connector.connector_type(), ConnectorType::CHANGES);
}

// Test ChangesStats default initialization
TEST_F(ChangesConnectorTest, test_changes_stats_defaults) {
    ChangesStats stats;
    EXPECT_EQ(stats.insertion_rows, 0);
    EXPECT_EQ(stats.insertion_data_size, 0);
    EXPECT_EQ(stats.insertion_segment_count, 0);
    EXPECT_EQ(stats.insertion_segment_total_rows, 0);
    EXPECT_EQ(stats.deletion_rows, 0);
    EXPECT_EQ(stats.deletion_data_size, 0);
    EXPECT_EQ(stats.deletion_segment_count, 0);
    EXPECT_EQ(stats.deletion_segment_total_rows, 0);
    EXPECT_EQ(stats.load_count, 0);
    EXPECT_EQ(stats.compaction_count, 0);
    EXPECT_EQ(stats.metadata_count, 0);
    EXPECT_FALSE(stats.has_delete_predicate);
    EXPECT_EQ(stats.base_version_rows, 0);
    EXPECT_EQ(stats.base_version_data_size, 0);
    EXPECT_EQ(stats.base_version_segment_count, 0);
    EXPECT_EQ(stats.head_version_rows, 0);
    EXPECT_EQ(stats.head_version_data_size, 0);
    EXPECT_EQ(stats.head_version_segment_count, 0);
}

// Test ChangesStats accumulation
TEST_F(ChangesConnectorTest, test_changes_stats_accumulation) {
    ChangesStats stats;
    stats.insertion_rows += 100;
    stats.insertion_rows += 200;
    stats.insertion_data_size += 1024;
    stats.insertion_segment_count += 2;
    stats.load_count += 1;
    stats.compaction_count += 3;
    stats.metadata_count += 5;

    EXPECT_EQ(stats.insertion_rows, 300);
    EXPECT_EQ(stats.insertion_data_size, 1024);
    EXPECT_EQ(stats.insertion_segment_count, 2);
    EXPECT_EQ(stats.load_count, 1);
    EXPECT_EQ(stats.compaction_count, 3);
    EXPECT_EQ(stats.metadata_count, 5);
}

// ========== RowVersionFilter Tests ==========

TEST_F(ChangesConnectorTest, test_row_version_filter_eq) {
    RowVersionFilter f(TExprOpcode::EQ, 5);
    EXPECT_TRUE(f.evaluate(5));
    EXPECT_FALSE(f.evaluate(4));
    EXPECT_FALSE(f.evaluate(6));
}

TEST_F(ChangesConnectorTest, test_row_version_filter_ne) {
    RowVersionFilter f(TExprOpcode::NE, 5);
    EXPECT_FALSE(f.evaluate(5));
    EXPECT_TRUE(f.evaluate(4));
    EXPECT_TRUE(f.evaluate(6));
}

TEST_F(ChangesConnectorTest, test_row_version_filter_lt) {
    RowVersionFilter f(TExprOpcode::LT, 5);
    EXPECT_TRUE(f.evaluate(4));
    EXPECT_FALSE(f.evaluate(5));
    EXPECT_FALSE(f.evaluate(6));
}

TEST_F(ChangesConnectorTest, test_row_version_filter_le) {
    RowVersionFilter f(TExprOpcode::LE, 5);
    EXPECT_TRUE(f.evaluate(4));
    EXPECT_TRUE(f.evaluate(5));
    EXPECT_FALSE(f.evaluate(6));
}

TEST_F(ChangesConnectorTest, test_row_version_filter_gt) {
    RowVersionFilter f(TExprOpcode::GT, 5);
    EXPECT_FALSE(f.evaluate(4));
    EXPECT_FALSE(f.evaluate(5));
    EXPECT_TRUE(f.evaluate(6));
}

TEST_F(ChangesConnectorTest, test_row_version_filter_ge) {
    RowVersionFilter f(TExprOpcode::GE, 5);
    EXPECT_FALSE(f.evaluate(4));
    EXPECT_TRUE(f.evaluate(5));
    EXPECT_TRUE(f.evaluate(6));
}

TEST_F(ChangesConnectorTest, test_row_version_filter_unknown_op) {
    RowVersionFilter f(TExprOpcode::INVALID_OPCODE, 5);
    EXPECT_TRUE(f.evaluate(5));
    EXPECT_TRUE(f.evaluate(999));
}

TEST_F(ChangesConnectorTest, test_row_version_filter_multi_predicate_intersection) {
    // WHERE __ROW_VERSION__ >= 3 AND __ROW_VERSION__ <= 7
    std::vector<RowVersionFilter> filters;
    filters.emplace_back(TExprOpcode::GE, 3);
    filters.emplace_back(TExprOpcode::LE, 7);

    auto should_keep = [&](int64_t cv) {
        return std::all_of(filters.begin(), filters.end(),
                           [cv](const RowVersionFilter& f) { return f.evaluate(cv); });
    };

    EXPECT_FALSE(should_keep(1));
    EXPECT_FALSE(should_keep(2));
    EXPECT_TRUE(should_keep(3));
    EXPECT_TRUE(should_keep(5));
    EXPECT_TRUE(should_keep(7));
    EXPECT_FALSE(should_keep(8));
    EXPECT_FALSE(should_keep(10));
}

TEST_F(ChangesConnectorTest, test_row_version_filter_boundary_values) {
    RowVersionFilter f_max(TExprOpcode::LE, INT64_MAX);
    EXPECT_TRUE(f_max.evaluate(INT64_MAX));
    EXPECT_TRUE(f_max.evaluate(0));

    RowVersionFilter f_min(TExprOpcode::GE, 0);
    EXPECT_TRUE(f_min.evaluate(0));
    EXPECT_TRUE(f_min.evaluate(INT64_MAX));
    EXPECT_FALSE(f_min.evaluate(-1));

    RowVersionFilter f_eq(TExprOpcode::EQ, 0);
    EXPECT_TRUE(f_eq.evaluate(0));
    EXPECT_FALSE(f_eq.evaluate(1));
}

// Spec §3.6 E11: when the metadata ancestor chain cannot reach V_base the BE must
// emit a CHANGES_NOT_FOUND error that names the tablet and points operators at the
// recorded-ancestor config knob. The wording is part of the public error contract
// (FE re-throws it verbatim), so we lock it here against accidental rewording.
TEST_F(ChangesConnectorTest, AncestorChainExhaustedReportsCdcError) {
    const int64_t kTabletId = 42;
    auto message = format_ancestor_chain_exhausted_error(kTabletId);

    EXPECT_NE(std::string::npos,
              message.find("CHANGES_NOT_FOUND: ancestor chain insufficient for tablet 42"));
    EXPECT_NE(std::string::npos,
              message.find("consider raising cloud_native_tablet_metadata_ancestors_recorded"));
}

TEST_F(ChangesConnectorTest, test_cdc_rowset_version_filter) {
    struct TestRowset {
        int64_t version;
    };
    std::vector<TestRowset> rowsets = {{1}, {2}, {3}, {5}, {8}};

    std::vector<RowVersionFilter> filters;
    filters.emplace_back(TExprOpcode::GE, 3);
    filters.emplace_back(TExprOpcode::LE, 5);

    rowsets.erase(
            std::remove_if(rowsets.begin(), rowsets.end(),
                           [&](const TestRowset& rs) {
                               return std::any_of(filters.begin(), filters.end(),
                                                  [&](const RowVersionFilter& f) {
                                                      return !f.evaluate(rs.version);
                                                  });
                           }),
            rowsets.end());

    ASSERT_EQ(rowsets.size(), 2);
    EXPECT_EQ(rowsets[0].version, 3);
    EXPECT_EQ(rowsets[1].version, 5);
}

} // namespace starrocks::connector
