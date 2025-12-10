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

#include "storage/lake/autonomous_compaction_handler.h"

#include <gtest/gtest.h>

#include "common/config.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "storage/lake/join_path.h"
#include "storage/lake/test_util.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"

namespace starrocks::lake {

class AutonomousCompactionHandlerTest : public TestBase {
public:
    AutonomousCompactionHandlerTest() : TestBase(kTestDir), _handler(_tablet_mgr.get()) { clear_and_init_test_dir(); }

    ~AutonomousCompactionHandlerTest() override = default;

    void SetUp() override {
        // Create a tablet with metadata
        _tablet_metadata = generate_simple_tablet_metadata(DUP_KEYS);
        _tablet_id = _tablet_metadata->id();
        _tablet_metadata->set_version(1);
        CHECK_OK(_tablet_mgr->put_tablet_metadata(_tablet_metadata));
    }

    void TearDown() override { (void)fs::remove_all(_test_dir); }

protected:
    constexpr static const char* kTestDir = "test_autonomous_compaction_handler";
    AutonomousCompactionHandler _handler;
    std::shared_ptr<TabletMetadata> _tablet_metadata;
    int64_t _tablet_id = 0;
};

// ============== Test AutonomousCompactionStats default values ==============
TEST_F(AutonomousCompactionHandlerTest, test_stats_default_values) {
    AutonomousCompactionStats stats;
    EXPECT_EQ(0, stats.tablets_processed);
    EXPECT_EQ(0, stats.tablets_with_compaction);
    EXPECT_EQ(0, stats.tablets_succeeded);
    EXPECT_EQ(0, stats.tablets_scheduled);
    EXPECT_EQ(0, stats.result_files_cleaned);
}

// ============== Test last_stats initially empty ==============
TEST_F(AutonomousCompactionHandlerTest, test_last_stats_initially_empty) {
    const auto& stats = _handler.last_stats();
    EXPECT_EQ(0, stats.tablets_processed);
    EXPECT_EQ(0, stats.tablets_with_compaction);
    EXPECT_EQ(0, stats.tablets_succeeded);
    EXPECT_EQ(0, stats.tablets_scheduled);
    EXPECT_EQ(0, stats.result_files_cleaned);
}

// ============== Test process_request with non-autonomous request ==============
TEST_F(AutonomousCompactionHandlerTest, test_process_request_not_autonomous) {
    CompactRequest request;
    CompactResponse response;

    // Not set autonomous_compaction flag
    request.set_txn_id(next_id());
    request.add_tablet_ids(_tablet_id);
    request.set_version(1);

    auto status = _handler.process_request(&request, &response);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.is_invalid_argument());
}

// ============== Test process_request with autonomous request ==============
TEST_F(AutonomousCompactionHandlerTest, test_process_request_autonomous_empty_results) {
    CompactRequest request;
    CompactResponse response;

    request.set_autonomous_compaction(true);
    request.set_txn_id(next_id());
    request.add_tablet_ids(_tablet_id);
    request.set_version(1);
    request.set_visible_version(1);

    auto status = _handler.process_request(&request, &response);
    EXPECT_TRUE(status.ok());

    // Check stats
    const auto& stats = _handler.last_stats();
    EXPECT_EQ(1, stats.tablets_processed);
    // No compaction results, so tablets_with_compaction should be 0
    EXPECT_EQ(0, stats.tablets_with_compaction);
    EXPECT_EQ(0, stats.tablets_succeeded);
}

// ============== Test process_request with multiple tablets ==============
TEST_F(AutonomousCompactionHandlerTest, test_process_request_multiple_tablets) {
    // Create another tablet
    auto tablet_metadata2 = generate_simple_tablet_metadata(DUP_KEYS);
    int64_t tablet_id2 = tablet_metadata2->id();
    tablet_metadata2->set_version(1);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(tablet_metadata2));

    CompactRequest request;
    CompactResponse response;

    request.set_autonomous_compaction(true);
    request.set_txn_id(next_id());
    request.add_tablet_ids(_tablet_id);
    request.add_tablet_ids(tablet_id2);
    request.set_version(1);
    request.set_visible_version(1);

    auto status = _handler.process_request(&request, &response);
    EXPECT_TRUE(status.ok());

    // Check stats
    const auto& stats = _handler.last_stats();
    EXPECT_EQ(2, stats.tablets_processed);
}

// ============== Test process_request with non-existent tablet ==============
TEST_F(AutonomousCompactionHandlerTest, test_process_request_nonexistent_tablet) {
    CompactRequest request;
    CompactResponse response;

    request.set_autonomous_compaction(true);
    request.set_txn_id(next_id());
    request.add_tablet_ids(99999); // Non-existent tablet
    request.set_version(1);
    request.set_visible_version(1);

    auto status = _handler.process_request(&request, &response);
    EXPECT_TRUE(status.ok()); // Should not fail, just skip the tablet

    // Check stats
    const auto& stats = _handler.last_stats();
    EXPECT_EQ(1, stats.tablets_processed);
    EXPECT_EQ(0, stats.tablets_with_compaction);
}

// ============== Test process_request without visible_version ==============
TEST_F(AutonomousCompactionHandlerTest, test_process_request_without_visible_version) {
    CompactRequest request;
    CompactResponse response;

    request.set_autonomous_compaction(true);
    request.set_txn_id(next_id());
    request.add_tablet_ids(_tablet_id);
    request.set_version(5); // visible_version should default to version

    auto status = _handler.process_request(&request, &response);
    EXPECT_TRUE(status.ok());

    const auto& stats = _handler.last_stats();
    EXPECT_EQ(1, stats.tablets_processed);
}

// ============== Test process_request with config disabled ==============
TEST_F(AutonomousCompactionHandlerTest, test_process_request_config_disabled) {
    // Disable autonomous compaction
    bool old_value = config::enable_lake_autonomous_compaction;
    config::enable_lake_autonomous_compaction = false;

    CompactRequest request;
    CompactResponse response;

    request.set_autonomous_compaction(true);
    request.set_txn_id(next_id());
    request.add_tablet_ids(_tablet_id);
    request.set_version(1);
    request.set_visible_version(1);

    auto status = _handler.process_request(&request, &response);
    EXPECT_TRUE(status.ok());

    // Restore config
    config::enable_lake_autonomous_compaction = old_value;

    // With config disabled, tablets_scheduled should be 0
    const auto& stats = _handler.last_stats();
    EXPECT_EQ(0, stats.tablets_scheduled);
}

// ============== Test multiple process_request calls reset stats ==============
TEST_F(AutonomousCompactionHandlerTest, test_process_request_resets_stats) {
    // First request with 2 tablets
    auto tablet_metadata2 = generate_simple_tablet_metadata(DUP_KEYS);
    int64_t tablet_id2 = tablet_metadata2->id();
    tablet_metadata2->set_version(1);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(tablet_metadata2));

    {
        CompactRequest request;
        CompactResponse response;
        request.set_autonomous_compaction(true);
        request.set_txn_id(next_id());
        request.add_tablet_ids(_tablet_id);
        request.add_tablet_ids(tablet_id2);
        request.set_version(1);
        ASSERT_OK(_handler.process_request(&request, &response));
        EXPECT_EQ(2, _handler.last_stats().tablets_processed);
    }

    // Second request with 1 tablet - stats should be reset
    {
        CompactRequest request;
        CompactResponse response;
        request.set_autonomous_compaction(true);
        request.set_txn_id(next_id());
        request.add_tablet_ids(_tablet_id);
        request.set_version(1);
        ASSERT_OK(_handler.process_request(&request, &response));
        EXPECT_EQ(1, _handler.last_stats().tablets_processed);
    }
}

// ============== Test response status ==============
TEST_F(AutonomousCompactionHandlerTest, test_process_request_sets_response_status) {
    CompactRequest request;
    CompactResponse response;

    request.set_autonomous_compaction(true);
    request.set_txn_id(next_id());
    request.add_tablet_ids(_tablet_id);
    request.set_version(1);
    request.set_visible_version(1);

    auto status = _handler.process_request(&request, &response);
    EXPECT_TRUE(status.ok());

    // Response should have OK status
    EXPECT_TRUE(response.has_status());
    EXPECT_EQ(TStatusCode::OK, response.status().status_code());
}

// ============== Test empty tablet list ==============
TEST_F(AutonomousCompactionHandlerTest, test_process_request_empty_tablets) {
    CompactRequest request;
    CompactResponse response;

    request.set_autonomous_compaction(true);
    request.set_txn_id(next_id());
    // No tablet_ids added
    request.set_version(1);
    request.set_visible_version(1);

    auto status = _handler.process_request(&request, &response);
    EXPECT_TRUE(status.ok());

    const auto& stats = _handler.last_stats();
    EXPECT_EQ(0, stats.tablets_processed);
    EXPECT_EQ(0, stats.tablets_with_compaction);
}

// ============== Test with tablet having rowsets ==============
TEST_F(AutonomousCompactionHandlerTest, test_process_request_tablet_with_rowsets) {
    // Create a tablet with some rowsets
    auto metadata = generate_simple_tablet_metadata(DUP_KEYS);
    int64_t tablet_id = metadata->id();
    metadata->set_version(5);

    // Add several rowsets to create compaction opportunity
    for (int i = 0; i < 5; i++) {
        auto* rowset = metadata->add_rowsets();
        rowset->set_id(i + 1);
        rowset->set_overlapped(true);
        rowset->set_num_rows(100);
        rowset->set_data_size(1000);
    }

    CHECK_OK(_tablet_mgr->put_tablet_metadata(metadata));

    CompactRequest request;
    CompactResponse response;

    request.set_autonomous_compaction(true);
    request.set_txn_id(next_id());
    request.add_tablet_ids(tablet_id);
    request.set_version(5);
    request.set_visible_version(5);

    auto status = _handler.process_request(&request, &response);
    EXPECT_TRUE(status.ok());

    const auto& stats = _handler.last_stats();
    EXPECT_EQ(1, stats.tablets_processed);
}

// ============== Test constructor and destructor ==============
TEST_F(AutonomousCompactionHandlerTest, test_constructor_destructor) {
    // Test that we can create and destroy handler without issues
    {
        AutonomousCompactionHandler handler(_tablet_mgr.get());
        EXPECT_EQ(0, handler.last_stats().tablets_processed);
    }
    // Handler should be destroyed without any issues
}

// ============== Test with null tablet manager ==============
// Note: This is just to document behavior, not necessarily recommended
TEST_F(AutonomousCompactionHandlerTest, test_with_nullptr_tablet_mgr) {
    // Creating handler with nullptr tablet_mgr
    // The handler should be created but process_request would fail
    AutonomousCompactionHandler handler(nullptr);
    EXPECT_EQ(0, handler.last_stats().tablets_processed);

    // Don't call process_request as it would likely crash or fail badly
}

} // namespace starrocks::lake


