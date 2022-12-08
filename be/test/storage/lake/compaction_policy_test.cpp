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

#include "storage/lake/compaction_policy.h"

#include <gtest/gtest.h>

#include "fs/fs_util.h"
#include "runtime/exec_env.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_manager.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"

namespace starrocks::lake {

class CompactionPolicyTest : public testing::Test {
public:
    CompactionPolicyTest() {
        _tablet_manager = ExecEnv::GetInstance()->lake_tablet_manager();

        _location_provider = std::make_unique<FixedLocationProvider>(kTestGroupPath);
        _backup_location_provider = _tablet_manager->TEST_set_location_provider(_location_provider.get());

        _tablet_metadata = std::make_shared<TabletMetadata>();
        _tablet_metadata->set_id(next_id());
        _tablet_metadata->set_version(1);
        //
        //  | column | type | KEY | NULL |
        //  +--------+------+-----+------+
        //  |   c0   |  INT | YES |  NO  |
        //  |   c1   |  INT | NO  |  NO  |
        auto schema = _tablet_metadata->mutable_schema();
        schema->set_id(next_id());
        schema->set_num_short_key_columns(1);
        schema->set_keys_type(DUP_KEYS);
        schema->set_num_rows_per_row_block(65535);
        auto c0 = schema->add_column();
        {
            c0->set_unique_id(next_id());
            c0->set_name("c0");
            c0->set_type("INT");
            c0->set_is_key(true);
            c0->set_is_nullable(false);
        }
        auto c1 = schema->add_column();
        {
            c1->set_unique_id(next_id());
            c1->set_name("c1");
            c1->set_type("INT");
            c1->set_is_key(false);
            c1->set_is_nullable(false);
        }
    }

protected:
    constexpr static const char* const kTestGroupPath = "test_lake_compaction_policy";

    void SetUp() override {
        (void)ExecEnv::GetInstance()->lake_tablet_manager()->TEST_set_location_provider(_location_provider.get());
        (void)fs::remove_all(kTestGroupPath);
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kSegmentDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kMetadataDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kTxnLogDirectoryName)));
        CHECK_OK(_tablet_manager->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override {
        ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(_tablet_metadata->id()));
        (void)ExecEnv::GetInstance()->lake_tablet_manager()->TEST_set_location_provider(_backup_location_provider);
        (void)fs::remove_all(kTestGroupPath);
    }

    TabletManager* _tablet_manager;
    std::unique_ptr<FixedLocationProvider> _location_provider;
    LocationProvider* _backup_location_provider;
    std::shared_ptr<TabletMetadata> _tablet_metadata;
};

// rowsets: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
// cumulative point: 3
TEST_F(CompactionPolicyTest, test_cumulative_by_segment_num) {
    _tablet_metadata->set_cumulative_point(3);
    _tablet_metadata->set_version(2);
    for (int i = 1; i < 11; ++i) {
        auto* rowset_metadata = _tablet_metadata->mutable_rowsets()->Add();
        rowset_metadata->mutable_segments()->Add("file");
        if (i <= 3) {
            rowset_metadata->set_overlapped(false);
        } else {
            rowset_metadata->set_overlapped(true);
        }
        rowset_metadata->set_id(i);
    }
    CHECK_OK(_tablet_manager->put_tablet_metadata(*_tablet_metadata));

    ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(_tablet_metadata->id()));
    auto tablet_ptr = std::make_shared<Tablet>(tablet);
    ASSIGN_OR_ABORT(auto compaction_policy, CompactionPolicy::create_compaction_policy(tablet_ptr));
    ASSIGN_OR_ABORT(auto input_rowsets, compaction_policy->pick_rowsets(2));
    // input rowsets: [4, 5, 6, 7, 8, 9, 10]
    ASSERT_EQ(7, input_rowsets.size());
    for (int i = 0; i < input_rowsets.size(); ++i) {
        EXPECT_EQ(i + 4, input_rowsets[i]->id());
    }
}

// rowsets: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
// cumulative point: 6
TEST_F(CompactionPolicyTest, test_base_by_segment_num) {
    _tablet_metadata->set_cumulative_point(6);
    _tablet_metadata->set_version(2);
    for (int i = 1; i < 11; ++i) {
        auto* rowset_metadata = _tablet_metadata->mutable_rowsets()->Add();
        rowset_metadata->mutable_segments()->Add("file");
        if (i <= 6) {
            rowset_metadata->set_overlapped(false);
        } else {
            rowset_metadata->set_overlapped(true);
        }
        rowset_metadata->set_id(i);
    }
    CHECK_OK(_tablet_manager->put_tablet_metadata(*_tablet_metadata));

    ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(_tablet_metadata->id()));
    auto tablet_ptr = std::make_shared<Tablet>(tablet);
    ASSIGN_OR_ABORT(auto compaction_policy, CompactionPolicy::create_compaction_policy(tablet_ptr));
    ASSIGN_OR_ABORT(auto input_rowsets, compaction_policy->pick_rowsets(2));
    // input rowsets: [1, 2, 3, 4, 5, 6]
    ASSERT_EQ(6, input_rowsets.size());
    for (int i = 0; i < 6; ++i) {
        EXPECT_EQ(i + 1, input_rowsets[i]->id());
    }
}

} // namespace starrocks::lake
