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

#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "common/config_compaction_fwd.h"
#include "common/config_primary_key_fwd.h"
#include "exec/exec_env.h"
#include "fs/fs_util.h"
#include "storage/lake/join_path.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_manager.h"
#include "test_util.h"

namespace starrocks::lake {

class LakeCompactionPolicyTest : public TestBase {
public:
    LakeCompactionPolicyTest() : TestBase(kTestDirectory) {
        _tablet_metadata = generate_simple_tablet_metadata(DUP_KEYS);
    }

protected:
    constexpr static const char* const kTestDirectory = "test_lake_compaction_policy";

    void SetUp() override {
        config::tablet_max_versions = 1000;
        config::min_cumulative_compaction_num_singleton_deltas = 3;
        config::max_cumulative_compaction_num_singleton_deltas = 10;
        config::min_base_compaction_num_singleton_deltas = 5;
        config::size_tiered_min_level_size = 131072;
        config::size_tiered_level_multiple = 5;
        config::size_tiered_level_num = 7;

        clear_and_init_test_dir();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override { remove_test_dir_ignore_error(); }

    void add_data_rowset(uint32 id, bool overlap, int64_t level) {
        auto* rowset_metadata = _tablet_metadata->mutable_rowsets()->Add();
        rowset_metadata->add_segment_metas()->set_filename("file1");
        rowset_metadata->add_segment_metas()->set_filename("file2");
        rowset_metadata->set_overlapped(overlap);
        rowset_metadata->set_num_rows(1);
        rowset_metadata->set_data_size(config::size_tiered_min_level_size *
                                       pow(config::size_tiered_level_multiple, level - 1) / 2);
        rowset_metadata->set_id(id);
        std::cout << "data rowset: " << id << ", data size: " << rowset_metadata->data_size() << std::endl;
    }

    void add_delete_rowset(uint32 id) {
        auto* rowset_metadata = _tablet_metadata->mutable_rowsets()->Add();
        rowset_metadata->set_overlapped(false);
        rowset_metadata->set_num_rows(0);
        rowset_metadata->set_data_size(0);
        rowset_metadata->set_id(id);
        auto* delete_predicate = rowset_metadata->mutable_delete_predicate();
        delete_predicate->set_version(-1);
        auto* binary_predicate = delete_predicate->add_binary_predicates();
        binary_predicate->set_column_name("c0");
        binary_predicate->set_op("<");
        binary_predicate->set_value("4");
        std::cout << "delete rowset: " << id << std::endl;
    }

    std::shared_ptr<TabletMetadata> _tablet_metadata;
};

// ------ BaseAndCumulativeCompactionPolicy ------

// rowsets: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
// cumulative point: 3
// compaction input rowsets: [4, 5, 6, 7, 8, 9, 10]
TEST_F(LakeCompactionPolicyTest, test_cumulative_by_segment_num) {
    config::enable_size_tiered_compaction_strategy = false;

    _tablet_metadata->set_cumulative_point(3);
    _tablet_metadata->set_version(2);
    for (int i = 1; i < 11; ++i) {
        auto* rowset_metadata = _tablet_metadata->mutable_rowsets()->Add();
        rowset_metadata->add_segment_metas()->set_filename("file");
        if (i <= 3) {
            rowset_metadata->set_overlapped(false);
        } else {
            rowset_metadata->set_overlapped(true);
        }
        rowset_metadata->set_id(i);
    }
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));

    ASSERT_EQ(7, compaction_score(_tablet_mgr.get(), _tablet_metadata));

    ASSIGN_OR_ABORT(auto compaction_policy,
                    CompactionPolicy::create(_tablet_mgr.get(), _tablet_metadata, false /* force_base_compaction */));
    ASSIGN_OR_ABORT(auto input_rowsets, compaction_policy->pick_rowsets());
    // compaction input rowsets: [4, 5, 6, 7, 8, 9, 10]
    ASSERT_EQ(7, input_rowsets.size());
    for (int i = 0; i < input_rowsets.size(); ++i) {
        EXPECT_EQ(i + 4, input_rowsets[i]->id());
    }
}

// rowsets: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
// cumulative point: 6
// compaction input rowsets: [1, 2, 3, 4, 5, 6]
TEST_F(LakeCompactionPolicyTest, test_base_by_segment_num) {
    config::enable_size_tiered_compaction_strategy = false;

    _tablet_metadata->set_cumulative_point(6);
    _tablet_metadata->set_version(2);
    for (int i = 1; i < 11; ++i) {
        auto* rowset_metadata = _tablet_metadata->mutable_rowsets()->Add();
        rowset_metadata->add_segment_metas()->set_filename("file");
        if (i <= 6) {
            rowset_metadata->set_overlapped(false);
        } else {
            rowset_metadata->set_overlapped(true);
        }
        rowset_metadata->set_id(i);
    }
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));

    ASSERT_EQ(6, compaction_score(_tablet_mgr.get(), _tablet_metadata));

    ASSIGN_OR_ABORT(auto compaction_policy,
                    CompactionPolicy::create(_tablet_mgr.get(), _tablet_metadata, false /* force_base_compaction */));
    ASSIGN_OR_ABORT(auto input_rowsets, compaction_policy->pick_rowsets());
    // compaction input rowsets: [1, 2, 3, 4, 5, 6]
    ASSERT_EQ(6, input_rowsets.size());
    for (int i = 0; i < input_rowsets.size(); ++i) {
        EXPECT_EQ(i + 1, input_rowsets[i]->id());
    }
}

// ------ SizeTieredCompactionPolicy ------

// 1 rowset with 2 overlap segments
//
// rowsets:      [1]
// rowsets size: [327680]
// compaction input rowsets: []
TEST_F(LakeCompactionPolicyTest, test_size_tiered_min_compaction) {
    config::enable_size_tiered_compaction_strategy = true;

    add_data_rowset(1, true, 2);

    _tablet_metadata->set_version(2);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));

    ASSERT_EQ(2, compaction_score(_tablet_mgr.get(), _tablet_metadata));

    ASSIGN_OR_ABORT(auto compaction_policy,
                    CompactionPolicy::create(_tablet_mgr.get(), _tablet_metadata, false /* force_base_compaction */));
    ASSIGN_OR_ABORT(auto input_rowsets, compaction_policy->pick_rowsets());
    // compaction input rowsets: []
    ASSERT_TRUE(input_rowsets.empty());
}

// 6 rowsets in level 2
//
// rowsets:      [1, 2, 3, 4, 5, 6]
// rowsets size: [327680, 327680, 327680, 327680, 327680, 327680]
// compaction input rowsets: [1, 2, 3, 4, 5, 6]
TEST_F(LakeCompactionPolicyTest, test_size_tiered_max_compaction) {
    config::enable_size_tiered_compaction_strategy = true;

    for (int i = 1; i < 7; ++i) {
        add_data_rowset(i, false, 2);
    }

    _tablet_metadata->set_version(2);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));

    ASSERT_EQ(6, compaction_score(_tablet_mgr.get(), _tablet_metadata));

    ASSIGN_OR_ABORT(auto compaction_policy,
                    CompactionPolicy::create(_tablet_mgr.get(), _tablet_metadata, false /* force_base_compaction */));
    ASSIGN_OR_ABORT(auto input_rowsets, compaction_policy->pick_rowsets());
    // compaction input rowsets: [1, 2, 3, 4, 5, 6]
    ASSERT_EQ(6, input_rowsets.size());
    for (int i = 0; i < input_rowsets.size(); ++i) {
        EXPECT_EQ(i + 1, input_rowsets[i]->id());
    }
}

// 6 rowsets in level 2
// config::max_cumulative_compaction_num_singleton_deltas = 5
//
// rowsets:      [1, 2, 3, 4, 5, 6]
// rowsets size: [327680, 327680, 327680, 327680, 327680, 327680]
// compaction input rowsets: [1, 2, 3, 4, 5]
TEST_F(LakeCompactionPolicyTest, test_size_tiered_max_compaction_by_max_singleton_deltas_config) {
    config::enable_size_tiered_compaction_strategy = true;
    config::max_cumulative_compaction_num_singleton_deltas = 5;

    for (int i = 1; i < 7; ++i) {
        add_data_rowset(i, false, 2);
    }

    _tablet_metadata->set_version(2);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));

    ASSERT_EQ(6, compaction_score(_tablet_mgr.get(), _tablet_metadata));

    ASSIGN_OR_ABORT(auto compaction_policy,
                    CompactionPolicy::create(_tablet_mgr.get(), _tablet_metadata, false /* force_base_compaction */));
    ASSIGN_OR_ABORT(auto input_rowsets, compaction_policy->pick_rowsets());
    // compaction input rowsets: [1, 2, 3, 4, 5]
    ASSERT_EQ(5, input_rowsets.size());
    for (int i = 0; i < input_rowsets.size(); ++i) {
        EXPECT_EQ(i + 1, input_rowsets[i]->id());
    }
}

// 2 data rowsets, 1 delete rowset middle
//
// rowsets:      [1, 2, 3]
// rowsets size: [327680, delete, 327680]
// compaction input rowsets: [1, 2, 3]
TEST_F(LakeCompactionPolicyTest, test_size_tiered_one_delete_middle) {
    config::enable_size_tiered_compaction_strategy = true;

    uint32 id = 1;
    add_data_rowset(id++, false, 2);
    add_delete_rowset(id++);
    add_data_rowset(id++, true, 2);

    _tablet_metadata->set_version(2);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));

    ASSERT_EQ(4, compaction_score(_tablet_mgr.get(), _tablet_metadata));

    ASSIGN_OR_ABORT(auto compaction_policy,
                    CompactionPolicy::create(_tablet_mgr.get(), _tablet_metadata, false /* force_base_compaction */));
    ASSIGN_OR_ABORT(auto input_rowsets, compaction_policy->pick_rowsets());
    // compaction input rowsets: [1, 2, 3]
    ASSERT_EQ(3, input_rowsets.size());
    for (int i = 0; i < input_rowsets.size(); ++i) {
        EXPECT_EQ(i + 1, input_rowsets[i]->id());
    }
}

// 2 data rowsets, 2 delete rowsets middle
//
// rowsets:      [1, 2, 3, 4]
// rowsets size: [327680, delete, delete, 327680]
// compaction input rowsets: [1, 2, 3, 4]
TEST_F(LakeCompactionPolicyTest, test_size_tiered_two_delete_middle) {
    config::enable_size_tiered_compaction_strategy = true;

    uint32 id = 1;
    add_data_rowset(id++, false, 2);
    add_delete_rowset(id++);
    add_delete_rowset(id++);
    add_data_rowset(id++, true, 2);

    _tablet_metadata->set_version(2);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));

    ASSERT_EQ(5, compaction_score(_tablet_mgr.get(), _tablet_metadata));

    ASSIGN_OR_ABORT(auto compaction_policy,
                    CompactionPolicy::create(_tablet_mgr.get(), _tablet_metadata, false /* force_base_compaction */));
    ASSIGN_OR_ABORT(auto input_rowsets, compaction_policy->pick_rowsets());
    // compaction input rowsets: [1, 2, 3, 4]
    ASSERT_EQ(4, input_rowsets.size());
    for (int i = 0; i < input_rowsets.size(); ++i) {
        EXPECT_EQ(i + 1, input_rowsets[i]->id());
    }
}

// 2 delete rowsets first, 2 data rowsets
//
// rowsets:      [1, 2, 3, 4]
// rowsets size: [delete, delete, 327680, 327680]
// compaction input rowsets: [1, 2, 3, 4]
TEST_F(LakeCompactionPolicyTest, test_size_tiered_two_delete_first) {
    config::enable_size_tiered_compaction_strategy = true;

    uint32 id = 1;
    add_delete_rowset(id++);
    add_delete_rowset(id++);
    add_data_rowset(id++, false, 2);
    add_data_rowset(id++, true, 2);

    _tablet_metadata->set_version(2);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));

    ASSERT_EQ(5, compaction_score(_tablet_mgr.get(), _tablet_metadata));

    ASSIGN_OR_ABORT(auto compaction_policy,
                    CompactionPolicy::create(_tablet_mgr.get(), _tablet_metadata, false /* force_base_compaction */));
    ASSIGN_OR_ABORT(auto input_rowsets, compaction_policy->pick_rowsets());
    // compaction input rowsets: [1, 2, 3, 4]
    ASSERT_EQ(4, input_rowsets.size());
    for (int i = 0; i < input_rowsets.size(); ++i) {
        EXPECT_EQ(i + 1, input_rowsets[i]->id());
    }
}

// 6 data rowsets, 1 delete rowset, config::tablet_max_versions = 10
//
// rowsets:      [1, 2, 3, 4, 5, 6, 7]
// rowsets size: [1638400, 1638400, 327680, 327680, 327680, 65536, delete]
// compaction input rowsets: [1, 2, 3, 4, 5, 6, 7]
TEST_F(LakeCompactionPolicyTest, test_size_tiered_delete_limit_force_base_compaction) {
    config::enable_size_tiered_compaction_strategy = true;
    config::tablet_max_versions = 10;

    uint32 id = 1;
    add_data_rowset(id++, false, 3);
    add_data_rowset(id++, false, 3);
    add_data_rowset(id++, false, 2);
    add_data_rowset(id++, false, 2);
    add_data_rowset(id++, false, 2);
    add_data_rowset(id++, false, 1);
    add_delete_rowset(id++);

    _tablet_metadata->set_version(2);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));

    ASSERT_EQ(7, compaction_score(_tablet_mgr.get(), _tablet_metadata));

    ASSIGN_OR_ABORT(auto compaction_policy,
                    CompactionPolicy::create(_tablet_mgr.get(), _tablet_metadata, false /* force_base_compaction */));
    ASSIGN_OR_ABORT(auto input_rowsets, compaction_policy->pick_rowsets());
    // compaction input rowsets: [1, 2, 3, 4, 5, 6, 7]
    ASSERT_EQ(7, input_rowsets.size());
    for (int i = 0; i < input_rowsets.size(); ++i) {
        EXPECT_EQ(i + 1, input_rowsets[i]->id());
    }
}

// 3 rowsets, descending order level size
//
// rowsets:      [1, 2, 3]
// rowsets size: [8192000, 1638400, 327680]
// compaction input rowsets: []
TEST_F(LakeCompactionPolicyTest, test_size_tiered_descending_order_level_size) {
    config::enable_size_tiered_compaction_strategy = true;

    uint32 id = 1;
    add_data_rowset(id++, false, 4);
    add_data_rowset(id++, true, 3);
    add_data_rowset(id++, true, 2);

    _tablet_metadata->set_version(2);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));

    ASSERT_EQ(2, compaction_score(_tablet_mgr.get(), _tablet_metadata));

    ASSIGN_OR_ABORT(auto compaction_policy,
                    CompactionPolicy::create(_tablet_mgr.get(), _tablet_metadata, false /* force_base_compaction */));
    ASSIGN_OR_ABORT(auto input_rowsets, compaction_policy->pick_rowsets());
    // compaction input rowsets: []
    ASSERT_TRUE(input_rowsets.empty());
}

// 8 rowsets, multi descending order level size
//
// rowsets:      [1, 2, 3, 4, 5, 6, 7, 8]
// rowsets size: [8192000, 1638400, 1638400, 1638400, 327680, 327680, 327680, 327680]
TEST_F(LakeCompactionPolicyTest, test_size_tiered_multi_descending_order_level_size) {
    config::enable_size_tiered_compaction_strategy = true;

    // compaction version 2
    // rowsets: [1, 2, 3, 4, 5, 6, 7, 8]
    uint32 id = 1;
    add_data_rowset(id++, true, 4);
    add_data_rowset(id++, true, 3);
    add_data_rowset(id++, false, 3);
    add_data_rowset(id++, true, 3);
    add_data_rowset(id++, true, 2);
    add_data_rowset(id++, false, 2);
    add_data_rowset(id++, true, 2);
    add_data_rowset(id++, true, 2);

    _tablet_metadata->set_version(2);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));

    ASSERT_EQ(7, compaction_score(_tablet_mgr.get(), _tablet_metadata));

    ASSIGN_OR_ABORT(auto compaction_policy,
                    CompactionPolicy::create(_tablet_mgr.get(), _tablet_metadata, false /* force_base_compaction */));

    // compact 5 ~ 8
    ASSIGN_OR_ABORT(auto input_rowsets, compaction_policy->pick_rowsets());
    // compaction input rowsets: [5, 6, 7, 8]
    ASSERT_EQ(4, input_rowsets.size());
    for (int i = 0; i < input_rowsets.size(); ++i) {
        EXPECT_EQ(i + 5, input_rowsets[i]->id());
    }
    input_rowsets.clear();
    for (int i = 0; i < 4; i++) {
        _tablet_metadata->mutable_rowsets()->RemoveLast();
    }

    // compaction version 3
    // rowsets: [1, 2, 3, 4, 9]
    add_data_rowset(id++, false, 3);
    ASSERT_EQ(5, _tablet_metadata->rowsets_size());

    _tablet_metadata->set_version(3);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));

    ASSERT_EQ(6, compaction_score(_tablet_mgr.get(), _tablet_metadata));
    ASSIGN_OR_ABORT(compaction_policy,
                    CompactionPolicy::create(_tablet_mgr.get(), _tablet_metadata, false /* force_base_compaction */));

    // compact 2 ~ 4, 9
    ASSIGN_OR_ABORT(input_rowsets, compaction_policy->pick_rowsets());
    // compaction input rowsets: [2, 3, 4, 9]
    ASSERT_EQ(4, input_rowsets.size());
    for (int i = 0, size = input_rowsets.size(); i < size; ++i) {
        if (i == size - 1) {
            EXPECT_EQ(9, input_rowsets[i]->id());
        } else {
            EXPECT_EQ(i + 2, input_rowsets[i]->id());
        }
    }
    input_rowsets.clear();
    for (int i = 0, size = 4; i < size; ++i) {
        _tablet_metadata->mutable_rowsets()->RemoveLast();
    }

    // compaction version 4
    // rowsets: [1, 10], rowset 1 is overlap
    add_data_rowset(id++, false, 4);
    ASSERT_EQ(2, _tablet_metadata->rowsets_size());

    _tablet_metadata->set_version(4);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));

    ASSERT_EQ(3, compaction_score(_tablet_mgr.get(), _tablet_metadata));
    ASSIGN_OR_ABORT(compaction_policy,
                    CompactionPolicy::create(_tablet_mgr.get(), _tablet_metadata, false /* force_base_compaction */));

    // compact 1, 10
    ASSIGN_OR_ABORT(input_rowsets, compaction_policy->pick_rowsets());
    // compaction input rowsets: [1, 10]
    ASSERT_EQ(2, input_rowsets.size());
    EXPECT_EQ(1, input_rowsets[0]->id());
    EXPECT_EQ(10, input_rowsets[1]->id());
}

// 3 rowsets, order level size
//
// rowsets:      [1, 2, 3]
// rowsets size: [327680, 1638400, 8192000]
// compaction input rowsets: [1, 2, 3]
TEST_F(LakeCompactionPolicyTest, test_size_tiered_order_level_size) {
    config::enable_size_tiered_compaction_strategy = true;

    uint32 id = 1;
    add_data_rowset(id++, false, 2);
    add_data_rowset(id++, true, 3);
    add_data_rowset(id++, true, 4);

    _tablet_metadata->set_version(2);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));

    ASSERT_EQ(5, compaction_score(_tablet_mgr.get(), _tablet_metadata));

    ASSIGN_OR_ABORT(auto compaction_policy,
                    CompactionPolicy::create(_tablet_mgr.get(), _tablet_metadata, false /* force_base_compaction */));
    ASSIGN_OR_ABORT(auto input_rowsets, compaction_policy->pick_rowsets());
    // compaction input rowsets: [1, 2, 3]
    ASSERT_EQ(3, input_rowsets.size());
    for (int i = 0; i < input_rowsets.size(); ++i) {
        EXPECT_EQ(i + 1, input_rowsets[i]->id());
    }
}

// 2 data rowsets, 1 delete rowset at last
//
// rowsets:      [1, 2, 3]
// rowsets size: [1638400, 327680, delete]
// compaction input rowsets: [1, 2, 3]
TEST_F(LakeCompactionPolicyTest, test_size_tiered_backtrace_base_compaction_delete_last) {
    config::enable_size_tiered_compaction_strategy = true;

    uint32 id = 1;
    add_data_rowset(id++, false, 3);
    add_data_rowset(id++, false, 2);
    add_delete_rowset(id++);

    _tablet_metadata->set_version(2);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));

    ASSERT_EQ(3, compaction_score(_tablet_mgr.get(), _tablet_metadata));

    ASSIGN_OR_ABORT(auto compaction_policy,
                    CompactionPolicy::create(_tablet_mgr.get(), _tablet_metadata, false /* force_base_compaction */));
    ASSIGN_OR_ABORT(auto input_rowsets, compaction_policy->pick_rowsets());
    // compaction input rowsets: [1, 2, 3]
    ASSERT_EQ(3, input_rowsets.size());
    for (int i = 0; i < input_rowsets.size(); ++i) {
        EXPECT_EQ(i + 1, input_rowsets[i]->id());
    }
}

// 6 data rowsets, 1 delete rowset at last
// 3 data rowsets in level 2
//
// rowsets:      [1, 2, 3, 4, 5, 6, 7]
// rowsets size: [1638400, 1638400, 327680, 327680, 327680, 65536, delete]
TEST_F(LakeCompactionPolicyTest, test_size_tiered_backtrace_base_compaction_delete_last_2) {
    config::enable_size_tiered_compaction_strategy = true;

    // compaction version 2
    // rowsets: [1, 2, 3, 4, 5, 6, 7]
    uint32 id = 1;
    add_data_rowset(id++, false, 3);
    add_data_rowset(id++, false, 3);
    add_data_rowset(id++, false, 2);
    add_data_rowset(id++, false, 2);
    add_data_rowset(id++, false, 2);
    add_data_rowset(id++, false, 1);
    add_delete_rowset(id++);

    _tablet_metadata->set_version(2);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));

    ASSERT_EQ(3, compaction_score(_tablet_mgr.get(), _tablet_metadata));

    ASSIGN_OR_ABORT(auto compaction_policy,
                    CompactionPolicy::create(_tablet_mgr.get(), _tablet_metadata, false /* force_base_compaction */));
    // compact 3 ~ 5
    ASSIGN_OR_ABORT(auto input_rowsets, compaction_policy->pick_rowsets());
    // compaction input rowsets: [3, 4, 5]
    ASSERT_EQ(3, input_rowsets.size());
    for (int i = 0; i < input_rowsets.size(); ++i) {
        EXPECT_EQ(i + 3, input_rowsets[i]->id());
    }

    // compaction version 3
    // rowsets: [1, 2, 8, 6, 7]
    _tablet_metadata->clear_rowsets();
    add_data_rowset(1, false, 3);
    add_data_rowset(2, false, 3);
    add_data_rowset(8, false, 3);
    add_data_rowset(6, false, 1);
    add_delete_rowset(7);

    _tablet_metadata->set_version(3);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));

    ASSERT_EQ(5, compaction_score(_tablet_mgr.get(), _tablet_metadata));
    ASSIGN_OR_ABORT(compaction_policy,
                    CompactionPolicy::create(_tablet_mgr.get(), _tablet_metadata, false /* force_base_compaction */));

    // compact 1, 2, 8, 6, 7
    ASSIGN_OR_ABORT(input_rowsets, compaction_policy->pick_rowsets());
    // compaction input rowsets: [1, 2, 8, 6, 7]
    ASSERT_EQ(5, input_rowsets.size());
    EXPECT_EQ(1, input_rowsets[0]->id());
    EXPECT_EQ(2, input_rowsets[1]->id());
    EXPECT_EQ(8, input_rowsets[2]->id());
    EXPECT_EQ(6, input_rowsets[3]->id());
    EXPECT_EQ(7, input_rowsets[4]->id());
}

// 4 data rowsets, multi delete rowset, 1 data rowset at last
//
// rowsets:      [1, 2, 3, 4, 5, 6]
// rowsets size: [1638400, 327680, delete, 65536, delete, 65536]
// compaction input rowsets: [1, 2, 3, 4, 5]
TEST_F(LakeCompactionPolicyTest, test_size_tiered_backtrace_base_compaction_multi_delete_middle) {
    config::enable_size_tiered_compaction_strategy = true;

    uint32 id = 1;
    add_data_rowset(id++, false, 3);
    add_data_rowset(id++, false, 2);
    add_delete_rowset(id++);
    add_data_rowset(id++, false, 1);
    add_delete_rowset(id++);
    add_data_rowset(id++, false, 1);

    _tablet_metadata->set_version(2);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));

    ASSERT_EQ(5, compaction_score(_tablet_mgr.get(), _tablet_metadata));

    ASSIGN_OR_ABORT(auto compaction_policy,
                    CompactionPolicy::create(_tablet_mgr.get(), _tablet_metadata, false /* force_base_compaction */));
    ASSIGN_OR_ABORT(auto input_rowsets, compaction_policy->pick_rowsets());
    // compaction input rowsets: [1, 2, 3, 4, 5]
    ASSERT_EQ(5, input_rowsets.size());
    for (int i = 0; i < input_rowsets.size(); ++i) {
        EXPECT_EQ(i + 1, input_rowsets[i]->id());
    }
}

// 3 data rowsets, continuous delete rowsets, 1 data rowset at last
//
// rowsets:      [1, 2, 3, 4, 5, 6]
// rowsets size: [1638400, 327680, delete, delete, delete, 65536]
// compaction input rowsets: [1, 2, 3, 4, 5]
TEST_F(LakeCompactionPolicyTest, test_size_tiered_backtrace_base_compaction_continous_delete_middle) {
    config::enable_size_tiered_compaction_strategy = true;

    uint32 id = 1;
    add_data_rowset(id++, false, 3);
    add_data_rowset(id++, false, 2);
    add_delete_rowset(id++);
    add_delete_rowset(id++);
    add_delete_rowset(id++);
    add_data_rowset(id++, false, 1);

    _tablet_metadata->set_version(2);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));

    ASSERT_EQ(5, compaction_score(_tablet_mgr.get(), _tablet_metadata));

    ASSIGN_OR_ABORT(auto compaction_policy,
                    CompactionPolicy::create(_tablet_mgr.get(), _tablet_metadata, false /* force_base_compaction */));
    ASSIGN_OR_ABORT(auto input_rowsets, compaction_policy->pick_rowsets());
    // compaction input rowsets: [1, 2, 3, 4, 5]
    ASSERT_EQ(5, input_rowsets.size());
    for (int i = 0; i < input_rowsets.size(); ++i) {
        EXPECT_EQ(i + 1, input_rowsets[i]->id());
    }
}

// Build a primary-key tablet whose rowsets carry explicit num_dels (so the policy does not need
// real delete-vector files) and verify PrimaryCompactionPolicy switches to base compaction --
// reclaiming the delete-bearing rowsets, most deleted rows first -- when the tablet's delete ratio
// or absolute delete-row count crosses its threshold, or a base compaction is forced, while
// leaving the clean small rowsets alone.
TEST_F(LakeCompactionPolicyTest, test_pk_base_compaction_triggers) {
    // Restore the base-compaction configs on every exit path, including a mid-test ASSERT failure,
    // so modified values don't leak into other tests.
    struct ConfigGuard {
        double ratio = config::lake_pk_compaction_base_delete_ratio_threshold;
        int64_t rows = config::lake_pk_compaction_base_delete_rows_threshold;
        ~ConfigGuard() {
            config::lake_pk_compaction_base_delete_ratio_threshold = ratio;
            config::lake_pk_compaction_base_delete_rows_threshold = rows;
        }
    } config_guard;

    constexpr int64_t kBig = 100 * 1024 * 1024; // 100 MB
    constexpr int64_t kSmall = 2 * 1024 * 1024; // 2 MB
    // Big value that effectively disables the threshold it is assigned to.
    constexpr int64_t kDisabledRows = 1'000'000'000LL;
    constexpr double kDisabledRatio = 0.99;

    auto metadata = generate_simple_tablet_metadata(PRIMARY_KEYS);
    metadata->set_version(2);
    struct RowsetSpec {
        uint32_t id;
        int64_t num_rows;
        int64_t num_dels;
        int64_t data_size;
    };
    // rowset 1: 80% deleted (3.2M dels), rowset 2: 50% deleted (2.0M dels) -- both delete-bearing;
    // rowsets 3,4: fresh, clean small rowsets with no deletes.
    // Tablet aggregate: sum(num_dels) = 5.2M, sum(num_rows) = 8.1M -> ratio ~= 0.64.
    const std::vector<RowsetSpec> specs = {
            {1, 4000000, 3200000, kBig}, {2, 4000000, 2000000, kBig}, {3, 50000, 0, kSmall}, {4, 50000, 0, kSmall}};
    for (const auto& s : specs) {
        auto* r = metadata->mutable_rowsets()->Add();
        r->set_id(s.id);
        r->set_overlapped(false);
        r->add_segment_metas()->set_filename("seg");
        r->set_num_rows(s.num_rows);
        r->set_num_dels(s.num_dels);
        r->set_data_size(s.data_size);
    }
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*metadata));

    // Base compaction is a full merge of all rowsets, ordered by absolute delete-row count
    // (num_dels) descending: the two delete-bearing rowsets (1 with 3.2M, then 2 with 2.0M) come
    // first, ahead of the clean rowsets 3 and 4 (num_dels 0, either order). All four fit within the
    // result-bytes budget, so all four are picked.
    auto expect_base_pick = [](const std::vector<RowsetPtr>& rowsets) {
        ASSERT_EQ(4, rowsets.size());
        EXPECT_EQ(1, rowsets[0]->id());
        EXPECT_EQ(2, rowsets[1]->id());
        // The remaining two are the clean rowsets 3 and 4, in either order.
        EXPECT_EQ(7, rowsets[2]->id() + rowsets[3]->id());
    };

    // Case A: ratio trigger. Aggregate ratio (0.64) >= ratio threshold (0.5); count trigger off.
    config::lake_pk_compaction_base_delete_ratio_threshold = 0.5;
    config::lake_pk_compaction_base_delete_rows_threshold = kDisabledRows;
    ASSIGN_OR_ABORT(auto ratio_policy,
                    CompactionPolicy::create(_tablet_mgr.get(), metadata, false /* force_base_compaction */));
    ASSIGN_OR_ABORT(auto ratio_rowsets, ratio_policy->pick_rowsets());
    expect_base_pick(ratio_rowsets);

    // Case B: absolute-count trigger (the hot-table case). Aggregate ratio (0.64) < ratio threshold
    // (0.99) so the ratio does NOT trigger, but sum(num_dels)=5.2M >= count threshold (1M) does --
    // this is exactly the account case where a low aggregate ratio hides a large absolute delete
    // volume.
    config::lake_pk_compaction_base_delete_ratio_threshold = kDisabledRatio;
    config::lake_pk_compaction_base_delete_rows_threshold = 1000000;
    ASSIGN_OR_ABORT(auto count_policy,
                    CompactionPolicy::create(_tablet_mgr.get(), metadata, false /* force_base_compaction */));
    ASSIGN_OR_ABORT(auto count_rowsets, count_policy->pick_rowsets());
    expect_base_pick(count_rowsets);

    // Case C: force_base_compaction (from ALTER ... COMPACT) triggers base even with both
    // thresholds disabled.
    config::lake_pk_compaction_base_delete_ratio_threshold = kDisabledRatio;
    config::lake_pk_compaction_base_delete_rows_threshold = kDisabledRows;
    ASSIGN_OR_ABORT(auto forced_policy,
                    CompactionPolicy::create(_tablet_mgr.get(), metadata, true /* force_base_compaction */));
    ASSIGN_OR_ABORT(auto forced_rowsets, forced_policy->pick_rowsets());
    expect_base_pick(forced_rowsets);

    // Case D: neither trigger met and no force -> cumulative (size-tiered) selection, which does
    // NOT force-pick the delete-heavy rowsets. With only 4 non-overlapped segments
    // (< lake_pk_compaction_min_input_segments), size-tiered declines to compact, so the result is
    // empty -- proving the base pick was not taken.
    config::lake_pk_compaction_base_delete_ratio_threshold = kDisabledRatio;
    config::lake_pk_compaction_base_delete_rows_threshold = kDisabledRows;
    ASSIGN_OR_ABORT(auto cumulative_policy,
                    CompactionPolicy::create(_tablet_mgr.get(), metadata, false /* force_base_compaction */));
    ASSIGN_OR_ABORT(auto cumulative_rowsets, cumulative_policy->pick_rowsets());
    EXPECT_TRUE(cumulative_rowsets.empty());
}

} // namespace starrocks::lake
