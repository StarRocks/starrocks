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

#include "storage/compaction_manager.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <random>

#include "storage/compaction_context.h"
#include "storage/compaction_task.h"
#include "storage/compaction_utils.h"
#include "storage/default_compaction_policy.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage/tablet_updates.h"

namespace starrocks {

TEST(CompactionManagerTest, test_candidates) {
    std::vector<CompactionCandidate> candidates;
    DataDir data_dir("./data_dir");
    for (int i = 0; i <= 10; i++) {
        TabletSharedPtr tablet = std::make_shared<Tablet>();
        TabletMetaSharedPtr tablet_meta = std::make_shared<TabletMeta>();
        tablet_meta->set_tablet_id(i);
        tablet->set_tablet_meta(tablet_meta);
        tablet->set_data_dir(&data_dir);
        tablet->set_tablet_state(TABLET_RUNNING);

        // for i == 9 and i == 10, compaction scores are equal
        CompactionCandidate candidate;
        candidate.tablet = tablet;
        if (i == 10) {
            candidate.score = 10;
        } else {
            candidate.score = 1 + i;
        }
        candidates.push_back(candidate);
    }

    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(candidates.begin(), candidates.end(), g);

    StorageEngine::instance()->compaction_manager()->update_candidates(candidates);

    {
        ASSERT_EQ(11, StorageEngine::instance()->compaction_manager()->candidates_size());
        CompactionCandidate candidate_1;
        StorageEngine::instance()->compaction_manager()->pick_candidate(&candidate_1);
        ASSERT_EQ(9, candidate_1.tablet->tablet_id());
        CompactionCandidate candidate_2;
        StorageEngine::instance()->compaction_manager()->pick_candidate(&candidate_2);
        ASSERT_EQ(10, candidate_2.tablet->tablet_id());
        ASSERT_EQ(candidate_1.score, candidate_2.score);
        double last_score = candidate_2.score;
        while (true) {
            CompactionCandidate candidate;
            auto valid = StorageEngine::instance()->compaction_manager()->pick_candidate(&candidate);
            if (!valid) {
                break;
            }
            ASSERT_LE(candidate.score, last_score);
            last_score = candidate.score;
        }
    }
}

class MockCompactionTask : public CompactionTask {
public:
    MockCompactionTask() : CompactionTask(HORIZONTAL_COMPACTION) {}

    ~MockCompactionTask() override = default;

    Status run_impl() override { return Status::OK(); }
};

TEST(CompactionManagerTest, test_compaction_tasks) {
    std::vector<TabletSharedPtr> tablets;
    std::vector<std::shared_ptr<MockCompactionTask>> tasks;
    DataDir data_dir("./data_dir");
    // generate compaction task
    config::max_compaction_concurrency = 2;
    config::cumulative_compaction_num_threads_per_disk = config::max_compaction_concurrency;
    for (int i = 0; i < config::max_compaction_concurrency + 1; i++) {
        TabletSharedPtr tablet = std::make_shared<Tablet>();
        TabletMetaSharedPtr tablet_meta = std::make_shared<TabletMeta>();
        tablet_meta->set_tablet_id(i);
        tablet->set_tablet_meta(tablet_meta);
        tablet->set_data_dir(&data_dir);
        std::unique_ptr<CompactionContext> compaction_context = std::make_unique<CompactionContext>();
        compaction_context->policy = std::make_unique<DefaultCumulativeBaseCompactionPolicy>(tablet.get());
        tablet->set_compaction_context(compaction_context);
        tablets.push_back(tablet);
        std::shared_ptr<MockCompactionTask> task = std::make_shared<MockCompactionTask>();
        task->set_tablet(tablet);
        task->set_task_id(i);
        task->set_compaction_type(CUMULATIVE_COMPACTION);
        tasks.emplace_back(std::move(task));
    }

    StorageEngine::instance()->compaction_manager()->init_max_task_num(config::max_compaction_concurrency);

    for (int i = 0; i < config::max_compaction_concurrency; i++) {
        bool ret = StorageEngine::instance()->compaction_manager()->register_task(tasks[i].get());
        ASSERT_TRUE(ret);
    }

    ASSERT_EQ(config::max_compaction_concurrency, StorageEngine::instance()->compaction_manager()->running_tasks_num());

    StorageEngine::instance()->compaction_manager()->clear_tasks();
    ASSERT_EQ(0, StorageEngine::instance()->compaction_manager()->running_tasks_num());

    config::cumulative_compaction_num_threads_per_disk = 1;
    for (int i = 0; i < config::max_compaction_concurrency; i++) {
        bool ret = StorageEngine::instance()->compaction_manager()->register_task(tasks[i].get());
        ASSERT_TRUE(ret);
    }
    ASSERT_EQ(config::max_compaction_concurrency, StorageEngine::instance()->compaction_manager()->running_tasks_num());
    StorageEngine::instance()->compaction_manager()->clear_tasks();
    ASSERT_EQ(0, StorageEngine::instance()->compaction_manager()->running_tasks_num());

    config::cumulative_compaction_num_threads_per_disk = 4;
    for (int i = 0; i < 1; i++) {
        bool ret = StorageEngine::instance()->compaction_manager()->register_task(tasks[i].get());
        ASSERT_TRUE(ret);
    }

    StorageEngine::instance()->compaction_manager()->clear_tasks();

    config::base_compaction_num_threads_per_disk = 4;
    for (int i = 0; i < 1; i++) {
        tasks[i]->set_compaction_type(BASE_COMPACTION);
        bool ret = StorageEngine::instance()->compaction_manager()->register_task(tasks[i].get());
        ASSERT_TRUE(ret);
    }
}

TEST(CompactionManagerTest, test_next_compaction_task_id) {
    uint64_t start_task_id = StorageEngine::instance()->compaction_manager()->next_compaction_task_id();
    ASSERT_LT(0, start_task_id);
}

} // namespace starrocks
