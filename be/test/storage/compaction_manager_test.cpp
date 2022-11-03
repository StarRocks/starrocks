// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/compaction_manager.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <random>

#include "storage/compaction_context.h"
#include "storage/compaction_task.h"
#include "storage/compaction_utils.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage/tablet_updates.h"

namespace starrocks {

TEST(CompactionManagerTest, test_candidates) {
    std::vector<TabletSharedPtr> tablets;
    DataDir data_dir("./data_dir");
    for (int i = 0; i <= 10; i++) {
        TabletSharedPtr tablet = std::make_shared<Tablet>();
        TabletMetaSharedPtr tablet_meta = std::make_shared<TabletMeta>();
        tablet_meta->set_tablet_id(i);
        tablet->set_tablet_meta(tablet_meta);
        tablet->set_data_dir(&data_dir);
        std::unique_ptr<CompactionContext> compaction_context = std::make_unique<CompactionContext>();
        compaction_context->tablet = tablet;
        // for i == 9 and i == 10, compaction scores are equal
        if (i == 10) {
            compaction_context->cumulative_score = 10;
        } else {
            compaction_context->cumulative_score = 1 + i;
        }
        compaction_context->base_score = 1 + 0.5 * i;
        tablet->set_compaction_context(compaction_context);
        tablets.push_back(tablet);
    }

    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(tablets.begin(), tablets.end(), g);

    for (auto& tablet : tablets) {
        StorageEngine::instance()->compaction_manager()->update_tablet(tablet, false, false);
    }

    {
        ASSERT_EQ(22, StorageEngine::instance()->compaction_manager()->candidates_size());
        CompactionCandidate candidate_1 = StorageEngine::instance()->compaction_manager()->pick_candidate();
        ASSERT_EQ(9, candidate_1.tablet->tablet_id());
        CompactionCandidate candidate_2 = StorageEngine::instance()->compaction_manager()->pick_candidate();
        ASSERT_EQ(10, candidate_2.tablet->tablet_id());
        ASSERT_EQ(candidate_1.tablet->compaction_score(CUMULATIVE_COMPACTION),
                  candidate_2.tablet->compaction_score(CUMULATIVE_COMPACTION));
        double last_score = candidate_2.tablet->compaction_score(CUMULATIVE_COMPACTION);
        while (true) {
            CompactionCandidate candidate = StorageEngine::instance()->compaction_manager()->pick_candidate();
            if (!candidate.is_valid()) {
                break;
            }
            ASSERT_LE(candidate.tablet->compaction_score(candidate.type), last_score);
            last_score = candidate.tablet->compaction_score(candidate.type);
        }
    }

    for (auto & tablet : tablets) {
        std::unique_ptr<CompactionContext> compaction_context;
        tablet->set_compaction_context(compaction_context);
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
        compaction_context->tablet = tablet;
        compaction_context->chosen_compaction_type = CUMULATIVE_COMPACTION;
        compaction_context->cumulative_score = 1 + i;
        compaction_context->base_score = 1 + 0.5 * i;
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
    bool ret = StorageEngine::instance()->compaction_manager()->register_task(
            tasks[config::max_compaction_concurrency].get());
    ASSERT_FALSE(ret);

    ASSERT_EQ(config::max_compaction_concurrency, StorageEngine::instance()->compaction_manager()->running_tasks_num());

    StorageEngine::instance()->compaction_manager()->clear_tasks();
    ASSERT_EQ(0, StorageEngine::instance()->compaction_manager()->running_tasks_num());

    config::cumulative_compaction_num_threads_per_disk = 1;
    for (int i = 0; i < config::max_compaction_concurrency; i++) {
        bool ret = StorageEngine::instance()->compaction_manager()->register_task(tasks[i].get());
        if (i == 0) {
            ASSERT_TRUE(ret);
        } else {
            ASSERT_FALSE(ret);
        }
    }
    ASSERT_EQ(1, StorageEngine::instance()->compaction_manager()->running_tasks_num());
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
    for (auto & tablet : tablets) {
        std::unique_ptr<CompactionContext> compaction_context;
        tablet->set_compaction_context(compaction_context);
    }
}

TEST(CompactionManagerTest, test_next_compaction_task_id) {
    uint64_t start_task_id = StorageEngine::instance()->compaction_manager()->next_compaction_task_id();
    ASSERT_LT(0, start_task_id);
}

} // namespace starrocks
