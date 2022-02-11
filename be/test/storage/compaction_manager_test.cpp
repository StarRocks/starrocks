// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/compaction_manager.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <random>

#include "storage/compaction_context.h"
#include "storage/compaction_task.h"
#include "storage/compaction_utils.h"
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
        compaction_context->tablet = tablet.get();
        compaction_context->current_level = 0;
        // for i == 9 and i == 10, compaction scores are equal
        if (i == 10) {
            compaction_context->compaction_scores[0] = 10;
        } else {
            compaction_context->compaction_scores[0] = 1 + i;
        }
        compaction_context->compaction_scores[1] = 1 + 0.5 * i;
        tablet->set_compaction_context(compaction_context);
        tablets.push_back(tablet);
    }

    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(tablets.begin(), tablets.end(), g);

    for (auto& tablet : tablets) {
        CompactionManager::instance()->update_candidate(tablet.get());
    }

    ASSERT_EQ(11, CompactionManager::instance()->candidates_size());
    Tablet* candidate_1 = CompactionManager::instance()->pick_candidate();
    ASSERT_EQ(9, candidate_1->tablet_id());
    Tablet* candidate_2 = CompactionManager::instance()->pick_candidate();
    ASSERT_EQ(10, candidate_2->tablet_id());
    ASSERT_EQ(candidate_1->compaction_score(), candidate_2->compaction_score());
    double last_score = candidate_2->compaction_score();
    for (int i = 8; i >= 0; i--) {
        Tablet* candidate = CompactionManager::instance()->pick_candidate();
        ASSERT_EQ(i, candidate->tablet_id());
        ASSERT_LT(candidate->compaction_score(), last_score);
        last_score = candidate->compaction_score();
    }
}

class MockCompactionTask : public CompactionTask {
    MockCompactionTask() : CompactionTask(HORIZONTAL_COMPACTION) {}

    ~MockCompactionTask() = default;

    Status run_impl() override { return Status::OK(); }
};

TEST(CompactionManagerTest, test_compaction_tasks) {
    std::vector<TabletSharedPtr> tablets;
    std::vector<std::shared_ptr<MockCompactionTask>> tasks;
    DataDir data_dir("./data_dir");
    // generate compaction task
    config::max_compaction_task_num = 2;
    config::max_compaction_task_per_disk = config::max_compaction_task_num;
    for (int i = 0; i < config::max_compaction_task_num + 1; i++) {
        TabletSharedPtr tablet = std::make_shared<Tablet>();
        TabletMetaSharedPtr tablet_meta = std::make_shared<TabletMeta>();
        tablet_meta->set_tablet_id(i);
        tablet->set_tablet_meta(tablet_meta);
        tablet->set_data_dir(&data_dir);
        std::unique_ptr<CompactionContext> compaction_context = std::make_unique<CompactionContext>();
        compaction_context->tablet = tablet.get();
        compaction_context->current_level = 0;
        compaction_context->compaction_scores[0] = 1 + i;
        compaction_context->compaction_scores[1] = 1 + 0.5 * i;
        tablet->set_compaction_context(compaction_context);
        tablets.push_back(tablet);
        std::shared_ptr<MockCompactionTask> task = std::make_shared<MockCompactionTask>();
        task->set_tablet(tablet.get());
        task->set_task_id(i);
        task->set_compaction_level(0);
        tasks.emplace_back(std::move(task));
    }

    for (int i = 0; i < config::max_compaction_task_num; i++) {
        bool ret = CompactionManager::instance()->register_task(tasks[i].get());
        ASSERT_TRUE(ret);
    }
    bool ret = CompactionManager::instance()->register_task(tasks[config::max_compaction_task_num].get());
    ASSERT_FALSE(ret);

    ASSERT_EQ(config::max_compaction_task_num, CompactionManager::instance()->running_tasks_num());
    ASSERT_EQ(config::max_compaction_task_num, CompactionManager::instance()->running_tasks_num_for_level(0));
    ASSERT_EQ(0, CompactionManager::instance()->running_tasks_num_for_level(1));
    CompactionManager::instance()->clear_tasks();
    ASSERT_EQ(0, CompactionManager::instance()->running_tasks_num());

    config::max_compaction_task_per_disk = 1;
    for (int i = 0; i < config::max_compaction_task_num; i++) {
        bool ret = CompactionManager::instance()->register_task(tasks[i].get());
        if (i == 0) {
            ASSERT_TRUE(ret);
        } else {
            ASSERT_FALSE(ret);
        }
    }
    ASSERT_EQ(1, CompactionManager::instance()->running_tasks_num());
    CompactionManager::instance()->clear_tasks();
    ASSERT_EQ(0, CompactionManager::instance()->running_tasks_num());

    config::max_level_0_compaction_task = 1;
    for (int i = 0; i < config::max_level_0_compaction_task; i++) {
        bool ret = CompactionManager::instance()->register_task(tasks[i].get());
        ASSERT_TRUE(ret);
    }

    ret = CompactionManager::instance()->register_task(tasks[config::max_level_0_compaction_task].get());
    ASSERT_FALSE(ret);

    ASSERT_EQ(config::max_level_0_compaction_task, CompactionManager::instance()->running_tasks_num());
    ASSERT_EQ(1, CompactionManager::instance()->running_tasks_num_for_level(0));
    CompactionManager::instance()->clear_tasks();

    config::max_level_1_compaction_task = 1;
    for (int i = 0; i < config::max_level_1_compaction_task + 1; i++) {
        tasks[i]->set_compaction_level(1);
        bool ret = CompactionManager::instance()->register_task(tasks[i].get());
        if (i < config::max_level_1_compaction_task) {
            ASSERT_TRUE(ret);
        } else {
            ASSERT_FALSE(ret);
        }
    }
    ASSERT_EQ(config::max_level_1_compaction_task, CompactionManager::instance()->running_tasks_num());
    ASSERT_EQ(1, CompactionManager::instance()->running_tasks_num_for_level(1));
}

TEST(CompactionManagerTest, test_next_compaction_task_id) {
    uint64_t start_task_id = CompactionManager::instance()->next_compaction_task_id();
    ASSERT_LT(0, start_task_id);
}

} // namespace starrocks
