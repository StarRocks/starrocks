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

#include "fs/fs_util.h"
#include "runtime/mem_pool.h"
#include "storage/compaction.h"
#include "storage/compaction_context.h"
#include "storage/compaction_task.h"
#include "storage/compaction_utils.h"
#include "storage/default_compaction_policy.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage/tablet_updates.h"
#include "testutil/assert.h"

namespace starrocks {

class CompactionManagerTest : public testing::Test {
public:
    ~CompactionManagerTest() override {
        if (_engine) {
            _engine->stop();
            delete _engine;
            _engine = nullptr;
        }
    }

    void SetUp() override {
        config::min_cumulative_compaction_num_singleton_deltas = 2;
        //config::max_cumulative_compaction_num_singleton_deltas = 5;
        config::max_compaction_concurrency = 10;
        config::enable_event_based_compaction_framework = false;
        config::vertical_compaction_max_columns_per_group = 5;
        Compaction::init(config::max_compaction_concurrency);

        _default_storage_root_path = config::storage_root_path;
        config::storage_root_path = std::filesystem::current_path().string() + "/compaction_manager_test";
        fs::remove_all(config::storage_root_path);
        ASSERT_TRUE(fs::create_directories(config::storage_root_path).ok());
        std::vector<StorePath> paths;
        paths.emplace_back(config::storage_root_path);

        starrocks::EngineOptions options;
        options.store_paths = paths;
        options.compaction_mem_tracker = _compaction_mem_tracker.get();
        if (_engine == nullptr) {
            Status s = starrocks::StorageEngine::open(options, &_engine);
            ASSERT_TRUE(s.ok()) << s.to_string();
        }

        _schema_hash_path = fmt::format("{}/data/0/12345/1111", config::storage_root_path);
        ASSERT_OK(fs::create_directories(_schema_hash_path));

        _metadata_mem_tracker = std::make_unique<MemTracker>(-1);
        _mem_pool = std::make_unique<MemPool>();

        _compaction_mem_tracker = std::make_unique<MemTracker>(-1);
    }

    void TearDown() override {
        if (fs::path_exist(config::storage_root_path)) {
            ASSERT_TRUE(fs::remove_all(config::storage_root_path).ok());
        }
        config::storage_root_path = _default_storage_root_path;
    }

protected:
    StorageEngine* _engine = nullptr;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::string _schema_hash_path;
    std::unique_ptr<MemTracker> _metadata_mem_tracker;
    std::unique_ptr<MemTracker> _compaction_mem_tracker;
    std::unique_ptr<MemPool> _mem_pool;
    std::string _default_storage_root_path;
};

TEST_F(CompactionManagerTest, test_candidates) {
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

    _engine->compaction_manager()->update_candidates(candidates);

    {
        ASSERT_EQ(11, _engine->compaction_manager()->candidates_size());
        CompactionCandidate candidate_1;
        _engine->compaction_manager()->pick_candidate(&candidate_1);
        ASSERT_EQ(9, candidate_1.tablet->tablet_id());
        CompactionCandidate candidate_2;
        _engine->compaction_manager()->pick_candidate(&candidate_2);
        ASSERT_EQ(10, candidate_2.tablet->tablet_id());
        ASSERT_EQ(candidate_1.score, candidate_2.score);
        double last_score = candidate_2.score;
        while (true) {
            CompactionCandidate candidate;
            auto valid = _engine->compaction_manager()->pick_candidate(&candidate);
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

TEST_F(CompactionManagerTest, test_compaction_tasks) {
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

    _engine->compaction_manager()->init_max_task_num(config::max_compaction_concurrency);

    for (int i = 0; i < config::max_compaction_concurrency; i++) {
        bool ret = _engine->compaction_manager()->register_task(tasks[i].get());
        ASSERT_TRUE(ret);
    }

    ASSERT_EQ(config::max_compaction_concurrency, _engine->compaction_manager()->running_tasks_num());

    _engine->compaction_manager()->clear_tasks();
    ASSERT_EQ(0, _engine->compaction_manager()->running_tasks_num());

    config::cumulative_compaction_num_threads_per_disk = 1;
    for (int i = 0; i < config::max_compaction_concurrency; i++) {
        bool ret = _engine->compaction_manager()->register_task(tasks[i].get());
        ASSERT_TRUE(ret);
    }
    ASSERT_EQ(config::max_compaction_concurrency, _engine->compaction_manager()->running_tasks_num());
    _engine->compaction_manager()->clear_tasks();
    ASSERT_EQ(0, _engine->compaction_manager()->running_tasks_num());

    config::cumulative_compaction_num_threads_per_disk = 4;
    for (int i = 0; i < 1; i++) {
        bool ret = _engine->compaction_manager()->register_task(tasks[i].get());
        ASSERT_TRUE(ret);
    }

    _engine->compaction_manager()->clear_tasks();

    config::base_compaction_num_threads_per_disk = 4;
    for (int i = 0; i < 1; i++) {
        tasks[i]->set_compaction_type(BASE_COMPACTION);
        bool ret = _engine->compaction_manager()->register_task(tasks[i].get());
        ASSERT_TRUE(ret);
    }
}

TEST_F(CompactionManagerTest, test_next_compaction_task_id) {
    uint64_t start_task_id = _engine->compaction_manager()->next_compaction_task_id();
    ASSERT_LT(0, start_task_id);
}

TEST_F(CompactionManagerTest, test_compaction_parallel) {
    std::vector<TabletSharedPtr> tablets;
    std::vector<std::shared_ptr<MockCompactionTask>> tasks;
    DataDir data_dir("./data_dir");
    // generate compaction task
    config::max_compaction_concurrency = 10;
    int tablet_num = 3;
    int task_id = 0;
    // each tablet has 3 compaction tasks
    for (int i = 0; i < tablet_num; i++) {
        TabletSharedPtr tablet = std::make_shared<Tablet>();
        TabletMetaSharedPtr tablet_meta = std::make_shared<TabletMeta>();
        tablet_meta->set_tablet_id(i);
        tablet->set_tablet_meta(tablet_meta);
        tablet->set_data_dir(&data_dir);
        std::unique_ptr<CompactionContext> compaction_context = std::make_unique<CompactionContext>();
        compaction_context->policy = std::make_unique<DefaultCumulativeBaseCompactionPolicy>(tablet.get());
        tablet->set_compaction_context(compaction_context);
        tablets.push_back(tablet);

        // create base compaction
        std::shared_ptr<MockCompactionTask> task = std::make_shared<MockCompactionTask>();
        task->set_tablet(tablet);
        task->set_task_id(task_id++);
        task->set_compaction_type(BASE_COMPACTION);
        tasks.emplace_back(std::move(task));

        // create cumulative compaction1
        task = std::make_shared<MockCompactionTask>();
        task->set_tablet(tablet);
        task->set_task_id(task_id++);
        task->set_compaction_type(CUMULATIVE_COMPACTION);
        tasks.emplace_back(std::move(task));

        // create cumulative compaction2
        task = std::make_shared<MockCompactionTask>();
        task->set_tablet(tablet);
        task->set_task_id(task_id++);
        task->set_compaction_type(CUMULATIVE_COMPACTION);
        tasks.emplace_back(std::move(task));
    }

    _engine->compaction_manager()->init_max_task_num(config::max_compaction_concurrency);

    for (int i = 0; i < 9; i++) {
        bool ret = _engine->compaction_manager()->register_task(tasks[i].get());
        ASSERT_TRUE(ret);
    }

    ASSERT_EQ(9, _engine->compaction_manager()->running_tasks_num());

    _engine->compaction_manager()->clear_tasks();
    ASSERT_EQ(0, _engine->compaction_manager()->running_tasks_num());
}

} // namespace starrocks
