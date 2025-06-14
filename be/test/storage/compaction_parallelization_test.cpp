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

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <memory>

#include "column/schema.h"
#include "fs/fs_util.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "storage/chunk_helper.h"
#include "storage/compaction.h"
#include "storage/compaction_manager.h"
#include "storage/compaction_utils.h"
#include "storage/horizontal_compaction_task.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/storage_engine.h"
#include "storage/tablet_meta.h"
#include "storage/tablet_reader.h"
#include "storage/tablet_reader_params.h"
#include "testutil/assert.h"

namespace starrocks {
class CompactionParallelizationTest : public testing::Test {
public:
    ~CompactionParallelizationTest() override {
        if (_engine) {
            _engine->stop();
            delete _engine;
            _engine = nullptr;
        }
    }

    void write_new_version(const TabletSharedPtr& tablet) {
        RowsetWriterContext rowset_writer_context;
        create_rowset_writer_context(&rowset_writer_context, _version);

        std::unique_ptr<RowsetWriter> rowset_writer;
        ASSERT_TRUE(RowsetFactory::create_rowset_writer(rowset_writer_context, &rowset_writer).ok());

        rowset_writer_add_rows(rowset_writer);

        rowset_writer->flush();
        RowsetSharedPtr src_rowset = *rowset_writer->build();
        ASSERT_TRUE(src_rowset != nullptr);
        ASSERT_EQ(1024, src_rowset->num_rows());

        ASSERT_TRUE(tablet->add_inc_rowset(src_rowset, _version++).ok());
    }

    void create_rowset_writer_context(RowsetWriterContext* rowset_writer_context, int64_t version) {
        RowsetId rowset_id;
        rowset_id.init(_rowset_id++);
        rowset_writer_context->rowset_id = rowset_id;
        rowset_writer_context->tablet_id = 12345;
        rowset_writer_context->tablet_schema_hash = 1111;
        rowset_writer_context->partition_id = 10;
        rowset_writer_context->rowset_path_prefix = _default_storage_root_path + "/data/0/12345/1111";
        rowset_writer_context->rowset_state = VISIBLE;
        rowset_writer_context->tablet_schema = _tablet_schema;
        rowset_writer_context->version.first = version;
        rowset_writer_context->version.second = version;
    }

    void create_tablet_schema(KeysType keys_type) {
        TabletSchemaPB tablet_schema_pb;
        tablet_schema_pb.set_keys_type(keys_type);
        tablet_schema_pb.set_num_short_key_columns(2);
        tablet_schema_pb.set_num_rows_per_row_block(1024);
        tablet_schema_pb.set_next_column_unique_id(4);

        ColumnPB* column_1 = tablet_schema_pb.add_column();
        column_1->set_unique_id(1);
        column_1->set_name("k1");
        column_1->set_type("INT");
        column_1->set_is_key(true);
        column_1->set_length(4);
        column_1->set_index_length(4);
        column_1->set_is_nullable(false);
        column_1->set_is_bf_column(false);

        ColumnPB* column_2 = tablet_schema_pb.add_column();
        column_2->set_unique_id(2);
        column_2->set_name("k2");
        column_2->set_type("VARCHAR");
        column_2->set_length(20);
        column_2->set_index_length(20);
        column_2->set_is_key(true);
        column_2->set_is_nullable(false);
        column_2->set_is_bf_column(false);

        ColumnPB* column_3 = tablet_schema_pb.add_column();
        column_3->set_unique_id(3);
        column_3->set_name("v1");
        column_3->set_type("INT");
        column_3->set_length(4);
        column_3->set_is_key(false);
        column_3->set_is_nullable(false);
        column_3->set_is_bf_column(false);
        column_3->set_aggregation("SUM");

        _tablet_schema = std::make_unique<TabletSchema>(tablet_schema_pb);
    }

    void create_tablet_meta(TabletMeta* tablet_meta) {
        TabletMetaPB tablet_meta_pb;
        tablet_meta_pb.set_table_id(10000);
        tablet_meta_pb.set_tablet_id(12345);
        tablet_meta_pb.set_schema_hash(1111);
        tablet_meta_pb.set_partition_id(10);
        tablet_meta_pb.set_shard_id(0);
        tablet_meta_pb.set_creation_time(1575020449);
        tablet_meta_pb.set_tablet_state(PB_RUNNING);
        PUniqueId* tablet_uid = tablet_meta_pb.mutable_tablet_uid();
        tablet_uid->set_hi(10);
        tablet_uid->set_lo(10);

        TabletSchemaPB* tablet_schema_pb = tablet_meta_pb.mutable_schema();
        _tablet_schema->to_schema_pb(tablet_schema_pb);

        tablet_meta->init_from_pb(&tablet_meta_pb);
    }

    void rowset_writer_add_rows(std::unique_ptr<RowsetWriter>& writer) {
        std::vector<std::string> test_data;
        auto schema = ChunkHelper::convert_schema(_tablet_schema);
        for (size_t j = 0; j < 8; ++j) {
            auto chunk = ChunkHelper::new_chunk(schema, 128);
            for (size_t i = 0; i < 128; ++i) {
                test_data.push_back("well" + std::to_string(i));
                auto& cols = chunk->columns();
                cols[0]->append_datum(Datum(static_cast<int32_t>(i)));
                Slice field_1(test_data[i]);
                cols[1]->append_datum(Datum(field_1));
                cols[2]->append_datum(Datum(static_cast<int32_t>(10000 + i)));
            }
            CHECK_OK(writer->add_chunk(*chunk));
        }
    }

    void SetUp() override {
        config::min_cumulative_compaction_num_singleton_deltas = 2;
        config::max_cumulative_compaction_num_singleton_deltas = 1000;
        config::max_compaction_concurrency = 10;
        config::enable_event_based_compaction_framework = false;
        config::vertical_compaction_max_columns_per_group = 5;
        config::enable_size_tiered_compaction_strategy = true;
        Compaction::init(config::max_compaction_concurrency);

        _default_storage_root_path = std::filesystem::current_path().string() + "/data_test_cumulative_compaction";
        fs::remove_all(_default_storage_root_path);
        ASSERT_TRUE(fs::create_directories(_default_storage_root_path).ok());
        std::vector<StorePath> paths;
        paths.emplace_back(_default_storage_root_path);

        starrocks::EngineOptions options;
        options.store_paths = paths;
        options.compaction_mem_tracker = _compaction_mem_tracker.get();
        if (_engine == nullptr) {
            Status s = starrocks::StorageEngine::open(options, &_engine);
            ASSERT_TRUE(s.ok()) << s.to_string();
        }

        _schema_hash_path = fmt::format("{}/data/0/12345/1111", _default_storage_root_path);
        ASSERT_OK(fs::create_directories(_schema_hash_path));

        _metadata_mem_tracker = std::make_unique<MemTracker>(-1);
        _mem_pool = std::make_unique<MemPool>();

        _compaction_mem_tracker = std::make_unique<MemTracker>(-1);

        _rowset_id = 10000;
        _version = 0;
    }

    void TearDown() override {
        if (fs::path_exist(_default_storage_root_path)) {
            ASSERT_TRUE(fs::remove_all(_default_storage_root_path).ok());
        }
    }

protected:
    StorageEngine* _engine = nullptr;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::string _schema_hash_path;
    std::unique_ptr<MemTracker> _metadata_mem_tracker;
    std::unique_ptr<MemTracker> _compaction_mem_tracker;
    std::unique_ptr<MemPool> _mem_pool;
    std::string _default_storage_root_path;

    int64_t _rowset_id;
    int64_t _version;
};

/*
 select and execute new compaction task while the other one is in the execution progress：
 1. selection thread selects rowsets and adds candidate_a into queue
 2. schedule thread picks candidate_a out of queue, creates task and executes
 3. selection thread selects rowsets and adds candidate_b into queue
 4. schedule thread picks candidate_b out of queue, creates task and executes
 5. selection thread selects rowsets and adds candidate_c into queue
 6. schedule thread picks candidate_c out of queue, creates task and executes
*/
TEST_F(CompactionParallelizationTest, test_size_tiered_compaction_parallel) {
    LOG(INFO) << "test_size_tiered_compaction_parallel";
    create_tablet_schema(UNIQUE_KEYS);

    TabletMetaSharedPtr tablet_meta = std::make_shared<TabletMeta>();
    create_tablet_meta(tablet_meta.get());

    TabletSharedPtr tablet = Tablet::create_tablet_from_meta(tablet_meta, _engine->get_stores()[0]);
    ASSERT_TRUE(tablet->init().ok());

    // task 1
    // write 5 rowsets
    for (int i = 0; i < 5; ++i) {
        write_new_version(tablet);
    }

    // select rowsets for compaction
    _engine->compaction_manager()->update_tablet(tablet);
    ASSERT_EQ(1, _engine->compaction_manager()->get_waiting_task_num());

    // create compaction
    CompactionCandidate compaction_candidate;
    std::shared_ptr<CompactionTask> compaction_task = nullptr;
    ASSERT_TRUE(_engine->compaction_manager()->pick_candidate(&compaction_candidate));
    ASSERT_EQ(0, _engine->compaction_manager()->get_waiting_task_num());
    compaction_task = compaction_candidate.tablet->create_compaction_task();
    for (const auto& rowset : compaction_task->input_rowsets()) {
        ASSERT_TRUE(rowset->get_is_compacting());
    }

    // execute compaction
    ASSERT_TRUE(_engine->compaction_manager()->register_task(compaction_task.get()));
    ASSERT_EQ(1, _engine->compaction_manager()->running_tasks_num());
    std::shared_lock<std::shared_mutex> _compaction_lock;
    if (compaction_task->compaction_type() == CUMULATIVE_COMPACTION) {
        _compaction_lock = std::shared_lock(compaction_task->tablet()->get_cumulative_lock(), std::try_to_lock);
    } else {
        _compaction_lock = std::shared_lock(compaction_task->tablet()->get_base_lock(), std::try_to_lock);
    }
    ASSERT_TRUE(_compaction_lock.owns_lock());
    // execution progress
    ASSERT_TRUE(tablet->has_compaction_task());

    // task 2
    // write 6 rowsets
    for (int i = 0; i < 6; ++i) {
        write_new_version(tablet);
    }

    // select rowsets for compaction
    _engine->compaction_manager()->update_tablet(tablet);
    ASSERT_EQ(1, _engine->compaction_manager()->get_waiting_task_num());

    // create compaction
    std::shared_ptr<CompactionTask> compaction_task2 = nullptr;
    ASSERT_TRUE(_engine->compaction_manager()->pick_candidate(&compaction_candidate));
    ASSERT_EQ(0, _engine->compaction_manager()->get_waiting_task_num());
    compaction_task2 = compaction_candidate.tablet->create_compaction_task();
    for (const auto& rowset : compaction_task2->input_rowsets()) {
        ASSERT_TRUE(rowset->get_is_compacting());
    }
    // execute compaction
    ASSERT_TRUE(_engine->compaction_manager()->register_task(compaction_task2.get()));
    ASSERT_EQ(2, _engine->compaction_manager()->running_tasks_num());
    std::shared_lock<std::shared_mutex> _compaction_lock2;
    if (compaction_task2->compaction_type() == CUMULATIVE_COMPACTION) {
        _compaction_lock2 = std::shared_lock(compaction_task2->tablet()->get_cumulative_lock(), std::try_to_lock);
    } else {
        _compaction_lock2 = std::shared_lock(compaction_task2->tablet()->get_base_lock(), std::try_to_lock);
    }
    ASSERT_TRUE(_compaction_lock2.owns_lock());
    // execution progress
    ASSERT_TRUE(tablet->has_compaction_task());

    // task 3
    // write 7 rowsets
    for (int i = 0; i < 7; ++i) {
        write_new_version(tablet);
    }

    // select rowsets for compaction
    _engine->compaction_manager()->update_tablet(tablet);
    ASSERT_EQ(1, _engine->compaction_manager()->get_waiting_task_num());

    // create compaction
    std::shared_ptr<CompactionTask> compaction_task3 = nullptr;
    ASSERT_TRUE(_engine->compaction_manager()->pick_candidate(&compaction_candidate));
    ASSERT_EQ(0, _engine->compaction_manager()->get_waiting_task_num());
    compaction_task3 = compaction_candidate.tablet->create_compaction_task();
    for (const auto& rowset : compaction_task3->input_rowsets()) {
        ASSERT_TRUE(rowset->get_is_compacting());
    }
    // execute compaction
    ASSERT_TRUE(_engine->compaction_manager()->register_task(compaction_task3.get()));
    ASSERT_EQ(3, _engine->compaction_manager()->running_tasks_num());
    std::shared_lock<std::shared_mutex> _compaction_lock3;
    if (compaction_task3->compaction_type() == CUMULATIVE_COMPACTION) {
        _compaction_lock3 = std::shared_lock(compaction_task3->tablet()->get_cumulative_lock(), std::try_to_lock);
    } else {
        _compaction_lock3 = std::shared_lock(compaction_task3->tablet()->get_base_lock(), std::try_to_lock);
    }
    ASSERT_TRUE(_compaction_lock3.owns_lock());
    // execution progress
    ASSERT_TRUE(tablet->has_compaction_task());

    auto task_set = _engine->compaction_manager()->get_running_task(tablet);
    int base_count = 0, cumu_count = 0;
    for (auto& task : task_set) {
        if (task->compaction_type() == BASE_COMPACTION) {
            base_count++;
        } else {
            cumu_count++;
        }
    }
    ASSERT_EQ(1, base_count);
    ASSERT_EQ(2, cumu_count);

    // execute task
    ASSERT_TRUE(dynamic_cast<HorizontalCompactionTask*>(compaction_task.get())->run_impl().ok());
    ASSERT_TRUE(dynamic_cast<HorizontalCompactionTask*>(compaction_task2.get())->run_impl().ok());
    ASSERT_TRUE(dynamic_cast<HorizontalCompactionTask*>(compaction_task3.get())->run_impl().ok());

    // finish executing task
    _engine->compaction_manager()->unregister_task(compaction_task.get());
    ASSERT_EQ(2, _engine->compaction_manager()->running_tasks_num());
    _engine->compaction_manager()->unregister_task(compaction_task2.get());
    ASSERT_EQ(1, _engine->compaction_manager()->running_tasks_num());
    _engine->compaction_manager()->unregister_task(compaction_task3.get());
    ASSERT_EQ(0, _engine->compaction_manager()->running_tasks_num());
    ASSERT_EQ(0, _engine->compaction_manager()->get_waiting_task_num());
    ASSERT_FALSE(_engine->compaction_manager()->has_running_task(tablet));

    ASSERT_EQ(3, tablet->version_count());
    std::vector<Version> versions;
    tablet->list_versions(&versions);
    ASSERT_EQ(3, versions.size());
    ASSERT_EQ(0, versions[0].first);
    ASSERT_EQ(4, versions[0].second);
    ASSERT_EQ(5, versions[1].first);
    ASSERT_EQ(10, versions[1].second);
    ASSERT_EQ(11, versions[2].first);
    ASSERT_EQ(17, versions[2].second);
}

// select and execute new compaction task while the other one is still in the task priority queue, new candidate will replace the old one
TEST_F(CompactionParallelizationTest, test_size_tiered_compaction_parallel2) {
    LOG(INFO) << "test_size_tiered_compaction_parallel2";

    create_tablet_schema(UNIQUE_KEYS);
    TabletMetaSharedPtr tablet_meta = std::make_shared<TabletMeta>();
    create_tablet_meta(tablet_meta.get());
    TabletSharedPtr tablet = Tablet::create_tablet_from_meta(tablet_meta, _engine->get_stores()[0]);
    ASSERT_TRUE(tablet->init().ok());

    // write 5 rowsets
    for (int i = 0; i < 5; ++i) {
        write_new_version(tablet);
    }

    // select rowsets for compaction
    _engine->compaction_manager()->update_tablet(tablet);
    ASSERT_EQ(1, _engine->compaction_manager()->get_waiting_task_num());

    // write 6 rowsets
    for (int i = 0; i < 6; ++i) {
        write_new_version(tablet);
    }

    // select rowsets and replace candidate
    _engine->compaction_manager()->update_tablet(tablet);
    ASSERT_EQ(1, _engine->compaction_manager()->get_waiting_task_num());

    // create compaction
    CompactionCandidate compaction_candidate;
    std::shared_ptr<CompactionTask> compaction_task = nullptr;
    ASSERT_TRUE(_engine->compaction_manager()->pick_candidate(&compaction_candidate));
    ASSERT_EQ(0, _engine->compaction_manager()->get_waiting_task_num());
    compaction_task = compaction_candidate.tablet->create_compaction_task();
    ASSERT_EQ(11, compaction_task->input_rowsets().size());
    for (const auto& rowset : compaction_task->input_rowsets()) {
        ASSERT_TRUE(rowset->get_is_compacting());
    }

    // execute compaction
    ASSERT_TRUE(_engine->compaction_manager()->register_task(compaction_task.get()));
    ASSERT_EQ(1, _engine->compaction_manager()->running_tasks_num());
    {
        std::shared_lock<std::shared_mutex> _compaction_lock;
        if (compaction_task->compaction_type() == CUMULATIVE_COMPACTION) {
            _compaction_lock = std::shared_lock(compaction_task->tablet()->get_cumulative_lock(), std::try_to_lock);
        } else {
            _compaction_lock = std::shared_lock(compaction_task->tablet()->get_base_lock(), std::try_to_lock);
        }
        ASSERT_TRUE(_compaction_lock.owns_lock());
        ASSERT_TRUE(tablet->has_compaction_task());
        ASSERT_EQ(BASE_COMPACTION, compaction_task->compaction_type());
        ASSERT_TRUE(dynamic_cast<HorizontalCompactionTask*>(compaction_task.get())->run_impl().ok());

        // finish
        _engine->compaction_manager()->unregister_task(compaction_task.get());
        ASSERT_EQ(0, _engine->compaction_manager()->running_tasks_num());
        ASSERT_EQ(0, _engine->compaction_manager()->get_waiting_task_num());
        ASSERT_FALSE(_engine->compaction_manager()->has_running_task(tablet));

        ASSERT_EQ(1, tablet->version_count());
        std::vector<Version> versions;
        tablet->list_versions(&versions);
        ASSERT_EQ(1, versions.size());
        ASSERT_EQ(0, versions[0].first);
        ASSERT_EQ(10, versions[0].second);
    }

    // task 2
    // write 7 rowsets
    for (int i = 0; i < 7; ++i) {
        write_new_version(tablet);
    }

    // select rowsets for compaction
    _engine->compaction_manager()->update_tablet(tablet);
    ASSERT_EQ(1, _engine->compaction_manager()->get_waiting_task_num());

    // create compaction
    std::shared_ptr<CompactionTask> compaction_task2 = nullptr;
    ASSERT_TRUE(_engine->compaction_manager()->pick_candidate(&compaction_candidate));
    ASSERT_EQ(0, _engine->compaction_manager()->get_waiting_task_num());
    compaction_task2 = compaction_candidate.tablet->create_compaction_task();
    ASSERT_EQ(BASE_COMPACTION, compaction_task2->compaction_type());
    ASSERT_EQ(8, compaction_task2->input_rowsets().size());
    for (const auto& rowset : compaction_task2->input_rowsets()) {
        ASSERT_TRUE(rowset->get_is_compacting());
    }
    // execute compaction
    ASSERT_TRUE(_engine->compaction_manager()->register_task(compaction_task2.get()));
    ASSERT_EQ(1, _engine->compaction_manager()->running_tasks_num());
    std::shared_lock<std::shared_mutex> _compaction_lock2;
    if (compaction_task2->compaction_type() == CUMULATIVE_COMPACTION) {
        _compaction_lock2 = std::shared_lock(compaction_task2->tablet()->get_cumulative_lock(), std::try_to_lock);
    } else {
        _compaction_lock2 = std::shared_lock(compaction_task2->tablet()->get_base_lock(), std::try_to_lock);
    }
    ASSERT_TRUE(_compaction_lock2.owns_lock());
    ASSERT_TRUE(tablet->has_compaction_task());
    ASSERT_EQ(BASE_COMPACTION, compaction_task2->compaction_type());
    ASSERT_TRUE(dynamic_cast<HorizontalCompactionTask*>(compaction_task2.get())->run_impl().ok());

    // finish
    _engine->compaction_manager()->unregister_task(compaction_task2.get());
    ASSERT_EQ(0, _engine->compaction_manager()->running_tasks_num());
    ASSERT_EQ(0, _engine->compaction_manager()->get_waiting_task_num());
    ASSERT_FALSE(_engine->compaction_manager()->has_running_task(tablet));

    ASSERT_EQ(1, tablet->version_count());
    std::vector<Version> versions;
    tablet->list_versions(&versions);
    ASSERT_EQ(1, versions.size());
    ASSERT_EQ(0, versions[0].first);
    ASSERT_EQ(17, versions[0].second);
}

// select and execute new compaction task while the other one is still in the task priority queue, new candidate will replace the old one
TEST_F(CompactionParallelizationTest, test_size_tiered_compaction_parallel_replace) {
    LOG(INFO) << "test_size_tiered_compaction_parallel_replace";

    create_tablet_schema(UNIQUE_KEYS);
    TabletMetaSharedPtr tablet_meta = std::make_shared<TabletMeta>();
    create_tablet_meta(tablet_meta.get());
    TabletSharedPtr tablet = Tablet::create_tablet_from_meta(tablet_meta, _engine->get_stores()[0]);
    ASSERT_TRUE(tablet->init().ok());

    // write 5 rowsets
    for (int i = 0; i < 5; ++i) {
        write_new_version(tablet);
    }

    // select rowsets for compaction
    _engine->compaction_manager()->update_tablet(tablet);
    ASSERT_EQ(1, _engine->compaction_manager()->get_waiting_task_num());

    // write 6 rowsets
    for (int i = 0; i < 6; ++i) {
        write_new_version(tablet);
    }

    // select rowsets and replace the old candidate
    _engine->compaction_manager()->update_tablet(tablet);
    ASSERT_EQ(1, _engine->compaction_manager()->get_waiting_task_num());

    // create compaction task1
    CompactionCandidate compaction_candidate;
    std::shared_ptr<CompactionTask> compaction_task = nullptr;
    ASSERT_TRUE(_engine->compaction_manager()->pick_candidate(&compaction_candidate));
    ASSERT_EQ(0, _engine->compaction_manager()->get_waiting_task_num());
    compaction_task = compaction_candidate.tablet->create_compaction_task();
    ASSERT_EQ(11, compaction_task->input_rowsets().size());
    for (const auto& rowset : compaction_task->input_rowsets()) {
        ASSERT_TRUE(rowset->get_is_compacting());
    }

    // write 7 rowsets
    for (int i = 0; i < 7; ++i) {
        write_new_version(tablet);
    }
    // select rowsets and add candidate into queue, which excludes rowsets being compacted
    _engine->compaction_manager()->update_tablet(tablet);
    ASSERT_EQ(1, _engine->compaction_manager()->get_waiting_task_num());

    // execute task1
    ASSERT_TRUE(_engine->compaction_manager()->register_task(compaction_task.get()));
    ASSERT_EQ(1, _engine->compaction_manager()->running_tasks_num());
    {
        std::shared_lock<std::shared_mutex> _compaction_lock;
        if (compaction_task->compaction_type() == CUMULATIVE_COMPACTION) {
            _compaction_lock = std::shared_lock(compaction_task->tablet()->get_cumulative_lock(), std::try_to_lock);
        } else {
            _compaction_lock = std::shared_lock(compaction_task->tablet()->get_base_lock(), std::try_to_lock);
        }
        ASSERT_TRUE(_compaction_lock.owns_lock());
        ASSERT_TRUE(tablet->has_compaction_task());
        ASSERT_EQ(BASE_COMPACTION, compaction_task->compaction_type());
        ASSERT_TRUE(dynamic_cast<HorizontalCompactionTask*>(compaction_task.get())->run_impl().ok());

        // finish
        _engine->compaction_manager()->unregister_task(compaction_task.get());
        ASSERT_EQ(0, _engine->compaction_manager()->running_tasks_num());
        ASSERT_EQ(1, _engine->compaction_manager()->get_waiting_task_num());
        ASSERT_FALSE(_engine->compaction_manager()->has_running_task(tablet));

        ASSERT_EQ(8, tablet->version_count());
        std::vector<Version> versions;
        tablet->list_versions(&versions);
        ASSERT_EQ(8, versions.size());
        ASSERT_EQ(0, versions[0].first);
        ASSERT_EQ(10, versions[0].second);
    }

    // create compaction task2
    std::shared_ptr<CompactionTask> compaction_task2 = nullptr;
    ASSERT_TRUE(_engine->compaction_manager()->pick_candidate(&compaction_candidate));
    ASSERT_EQ(0, _engine->compaction_manager()->get_waiting_task_num());
    compaction_task2 = compaction_candidate.tablet->create_compaction_task();
    ASSERT_EQ(7, compaction_task2->input_rowsets().size());
    for (const auto& rowset : compaction_task2->input_rowsets()) {
        ASSERT_TRUE(rowset->get_is_compacting());
    }

    // execute compaction task2
    ASSERT_TRUE(_engine->compaction_manager()->register_task(compaction_task2.get()));
    ASSERT_EQ(1, _engine->compaction_manager()->running_tasks_num());
    std::shared_lock<std::shared_mutex> _compaction_lock2;
    if (compaction_task2->compaction_type() == CUMULATIVE_COMPACTION) {
        _compaction_lock2 = std::shared_lock(compaction_task2->tablet()->get_cumulative_lock(), std::try_to_lock);
    } else {
        _compaction_lock2 = std::shared_lock(compaction_task2->tablet()->get_base_lock(), std::try_to_lock);
    }
    ASSERT_TRUE(_compaction_lock2.owns_lock());
    ASSERT_TRUE(tablet->has_compaction_task());
    ASSERT_EQ(CUMULATIVE_COMPACTION, compaction_task2->compaction_type());
    ASSERT_TRUE(dynamic_cast<HorizontalCompactionTask*>(compaction_task2.get())->run_impl().ok());

    // finish
    _engine->compaction_manager()->unregister_task(compaction_task2.get());
    ASSERT_EQ(0, _engine->compaction_manager()->running_tasks_num());
    ASSERT_EQ(0, _engine->compaction_manager()->get_waiting_task_num());
    ASSERT_FALSE(_engine->compaction_manager()->has_running_task(tablet));

    ASSERT_EQ(2, tablet->version_count());
    std::vector<Version> versions;
    tablet->list_versions(&versions);
    ASSERT_EQ(2, versions.size());
    ASSERT_EQ(0, versions[0].first);
    ASSERT_EQ(10, versions[0].second);
    ASSERT_EQ(11, versions[1].first);
    ASSERT_EQ(17, versions[1].second);
}

/*
 Select new compaction candidate while the other one is taken out from queue before creating task, the
'is_compacting' status of rowsets in the old candidate is false, causing new candidate select the same rowset
 1. selection thread selects rowsets and adds candidate_a into queue
 2. schedule thread picks candidate_a out of queue
 3. selection thread selects rowsets and adds candidate_b into queue, which may includes rowsets being compacting
 4. schedule thread creates task for candidate_a, executed by execution thread
 5. schedule thread creates task for candidate_b, but failed
*/
TEST_F(CompactionParallelizationTest, test_size_tiered_compaction_parallel3) {
    LOG(INFO) << "test_size_tiered_compaction_parallel3";

    create_tablet_schema(UNIQUE_KEYS);
    TabletMetaSharedPtr tablet_meta = std::make_shared<TabletMeta>();
    create_tablet_meta(tablet_meta.get());
    TabletSharedPtr tablet = Tablet::create_tablet_from_meta(tablet_meta, _engine->get_stores()[0]);
    ASSERT_TRUE(tablet->init().ok());

    // write 5 rowsets
    for (int i = 0; i < 5; ++i) {
        write_new_version(tablet);
    }

    // selection thread selects rowsets and adds candidate into queue
    _engine->compaction_manager()->update_tablet(tablet);
    ASSERT_EQ(1, _engine->compaction_manager()->get_waiting_task_num());

    // schedule thread picks candidate out of queue
    CompactionCandidate compaction_candidate;
    ASSERT_TRUE(_engine->compaction_manager()->pick_candidate(&compaction_candidate));
    ASSERT_EQ(0, _engine->compaction_manager()->get_waiting_task_num());

    // write 6 rowsets
    for (int i = 0; i < 6; ++i) {
        write_new_version(tablet);
    }

    // selection thread selects rowsets and adds new candidate into queue, which may includes rowsets being compacting
    _engine->compaction_manager()->update_tablet(tablet);
    ASSERT_EQ(1, _engine->compaction_manager()->get_waiting_task_num());

    // schedule thread creates task for old candidate
    std::shared_ptr<CompactionTask> compaction_task = nullptr;
    compaction_task = compaction_candidate.tablet->create_compaction_task();
    ASSERT_EQ(11, compaction_task->input_rowsets().size());
    for (const auto& rowset : compaction_task->input_rowsets()) {
        ASSERT_TRUE(rowset->get_is_compacting());
    }

    // execute compaction of for old candidate
    ASSERT_TRUE(_engine->compaction_manager()->register_task(compaction_task.get()));
    ASSERT_EQ(1, _engine->compaction_manager()->running_tasks_num());
    {
        std::shared_lock<std::shared_mutex> _compaction_lock;
        if (compaction_task->compaction_type() == CUMULATIVE_COMPACTION) {
            _compaction_lock = std::shared_lock(compaction_task->tablet()->get_cumulative_lock(), std::try_to_lock);
        } else {
            _compaction_lock = std::shared_lock(compaction_task->tablet()->get_base_lock(), std::try_to_lock);
        }
        ASSERT_TRUE(_compaction_lock.owns_lock());
        ASSERT_TRUE(tablet->has_compaction_task());
        ASSERT_EQ(BASE_COMPACTION, compaction_task->compaction_type());
        ASSERT_TRUE(dynamic_cast<HorizontalCompactionTask*>(compaction_task.get())->run_impl().ok());

        // finish executing task of old candidate
        _engine->compaction_manager()->unregister_task(compaction_task.get());
        ASSERT_EQ(0, _engine->compaction_manager()->running_tasks_num());
        ASSERT_EQ(1, _engine->compaction_manager()->get_waiting_task_num());
        ASSERT_FALSE(_engine->compaction_manager()->has_running_task(tablet));
    }

    // schedule thread creates task for new candidate, but failed
    ASSERT_TRUE(_engine->compaction_manager()->pick_candidate(&compaction_candidate));
    ASSERT_EQ(0, _engine->compaction_manager()->get_waiting_task_num());
    std::shared_ptr<CompactionTask> compaction_task2 = nullptr;
    compaction_task2 = compaction_candidate.tablet->create_compaction_task();
    ASSERT_EQ(nullptr, compaction_task2);

    ASSERT_EQ(1, tablet->version_count());
    std::vector<Version> versions;
    tablet->list_versions(&versions);
    ASSERT_EQ(1, versions.size());
    ASSERT_EQ(0, versions[0].first);
    ASSERT_EQ(10, versions[0].second);
}

/*
 select and execute new compaction task while the other one is in the execution progress：
 1. selection thread selects rowsets and adds candidate_a into queue
 2. schedule thread picks candidate_a out of queue, creates task_a
 3. selection thread selects rowsets and adds candidate_b into queue
 4. schedule thread picks candidate_b out of queue, creates task_b
 5. execute task_a and task_b in parallel
*/
TEST_F(CompactionParallelizationTest, test_size_tiered_compaction_parallel4) {
    LOG(INFO) << "test_size_tiered_compaction_parallel4";
    create_tablet_schema(UNIQUE_KEYS);

    TabletMetaSharedPtr tablet_meta = std::make_shared<TabletMeta>();
    create_tablet_meta(tablet_meta.get());

    TabletSharedPtr tablet = Tablet::create_tablet_from_meta(tablet_meta, _engine->get_stores()[0]);
    ASSERT_TRUE(tablet->init().ok());

    // task 1
    // write 5 rowsets
    for (int i = 0; i < 5; ++i) {
        write_new_version(tablet);
    }

    // select rowsets for compaction
    _engine->compaction_manager()->update_tablet(tablet);
    ASSERT_EQ(1, _engine->compaction_manager()->get_waiting_task_num());

    // create compaction
    CompactionCandidate compaction_candidate;
    std::shared_ptr<CompactionTask> compaction_task = nullptr;
    ASSERT_TRUE(_engine->compaction_manager()->pick_candidate(&compaction_candidate));
    ASSERT_EQ(0, _engine->compaction_manager()->get_waiting_task_num());
    compaction_task = compaction_candidate.tablet->create_compaction_task();
    ASSERT_EQ(5, compaction_task->input_rowsets().size());
    for (const auto& rowset : compaction_task->input_rowsets()) {
        ASSERT_TRUE(rowset->get_is_compacting());
    }

    // task 2
    // write 6 rowsets
    for (int i = 0; i < 6; ++i) {
        write_new_version(tablet);
    }

    // select rowsets for compaction
    _engine->compaction_manager()->update_tablet(tablet);
    ASSERT_EQ(1, _engine->compaction_manager()->get_waiting_task_num());

    // create compaction
    std::shared_ptr<CompactionTask> compaction_task2 = nullptr;
    ASSERT_TRUE(_engine->compaction_manager()->pick_candidate(&compaction_candidate));
    ASSERT_EQ(0, _engine->compaction_manager()->get_waiting_task_num());
    compaction_task2 = compaction_candidate.tablet->create_compaction_task();
    ASSERT_EQ(6, compaction_task2->input_rowsets().size());
    for (const auto& rowset : compaction_task2->input_rowsets()) {
        ASSERT_TRUE(rowset->get_is_compacting());
    }

    // execute task2
    ASSERT_TRUE(_engine->compaction_manager()->register_task(compaction_task2.get()));
    ASSERT_EQ(1, _engine->compaction_manager()->running_tasks_num());
    std::shared_lock<std::shared_mutex> _compaction_lock2;
    if (compaction_task2->compaction_type() == CUMULATIVE_COMPACTION) {
        _compaction_lock2 = std::shared_lock(compaction_task2->tablet()->get_cumulative_lock(), std::try_to_lock);
    } else {
        _compaction_lock2 = std::shared_lock(compaction_task2->tablet()->get_base_lock(), std::try_to_lock);
    }
    ASSERT_TRUE(_compaction_lock2.owns_lock());
    // execution progress
    ASSERT_TRUE(tablet->has_compaction_task());

    // execute task1
    ASSERT_TRUE(_engine->compaction_manager()->register_task(compaction_task.get()));
    ASSERT_EQ(2, _engine->compaction_manager()->running_tasks_num());
    std::shared_lock<std::shared_mutex> _compaction_lock;
    if (compaction_task->compaction_type() == CUMULATIVE_COMPACTION) {
        _compaction_lock = std::shared_lock(compaction_task->tablet()->get_cumulative_lock(), std::try_to_lock);
    } else {
        _compaction_lock = std::shared_lock(compaction_task->tablet()->get_base_lock(), std::try_to_lock);
    }
    ASSERT_TRUE(_compaction_lock.owns_lock());
    // execution progress
    ASSERT_TRUE(tablet->has_compaction_task());

    auto task_set = _engine->compaction_manager()->get_running_task(tablet);
    int base_count = 0, cumu_count = 0;
    for (auto& task : task_set) {
        if (task->compaction_type() == BASE_COMPACTION) {
            base_count++;
        } else {
            cumu_count++;
        }
    }
    ASSERT_EQ(1, base_count);
    ASSERT_EQ(1, cumu_count);

    // execute task
    ASSERT_TRUE(dynamic_cast<HorizontalCompactionTask*>(compaction_task2.get())->run_impl().ok());
    ASSERT_TRUE(dynamic_cast<HorizontalCompactionTask*>(compaction_task.get())->run_impl().ok());

    // finish executing task
    _engine->compaction_manager()->unregister_task(compaction_task.get());
    ASSERT_EQ(1, _engine->compaction_manager()->running_tasks_num());
    _engine->compaction_manager()->unregister_task(compaction_task2.get());
    ASSERT_EQ(0, _engine->compaction_manager()->running_tasks_num());
    ASSERT_EQ(0, _engine->compaction_manager()->get_waiting_task_num());
    ASSERT_FALSE(_engine->compaction_manager()->has_running_task(tablet));

    ASSERT_EQ(2, tablet->version_count());
    std::vector<Version> versions;
    tablet->list_versions(&versions);
    ASSERT_EQ(2, versions.size());
    ASSERT_EQ(0, versions[0].first);
    ASSERT_EQ(4, versions[0].second);
    ASSERT_EQ(5, versions[1].first);
    ASSERT_EQ(10, versions[1].second);
}

// does not support compaction parallelization under one tablet when using default compaction strategy
TEST_F(CompactionParallelizationTest, test_default_base_cumu_compaction_parallel) {
    config::enable_size_tiered_compaction_strategy = false;
    create_tablet_schema(UNIQUE_KEYS);

    TabletMetaSharedPtr tablet_meta = std::make_shared<TabletMeta>();
    create_tablet_meta(tablet_meta.get());

    TabletSharedPtr tablet = Tablet::create_tablet_from_meta(tablet_meta, _engine->get_stores()[0]);
    ASSERT_TRUE(tablet->init().ok());

    // task 1
    // write 5 rowsets
    for (int i = 0; i < 5; ++i) {
        write_new_version(tablet);
    }

    // select rowsets for compaction
    _engine->compaction_manager()->update_tablet(tablet);
    ASSERT_EQ(1, _engine->compaction_manager()->get_waiting_task_num());

    // create compaction
    CompactionCandidate compaction_candidate;
    std::shared_ptr<CompactionTask> compaction_task = nullptr;
    ASSERT_TRUE(_engine->compaction_manager()->pick_candidate(&compaction_candidate));
    ASSERT_EQ(0, _engine->compaction_manager()->get_waiting_task_num());
    compaction_task = compaction_candidate.tablet->create_compaction_task();

    // execute compaction
    ASSERT_TRUE(_engine->compaction_manager()->register_task(compaction_task.get()));
    ASSERT_EQ(1, _engine->compaction_manager()->running_tasks_num());

    // task 2
    // write 6 rowsets
    for (int i = 0; i < 6; ++i) {
        write_new_version(tablet);
    }
    // select rowsets for compaction, but failed
    ASSERT_FALSE(tablet->need_compaction());
    ASSERT_EQ(0, _engine->compaction_manager()->get_waiting_task_num());

    std::shared_lock<std::shared_mutex> _compaction_lock;
    if (compaction_task->compaction_type() == CUMULATIVE_COMPACTION) {
        _compaction_lock = std::shared_lock(compaction_task->tablet()->get_cumulative_lock(), std::try_to_lock);
    } else {
        _compaction_lock = std::shared_lock(compaction_task->tablet()->get_base_lock(), std::try_to_lock);
    }
    ASSERT_TRUE(_compaction_lock.owns_lock());
    ASSERT_TRUE(tablet->has_compaction_task());
    ASSERT_TRUE(dynamic_cast<HorizontalCompactionTask*>(compaction_task.get())->run_impl().ok());

    // finish executing task
    tablet->reset_compaction_status();
    _engine->compaction_manager()->unregister_task(compaction_task.get());
    ASSERT_EQ(0, _engine->compaction_manager()->running_tasks_num());
    ASSERT_EQ(0, _engine->compaction_manager()->get_waiting_task_num());
    ASSERT_FALSE(_engine->compaction_manager()->has_running_task(tablet));

    // task 2
    // write 6 rowsets
    for (int i = 0; i < 6; ++i) {
        write_new_version(tablet);
    }

    // select rowsets for compaction
    _engine->compaction_manager()->update_tablet(tablet);
    ASSERT_EQ(1, _engine->compaction_manager()->get_waiting_task_num());

    // create compaction
    std::shared_ptr<CompactionTask> compaction_task2 = nullptr;
    ASSERT_TRUE(_engine->compaction_manager()->pick_candidate(&compaction_candidate));
    ASSERT_EQ(0, _engine->compaction_manager()->get_waiting_task_num());
    compaction_task2 = compaction_candidate.tablet->create_compaction_task();

    // execute compaction
    ASSERT_TRUE(_engine->compaction_manager()->register_task(compaction_task2.get()));
    ASSERT_EQ(1, _engine->compaction_manager()->running_tasks_num());
    std::shared_lock<std::shared_mutex> _compaction_lock2;
    if (compaction_task2->compaction_type() == CUMULATIVE_COMPACTION) {
        _compaction_lock2 = std::shared_lock(compaction_task2->tablet()->get_cumulative_lock(), std::try_to_lock);
    } else {
        _compaction_lock2 = std::shared_lock(compaction_task2->tablet()->get_base_lock(), std::try_to_lock);
    }
    ASSERT_TRUE(_compaction_lock2.owns_lock());
    ASSERT_TRUE(tablet->has_compaction_task());
    ASSERT_TRUE(dynamic_cast<HorizontalCompactionTask*>(compaction_task2.get())->run_impl().ok());

    // finish executing task
    _engine->compaction_manager()->unregister_task(compaction_task2.get());
    ASSERT_EQ(0, _engine->compaction_manager()->running_tasks_num());
    ASSERT_EQ(0, _engine->compaction_manager()->get_waiting_task_num());
    ASSERT_FALSE(_engine->compaction_manager()->has_running_task(tablet));

    ASSERT_EQ(2, tablet->version_count());
    std::vector<Version> versions;
    tablet->list_versions(&versions);
    ASSERT_EQ(2, versions.size());
    ASSERT_EQ(0, versions[0].first);
    ASSERT_EQ(4, versions[0].second);
    ASSERT_EQ(5, versions[1].first);
    ASSERT_EQ(16, versions[1].second);
}

// replace old candidate when submitting new candidate with the same tabletId
TEST_F(CompactionParallelizationTest, test_default_base_cumu_compaction_parallel2) {
    LOG(INFO) << "test_default_base_cumu_compaction_parallel";
    config::enable_size_tiered_compaction_strategy = false;
    create_tablet_schema(UNIQUE_KEYS);

    TabletMetaSharedPtr tablet_meta = std::make_shared<TabletMeta>();
    create_tablet_meta(tablet_meta.get());

    TabletSharedPtr tablet = Tablet::create_tablet_from_meta(tablet_meta, _engine->get_stores()[0]);
    ASSERT_TRUE(tablet->init().ok());

    // task 1
    // write 5 rowsets
    for (int i = 0; i < 5; ++i) {
        write_new_version(tablet);
    }

    // select rowsets for compaction
    _engine->compaction_manager()->update_tablet(tablet);
    ASSERT_EQ(1, _engine->compaction_manager()->get_waiting_task_num());

    // task 2
    // write 6 rowsets
    for (int i = 0; i < 6; ++i) {
        write_new_version(tablet);
    }
    // select rowsets and add into task priority queue, which will replace the old candidate
    _engine->compaction_manager()->update_tablet(tablet);
    ASSERT_EQ(1, _engine->compaction_manager()->get_waiting_task_num());

    // create compaction
    CompactionCandidate compaction_candidate;
    std::shared_ptr<CompactionTask> compaction_task = nullptr;
    ASSERT_TRUE(_engine->compaction_manager()->pick_candidate(&compaction_candidate));
    ASSERT_EQ(0, _engine->compaction_manager()->get_waiting_task_num());
    compaction_task = compaction_candidate.tablet->create_compaction_task();
    ASSERT_EQ(11, compaction_task->input_rowsets().size());

    // todo: select another task, but failed
    // write 7 rowsets
    for (int i = 0; i < 7; ++i) {
        write_new_version(tablet);
    }
    // select rowsets for compaction, but failed
    ASSERT_FALSE(tablet->need_compaction());
    ASSERT_EQ(0, _engine->compaction_manager()->get_waiting_task_num());

    // execute compaction
    ASSERT_TRUE(_engine->compaction_manager()->register_task(compaction_task.get()));
    ASSERT_EQ(1, _engine->compaction_manager()->running_tasks_num());

    // select rowsets for compaction, but failed
    ASSERT_FALSE(tablet->need_compaction());
    ASSERT_EQ(0, _engine->compaction_manager()->get_waiting_task_num());

    std::shared_lock<std::shared_mutex> _compaction_lock;
    if (compaction_task->compaction_type() == CUMULATIVE_COMPACTION) {
        _compaction_lock = std::shared_lock(compaction_task->tablet()->get_cumulative_lock(), std::try_to_lock);
    } else {
        _compaction_lock = std::shared_lock(compaction_task->tablet()->get_base_lock(), std::try_to_lock);
    }
    ASSERT_TRUE(_compaction_lock.owns_lock());
    ASSERT_TRUE(tablet->has_compaction_task());
    ASSERT_TRUE(dynamic_cast<HorizontalCompactionTask*>(compaction_task.get())->run_impl().ok());

    // finish executing task
    _engine->compaction_manager()->unregister_task(compaction_task.get());
    ASSERT_EQ(0, _engine->compaction_manager()->running_tasks_num());
    ASSERT_EQ(0, _engine->compaction_manager()->get_waiting_task_num());
    ASSERT_FALSE(_engine->compaction_manager()->has_running_task(tablet));

    ASSERT_EQ(8, tablet->version_count());
    std::vector<Version> versions;
    tablet->list_versions(&versions);
    ASSERT_EQ(8, versions.size());
    ASSERT_EQ(0, versions[0].first);
    ASSERT_EQ(10, versions[0].second);
}

} // namespace starrocks
