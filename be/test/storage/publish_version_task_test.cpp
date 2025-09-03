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

#include <gtest/gtest.h>

#include <atomic>
#include <thread>

#include "agent/agent_common.h"
#include "agent/agent_server.h"
#include "agent/publish_version.h"
#include "butil/file_util.h"
#include "column/column_helper.h"
#include "common/config.h"
#include "exec/pipeline/query_context.h"
#include "fs/fs_util.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/current_thread.h"
#include "runtime/descriptor_helper.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/time_types.h"
#include "runtime/user_function_cache.h"
#include "storage/chunk_helper.h"
#include "storage/delta_writer.h"
#include "storage/options.h"
#include "storage/replication_txn_manager.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_meta.h"
#include "storage/txn_manager.h"
#include "storage/update_manager.h"
#include "testutil/assert.h"
#include "util/await.h"
#include "util/cpu_info.h"
#include "util/disk_info.h"
#include "util/failpoint/fail_point.h"
#include "util/logging.h"
#include "util/mem_info.h"
#include "util/threadpool.h"
#include "util/time.h"
#include "util/timezone_utils.h"

namespace starrocks {

class PublishVersionTaskTest : public testing::Test {
public:
    static void SetUpTestCase() { init(); }

    static void TearDownTestCase() {
        auto tablet_mgr = StorageEngine::instance()->tablet_manager();
        auto tablet = tablet_mgr->get_tablet(12345);
        (void)tablet_mgr->drop_tablet(tablet->tablet_id());
        (void)fs::remove_all(tablet->schema_hash_path());
    }

    static void init() {
        // create tablet first
        TCreateTabletReq request;
        set_default_create_tablet_request(&request);
        auto res = StorageEngine::instance()->create_tablet(request);
        ASSERT_TRUE(res.ok()) << res.to_string();
        TabletManager* tablet_manager = starrocks::StorageEngine::instance()->tablet_manager();
        TabletSharedPtr tablet = tablet_manager->get_tablet(12345);
        ASSERT_TRUE(tablet != nullptr);
        const TabletSchemaCSPtr& tablet_schema = tablet->tablet_schema();

        // create rowset
        RowsetWriterContext rowset_writer_context;
        create_rowset_writer_context(&rowset_writer_context, tablet->schema_hash_path(), tablet_schema);
        std::unique_ptr<RowsetWriter> rowset_writer;
        ASSERT_TRUE(RowsetFactory::create_rowset_writer(rowset_writer_context, &rowset_writer).ok());

        rowset_writer_add_rows(rowset_writer, tablet_schema);
        rowset_writer->flush();
        RowsetSharedPtr src_rowset = *rowset_writer->build();
        ASSERT_TRUE(src_rowset != nullptr);
        RowsetId src_rowset_id;
        src_rowset_id.init(10000);
        ASSERT_EQ(src_rowset_id, src_rowset->rowset_id());
        ASSERT_EQ(1024, src_rowset->num_rows());

        // add rowset to tablet
        auto st = tablet->add_rowset(src_rowset, true);
        ASSERT_TRUE(st.ok()) << st;
        sleep(2);
    }

    static void set_default_create_tablet_request(TCreateTabletReq* request) {
        request->tablet_id = 12345;
        request->__set_version(1);
        request->__set_version_hash(0);
        request->__set_partition_id(10);
        request->tablet_schema.schema_hash = 1111;
        request->tablet_schema.short_key_column_count = 2;
        request->tablet_schema.keys_type = TKeysType::DUP_KEYS;
        request->tablet_schema.storage_type = TStorageType::COLUMN;

        TColumn k1;
        k1.column_name = "k1";
        k1.__set_is_key(true);
        k1.column_type.type = TPrimitiveType::INT;
        request->tablet_schema.columns.push_back(k1);

        TColumn k2;
        k2.column_name = "k2";
        k2.__set_is_key(true);
        k2.column_type.__set_len(64);
        k2.column_type.type = TPrimitiveType::VARCHAR;
        request->tablet_schema.columns.push_back(k2);

        TColumn v;
        v.column_name = "v1";
        v.__set_is_key(false);
        v.column_type.type = TPrimitiveType::INT;
        v.__set_aggregation_type(TAggregationType::SUM);
        request->tablet_schema.columns.push_back(v);
    }

    static void create_rowset_writer_context(RowsetWriterContext* rowset_writer_context,
                                             const std::string& schema_hash_path,
                                             const TabletSchemaCSPtr& tablet_schema) {
        RowsetId rowset_id;
        rowset_id.init(10000);
        rowset_writer_context->rowset_id = rowset_id;
        rowset_writer_context->tablet_id = 12345;
        rowset_writer_context->tablet_schema_hash = 1111;
        rowset_writer_context->partition_id = 10;
        rowset_writer_context->rowset_path_prefix = schema_hash_path;
        rowset_writer_context->rowset_state = VISIBLE;
        rowset_writer_context->tablet_schema = tablet_schema;
        rowset_writer_context->version.first = 2;
        rowset_writer_context->version.second = 2;
    }

    static void rowset_writer_add_rows(std::unique_ptr<RowsetWriter>& writer, const TabletSchemaCSPtr& tablet_schema) {
        std::vector<std::string> test_data;
        auto schema = ChunkHelper::convert_schema(tablet_schema);
        auto chunk = ChunkHelper::new_chunk(schema, 1024);
        for (size_t i = 0; i < 1024; ++i) {
            test_data.push_back("well" + std::to_string(i));
            auto& cols = chunk->columns();
            cols[0]->append_datum(Datum(static_cast<int32_t>(i)));
            Slice field_1(test_data[i]);
            cols[1]->append_datum(Datum(field_1));
            cols[2]->append_datum(Datum(static_cast<int32_t>(10000 + i)));
        }
        auto st = writer->add_chunk(*chunk);
        ASSERT_TRUE(st.ok()) << st.to_string() << ", version:" << writer->version();
    }

    TSlotDescriptor _create_slot_desc(LogicalType type, const std::string& col_name, int col_pos) {
        TSlotDescriptorBuilder builder;

        if (type == LogicalType::TYPE_VARCHAR || type == LogicalType::TYPE_CHAR) {
            return builder.string_type(1024).column_name(col_name).column_pos(col_pos).nullable(false).build();
        } else {
            return builder.type(type).column_name(col_name).column_pos(col_pos).nullable(false).build();
        }
    }

    TupleDescriptor* _create_tuple_desc() {
        TDescriptorTableBuilder table_builder;
        TTupleDescriptorBuilder tuple_builder;

        for (size_t i = 0; i < 3; i++) {
            tuple_builder.add_slot(_create_slot_desc(_primitive_type[i], _names[i], i));
        }

        tuple_builder.build(&table_builder);

        std::vector<TTupleId> row_tuples = std::vector<TTupleId>{0};
        DescriptorTbl* tbl = nullptr;
        CHECK(DescriptorTbl::create(&_runtime_state, &_pool, table_builder.desc_tbl(), &tbl, config::vector_chunk_size)
                      .ok());

        auto* row_desc = _pool.add(new RowDescriptor(*tbl, row_tuples));
        auto* tuple_desc = row_desc->tuple_descriptors()[0];

        return tuple_desc;
    }

private:
    LogicalType _primitive_type[3] = {LogicalType::TYPE_INT, LogicalType::TYPE_VARCHAR, LogicalType::TYPE_INT};

    std::string _names[3] = {"k1", "k2", "v1"};
    RuntimeState _runtime_state;
    ObjectPool _pool;
};

TEST_F(PublishVersionTaskTest, test_publish_version) {
    TabletManager* tablet_manager = starrocks::StorageEngine::instance()->tablet_manager();
    DeltaWriterOptions writer_options;
    writer_options.tablet_id = 12345;
    writer_options.schema_hash = 1111;
    writer_options.txn_id = 2222;
    writer_options.partition_id = 10;
    writer_options.load_id.set_hi(1000);
    writer_options.load_id.set_lo(2222);
    writer_options.replica_state = Primary;
    TupleDescriptor* tuple_desc = _create_tuple_desc();
    writer_options.slots = &tuple_desc->slots();

    // publish version 3
    {
        MemTracker mem_checker(1024 * 1024 * 1024);
        auto writer_status = DeltaWriter::open(writer_options, &mem_checker);
        ASSERT_TRUE(writer_status.ok());
        auto delta_writer = std::move(writer_status.value());
        ASSERT_TRUE(delta_writer != nullptr);
        // prepare chunk
        std::vector<std::string> test_data;
        auto chunk = ChunkHelper::new_chunk(tuple_desc->slots(), 1024);
        std::vector<uint32_t> indexes;
        indexes.reserve(1024);
        for (size_t i = 0; i < 1024; ++i) {
            indexes.push_back(i);
            test_data.push_back("well" + std::to_string(i));
            auto& cols = chunk->columns();
            cols[0]->append_datum(Datum(static_cast<int32_t>(i)));
            Slice field_1(test_data[i]);
            cols[1]->append_datum(Datum(field_1));
            cols[2]->append_datum(Datum(static_cast<int32_t>(10000 + i)));
        }
        auto st = delta_writer->write(*chunk, indexes.data(), 0, indexes.size());
        ASSERT_TRUE(st.ok()) << st.to_string();
        st = delta_writer->close();
        ASSERT_TRUE(st.ok()) << st.to_string();
        st = delta_writer->commit();
        ASSERT_TRUE(st.ok()) << st.to_string();
    }

    std::map<TabletInfo, std::pair<RowsetSharedPtr, bool>> tablet_related_rs;
    StorageEngine::instance()->txn_manager()->get_txn_related_tablets(2222, 10, &tablet_related_rs);
    ASSERT_EQ(1, tablet_related_rs.size());
    TVersion version = 3;
    // publish version for txn
    auto tablet = tablet_manager->get_tablet(12345);
    for (auto& tablet_rs : tablet_related_rs) {
        const RowsetSharedPtr& rowset = tablet_rs.second.first;
        auto st = StorageEngine::instance()->txn_manager()->publish_txn(10, tablet, 2222, version, rowset);
        // success because the related transaction is GCed
        ASSERT_TRUE(st.ok()) << st.to_string();
    }
    Version max_version = tablet->max_version();
    ASSERT_EQ(3, max_version.first);

    // tablet already publish finish, get txn finished tablets
    tablet_related_rs.clear();
    StorageEngine::instance()->txn_manager()->get_txn_related_tablets(2222, 10, &tablet_related_rs);
    ASSERT_EQ(0, tablet_related_rs.size());
    tablet_related_rs.clear();

    // retry publish version 3
    auto token = ExecEnv::GetInstance()
                         ->agent_server()
                         ->get_thread_pool(TTaskType::PUBLISH_VERSION)
                         ->new_token(ThreadPool::ExecutionMode::CONCURRENT);
    std::unordered_set<DataDir*> affected_dirs;
    std::vector<TFinishTaskRequest> finish_task_requests;
    {
        auto& finish_task_request = finish_task_requests.emplace_back();
        TPublishVersionRequest publish_version_req;
        publish_version_req.transaction_id = 3333;
        TPartitionVersionInfo pvinfo;
        pvinfo.partition_id = 10;
        pvinfo.version = 10;
        publish_version_req.partition_version_infos.push_back(pvinfo);
        publish_version_req.enable_sync_publish = true;
        std::vector<TabletInfo> tablet_infos;
        StorageEngine::instance()->tablet_manager()->get_tablets_by_partition(10, tablet_infos);
        ASSERT_TRUE(tablet_infos.size() > 0);
        run_publish_version_task(token.get(), publish_version_req, finish_task_request, affected_dirs, 0);
        ASSERT_EQ(1, finish_task_request.error_tablet_ids.size());
    }
    auto& finish_task_request = finish_task_requests.emplace_back();
    // create req
    TPublishVersionRequest publish_version_req;
    publish_version_req.transaction_id = 2222;
    TPartitionVersionInfo pvinfo;
    pvinfo.partition_id = 10;
    pvinfo.version = 3;
    publish_version_req.partition_version_infos.push_back(pvinfo);
    // run publish version
    run_publish_version_task(token.get(), publish_version_req, finish_task_request, affected_dirs, 0);
    // check finish_task_request
    ASSERT_EQ(1, finish_task_request.tablet_versions.size());
    ASSERT_EQ(3, finish_task_request.tablet_versions[0].version);
    ASSERT_EQ(12345, finish_task_request.tablet_versions[0].tablet_id);
    // no actually publish
    ASSERT_EQ(0, affected_dirs.size());
}

TEST_F(PublishVersionTaskTest, test_publish_version2) {
    TabletManager* tablet_manager = starrocks::StorageEngine::instance()->tablet_manager();
    DeltaWriterOptions writer_options;
    writer_options.tablet_id = 12345;
    writer_options.schema_hash = 1111;
    writer_options.txn_id = 2223;
    writer_options.partition_id = 10;
    writer_options.load_id.set_hi(2000);
    writer_options.load_id.set_lo(3222);
    writer_options.replica_state = Primary;
    TupleDescriptor* tuple_desc = _create_tuple_desc();
    writer_options.slots = &tuple_desc->slots();

    // publish version 3
    {
        MemTracker mem_checker(1024 * 1024 * 1024);
        auto writer_status = DeltaWriter::open(writer_options, &mem_checker);
        ASSERT_TRUE(writer_status.ok()) << writer_status.status().to_string();
        auto delta_writer = std::move(writer_status.value());
        ASSERT_TRUE(delta_writer != nullptr);
        // prepare chunk
        std::vector<std::string> test_data;
        auto chunk = ChunkHelper::new_chunk(tuple_desc->slots(), 1024);
        std::vector<uint32_t> indexes;
        indexes.reserve(1024);
        for (size_t i = 0; i < 1024; ++i) {
            indexes.push_back(i);
            test_data.push_back("well" + std::to_string(i));
            auto& cols = chunk->columns();
            cols[0]->append_datum(Datum(static_cast<int32_t>(i)));
            Slice field_1(test_data[i]);
            cols[1]->append_datum(Datum(field_1));
            cols[2]->append_datum(Datum(static_cast<int32_t>(10000 + i)));
        }
        auto st = delta_writer->write(*chunk, indexes.data(), 0, indexes.size());
        ASSERT_TRUE(st.ok()) << st.to_string();
        st = delta_writer->close();
        ASSERT_TRUE(st.ok()) << st.to_string();
        st = delta_writer->commit();
        ASSERT_TRUE(st.ok()) << st.to_string();
    }
    // publish version 3
    auto token = ExecEnv::GetInstance()
                         ->agent_server()
                         ->get_thread_pool(TTaskType::PUBLISH_VERSION)
                         ->new_token(ThreadPool::ExecutionMode::CONCURRENT);
    {
        std::unordered_set<DataDir*> affected_dirs;
        std::vector<TFinishTaskRequest> finish_task_requests;
        auto& finish_task_request = finish_task_requests.emplace_back();
        // create req
        TPublishVersionRequest publish_version_req;
        publish_version_req.transaction_id = 2223;
        TPartitionVersionInfo pvinfo;
        pvinfo.partition_id = 10;
        pvinfo.version = 3;
        publish_version_req.partition_version_infos.push_back(pvinfo);
        // run publish version
        run_publish_version_task(token.get(), publish_version_req, finish_task_request, affected_dirs, 0);
        // check finish_task_request
        ASSERT_EQ(1, finish_task_request.tablet_versions.size());
        ASSERT_EQ(3, finish_task_request.tablet_versions[0].version);
        ASSERT_EQ(12345, finish_task_request.tablet_versions[0].tablet_id);
        ASSERT_EQ(1, affected_dirs.size());
    }
    auto tablet = tablet_manager->get_tablet(12345);
    Version max_version = tablet->max_version();
    ASSERT_EQ(3, max_version.first);
    {
        std::unordered_set<DataDir*> affected_dirs;
        std::vector<TFinishTaskRequest> finish_task_requests;
        auto& finish_task_request = finish_task_requests.emplace_back();
        // create req
        TPublishVersionRequest publish_version_req;
        publish_version_req.transaction_id = 2223;
        TPartitionVersionInfo pvinfo;
        pvinfo.partition_id = 10;
        pvinfo.version = 3;
        publish_version_req.partition_version_infos.push_back(pvinfo);
        // run publish version
        run_publish_version_task(token.get(), publish_version_req, finish_task_request, affected_dirs, 0);
        // check finish_task_request
        ASSERT_EQ(1, finish_task_request.tablet_versions.size());
        ASSERT_EQ(3, finish_task_request.tablet_versions[0].version);
        ASSERT_EQ(12345, finish_task_request.tablet_versions[0].tablet_id);
        ASSERT_EQ(0, affected_dirs.size());
    }
}

TEST_F(PublishVersionTaskTest, test_publish_version_cancellation) {
    // Prepare a transaction with data (similar to previous tests)
    DeltaWriterOptions writer_options;
    writer_options.tablet_id = 12345;
    writer_options.schema_hash = 1111;
    writer_options.txn_id = 4445;
    writer_options.partition_id = 10;
    writer_options.load_id.set_hi(3000);
    writer_options.load_id.set_lo(4445);
    writer_options.replica_state = Primary;
    TupleDescriptor* tuple_desc = _create_tuple_desc();
    writer_options.slots = &tuple_desc->slots();

    {
        MemTracker mem_checker(1024 * 1024 * 1024);
        auto writer_status = DeltaWriter::open(writer_options, &mem_checker);
        ASSERT_TRUE(writer_status.ok()) << writer_status.status().to_string();
        auto delta_writer = std::move(writer_status.value());
        ASSERT_TRUE(delta_writer != nullptr);

        std::vector<std::string> test_data;
        auto chunk = ChunkHelper::new_chunk(tuple_desc->slots(), 128);
        std::vector<uint32_t> indexes;
        indexes.reserve(128);
        for (size_t i = 0; i < 128; ++i) {
            indexes.push_back(i);
            test_data.push_back("well" + std::to_string(i));
            auto& cols = chunk->columns();
            cols[0]->append_datum(Datum(static_cast<int32_t>(i)));
            Slice field_1(test_data[i]);
            cols[1]->append_datum(Datum(field_1));
            cols[2]->append_datum(Datum(static_cast<int32_t>(10000 + i)));
        }
        auto st = delta_writer->write(*chunk, indexes.data(), 0, indexes.size());
        ASSERT_TRUE(st.ok()) << st.to_string();
        st = delta_writer->close();
        ASSERT_TRUE(st.ok()) << st.to_string();
        st = delta_writer->commit();
        ASSERT_TRUE(st.ok()) << st.to_string();
    }

    // Build a dedicated thread pool with a single worker
    std::unique_ptr<ThreadPool> pool;
    ASSERT_TRUE(ThreadPoolBuilder("publish-cancel-test")
                        .set_min_threads(1)
                        .set_max_threads(1)
                        .set_max_queue_size(128)
                        .build(&pool)
                        .ok());

    // Submit a blocking task to occupy the only worker thread so that publish tasks queue up
    std::mutex mu;
    std::condition_variable cv;
    bool release_blocker = false;
    std::atomic<bool> blocker_started{false};
    auto blocker = std::make_shared<CancellableRunnable>(
            [&]() {
                blocker_started.store(true, std::memory_order_release);
                std::unique_lock<std::mutex> lk(mu);
                cv.wait(lk, [&] { return release_blocker; });
            },
            [&]() {
                // Cancel is a no-op since the blocker task is released via condition variable, not by cancellation.
            });
    ASSERT_TRUE(pool->submit(std::move(blocker)).ok());

    // Prepare publish request
    std::unordered_set<DataDir*> affected_dirs;
    TFinishTaskRequest finish_task_request;
    TPublishVersionRequest publish_version_req;
    publish_version_req.transaction_id = 4445;
    TPartitionVersionInfo pvinfo;
    pvinfo.partition_id = 10;
    pvinfo.version = 3;
    publish_version_req.partition_version_infos.push_back(pvinfo);
    publish_version_req.enable_sync_publish = true;

    // Ensure the blocker has started running before submitting publish tasks
    ASSERT_TRUE(Awaitility().timeout(60 * 1000 * 1000).until([&]() {
        return blocker_started.load(std::memory_order_acquire);
    }));

    // Run publish in a separate thread so we can shutdown the pool to trigger cancellation
    auto token = pool->new_token(ThreadPool::ExecutionMode::CONCURRENT);
    std::thread t([&]() {
        run_publish_version_task(token.get(), publish_version_req, finish_task_request, affected_dirs, 0);
    });

    // Wait until at least one publish task is queued behind the blocker (or timeout)
    ASSERT_TRUE(Awaitility().timeout(60 * 1000 * 1000).until([&]() { return pool->num_queued_tasks() > 0; }));

    // Shutdown the pool in a separate thread, then release the blocker so shutdown can complete
    std::thread shutdown_th([&]() { pool->shutdown(); });
    // Wait until the token has been shutdown
    ASSERT_TRUE(token->wait_for(MonoDelta::FromSeconds(60)));
    {
        std::lock_guard<std::mutex> lk(mu);
        release_blocker = true;
    }
    cv.notify_one();

    shutdown_th.join();
    t.join();

    // Expect that publish reports error for the tablet due to cancellation
    ASSERT_EQ(finish_task_request.error_tablet_ids.size(), 1);
    ASSERT_EQ(finish_task_request.error_tablet_ids[0], 12345);
}

TEST_F(PublishVersionTaskTest, test_publish_version_rowset_missing) {
    // Prepare a txn entry without committing a rowset so that rowset is nullptr
    auto* tablet_manager = StorageEngine::instance()->tablet_manager();
    auto tablet = tablet_manager->get_tablet(12345);
    ASSERT_TRUE(tablet != nullptr);

    PUniqueId load_id;
    load_id.set_hi(5555);
    load_id.set_lo(5555);
    auto st = StorageEngine::instance()->txn_manager()->prepare_txn(10, tablet, 5555, load_id);
    ASSERT_TRUE(st.ok()) << st.to_string();

    // Use the standard publish thread pool
    auto token = ExecEnv::GetInstance()
                         ->agent_server()
                         ->get_thread_pool(TTaskType::PUBLISH_VERSION)
                         ->new_token(ThreadPool::ExecutionMode::CONCURRENT);

    std::unordered_set<DataDir*> affected_dirs;
    TFinishTaskRequest finish_task_request;
    TPublishVersionRequest publish_version_req;
    publish_version_req.transaction_id = 5555;
    TPartitionVersionInfo pvinfo;
    pvinfo.partition_id = 10;
    pvinfo.version = 4;
    publish_version_req.partition_version_infos.push_back(pvinfo);

    run_publish_version_task(token.get(), publish_version_req, finish_task_request, affected_dirs, 0);

    // Expect the rowset-not-found branch to report the tablet as error
    ASSERT_EQ(1, finish_task_request.error_tablet_ids.size());
    ASSERT_EQ(12345, finish_task_request.error_tablet_ids[0]);
}

TEST_F(PublishVersionTaskTest, test_publish_version_overwrite_failed) {
    // Create a PRIMARY_KEYS tablet to enter updates() path for overwrite
    const int64_t pk_tablet_id = 223344;
    const int64_t pk_partition_id = 30;
    const int32_t pk_schema_hash = 3333;

    TCreateTabletReq request;
    set_default_create_tablet_request(&request);
    request.tablet_id = pk_tablet_id;
    request.__set_partition_id(pk_partition_id);
    request.tablet_schema.keys_type = TKeysType::PRIMARY_KEYS;
    request.tablet_schema.schema_hash = pk_schema_hash;
    ASSERT_TRUE(StorageEngine::instance()->create_tablet(request).ok());

    // Write one small txn via DeltaWriter
    DeltaWriterOptions writer_options;
    writer_options.tablet_id = pk_tablet_id;
    writer_options.schema_hash = pk_schema_hash;
    writer_options.txn_id = 777001;
    writer_options.partition_id = pk_partition_id;
    writer_options.load_id.set_hi(777001);
    writer_options.load_id.set_lo(777001);
    writer_options.replica_state = Primary;
    TupleDescriptor* tuple_desc = _create_tuple_desc();
    writer_options.slots = &tuple_desc->slots();
    {
        MemTracker mem_checker(1024 * 1024 * 1024);
        auto writer_status = DeltaWriter::open(writer_options, &mem_checker);
        ASSERT_TRUE(writer_status.ok());
        auto delta_writer = std::move(writer_status.value());
        ASSERT_TRUE(delta_writer != nullptr);
        auto chunk = ChunkHelper::new_chunk(tuple_desc->slots(), 8);
        std::vector<uint32_t> indexes;
        indexes.reserve(8);
        for (size_t i = 0; i < 8; ++i) {
            indexes.push_back(i);
            auto& cols = chunk->columns();
            cols[0]->append_datum(Datum(static_cast<int32_t>(i)));
            std::string s_str = std::string("owf") + std::to_string(i);
            Slice s(s_str);
            cols[1]->append_datum(Datum(s));
            cols[2]->append_datum(Datum(static_cast<int32_t>(i)));
        }
        ASSERT_TRUE(delta_writer->write(*chunk, indexes.data(), 0, indexes.size()).ok());
        ASSERT_TRUE(delta_writer->close().ok());
        ASSERT_TRUE(delta_writer->commit().ok());
    }

    // Put tablet updates into error state so rowset_commit returns error
    {
        auto* tablet_manager = StorageEngine::instance()->tablet_manager();
        auto tablet = tablet_manager->get_tablet(pk_tablet_id);
        ASSERT_TRUE(tablet != nullptr);
        ASSERT_TRUE(tablet->updates() != nullptr);
        tablet->updates()->set_error("inject overwrite failure for testing");
    }

    auto token = ExecEnv::GetInstance()
                         ->agent_server()
                         ->get_thread_pool(TTaskType::PUBLISH_VERSION)
                         ->new_token(ThreadPool::ExecutionMode::CONCURRENT);

    std::unordered_set<DataDir*> affected_dirs;
    TFinishTaskRequest finish_task_request;
    TPublishVersionRequest publish_version_req;
    publish_version_req.transaction_id = 777001;
    TPartitionVersionInfo pvinfo;
    pvinfo.partition_id = pk_partition_id;
    pvinfo.version = 6;
    publish_version_req.partition_version_infos.push_back(pvinfo);
    publish_version_req.__set_is_version_overwrite(true);

    // No wait needed; rowset_commit will fail immediately due to error state
    run_publish_version_task(token.get(), publish_version_req, finish_task_request, affected_dirs, 0);

    // Expect overwrite failure reported for the PK tablet
    ASSERT_EQ(1, finish_task_request.error_tablet_ids.size());
    ASSERT_EQ(pk_tablet_id, finish_task_request.error_tablet_ids[0]);
}

TEST_F(PublishVersionTaskTest, test_publish_version_submit_failure) {
    // Prepare a txn entry without committing a rowset (any task will do)
    auto* tablet_manager = StorageEngine::instance()->tablet_manager();
    auto tablet = tablet_manager->get_tablet(12345);
    ASSERT_TRUE(tablet != nullptr);
    PUniqueId load_id;
    load_id.set_hi(7777);
    load_id.set_lo(7777);
    ASSERT_TRUE(StorageEngine::instance()->txn_manager()->prepare_txn(10, tablet, 7777, load_id).ok());

    // Build a dedicated pool and shut it down to force submit() to fail
    std::unique_ptr<ThreadPool> pool;
    ASSERT_TRUE(ThreadPoolBuilder("publish-submit-fail-test").set_min_threads(1).set_max_threads(1).build(&pool).ok());
    auto token = pool->new_token(ThreadPool::ExecutionMode::CONCURRENT);
    pool->shutdown();

    std::unordered_set<DataDir*> affected_dirs;
    TFinishTaskRequest finish_task_request;
    TPublishVersionRequest publish_version_req;
    publish_version_req.transaction_id = 7777;
    TPartitionVersionInfo pvinfo;
    pvinfo.partition_id = 10;
    pvinfo.version = 5;
    publish_version_req.partition_version_infos.push_back(pvinfo);

    run_publish_version_task(token.get(), publish_version_req, finish_task_request, affected_dirs, 0);

    // Expect an error reported because tasks couldn't be submitted
    ASSERT_EQ(1, finish_task_request.error_tablet_ids.size());
    ASSERT_EQ(12345, finish_task_request.error_tablet_ids[0]);
}

TEST_F(PublishVersionTaskTest, test_publish_version_tablet_dropped) {
    // Create an isolated tablet in a different partition and commit a rowset to a txn
    const int64_t new_tablet_id = 54321;
    const int64_t new_partition_id = 20;
    const int32_t new_schema_hash = 2222;

    TCreateTabletReq request;
    set_default_create_tablet_request(&request);
    request.tablet_id = new_tablet_id;
    request.__set_partition_id(new_partition_id);
    request.tablet_schema.schema_hash = new_schema_hash;
    ASSERT_TRUE(StorageEngine::instance()->create_tablet(request).ok());

    auto* tablet_manager = StorageEngine::instance()->tablet_manager();
    auto new_tablet = tablet_manager->get_tablet(new_tablet_id);
    ASSERT_TRUE(new_tablet != nullptr);

    // Write a small rowset into txn 8889 for the new tablet
    DeltaWriterOptions writer_options;
    writer_options.tablet_id = new_tablet_id;
    writer_options.schema_hash = new_schema_hash;
    writer_options.txn_id = 8889;
    writer_options.partition_id = new_partition_id;
    writer_options.load_id.set_hi(8889);
    writer_options.load_id.set_lo(8889);
    writer_options.replica_state = Primary;
    TupleDescriptor* tuple_desc = _create_tuple_desc();
    writer_options.slots = &tuple_desc->slots();
    {
        MemTracker mem_checker(1024 * 1024 * 1024);
        auto writer_status = DeltaWriter::open(writer_options, &mem_checker);
        ASSERT_TRUE(writer_status.ok());
        auto delta_writer = std::move(writer_status.value());
        ASSERT_TRUE(delta_writer != nullptr);
        auto chunk = ChunkHelper::new_chunk(tuple_desc->slots(), 8);
        std::vector<uint32_t> indexes;
        indexes.reserve(8);
        for (size_t i = 0; i < 8; ++i) {
            indexes.push_back(i);
            auto& cols = chunk->columns();
            cols[0]->append_datum(Datum(static_cast<int32_t>(i)));
            std::string s_str = std::string("dropped") + std::to_string(i);
            Slice s(s_str);
            cols[1]->append_datum(Datum(s));
            cols[2]->append_datum(Datum(static_cast<int32_t>(i)));
        }
        ASSERT_TRUE(delta_writer->write(*chunk, indexes.data(), 0, indexes.size()).ok());
        ASSERT_TRUE(delta_writer->close().ok());
        ASSERT_TRUE(delta_writer->commit().ok());
    }

    // Drop the tablet before publishing so that get_tablet returns nullptr inside publish
    ASSERT_TRUE(tablet_manager->drop_tablet(new_tablet_id, kDeleteFiles).ok());
    (void)tablet_manager->delete_shutdown_tablet(new_tablet_id);

    auto token = ExecEnv::GetInstance()
                         ->agent_server()
                         ->get_thread_pool(TTaskType::PUBLISH_VERSION)
                         ->new_token(ThreadPool::ExecutionMode::CONCURRENT);

    std::unordered_set<DataDir*> affected_dirs;
    TFinishTaskRequest finish_task_request;
    TPublishVersionRequest publish_version_req;
    publish_version_req.transaction_id = 8889;
    TPartitionVersionInfo pvinfo;
    pvinfo.partition_id = new_partition_id;
    pvinfo.version = 5;
    publish_version_req.partition_version_infos.push_back(pvinfo);

    run_publish_version_task(token.get(), publish_version_req, finish_task_request, affected_dirs, 0);

    // Tablet was dropped; publish should skip it without reporting error
    ASSERT_EQ(0, finish_task_request.error_tablet_ids.size());
}

TEST_F(PublishVersionTaskTest, test_publish_version_replication_failed) {
    // Prepare a replication txn via remote_snapshot so that publish on replication path fails
    TRemoteSnapshotRequest remote_snapshot_request;
    remote_snapshot_request.__set_transaction_id(9090);
    remote_snapshot_request.__set_table_id(1);
    remote_snapshot_request.__set_partition_id(10);
    remote_snapshot_request.__set_tablet_id(12345);
    remote_snapshot_request.__set_tablet_type(TTabletType::TABLET_TYPE_DISK);
    remote_snapshot_request.__set_schema_hash(1111);
    // current tablet visible version is at least 3 in previous tests
    remote_snapshot_request.__set_visible_version(3);
    remote_snapshot_request.__set_src_token(ExecEnv::GetInstance()->token());
    remote_snapshot_request.__set_src_tablet_id(12345);
    remote_snapshot_request.__set_src_tablet_type(TTabletType::TABLET_TYPE_DISK);
    remote_snapshot_request.__set_src_schema_hash(1111);
    remote_snapshot_request.__set_src_visible_version(4);
    remote_snapshot_request.__set_src_backends(std::vector<TBackend>{TBackend()});

    TSnapshotInfo remote_snapshot_info;
    (void)StorageEngine::instance()->replication_txn_manager()->remote_snapshot(remote_snapshot_request,
                                                                                &remote_snapshot_info);

    auto token = ExecEnv::GetInstance()
                         ->agent_server()
                         ->get_thread_pool(TTaskType::PUBLISH_VERSION)
                         ->new_token(ThreadPool::ExecutionMode::CONCURRENT);

    std::unordered_set<DataDir*> affected_dirs;
    TFinishTaskRequest finish_task_request;
    TPublishVersionRequest publish_version_req;
    publish_version_req.transaction_id = 9090;
    publish_version_req.__set_txn_type(TTxnType::TXN_REPLICATION);
    TPartitionVersionInfo pvinfo;
    pvinfo.partition_id = 10;
    pvinfo.version = 4;
    publish_version_req.partition_version_infos.push_back(pvinfo);

    run_publish_version_task(token.get(), publish_version_req, finish_task_request, affected_dirs, 0);

    // Expect replication publish failure to be reported for tablet 12345
    ASSERT_EQ(1, finish_task_request.error_tablet_ids.size());
    ASSERT_EQ(12345, finish_task_request.error_tablet_ids[0]);
}

} // namespace starrocks
