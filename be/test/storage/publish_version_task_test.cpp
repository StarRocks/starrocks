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

#include "agent/agent_common.h"
#include "agent/agent_server.h"
#include "agent/publish_version.h"
#include "butil/file_util.h"
#include "column/column_helper.h"
#include "column/column_pool.h"
#include "common/config.h"
#include "exec/pipeline/query_context.h"
#include "fs/fs_util.h"
#include "gtest/gtest.h"
#include "runtime/current_thread.h"
#include "runtime/descriptor_helper.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/time_types.h"
#include "runtime/user_function_cache.h"
#include "storage/chunk_helper.h"
#include "storage/delta_writer.h"
#include "storage/options.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_meta.h"
#include "storage/txn_manager.h"
#include "storage/update_manager.h"
#include "testutil/assert.h"
#include "util/cpu_info.h"
#include "util/disk_info.h"
#include "util/logging.h"
#include "util/mem_info.h"
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
        const TabletSchema& tablet_schema = tablet->tablet_schema();

        // create rowset
        RowsetWriterContext rowset_writer_context;
        create_rowset_writer_context(&rowset_writer_context, tablet->schema_hash_path(), &tablet_schema);
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
                                             const std::string& schema_hash_path, const TabletSchema* tablet_schema) {
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

    static void rowset_writer_add_rows(std::unique_ptr<RowsetWriter>& writer, const TabletSchema& tablet_schema) {
        std::vector<std::string> test_data;
        auto schema = ChunkHelper::convert_schema(tablet_schema);
        auto chunk = ChunkHelper::new_chunk(schema, 1024);
        for (size_t i = 0; i < 1024; ++i) {
            test_data.push_back("well" + std::to_string(i));
            auto& cols = chunk->columns();
            cols[0]->append_datum(vectorized::Datum(static_cast<int32_t>(i)));
            Slice field_1(test_data[i]);
            cols[1]->append_datum(vectorized::Datum(field_1));
            cols[2]->append_datum(vectorized::Datum(static_cast<int32_t>(10000 + i)));
        }
        auto st = writer->add_chunk(*chunk);
        ASSERT_TRUE(st.ok()) << st.to_string() << ", version:" << writer->version();
    }

    TSlotDescriptor _create_slot_desc(PrimitiveType type, const std::string& col_name, int col_pos) {
        TSlotDescriptorBuilder builder;

        if (type == PrimitiveType::TYPE_VARCHAR || type == PrimitiveType::TYPE_CHAR) {
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
        std::vector<bool> nullable_tuples = std::vector<bool>{false};
        DescriptorTbl* tbl = nullptr;
        DescriptorTbl::create(&_runtime_state, &_pool, table_builder.desc_tbl(), &tbl, config::vector_chunk_size);

        auto* row_desc = _pool.add(new RowDescriptor(*tbl, row_tuples, nullable_tuples));
        auto* tuple_desc = row_desc->tuple_descriptors()[0];

        return tuple_desc;
    }

private:
    PrimitiveType _primitive_type[3] = {PrimitiveType::TYPE_INT, PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_INT};

    std::string _names[3] = {"k1", "k2", "v1"};
    RuntimeState _runtime_state;
    ObjectPool _pool;
};

TEST_F(PublishVersionTaskTest, test_publish_version) {
    TabletManager* tablet_manager = starrocks::StorageEngine::instance()->tablet_manager();
    vectorized::DeltaWriterOptions writer_options;
    writer_options.tablet_id = 12345;
    writer_options.schema_hash = 1111;
    writer_options.txn_id = 2222;
    writer_options.partition_id = 10;
    writer_options.load_id.set_hi(1000);
    writer_options.load_id.set_lo(2222);
    TupleDescriptor* tuple_desc = _create_tuple_desc();
    writer_options.slots = &tuple_desc->slots();

    // publish version 3
    {
        MemTracker mem_checker(1024 * 1024 * 1024);
        auto writer_status = vectorized::DeltaWriter::open(writer_options, &mem_checker);
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
            cols[0]->append_datum(vectorized::Datum(static_cast<int32_t>(i)));
            Slice field_1(test_data[i]);
            cols[1]->append_datum(vectorized::Datum(field_1));
            cols[2]->append_datum(vectorized::Datum(static_cast<int32_t>(10000 + i)));
        }
        auto st = delta_writer->write(*chunk, indexes.data(), 0, indexes.size());
        ASSERT_TRUE(st.ok());
        st = delta_writer->close();
        ASSERT_TRUE(st.ok());
        st = delta_writer->commit();
        ASSERT_TRUE(st.ok());
    }

    std::map<TabletInfo, RowsetSharedPtr> tablet_related_rs;
    StorageEngine::instance()->txn_manager()->get_txn_related_tablets(2222, 10, &tablet_related_rs);
    ASSERT_EQ(1, tablet_related_rs.size());
    TVersion version = 3;
    // publish version for txn
    auto tablet = tablet_manager->get_tablet(12345);
    for (auto& tablet_rs : tablet_related_rs) {
        const RowsetSharedPtr& rowset = tablet_rs.second;
        auto st = StorageEngine::instance()->txn_manager()->publish_txn(10, tablet, 2222, version, rowset);
        // success because the related transaction is GCed
        ASSERT_TRUE(st.ok());
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
    auto& finish_task_request = finish_task_requests.emplace_back();
    // create req
    TAgentTaskRequest tareq;
    TPublishVersionRequest publish_version_req;
    publish_version_req.transaction_id = 2222;
    TPartitionVersionInfo pvinfo;
    pvinfo.partition_id = 10;
    pvinfo.version = 3;
    publish_version_req.partition_version_infos.push_back(pvinfo);
    PublishVersionAgentTaskRequest publish_version_task(tareq, publish_version_req, time(nullptr));
    // run publish version
    run_publish_version_task(token.get(), publish_version_task, finish_task_request, affected_dirs, 0);
    // check finish_task_request
    ASSERT_EQ(1, finish_task_request.tablet_versions.size());
    ASSERT_EQ(3, finish_task_request.tablet_versions[0].version);
    ASSERT_EQ(12345, finish_task_request.tablet_versions[0].tablet_id);
    // no actually publish
    ASSERT_EQ(0, affected_dirs.size());
}

TEST_F(PublishVersionTaskTest, test_publish_version2) {
    TabletManager* tablet_manager = starrocks::StorageEngine::instance()->tablet_manager();
    vectorized::DeltaWriterOptions writer_options;
    writer_options.tablet_id = 12345;
    writer_options.schema_hash = 1111;
    writer_options.txn_id = 2223;
    writer_options.partition_id = 10;
    writer_options.load_id.set_hi(2000);
    writer_options.load_id.set_lo(3222);
    TupleDescriptor* tuple_desc = _create_tuple_desc();
    writer_options.slots = &tuple_desc->slots();

    // publish version 3
    {
        MemTracker mem_checker(1024 * 1024 * 1024);
        auto writer_status = vectorized::DeltaWriter::open(writer_options, &mem_checker);
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
            cols[0]->append_datum(vectorized::Datum(static_cast<int32_t>(i)));
            Slice field_1(test_data[i]);
            cols[1]->append_datum(vectorized::Datum(field_1));
            cols[2]->append_datum(vectorized::Datum(static_cast<int32_t>(10000 + i)));
        }
        auto st = delta_writer->write(*chunk, indexes.data(), 0, indexes.size());
        ASSERT_TRUE(st.ok());
        st = delta_writer->close();
        ASSERT_TRUE(st.ok());
        st = delta_writer->commit();
        ASSERT_TRUE(st.ok());
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
        TAgentTaskRequest tareq;
        TPublishVersionRequest publish_version_req;
        publish_version_req.transaction_id = 2223;
        TPartitionVersionInfo pvinfo;
        pvinfo.partition_id = 10;
        pvinfo.version = 3;
        publish_version_req.partition_version_infos.push_back(pvinfo);
        PublishVersionAgentTaskRequest publish_version_task(tareq, publish_version_req, time(nullptr));
        // run publish version
        run_publish_version_task(token.get(), publish_version_task, finish_task_request, affected_dirs, 0);
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
        TAgentTaskRequest tareq;
        TPublishVersionRequest publish_version_req;
        publish_version_req.transaction_id = 2223;
        TPartitionVersionInfo pvinfo;
        pvinfo.partition_id = 10;
        pvinfo.version = 3;
        publish_version_req.partition_version_infos.push_back(pvinfo);
        PublishVersionAgentTaskRequest publish_version_task(tareq, publish_version_req, time(nullptr));
        // run publish version
        run_publish_version_task(token.get(), publish_version_task, finish_task_request, affected_dirs, 0);
        // check finish_task_request
        ASSERT_EQ(1, finish_task_request.tablet_versions.size());
        ASSERT_EQ(3, finish_task_request.tablet_versions[0].version);
        ASSERT_EQ(12345, finish_task_request.tablet_versions[0].tablet_id);
        ASSERT_EQ(0, affected_dirs.size());
    }
}

} // namespace starrocks
