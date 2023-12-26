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

#include "storage/task/engine_storage_migration_task.h"

#include <gtest/gtest.h>

#include "butil/file_util.h"
#include "column/column_pool.h"
#include "common/config.h"
#include "exec/pipeline/query_context.h"
#include "fs/fs_util.h"
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
#include "storage/update_manager.h"
#include "testutil/assert.h"
#include "util/cpu_info.h"
#include "util/disk_info.h"
#include "util/logging.h"
#include "util/mem_info.h"
#include "util/timezone_utils.h"

namespace starrocks {

class EngineStorageMigrationTaskTest : public testing::Test {
public:
    static void SetUpTestCase() { init(); }

    static void TearDownTestCase() { config::enable_event_based_compaction_framework = true; }

    static TabletSharedPtr create_pk_tablet(int64_t tablet_id, int32_t schema_hash) {
        TCreateTabletReq request;
        request.tablet_id = tablet_id;
        request.__set_version(1);
        request.__set_version_hash(0);
        request.tablet_schema.schema_hash = schema_hash;
        request.tablet_schema.short_key_column_count = 1;
        request.tablet_schema.keys_type = TKeysType::PRIMARY_KEYS;
        request.tablet_schema.storage_type = TStorageType::COLUMN;

        TColumn k1;
        k1.column_name = "pk";
        k1.__set_is_key(true);
        k1.column_type.type = TPrimitiveType::BIGINT;
        request.tablet_schema.columns.push_back(k1);

        TColumn k2;
        k2.column_name = "v1";
        k2.__set_is_key(false);
        k2.column_type.type = TPrimitiveType::SMALLINT;
        request.tablet_schema.columns.push_back(k2);

        TColumn k3;
        k3.column_name = "v2";
        k3.__set_is_key(false);
        k3.column_type.type = TPrimitiveType::INT;
        request.tablet_schema.columns.push_back(k3);
        auto st = StorageEngine::instance()->create_tablet(request);
        CHECK(st.ok()) << st.to_string();
        return StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, false);
    }

    static RowsetSharedPtr create_pk_rowset(const TabletSharedPtr& tablet, const vector<int64_t>& keys) {
        RowsetWriterContext writer_context;
        RowsetId rowset_id = StorageEngine::instance()->next_rowset_id();
        writer_context.rowset_id = rowset_id;
        writer_context.tablet_id = tablet->tablet_id();
        writer_context.tablet_schema_hash = tablet->schema_hash();
        writer_context.partition_id = 0;
        writer_context.rowset_path_prefix = tablet->schema_hash_path();
        writer_context.rowset_state = COMMITTED;
        writer_context.tablet_schema = tablet->tablet_schema();
        writer_context.version.first = 0;
        writer_context.version.second = 0;
        writer_context.segments_overlap = NONOVERLAPPING;
        std::unique_ptr<RowsetWriter> writer;
        EXPECT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &writer).ok());

        auto schema = ChunkHelper::convert_schema(tablet->tablet_schema());
        auto chunk = ChunkHelper::new_chunk(schema, keys.size());
        auto& cols = chunk->columns();
        for (int64_t key : keys) {
            if (schema.num_key_fields() == 1) {
                cols[0]->append_datum(Datum(key));
            } else {
                cols[0]->append_datum(Datum(key));
                string v = fmt::to_string(key * 234234342345);
                cols[1]->append_datum(Datum(Slice(v)));
                cols[2]->append_datum(Datum((int32_t)key));
            }
            int vcol_start = schema.num_key_fields();
            cols[vcol_start]->append_datum(Datum((int16_t)(key % 100 + 1)));
            if (cols[vcol_start + 1]->is_binary()) {
                string v = fmt::to_string(key % 1000 + 2);
                cols[vcol_start + 1]->append_datum(Datum(Slice(v)));
            } else {
                cols[vcol_start + 1]->append_datum(Datum((int32_t)(key % 1000 + 2)));
            }
        }
        if (!keys.empty()) {
            CHECK_OK(writer->flush_chunk(*chunk));
        } else {
            CHECK_OK(writer->flush());
        }
        return *writer->build();
    }

    static void init() {
        config::enable_event_based_compaction_framework = false;
        /*
            create duplicated key tablet
        */
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

        /*
            create primary key tablet
        */
        TabletSharedPtr tablet_pk = create_pk_tablet(99999, 9999);
        tablet_pk->set_enable_persistent_index(false);
        const int N = 8000;
        std::vector<int64_t> keys;
        for (int i = 0; i < N; i++) {
            keys.push_back(i);
        }
        auto rs0 = create_pk_rowset(tablet_pk, keys);
        ASSERT_TRUE(tablet_pk->rowset_commit(2, rs0).ok());
        ASSERT_EQ(2, tablet_pk->updates()->max_version());
        auto rs1 = create_pk_rowset(tablet_pk, keys);
        ASSERT_TRUE(tablet_pk->rowset_commit(3, rs1).ok());
        ASSERT_EQ(3, tablet_pk->updates()->max_version());
        auto rs2 = create_pk_rowset(tablet_pk, keys);
        ASSERT_TRUE(tablet_pk->rowset_commit(4, rs2).ok());
        ASSERT_EQ(4, tablet_pk->updates()->max_version());

        sleep(2);
    }

    static void set_default_create_tablet_request(TCreateTabletReq* request) {
        request->tablet_id = 12345;
        request->__set_version(1);
        request->__set_version_hash(0);
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
        std::vector<bool> nullable_tuples = std::vector<bool>{false};
        DescriptorTbl* tbl = nullptr;
        DescriptorTbl::create(&_runtime_state, &_pool, table_builder.desc_tbl(), &tbl, config::vector_chunk_size);

        auto* row_desc = _pool.add(new RowDescriptor(*tbl, row_tuples, nullable_tuples));
        auto* tuple_desc = row_desc->tuple_descriptors()[0];

        return tuple_desc;
    }

    TupleDescriptor* _create_tuple_desc_pk() {
        TDescriptorTableBuilder table_builder;
        TTupleDescriptorBuilder tuple_builder;

        {
            TSlotDescriptorBuilder builder;
            tuple_builder.add_slot(
                    builder.type(LogicalType::TYPE_BIGINT).column_name("pk").column_pos(0).nullable(false).build());
        }
        {
            TSlotDescriptorBuilder builder;
            tuple_builder.add_slot(
                    builder.type(LogicalType::TYPE_SMALLINT).column_name("v1").column_pos(1).nullable(false).build());
        }
        {
            TSlotDescriptorBuilder builder;
            tuple_builder.add_slot(
                    builder.type(LogicalType::TYPE_INT).column_name("v2").column_pos(2).nullable(false).build());
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

    void do_cycle_migration(int64_t tablet_id, int32_t schema_hash) {
        TabletManager* tablet_manager = starrocks::StorageEngine::instance()->tablet_manager();
        TabletSharedPtr tablet = tablet_manager->get_tablet(tablet_id);
        ASSERT_TRUE(tablet != nullptr);
        ASSERT_EQ(tablet->tablet_id(), tablet_id);
        DataDir* source_path = tablet->data_dir();
        tablet.reset();
        DataDir* dest_path = nullptr;
        DataDir* data_dir_1 = starrocks::StorageEngine::instance()->get_stores()[0];
        DataDir* data_dir_2 = starrocks::StorageEngine::instance()->get_stores()[1];
        if (source_path == data_dir_1) {
            dest_path = data_dir_2;
        } else {
            dest_path = data_dir_1;
        }
        EngineStorageMigrationTask migration_task(tablet_id, schema_hash, dest_path);
        ASSERT_OK(migration_task.execute());
        // sleep 2 second for add latency for load
        sleep(2);
        EngineStorageMigrationTask migration_task_2(tablet_id, schema_hash, source_path);
        ASSERT_OK(migration_task_2.execute());
    }

    void do_migration_fail(int64_t tablet_id, int32_t schema_hash) {
        TabletManager* tablet_manager = starrocks::StorageEngine::instance()->tablet_manager();
        TabletSharedPtr tablet = tablet_manager->get_tablet(tablet_id);
        ASSERT_TRUE(tablet != nullptr);
        ASSERT_EQ(tablet->tablet_id(), tablet_id);
        DataDir* source_path = tablet->data_dir();
        tablet.reset();
        DataDir* dest_path = nullptr;
        DataDir* data_dir_1 = starrocks::StorageEngine::instance()->get_stores()[0];
        DataDir* data_dir_2 = starrocks::StorageEngine::instance()->get_stores()[1];
        if (source_path == data_dir_1) {
            dest_path = data_dir_2;
        } else {
            dest_path = data_dir_1;
        }
        EngineStorageMigrationTask migration_task(tablet_id, schema_hash, dest_path);
        auto st = migration_task.execute();
        ASSERT_FALSE(st.ok());
    }

private:
    LogicalType _primitive_type[3] = {LogicalType::TYPE_INT, LogicalType::TYPE_VARCHAR, LogicalType::TYPE_INT};

    std::string _names[3] = {"k1", "k2", "v1"};
    RuntimeState _runtime_state;
    ObjectPool _pool;
};

TEST_F(EngineStorageMigrationTaskTest, test_cycle_migration) {
    do_cycle_migration(12345, 1111);
}

TEST_F(EngineStorageMigrationTaskTest, test_cycle_migration_pk) {
    do_cycle_migration(99999, 9999);
}

TEST_F(EngineStorageMigrationTaskTest, test_concurrent_ingestion_and_migration) {
    TabletManager* tablet_manager = starrocks::StorageEngine::instance()->tablet_manager();
    TabletUid old_tablet_uid;
    {
        TabletSharedPtr tablet = tablet_manager->get_tablet(12345);
        old_tablet_uid = tablet->tablet_uid();
    }
    DeltaWriterOptions writer_options;
    writer_options.tablet_id = 12345;
    writer_options.schema_hash = 1111;
    writer_options.txn_id = 2222;
    writer_options.partition_id = 10;
    writer_options.load_id.set_hi(1000);
    writer_options.load_id.set_lo(2222);
    TupleDescriptor* tuple_desc = _create_tuple_desc();
    writer_options.slots = &tuple_desc->slots();

    {
        MemTracker mem_checker(1024 * 1024 * 1024);
        auto writer_status = DeltaWriter::open(writer_options, &mem_checker);
        ASSERT_TRUE(writer_status.ok());
        auto delta_writer = std::move(writer_status.value());
        ASSERT_TRUE(delta_writer != nullptr);
        // add sleep to add time of tablet create time gap
        sleep(2);
        // do migration check, migration will fail
        do_migration_fail(12345, 1111);
        TabletUid new_tablet_uid;
        {
            TabletSharedPtr tablet = tablet_manager->get_tablet(12345);
            new_tablet_uid = tablet->tablet_uid();
        }
        // the migration fail. so the tablet will not change
        ASSERT_TRUE(new_tablet_uid.hi == old_tablet_uid.hi && new_tablet_uid.lo == old_tablet_uid.lo);
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
        ASSERT_TRUE(st.ok());
        st = delta_writer->close();
        ASSERT_TRUE(st.ok());
        st = delta_writer->commit();
        ASSERT_TRUE(st.ok());
    }
    // make sure to release delta_writer from here
    // or it will not release the tablet in gc

    // clean trash and unused txns after commit
    // it will clean no tablet and txns
    tablet_manager->start_trash_sweep();
    starrocks::StorageEngine::instance()->_clean_unused_txns();

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
}

TEST_F(EngineStorageMigrationTaskTest, test_concurrent_ingestion_and_migration_pk) {
    TabletManager* tablet_manager = starrocks::StorageEngine::instance()->tablet_manager();
    TabletUid old_tablet_uid;
    {
        TabletSharedPtr tablet = tablet_manager->get_tablet(99999);
        old_tablet_uid = tablet->tablet_uid();
    }
    DeltaWriterOptions writer_options;
    writer_options.tablet_id = 99999;
    writer_options.schema_hash = 9999;
    writer_options.txn_id = 4444;
    writer_options.partition_id = 90;
    writer_options.load_id.set_hi(3000);
    writer_options.load_id.set_lo(4444);
    TupleDescriptor* tuple_desc = _create_tuple_desc_pk();
    writer_options.slots = &tuple_desc->slots();

    {
        MemTracker mem_checker(1024 * 1024 * 1024);
        auto writer_status = DeltaWriter::open(writer_options, &mem_checker);
        ASSERT_TRUE(writer_status.ok());
        auto delta_writer = std::move(writer_status.value());
        ASSERT_TRUE(delta_writer != nullptr);
        // add sleep to add time of tablet create time gap
        sleep(2);
        // do migration check, migration will fail
        do_migration_fail(99999, 9999);
        TabletUid new_tablet_uid;
        {
            TabletSharedPtr tablet = tablet_manager->get_tablet(99999);
            new_tablet_uid = tablet->tablet_uid();
        }
        // the migration fail. so the tablet will not change
        ASSERT_TRUE(new_tablet_uid.hi == old_tablet_uid.hi && new_tablet_uid.lo == old_tablet_uid.lo);
        // prepare chunk
        std::vector<std::string> test_data;
        auto chunk = ChunkHelper::new_chunk(tuple_desc->slots(), 1024);
        std::vector<uint32_t> indexes;
        indexes.reserve(1024);
        for (size_t i = 0; i < 1024; ++i) {
            indexes.push_back(i);
            auto& cols = chunk->columns();
            cols[0]->append_datum(Datum(static_cast<int64_t>(i)));
            cols[1]->append_datum(Datum(static_cast<int16_t>(i + 1)));
            cols[2]->append_datum(Datum(static_cast<int32_t>(i + 2)));
        }
        auto st = delta_writer->write(*chunk, indexes.data(), 0, indexes.size());
        ASSERT_TRUE(st.ok());
        st = delta_writer->close();
        ASSERT_TRUE(st.ok());
        st = delta_writer->commit();
        ASSERT_TRUE(st.ok());
    }
    // make sure to release delta_writer from here
    // or it will not release the tablet in gc

    // clean trash and unused txns after commit
    // it will clean no tablet and txns
    tablet_manager->start_trash_sweep();
    starrocks::StorageEngine::instance()->_clean_unused_txns();

    std::map<TabletInfo, RowsetSharedPtr> tablet_related_rs;
    StorageEngine::instance()->txn_manager()->get_txn_related_tablets(4444, 90, &tablet_related_rs);
    ASSERT_EQ(1, tablet_related_rs.size());
    TVersion version = 5;
    // publish version for txn
    auto tablet = tablet_manager->get_tablet(99999);
    for (auto& tablet_rs : tablet_related_rs) {
        const RowsetSharedPtr& rowset = tablet_rs.second;
        auto st = StorageEngine::instance()->txn_manager()->publish_txn(90, tablet, 4444, version, rowset);
        // success because the related transaction is GCed
        ASSERT_TRUE(st.ok());
    }
    ASSERT_EQ(5, tablet->updates()->max_version());
}

} // namespace starrocks

int main(int argc, char** argv) {
    starrocks::init_glog("be_test", true);
    ::testing::InitGoogleTest(&argc, argv);
    if (getenv("STARROCKS_HOME") == nullptr) {
        fprintf(stderr, "you need set STARROCKS_HOME environment variable.\n");
        exit(-1);
    }
    std::string conffile = std::string(getenv("STARROCKS_HOME")) + "/conf/be_test.conf";
    if (!starrocks::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }

    starrocks::config::sys_log_level = "INFO";
    butil::FilePath storage_root;
    CHECK(butil::CreateNewTempDirectory("tmp_ut_", &storage_root));
    std::string root_path_1 = storage_root.value() + "/migration_test_path_1";
    std::string root_path_2 = storage_root.value() + "/migration_test_path_2";
    starrocks::fs::remove_all(root_path_1);
    starrocks::fs::create_directories(root_path_1);
    starrocks::fs::remove_all(root_path_2);
    starrocks::fs::create_directories(root_path_2);

    starrocks::config::storage_root_path = root_path_1 + ";" + root_path_2;
    starrocks::config::storage_flood_stage_left_capacity_bytes = 10485600;

    starrocks::CpuInfo::init();
    starrocks::DiskInfo::init();
    starrocks::MemInfo::init();
    starrocks::UserFunctionCache::instance()->init(starrocks::config::user_function_dir);

    starrocks::date::init_date_cache();
    starrocks::TimezoneUtils::init_time_zones();

    // first create the starrocks::config::storage_root_path ahead
    std::vector<starrocks::StorePath> paths;
    auto olap_res = starrocks::parse_conf_store_paths(starrocks::config::storage_root_path, &paths);
    if (!olap_res.ok()) {
        LOG(FATAL) << "parse config storage path failed, path=" << starrocks::config::storage_root_path;
        exit(-1);
    }

    std::unique_ptr<starrocks::MemTracker> compaction_mem_tracker = std::make_unique<starrocks::MemTracker>();
    std::unique_ptr<starrocks::MemTracker> update_mem_tracker = std::make_unique<starrocks::MemTracker>();
    starrocks::StorageEngine* engine = nullptr;
    starrocks::EngineOptions options;
    options.store_paths = paths;
    options.compaction_mem_tracker = compaction_mem_tracker.get();
    options.update_mem_tracker = update_mem_tracker.get();
    starrocks::Status s = starrocks::StorageEngine::open(options, &engine);
    if (!s.ok()) {
        starrocks::fs::remove_all(root_path_1);
        starrocks::fs::remove_all(root_path_2);
        fprintf(stderr, "storage engine open failed, path=%s, msg=%s\n", starrocks::config::storage_root_path.c_str(),
                s.to_string().c_str());
        return -1;
    }
    auto* global_env = starrocks::GlobalEnv::GetInstance();
    auto st = global_env->init();
    st.permit_unchecked_error();
    auto* exec_env = starrocks::ExecEnv::GetInstance();
    st = exec_env->init(paths);
    st.permit_unchecked_error();
    int r = RUN_ALL_TESTS();

    sleep(10);

    // clear some trash objects kept in tablet_manager so mem_tracker checks will not fail
    starrocks::StorageEngine::instance()->tablet_manager()->start_trash_sweep();
    starrocks::fs::remove_all(storage_root.value());
    starrocks::TEST_clear_all_columns_this_thread();
    // delete engine
    engine->stop();
    delete engine;
    // destroy exec env
    starrocks::tls_thread_status.set_mem_tracker(nullptr);
    exec_env->stop();
    exec_env->destroy();
    global_env->stop();

    return r;
}
