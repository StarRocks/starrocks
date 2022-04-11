// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
#include "storage/task/engine_storage_migration_task.h"

#include <gtest/gtest.h>

#include "butil/file_util.h"
#include "column/column_helper.h"
#include "column/column_pool.h"
#include "common/config.h"
#include "exec/pipeline/query_context.h"
#include "gtest/gtest.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/memory/chunk_allocator.h"
#include "runtime/user_function_cache.h"
#include "runtime/vectorized/time_types.h"
#include "storage/options.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/storage_engine.h"
#include "storage/tablet_meta.h"
#include "storage/update_manager.h"
#include "storage/vectorized/chunk_helper.h"
#include "testutil/assert.h"
#include "util/cpu_info.h"
#include "util/disk_info.h"
#include "util/file_utils.h"
#include "util/logging.h"
#include "util/mem_info.h"
#include "util/timezone_utils.h"

namespace starrocks {

class EngineStorageMigrationTaskTest : public testing::Test {
public:
    static void SetUpTestCase() { init(); }

    static void TearDownTestCase() {}

    static void init() {
        // create tablet first
        TCreateTabletReq request;
        set_default_create_tablet_request(&request);
        auto res = StorageEngine::instance()->create_tablet(request);
        ASSERT_TRUE(res.ok()) << res.to_string();
        TabletManager* tablet_manager = starrocks::ExecEnv::GetInstance()->storage_engine()->tablet_manager();
        TabletSharedPtr tablet = tablet_manager->get_tablet(12345);
        ASSERT_TRUE(tablet != nullptr);
        const TabletSchema& tablet_schema = tablet->tablet_schema();

        // create rowset
        RowsetWriterContext rowset_writer_context(kDataFormatUnknown, config::storage_format_version);
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
        rowset_writer_context->rowset_type = BETA_ROWSET;
        rowset_writer_context->rowset_path_prefix = schema_hash_path;
        rowset_writer_context->rowset_state = VISIBLE;
        rowset_writer_context->tablet_schema = tablet_schema;
        rowset_writer_context->version.first = 2;
        rowset_writer_context->version.second = 2;
    }

    static void rowset_writer_add_rows(std::unique_ptr<RowsetWriter>& writer, const TabletSchema& tablet_schema) {
        std::vector<std::string> test_data;
        auto schema = vectorized::ChunkHelper::convert_schema_to_format_v2(tablet_schema);
        auto chunk = vectorized::ChunkHelper::new_chunk(schema, 1024);
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

    void do_cycle_migration() {
        TabletManager* tablet_manager = starrocks::ExecEnv::GetInstance()->storage_engine()->tablet_manager();
        TabletSharedPtr tablet = tablet_manager->get_tablet(12345);
        ASSERT_TRUE(tablet != nullptr);
        ASSERT_EQ(tablet->tablet_id(), 12345);
        DataDir* source_path = tablet->data_dir();
        tablet.reset();
        DataDir* dest_path = nullptr;
        DataDir* data_dir_1 = starrocks::ExecEnv::GetInstance()->storage_engine()->get_stores()[0];
        DataDir* data_dir_2 = starrocks::ExecEnv::GetInstance()->storage_engine()->get_stores()[1];
        if (source_path == data_dir_1) {
            dest_path = data_dir_2;
        } else {
            dest_path = data_dir_1;
        }
        EngineStorageMigrationTask migration_task(12345, 1111, dest_path);
        ASSERT_OK(migration_task.execute());
        // sleep 2 second for add latency for load
        sleep(2);
        EngineStorageMigrationTask migration_task_2(12345, 1111, source_path);
        ASSERT_OK(migration_task_2.execute());
    }
};

TEST_F(EngineStorageMigrationTaskTest, test_cycle_migration) {
    do_cycle_migration();
}

} // namespace starrocks

int main(int argc, char** argv) {
    starrocks::init_glog("be_test", true);
    ::testing::InitGoogleTest(&argc, argv);
    if (getenv("STARROCKS_HOME") == nullptr) {
        fprintf(stderr, "you need set STARROCKS_HOME environment variable.\n");
        exit(-1);
    }
    std::string conffile = std::string(getenv("STARROCKS_HOME")) + "/conf/be.conf";
    if (!starrocks::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }

    starrocks::config::sys_log_level = "INFO";
    butil::FilePath storage_root;
    CHECK(butil::CreateNewTempDirectory("tmp_ut_", &storage_root));
    std::string root_path_1 = storage_root.value() + "/migration_test_path_1";
    std::string root_path_2 = storage_root.value() + "/migration_test_path_2";
    starrocks::FileUtils::remove_all(root_path_1);
    starrocks::FileUtils::create_dir(root_path_1);
    starrocks::FileUtils::remove_all(root_path_2);
    starrocks::FileUtils::create_dir(root_path_2);

    starrocks::config::storage_root_path = root_path_1 + ";" + root_path_2;

    starrocks::CpuInfo::init();
    starrocks::DiskInfo::init();
    starrocks::MemInfo::init();
    starrocks::UserFunctionCache::instance()->init(starrocks::config::user_function_dir);

    starrocks::vectorized::date::init_date_cache();
    starrocks::TimezoneUtils::init_time_zones();

    // first create the starrocks::config::storage_root_path ahead
    std::vector<starrocks::StorePath> paths;
    auto olap_res = starrocks::parse_conf_store_paths(starrocks::config::storage_root_path, &paths);
    if (!olap_res.ok()) {
        LOG(FATAL) << "parse config storage path failed, path=" << starrocks::config::storage_root_path;
        exit(-1);
    }

    std::unique_ptr<starrocks::MemTracker> table_meta_mem_tracker = std::make_unique<starrocks::MemTracker>();
    std::unique_ptr<starrocks::MemTracker> schema_change_mem_tracker = std::make_unique<starrocks::MemTracker>();
    std::unique_ptr<starrocks::MemTracker> compaction_mem_tracker = std::make_unique<starrocks::MemTracker>();
    std::unique_ptr<starrocks::MemTracker> update_mem_tracker = std::make_unique<starrocks::MemTracker>();
    starrocks::StorageEngine* engine = nullptr;
    starrocks::EngineOptions options;
    options.store_paths = paths;
    options.tablet_meta_mem_tracker = table_meta_mem_tracker.get();
    options.schema_change_mem_tracker = schema_change_mem_tracker.get();
    options.compaction_mem_tracker = compaction_mem_tracker.get();
    options.update_mem_tracker = update_mem_tracker.get();
    starrocks::Status s = starrocks::StorageEngine::open(options, &engine);
    if (!s.ok()) {
        starrocks::FileUtils::remove_all(root_path_1);
        starrocks::FileUtils::remove_all(root_path_2);
        fprintf(stderr, "storage engine open failed, path=%s, msg=%s\n", starrocks::config::storage_root_path.c_str(),
                s.to_string().c_str());
        return -1;
    }
    auto* exec_env = starrocks::ExecEnv::GetInstance();
    exec_env->init_mem_tracker();
    starrocks::ExecEnv::init(exec_env, paths);
    exec_env->set_storage_engine(engine);
    int r = RUN_ALL_TESTS();

    // clear some trash objects kept in tablet_manager so mem_tracker checks will not fail
    starrocks::StorageEngine::instance()->tablet_manager()->start_trash_sweep();
    starrocks::FileUtils::remove_all(root_path_1);
    starrocks::FileUtils::remove_all(root_path_2);
    starrocks::vectorized::TEST_clear_all_columns_this_thread();
    // delete engine
    engine->stop();
    delete engine;
    exec_env->set_storage_engine(nullptr);
    starrocks::pipeline::QueryContextManager::instance()->clear();
    // destroy exec env
    starrocks::tls_thread_status.set_mem_tracker(nullptr);
    starrocks::ExecEnv::destroy(exec_env);

    return r;
}
