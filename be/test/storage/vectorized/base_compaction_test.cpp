// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "storage/vectorized/base_compaction.h"

#include <gtest/gtest.h>

#include "runtime/exec_env.h"
#include "storage/row_cursor.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/storage_engine.h"
#include "storage/tablet_meta.h"
#include "storage/vectorized/base_compaction.h"
#include "storage/vectorized/compaction.h"
#include "storage/vectorized/cumulative_compaction.h"
#include "util/file_utils.h"

namespace starrocks::vectorized {

static StorageEngine* k_engine = nullptr;

class BaseCompactionTest : public testing::Test {
public:
    void create_rowset_writer_context(RowsetWriterContext* rowset_writer_context) {
        RowsetId rowset_id;
        rowset_id.init(10000);
        rowset_writer_context->rowset_id = rowset_id;
        rowset_writer_context->tablet_id = 12345;
        rowset_writer_context->tablet_schema_hash = 1111;
        rowset_writer_context->partition_id = 10;
        rowset_writer_context->rowset_type = _rowset_type;
        rowset_writer_context->rowset_path_prefix = config::storage_root_path + "/data/0/12345/1111";
        rowset_writer_context->rowset_state = VISIBLE;
        rowset_writer_context->tablet_schema = _tablet_schema.get();
        rowset_writer_context->version.first = 0;
        rowset_writer_context->version.second = 1;
        rowset_writer_context->version_hash = 110;
    }

    void create_tablet_schema(KeysType keys_type) {
        TabletSchemaPB tablet_schema_pb;
        tablet_schema_pb.set_keys_type(keys_type);
        tablet_schema_pb.set_num_short_key_columns(2);
        tablet_schema_pb.set_num_rows_per_row_block(1024);
        tablet_schema_pb.set_compress_kind(COMPRESS_NONE);
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

        _tablet_schema.reset(new TabletSchema);
        _tablet_schema->init_from_pb(tablet_schema_pb);
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
        RowCursor row;
        ASSERT_EQ(row.init(*_tablet_schema.get()), OLAP_SUCCESS);
        std::vector<std::string> test_data;
        for (int i = 0; i < 1024; ++i) {
            test_data.push_back("well" + std::to_string(i));

            int32_t field_0 = i;
            row.set_field_content(0, reinterpret_cast<char*>(&field_0), _mem_pool.get());
            Slice field_1(test_data[i]);
            row.set_field_content(1, reinterpret_cast<char*>(&field_1), _mem_pool.get());
            int32_t field_2 = 10000 + i;
            row.set_field_content(2, reinterpret_cast<char*>(&field_2), _mem_pool.get());
            writer->add_row(row);
        }
    }

    void SetUp() override {
        config::max_compaction_concurrency = 1;
        Compaction::init(config::max_compaction_concurrency);

        config::storage_root_path = std::filesystem::current_path().string() + "/data_test_base_compaction";
        FileUtils::remove_all(config::storage_root_path);
        ASSERT_TRUE(FileUtils::create_dir(config::storage_root_path).ok());
        std::vector<StorePath> paths;
        paths.emplace_back(config::storage_root_path);

        starrocks::EngineOptions options;
        options.store_paths = paths;
        if (k_engine == nullptr) {
            Status s = starrocks::StorageEngine::open(options, &k_engine);
            ASSERT_TRUE(s.ok()) << s.to_string();
        }

        ExecEnv* exec_env = starrocks::ExecEnv::GetInstance();
        exec_env->set_storage_engine(k_engine);

        std::string data_path = config::storage_root_path + "/data";
        ASSERT_TRUE(FileUtils::create_dir(data_path).ok());
        std::string shard_path = data_path + "/0";
        ASSERT_TRUE(FileUtils::create_dir(shard_path).ok());
        std::string tablet_path = shard_path + "/12345";
        ASSERT_TRUE(FileUtils::create_dir(tablet_path).ok());
        _schema_hash_path = tablet_path + "/1111";
        ASSERT_TRUE(FileUtils::create_dir(_schema_hash_path).ok());

        _mem_pool.reset(new MemPool());

        _compaction_mem_tracker.reset(new MemTracker(-1));
        _tablet_meta_mem_tracker = std::make_unique<MemTracker>();
    }

    void TearDown() override {
        if (FileUtils::check_exist(config::storage_root_path)) {
            ASSERT_TRUE(FileUtils::remove_all(config::storage_root_path).ok());
        }
    }

protected:
    std::unique_ptr<TabletSchema> _tablet_schema;
    RowsetTypePB _rowset_type = BETA_ROWSET;
    std::string _schema_hash_path;
    std::unique_ptr<MemTracker> _compaction_mem_tracker;
    std::unique_ptr<MemPool> _mem_pool;
    std::unique_ptr<MemTracker> _tablet_meta_mem_tracker;
};

TEST_F(BaseCompactionTest, test_init_succeeded) {
    TabletMetaSharedPtr tablet_meta(new TabletMeta());
    TabletSharedPtr tablet = Tablet::create_tablet_from_meta(_tablet_meta_mem_tracker.get(), tablet_meta, nullptr);
    BaseCompaction base_compaction(_compaction_mem_tracker.get(), tablet);
    ASSERT_FALSE(base_compaction.compact().ok());
}

TEST_F(BaseCompactionTest, test_input_rowsets_LE_1) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    auto schema = std::make_shared<const TabletSchema>(schema_pb);
    TabletMetaSharedPtr tablet_meta(new TabletMeta());
    tablet_meta->set_tablet_schema(schema);

    TabletSharedPtr tablet = Tablet::create_tablet_from_meta(_tablet_meta_mem_tracker.get(), tablet_meta, nullptr);
    tablet->init();
    BaseCompaction base_compaction(_compaction_mem_tracker.get(), tablet);
    ASSERT_FALSE(base_compaction.compact().ok());
}

TEST_F(BaseCompactionTest, test_input_rowsets_EQ_2) {
    config::storage_format_version = 2;
    create_tablet_schema(UNIQUE_KEYS);
    RowsetWriterContext rowset_writer_context(kDataFormatUnknown, config::storage_format_version);
    create_rowset_writer_context(&rowset_writer_context);
    std::unique_ptr<RowsetWriter> _rowset_writer;
    ASSERT_TRUE(RowsetFactory::create_rowset_writer(rowset_writer_context, &_rowset_writer).ok());

    rowset_writer_add_rows(_rowset_writer);

    _rowset_writer->flush();
    RowsetSharedPtr src_rowset = _rowset_writer->build();
    ASSERT_TRUE(src_rowset != nullptr);
    RowsetId src_rowset_id;
    src_rowset_id.init(10000);
    ASSERT_EQ(src_rowset_id, src_rowset->rowset_id());
    ASSERT_EQ(1024, src_rowset->num_rows());

    TabletMetaSharedPtr tablet_meta(new TabletMeta());
    create_tablet_meta(tablet_meta.get());
    tablet_meta->add_rs_meta(src_rowset->rowset_meta());

    {
        RowsetId src_rowset_id;
        src_rowset_id.init(10001);
        rowset_writer_context.rowset_id = src_rowset_id;
        rowset_writer_context.version =
                Version(rowset_writer_context.version.second + 1, rowset_writer_context.version.second + 2);

        std::unique_ptr<RowsetWriter> _rowset_writer;
        ASSERT_TRUE(RowsetFactory::create_rowset_writer(rowset_writer_context, &_rowset_writer).ok());

        rowset_writer_add_rows(_rowset_writer);

        _rowset_writer->flush();
        RowsetSharedPtr src_rowset = _rowset_writer->build();
        ASSERT_TRUE(src_rowset != nullptr);
        ASSERT_EQ(src_rowset_id, src_rowset->rowset_id());
        ASSERT_EQ(1024, src_rowset->num_rows());

        tablet_meta->add_rs_meta(src_rowset->rowset_meta());
    }

    TabletSharedPtr tablet = Tablet::create_tablet_from_meta(_tablet_meta_mem_tracker.get(), tablet_meta, nullptr);
    tablet->init();
    tablet->calculate_cumulative_point();

    BaseCompaction base_compaction(_compaction_mem_tracker.get(), tablet);

    ASSERT_FALSE(base_compaction.compact().ok());
}

TEST_F(BaseCompactionTest, test_compact_succeed) {
    config::storage_format_version = 2;
    create_tablet_schema(UNIQUE_KEYS);

    RowsetWriterContext rowset_writer_context(kDataFormatUnknown, config::storage_format_version);
    create_rowset_writer_context(&rowset_writer_context);
    std::unique_ptr<RowsetWriter> _rowset_writer;
    ASSERT_TRUE(RowsetFactory::create_rowset_writer(rowset_writer_context, &_rowset_writer).ok());

    rowset_writer_add_rows(_rowset_writer);

    _rowset_writer->flush();
    RowsetSharedPtr src_rowset = _rowset_writer->build();
    ASSERT_TRUE(src_rowset != nullptr);
    RowsetId src_rowset_id;
    src_rowset_id.init(10000);
    ASSERT_EQ(src_rowset_id, src_rowset->rowset_id());
    ASSERT_EQ(1024, src_rowset->num_rows());

    TabletMetaSharedPtr tablet_meta(new TabletMeta());
    create_tablet_meta(tablet_meta.get());
    tablet_meta->add_rs_meta(src_rowset->rowset_meta());

    {
        RowsetId src_rowset_id;
        src_rowset_id.init(10001);
        rowset_writer_context.rowset_id = src_rowset_id;
        rowset_writer_context.version =
                Version(rowset_writer_context.version.second + 1, rowset_writer_context.version.second + 2);

        std::unique_ptr<RowsetWriter> _rowset_writer;
        ASSERT_TRUE(RowsetFactory::create_rowset_writer(rowset_writer_context, &_rowset_writer).ok());

        rowset_writer_add_rows(_rowset_writer);

        _rowset_writer->flush();
        RowsetSharedPtr src_rowset = _rowset_writer->build();
        ASSERT_TRUE(src_rowset != nullptr);
        ASSERT_EQ(src_rowset_id, src_rowset->rowset_id());
        ASSERT_EQ(1024, src_rowset->num_rows());

        tablet_meta->add_rs_meta(src_rowset->rowset_meta());
    }

    {
        RowsetId src_rowset_id;
        src_rowset_id.init(10002);
        rowset_writer_context.rowset_id = src_rowset_id;
        rowset_writer_context.version =
                Version(rowset_writer_context.version.second + 1, rowset_writer_context.version.second + 2);

        std::unique_ptr<RowsetWriter> _rowset_writer;
        ASSERT_TRUE(RowsetFactory::create_rowset_writer(rowset_writer_context, &_rowset_writer).ok());

        rowset_writer_add_rows(_rowset_writer);

        _rowset_writer->flush();
        RowsetSharedPtr src_rowset = _rowset_writer->build();
        ASSERT_TRUE(src_rowset != nullptr);
        ASSERT_EQ(src_rowset_id, src_rowset->rowset_id());
        ASSERT_EQ(1024, src_rowset->num_rows());

        tablet_meta->add_rs_meta(src_rowset->rowset_meta());
    }

    TabletSharedPtr tablet =
            Tablet::create_tablet_from_meta(_tablet_meta_mem_tracker.get(), tablet_meta,
                                            starrocks::ExecEnv::GetInstance()->storage_engine()->get_stores()[0]);
    tablet->init();
    tablet->calculate_cumulative_point();

    BaseCompaction base_compaction(_compaction_mem_tracker.get(), tablet);

    ASSERT_TRUE(base_compaction.compact().ok());
}

} // namespace starrocks::vectorized
