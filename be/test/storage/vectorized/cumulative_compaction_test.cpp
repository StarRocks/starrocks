// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/vectorized/cumulative_compaction.h"

#include <gtest/gtest.h>

#include "runtime/exec_env.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/storage_engine.h"
#include "storage/tablet_meta.h"
#include "storage/vectorized/chunk_helper.h"
#include "storage/vectorized/compaction.h"
#include "testutil/assert.h"
#include "util/file_utils.h"

namespace starrocks::vectorized {

static StorageEngine* k_engine = nullptr;

class CumulativeCompactionTest : public testing::Test {
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
        rowset_writer_context->version.second = 0;
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
        auto schema = vectorized::ChunkHelper::convert_schema_to_format_v2(*_tablet_schema);
        auto chunk = vectorized::ChunkHelper::new_chunk(schema, 1024);
        for (size_t i = 0; i < 1024; ++i) {
            test_data.push_back("well" + std::to_string(i));
            auto& cols = chunk->columns();
            cols[0]->append_datum(vectorized::Datum(static_cast<int32_t>(i)));
            Slice field_1(test_data[i]);
            cols[1]->append_datum(vectorized::Datum(field_1));
            cols[2]->append_datum(vectorized::Datum(static_cast<int32_t>(10000 + i)));
        }
        CHECK_OK(writer->add_chunk(*chunk));
    }

    void do_compaction() {
        config::storage_format_version = 2;
        create_tablet_schema(UNIQUE_KEYS);

        RowsetWriterContext rowset_writer_context(kDataFormatUnknown, config::storage_format_version);
        create_rowset_writer_context(&rowset_writer_context);
        std::unique_ptr<RowsetWriter> _rowset_writer;
        ASSERT_TRUE(RowsetFactory::create_rowset_writer(rowset_writer_context, &_rowset_writer).ok());

        rowset_writer_add_rows(_rowset_writer);

        _rowset_writer->flush();
        RowsetSharedPtr src_rowset = *_rowset_writer->build();
        ASSERT_TRUE(src_rowset != nullptr);
        RowsetId src_rowset_id;
        src_rowset_id.init(10000);
        ASSERT_EQ(src_rowset_id, src_rowset->rowset_id());
        ASSERT_EQ(1024, src_rowset->num_rows());

        TabletMetaSharedPtr tablet_meta = std::make_shared<TabletMeta>();
        create_tablet_meta(tablet_meta.get());
        tablet_meta->add_rs_meta(src_rowset->rowset_meta());

        {
            RowsetId src_rowset_id;
            src_rowset_id.init(10001);
            rowset_writer_context.rowset_id = src_rowset_id;
            rowset_writer_context.version =
                    Version(rowset_writer_context.version.second + 1, rowset_writer_context.version.second + 1);

            std::unique_ptr<RowsetWriter> _rowset_writer;
            ASSERT_TRUE(RowsetFactory::create_rowset_writer(rowset_writer_context, &_rowset_writer).ok());

            rowset_writer_add_rows(_rowset_writer);

            _rowset_writer->flush();
            RowsetSharedPtr src_rowset = *_rowset_writer->build();
            ASSERT_TRUE(src_rowset != nullptr);
            ASSERT_EQ(src_rowset_id, src_rowset->rowset_id());
            ASSERT_EQ(1024, src_rowset->num_rows());

            tablet_meta->add_rs_meta(src_rowset->rowset_meta());
        }

        TabletSharedPtr tablet = Tablet::create_tablet_from_meta(
                tablet_meta, starrocks::ExecEnv::GetInstance()->storage_engine()->get_stores()[0]);
        tablet->init();

        config::cumulative_compaction_skip_window_seconds = -2;

        CumulativeCompaction cumulative_compaction(_compaction_mem_tracker.get(), tablet);

        ASSERT_TRUE(cumulative_compaction.compact().ok());
    }

    void SetUp() override {
        config::min_cumulative_compaction_num_singleton_deltas = 2;
        config::max_compaction_concurrency = 1;
        Compaction::init(config::max_compaction_concurrency);

        config::storage_root_path = std::filesystem::current_path().string() + "/data_test_cumulative_compaction";
        FileUtils::remove_all(config::storage_root_path);
        ASSERT_TRUE(FileUtils::create_dir(config::storage_root_path).ok());
        std::vector<StorePath> paths;
        paths.emplace_back(config::storage_root_path);

        starrocks::EngineOptions options;
        options.store_paths = paths;
        options.compaction_mem_tracker = _compaction_mem_tracker.get();
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
};

TEST_F(CumulativeCompactionTest, test_init_succeeded) {
    TabletMetaSharedPtr tablet_meta(new TabletMeta());
    TabletSharedPtr tablet = Tablet::create_tablet_from_meta(tablet_meta, nullptr);
    CumulativeCompaction cumulative_compaction(_compaction_mem_tracker.get(), tablet);
    ASSERT_FALSE(cumulative_compaction.compact().ok());
}

TEST_F(CumulativeCompactionTest, test_candidate_rowsets_empty) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);

    auto schema = std::make_shared<const TabletSchema>(schema_pb);
    TabletMetaSharedPtr tablet_meta(new TabletMeta());
    tablet_meta->set_tablet_schema(schema);

    TabletSharedPtr tablet = Tablet::create_tablet_from_meta(tablet_meta, nullptr);
    tablet->init();
    CumulativeCompaction cumulative_compaction(_compaction_mem_tracker.get(), tablet);
    ASSERT_FALSE(cumulative_compaction.compact().ok());
}

TEST_F(CumulativeCompactionTest, test_horizontal_compact_succeed) {
    config::vertical_compaction_max_columns_per_group = 5;
    do_compaction();
}

TEST_F(CumulativeCompactionTest, test_vertical_compact_succeed) {
    config::vertical_compaction_max_columns_per_group = 1;
    do_compaction();
}

TEST_F(CumulativeCompactionTest, test_read_chunk_size) {
    // total row size is 0 in old segment
    int64_t mem_limit = 2147483648;
    int32_t config_chunk_size = 4096;
    int64_t total_num_rows = 10000;
    int64_t total_mem_footprint = 0;
    size_t source_num = 10;
    ASSERT_EQ(config_chunk_size, Compaction::get_read_chunk_size(mem_limit, config_chunk_size, total_num_rows,
                                                                 total_mem_footprint, source_num));

    // normal total memory footprint
    total_mem_footprint = 1073741824;
    ASSERT_EQ(2001, Compaction::get_read_chunk_size(mem_limit, config_chunk_size, total_num_rows, total_mem_footprint,
                                                    source_num));

    // mem limit is 0
    mem_limit = 0;
    ASSERT_EQ(config_chunk_size, Compaction::get_read_chunk_size(mem_limit, config_chunk_size, total_num_rows,
                                                                 total_mem_footprint, source_num));
}

} // namespace starrocks::vectorized