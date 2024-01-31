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

#include "storage/base_compaction.h"

#include <fmt/format.h>
#include <gtest/gtest.h>

#include "column/schema.h"
#include "fs/fs_util.h"
#include "runtime/exec_env.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "storage/chunk_helper.h"
#include "storage/compaction.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/storage_engine.h"
#include "storage/tablet_meta.h"
#include "testutil/assert.h"
#include "util/json.h"

namespace starrocks {

class BaseCompactionTest : public testing::Test {
public:
    ~BaseCompactionTest() override {
        if (_engine) {
            _engine->stop();
            delete _engine;
            _engine = nullptr;
        }
    }
    void create_rowset_writer_context(RowsetWriterContext* rowset_writer_context) {
        RowsetId rowset_id;
        rowset_id.init(10000);
        rowset_writer_context->rowset_id = rowset_id;
        rowset_writer_context->tablet_id = 12345;
        rowset_writer_context->tablet_schema_hash = 1111;
        rowset_writer_context->partition_id = 10;
        rowset_writer_context->rowset_path_prefix = config::storage_root_path + "/data/0/12345/1111";
        rowset_writer_context->rowset_state = VISIBLE;
        rowset_writer_context->tablet_schema = _tablet_schema;
        rowset_writer_context->version.first = 0;
        rowset_writer_context->version.second = 1;
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

        ColumnPB* column_4 = tablet_schema_pb.add_column();
        column_4->set_unique_id(3);
        column_4->set_name("j1");
        column_4->set_type("JSON");
        column_4->set_length(65535);
        column_4->set_is_key(false);
        column_4->set_is_nullable(true);
        column_4->set_is_bf_column(false);
        column_4->set_aggregation("REPLACE");

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
        auto chunk = ChunkHelper::new_chunk(schema, 1024);
        for (size_t i = 0; i < 1024; ++i) {
            test_data.push_back("well" + std::to_string(i));
            auto& cols = chunk->columns();
            cols[0]->append_datum(Datum(static_cast<int32_t>(i)));
            Slice field_1(test_data[i]);
            cols[1]->append_datum(Datum(field_1));
            cols[2]->append_datum(Datum(static_cast<int32_t>(10000 + i)));
            JsonValue json = JsonValue::from_string(R"({"k1":)" + std::to_string(i) + R"("k2": )" +
                                                    std::to_string(10000 + 1) + "}");
            cols[3]->append_datum(Datum(&json));
        }
        CHECK_OK(writer->add_chunk(*chunk));
    }

    void do_compaction() {
        create_tablet_schema(UNIQUE_KEYS);

        RowsetWriterContext rowset_writer_context;
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
            RowsetSharedPtr src_rowset = *_rowset_writer->build();
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
            RowsetSharedPtr src_rowset = *_rowset_writer->build();
            ASSERT_TRUE(src_rowset != nullptr);
            ASSERT_EQ(src_rowset_id, src_rowset->rowset_id());
            ASSERT_EQ(1024, src_rowset->num_rows());

            tablet_meta->add_rs_meta(src_rowset->rowset_meta());
        }

        TabletSharedPtr tablet =
                Tablet::create_tablet_from_meta(tablet_meta, starrocks::StorageEngine::instance()->get_stores()[0]);
        ASSERT_OK(tablet->init());
        tablet->calculate_cumulative_point();

        BaseCompaction base_compaction(_compaction_mem_tracker.get(), tablet);

        ASSERT_TRUE(base_compaction.compact().ok());
    }

    void SetUp() override {
        config::max_compaction_concurrency = 1;
        Compaction::init(config::max_compaction_concurrency);
        config::enable_event_based_compaction_framework = false;

        _default_storage_root_path = config::storage_root_path;
        config::storage_root_path = std::filesystem::current_path().string() + "/data_test_base_compaction";
        fs::remove_all(config::storage_root_path);
        ASSERT_TRUE(fs::create_directories(config::storage_root_path).ok());
        std::vector<StorePath> paths;
        paths.emplace_back(config::storage_root_path);

        starrocks::EngineOptions options;
        options.store_paths = paths;
        if (_engine == nullptr) {
            _origin_engine = StorageEngine::instance();
            Status s = starrocks::StorageEngine::open(options, &_engine);
            ASSERT_TRUE(s.ok()) << s.to_string();
        }

        _schema_hash_path = fmt::format("{}/data/0/12345/1111", config::storage_root_path);
        ASSERT_OK(fs::create_directories(_schema_hash_path));

        _mem_pool = std::make_unique<MemPool>();

        _compaction_mem_tracker = std::make_unique<MemTracker>(-1);
        _metadata_mem_tracker = std::make_unique<MemTracker>();
    }

    void TearDown() override {
        if (fs::path_exist(config::storage_root_path)) {
            ASSERT_TRUE(fs::remove_all(config::storage_root_path).ok());
        }
        config::storage_root_path = _default_storage_root_path;
    }

protected:
    StorageEngine* _engine = nullptr;
    StorageEngine* _origin_engine = nullptr;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::string _schema_hash_path;
    std::unique_ptr<MemTracker> _compaction_mem_tracker;
    std::unique_ptr<MemPool> _mem_pool;
    std::unique_ptr<MemTracker> _metadata_mem_tracker;
    std::string _default_storage_root_path;
};

TEST_F(BaseCompactionTest, test_init_succeeded) {
    TabletMetaSharedPtr tablet_meta(new TabletMeta());
    TabletSharedPtr tablet = Tablet::create_tablet_from_meta(tablet_meta, nullptr);
    BaseCompaction base_compaction(_compaction_mem_tracker.get(), tablet);
    ASSERT_FALSE(base_compaction.compact().ok());
}

TEST_F(BaseCompactionTest, test_input_rowsets_LE_1) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    auto schema = std::make_shared<const TabletSchema>(schema_pb);
    TabletMetaSharedPtr tablet_meta(new TabletMeta());
    tablet_meta->set_tablet_schema(schema);

    TabletSharedPtr tablet = Tablet::create_tablet_from_meta(tablet_meta, nullptr);
    ASSERT_OK(tablet->init());
    BaseCompaction base_compaction(_compaction_mem_tracker.get(), tablet);
    ASSERT_FALSE(base_compaction.compact().ok());
}

TEST_F(BaseCompactionTest, test_input_rowsets_EQ_2) {
    create_tablet_schema(UNIQUE_KEYS);
    RowsetWriterContext rowset_writer_context;
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
        RowsetSharedPtr src_rowset = *_rowset_writer->build();
        ASSERT_TRUE(src_rowset != nullptr);
        ASSERT_EQ(src_rowset_id, src_rowset->rowset_id());
        ASSERT_EQ(1024, src_rowset->num_rows());

        tablet_meta->add_rs_meta(src_rowset->rowset_meta());
    }

    TabletSharedPtr tablet = Tablet::create_tablet_from_meta(tablet_meta, nullptr);
    ASSERT_OK(tablet->init());
    tablet->calculate_cumulative_point();

    BaseCompaction base_compaction(_compaction_mem_tracker.get(), tablet);

    ASSERT_FALSE(base_compaction.compact().ok());
}

TEST_F(BaseCompactionTest, test_horizontal_compact_succeed) {
    config::vertical_compaction_max_columns_per_group = 5;
    do_compaction();
}

TEST_F(BaseCompactionTest, test_vertical_compact_succeed) {
    config::vertical_compaction_max_columns_per_group = 1;
    do_compaction();
}

} // namespace starrocks
