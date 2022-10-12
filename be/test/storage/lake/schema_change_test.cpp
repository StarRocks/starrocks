// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/lake/schema_change.h"

#include <gtest/gtest.h>

#include "column/datum_tuple.h"
#include "column/fixed_length_column.h"
#include "fs/fs_util.h"
#include "runtime/exec_env.h"
#include "storage/chunk_helper.h"
#include "storage/lake/delta_writer.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_reader.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"

namespace starrocks::lake {

using namespace starrocks::vectorized;

using VSchema = starrocks::vectorized::Schema;
using VChunk = starrocks::vectorized::Chunk;

class LinkedSchemaChangeTest : public testing::Test {
public:
    LinkedSchemaChangeTest() {
        _mem_tracker = std::make_unique<MemTracker>(-1);
        _tablet_manager = ExecEnv::GetInstance()->lake_tablet_manager();
        _location_provider = std::make_unique<FixedLocationProvider>(kTestGroupPath);
        _backup_location_provider = _tablet_manager->TEST_set_location_provider(_location_provider.get());

        // base tablet
        _base_tablet_metadata = std::make_shared<TabletMetadata>();
        _base_tablet_metadata->set_id(next_id());
        _base_tablet_metadata->set_version(1);
        //
        //  | column | type | KEY | NULL |
        //  +--------+------+-----+------+
        //  |   c0   |  INT | YES |  NO  |
        //  |   c1   |  INT | NO  |  NO  |
        auto base_schema = _base_tablet_metadata->mutable_schema();
        base_schema->set_id(next_id());
        base_schema->set_num_short_key_columns(1);
        base_schema->set_keys_type(DUP_KEYS);
        base_schema->set_num_rows_per_row_block(65535);
        auto c0_id = next_id();
        auto c1_id = next_id();
        {
            auto c0 = base_schema->add_column();
            c0->set_unique_id(c0_id);
            c0->set_name("c0");
            c0->set_type("INT");
            c0->set_is_key(true);
            c0->set_is_nullable(false);
        }
        {
            auto c1 = base_schema->add_column();
            c1->set_unique_id(c1_id);
            c1->set_name("c1");
            c1->set_type("INT");
            c1->set_is_key(false);
            c1->set_is_nullable(false);
        }

        _base_tablet_schema = TabletSchema::create(*base_schema);
        _base_schema = std::make_shared<VSchema>(ChunkHelper::convert_schema(*_base_tablet_schema));

        // new tablet
        _new_tablet_metadata = std::make_shared<TabletMetadata>();
        _new_tablet_metadata->set_id(next_id());
        _new_tablet_metadata->set_version(1);
        //
        //  | column | type | KEY | NULL | DEFAULT |
        //  +--------+------+-----+------+---------+
        //  |   c0   |  INT | YES |  NO  |         |
        //  |   c1   |  INT | NO  |  NO  |         |
        //  |   c2   |  INT | NO  |  NO  |  10     |
        auto new_schema = _new_tablet_metadata->mutable_schema();
        new_schema->set_id(next_id());
        new_schema->set_num_short_key_columns(1);
        new_schema->set_keys_type(DUP_KEYS);
        new_schema->set_num_rows_per_row_block(65535);
        // c0 c1 id should be same as base tablet schema
        {
            auto c0 = new_schema->add_column();
            c0->set_unique_id(c0_id);
            c0->set_name("c0");
            c0->set_type("INT");
            c0->set_is_key(true);
            c0->set_is_nullable(false);
        }
        {
            auto c1 = new_schema->add_column();
            c1->set_unique_id(c1_id);
            c1->set_name("c1");
            c1->set_type("INT");
            c1->set_is_key(false);
            c1->set_is_nullable(false);
        }
        {
            auto c2 = new_schema->add_column();
            c2->set_unique_id(next_id());
            c2->set_name("c2");
            c2->set_type("INT");
            c2->set_is_key(false);
            c2->set_is_nullable(false);
            c2->set_default_value("10");
        }

        _new_tablet_schema = TabletSchema::create(*new_schema);
        _new_schema = std::make_shared<VSchema>(ChunkHelper::convert_schema(*_new_tablet_schema));
    }

protected:
    constexpr static const char* const kTestGroupPath = "test_lake_linked_schema_change";

    void SetUp() override {
        (void)ExecEnv::GetInstance()->lake_tablet_manager()->TEST_set_location_provider(_location_provider.get());
        (void)fs::remove_all(kTestGroupPath);
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kSegmentDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kMetadataDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kTxnLogDirectoryName)));
        CHECK_OK(_tablet_manager->put_tablet_metadata(*_base_tablet_metadata));
        CHECK_OK(_tablet_manager->put_tablet_metadata(*_new_tablet_metadata));
    }

    void TearDown() override {
        (void)ExecEnv::GetInstance()->lake_tablet_manager()->TEST_set_location_provider(_backup_location_provider);
        (void)fs::remove_all(kTestGroupPath);
    }

    std::unique_ptr<MemTracker> _mem_tracker;
    TabletManager* _tablet_manager;
    std::unique_ptr<FixedLocationProvider> _location_provider;
    LocationProvider* _backup_location_provider;

    std::shared_ptr<TabletMetadata> _base_tablet_metadata;
    std::shared_ptr<TabletSchema> _base_tablet_schema;
    std::shared_ptr<VSchema> _base_schema;

    std::shared_ptr<TabletMetadata> _new_tablet_metadata;
    std::shared_ptr<TabletSchema> _new_tablet_schema;
    std::shared_ptr<VSchema> _new_schema;

    int64_t _partition_id = 100;
};

TEST_F(LinkedSchemaChangeTest, test_add_column) {
    std::vector<int> k0{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
    std::vector<int> v0{2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24};

    std::vector<int> k1{30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41};
    std::vector<int> v1{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};

    auto c0 = Int32Column::create();
    auto c1 = Int32Column::create();
    auto c2 = Int32Column::create();
    auto c3 = Int32Column::create();
    c0->append_numbers(k0.data(), k0.size() * sizeof(int));
    c1->append_numbers(v0.data(), v0.size() * sizeof(int));
    c2->append_numbers(k1.data(), k1.size() * sizeof(int));
    c3->append_numbers(v1.data(), v1.size() * sizeof(int));

    VChunk chunk0({c0, c1}, _base_schema);
    VChunk chunk1({c2, c3}, _base_schema);

    auto indexes = std::vector<uint32_t>(k0.size());
    for (int i = 0; i < k0.size(); i++) {
        indexes[i] = i;
    }

    // write 2 rowsets
    int64_t version = 1;
    int64_t txn_id = 1000;
    auto base_tablet_id = _base_tablet_metadata->id();
    {
        auto delta_writer = DeltaWriter::create(base_tablet_id, txn_id, _partition_id, nullptr, _mem_tracker.get());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish());
        delta_writer->close();
        ASSERT_OK(_tablet_manager->publish_version(base_tablet_id, version, version + 1, &txn_id, 1));
        version++;
        txn_id++;
    }
    {
        auto delta_writer = DeltaWriter::create(base_tablet_id, txn_id, _partition_id, nullptr, _mem_tracker.get());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk1, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish());
        delta_writer->close();
        ASSERT_OK(_tablet_manager->publish_version(base_tablet_id, version, version + 1, &txn_id, 1));
        version++;
        txn_id++;
    }

    // do schema change
    auto new_tablet_id = _new_tablet_metadata->id();

    TAlterTabletReqV2 request;
    request.base_tablet_id = base_tablet_id;
    request.new_tablet_id = new_tablet_id;
    request.alter_version = version;
    request.txn_id = txn_id;

    SchemaChangeHandler handler;
    ASSERT_OK(handler.process_alter_tablet(request));
    ASSERT_OK(_tablet_manager->publish_version(new_tablet_id, 1, version + 1, &txn_id, 1));
    version++;
    txn_id++;

    // check new tablet data
    ASSIGN_OR_ABORT(auto new_tablet, _tablet_manager->get_tablet(new_tablet_id));
    ASSIGN_OR_ABORT(auto reader, new_tablet.new_reader(version, *_new_schema));
    CHECK_OK(reader->prepare());
    CHECK_OK(reader->open(TabletReaderParams()));

    auto chunk = ChunkHelper::new_chunk(*_new_schema, 1024);

    CHECK_OK(reader->get_next(chunk.get()));
    for (int i = 0, sz = k0.size(); i < sz; i++) {
        EXPECT_EQ(k0[i], chunk->get(i)[0].get_int32());
        EXPECT_EQ(v0[i], chunk->get(i)[1].get_int32());
        EXPECT_EQ(10, chunk->get(i)[2].get_int32());
    }
    chunk->reset();

    CHECK_OK(reader->get_next(chunk.get()));
    for (int i = 0, sz = k1.size(); i < sz; i++) {
        EXPECT_EQ(k1[i], chunk->get(i)[0].get_int32());
        EXPECT_EQ(v1[i], chunk->get(i)[1].get_int32());
        EXPECT_EQ(10, chunk->get(i)[2].get_int32());
    }
    chunk->reset();

    auto st = reader->get_next(chunk.get());
    ASSERT_TRUE(st.is_end_of_file());
}

class DirectSchemaChangeTest : public testing::Test {
public:
    DirectSchemaChangeTest() {
        _mem_tracker = std::make_unique<MemTracker>(-1);
        _tablet_manager = ExecEnv::GetInstance()->lake_tablet_manager();
        _location_provider = std::make_unique<FixedLocationProvider>(kTestGroupPath);
        _backup_location_provider = _tablet_manager->TEST_set_location_provider(_location_provider.get());

        // base tablet
        _base_tablet_metadata = std::make_shared<TabletMetadata>();
        _base_tablet_metadata->set_id(next_id());
        _base_tablet_metadata->set_version(1);
        //
        //  | column | type | KEY | NULL |
        //  +--------+------+-----+------+
        //  |   c0   |  INT | YES |  NO  |
        //  |   c1   |  INT | NO  |  NO  |
        auto base_schema = _base_tablet_metadata->mutable_schema();
        base_schema->set_id(next_id());
        base_schema->set_num_short_key_columns(1);
        base_schema->set_keys_type(DUP_KEYS);
        base_schema->set_num_rows_per_row_block(65535);
        auto c0_id = next_id();
        auto c1_id = next_id();
        {
            auto c0 = base_schema->add_column();
            c0->set_unique_id(c0_id);
            c0->set_name("c0");
            c0->set_type("INT");
            c0->set_is_key(true);
            c0->set_is_nullable(false);
        }
        {
            auto c1 = base_schema->add_column();
            c1->set_unique_id(c1_id);
            c1->set_name("c1");
            c1->set_type("INT");
            c1->set_is_key(false);
            c1->set_is_nullable(false);
        }

        _base_tablet_schema = TabletSchema::create(*base_schema);
        _base_schema = std::make_shared<VSchema>(ChunkHelper::convert_schema(*_base_tablet_schema));

        // new tablet
        _new_tablet_metadata = std::make_shared<TabletMetadata>();
        _new_tablet_metadata->set_id(next_id());
        _new_tablet_metadata->set_version(1);
        //
        //  | column | type | KEY | NULL |
        //  +--------+------+-----+------+
        //  |   c0   |  INT | YES |  NO  |
        //  |   c1   |BIGINT| NO  |  NO  |
        auto new_schema = _new_tablet_metadata->mutable_schema();
        new_schema->set_id(next_id());
        new_schema->set_num_short_key_columns(1);
        new_schema->set_keys_type(DUP_KEYS);
        new_schema->set_num_rows_per_row_block(65535);
        // c0 c1 id should be same as base tablet schema
        {
            auto c0 = new_schema->add_column();
            c0->set_unique_id(c0_id);
            c0->set_name("c0");
            c0->set_type("INT");
            c0->set_is_key(true);
            c0->set_is_nullable(false);
        }
        {
            auto c1 = new_schema->add_column();
            c1->set_unique_id(c1_id);
            c1->set_name("c1");
            c1->set_type("BIGINT");
            c1->set_is_key(false);
            c1->set_is_nullable(false);
        }

        _new_tablet_schema = TabletSchema::create(*new_schema);
        _new_schema = std::make_shared<VSchema>(ChunkHelper::convert_schema(*_new_tablet_schema));
    }

protected:
    constexpr static const char* const kTestGroupPath = "test_lake_direct_schema_change";

    void SetUp() override {
        (void)ExecEnv::GetInstance()->lake_tablet_manager()->TEST_set_location_provider(_location_provider.get());
        (void)fs::remove_all(kTestGroupPath);
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kSegmentDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kMetadataDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kTxnLogDirectoryName)));
        CHECK_OK(_tablet_manager->put_tablet_metadata(*_base_tablet_metadata));
        CHECK_OK(_tablet_manager->put_tablet_metadata(*_new_tablet_metadata));
    }

    void TearDown() override {
        (void)ExecEnv::GetInstance()->lake_tablet_manager()->TEST_set_location_provider(_backup_location_provider);
        (void)fs::remove_all(kTestGroupPath);
    }

    std::unique_ptr<MemTracker> _mem_tracker;
    TabletManager* _tablet_manager;
    std::unique_ptr<FixedLocationProvider> _location_provider;
    LocationProvider* _backup_location_provider;

    std::shared_ptr<TabletMetadata> _base_tablet_metadata;
    std::shared_ptr<TabletSchema> _base_tablet_schema;
    std::shared_ptr<VSchema> _base_schema;

    std::shared_ptr<TabletMetadata> _new_tablet_metadata;
    std::shared_ptr<TabletSchema> _new_tablet_schema;
    std::shared_ptr<VSchema> _new_schema;

    int64_t _partition_id = 100;
};

TEST_F(DirectSchemaChangeTest, test_alter_column_type) {
    std::vector<int> k0{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
    std::vector<int> v0{2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24};

    std::vector<int> k1{30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41};
    std::vector<int> v1{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};

    auto c0 = Int32Column::create();
    auto c1 = Int32Column::create();
    auto c2 = Int32Column::create();
    auto c3 = Int32Column::create();
    c0->append_numbers(k0.data(), k0.size() * sizeof(int));
    c1->append_numbers(v0.data(), v0.size() * sizeof(int));
    c2->append_numbers(k1.data(), k1.size() * sizeof(int));
    c3->append_numbers(v1.data(), v1.size() * sizeof(int));

    VChunk chunk0({c0, c1}, _base_schema);
    VChunk chunk1({c2, c3}, _base_schema);

    auto indexes = std::vector<uint32_t>(k0.size());
    for (int i = 0; i < k0.size(); i++) {
        indexes[i] = i;
    }

    // write 2 rowsets
    int64_t version = 1;
    int64_t txn_id = 1000;
    auto base_tablet_id = _base_tablet_metadata->id();
    {
        auto delta_writer = DeltaWriter::create(base_tablet_id, txn_id, _partition_id, nullptr, _mem_tracker.get());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish());
        delta_writer->close();
        ASSERT_OK(_tablet_manager->publish_version(base_tablet_id, version, version + 1, &txn_id, 1));
        version++;
        txn_id++;
    }
    {
        auto delta_writer = DeltaWriter::create(base_tablet_id, txn_id, _partition_id, nullptr, _mem_tracker.get());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk1, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish());
        delta_writer->close();
        ASSERT_OK(_tablet_manager->publish_version(base_tablet_id, version, version + 1, &txn_id, 1));
        version++;
        txn_id++;
    }

    // do schema change
    auto new_tablet_id = _new_tablet_metadata->id();

    TAlterTabletReqV2 request;
    request.base_tablet_id = base_tablet_id;
    request.new_tablet_id = new_tablet_id;
    request.alter_version = version;
    request.txn_id = txn_id;

    SchemaChangeHandler handler;
    ASSERT_OK(handler.process_alter_tablet(request));
    ASSERT_OK(_tablet_manager->publish_version(new_tablet_id, 1, version + 1, &txn_id, 1));
    version++;
    txn_id++;

    // check new tablet data
    ASSIGN_OR_ABORT(auto new_tablet, _tablet_manager->get_tablet(new_tablet_id));
    ASSIGN_OR_ABORT(auto reader, new_tablet.new_reader(version, *_new_schema));
    CHECK_OK(reader->prepare());
    CHECK_OK(reader->open(TabletReaderParams()));

    auto chunk = ChunkHelper::new_chunk(*_new_schema, 1024);

    CHECK_OK(reader->get_next(chunk.get()));
    for (int i = 0, sz = k0.size(); i < sz; i++) {
        EXPECT_EQ(k0[i], chunk->get(i)[0].get_int32());
        EXPECT_EQ(v0[i], chunk->get(i)[1].get_int64());
    }
    chunk->reset();

    CHECK_OK(reader->get_next(chunk.get()));
    for (int i = 0, sz = k1.size(); i < sz; i++) {
        EXPECT_EQ(k1[i], chunk->get(i)[0].get_int32());
        EXPECT_EQ(v1[i], chunk->get(i)[1].get_int64());
    }
    chunk->reset();

    auto st = reader->get_next(chunk.get());
    ASSERT_TRUE(st.is_end_of_file());
}

class SortedSchemaChangeTest : public testing::Test {
public:
    SortedSchemaChangeTest() {
        _mem_tracker = std::make_unique<MemTracker>(-1);
        _tablet_manager = ExecEnv::GetInstance()->lake_tablet_manager();
        _location_provider = std::make_unique<FixedLocationProvider>(kTestGroupPath);
        _backup_location_provider = _tablet_manager->TEST_set_location_provider(_location_provider.get());

        // base tablet
        _base_tablet_metadata = std::make_shared<TabletMetadata>();
        _base_tablet_metadata->set_id(next_id());
        _base_tablet_metadata->set_version(1);
        //
        //  | column | type | KEY | NULL |
        //  +--------+------+-----+------+
        //  |   c0   |  INT | YES |  NO  |
        //  |   c1   |  INT | YES |  NO  |
        //  |   c2   |  INT | NO  |  NO  |
        auto base_schema = _base_tablet_metadata->mutable_schema();
        base_schema->set_id(next_id());
        base_schema->set_num_short_key_columns(1);
        base_schema->set_keys_type(DUP_KEYS);
        base_schema->set_num_rows_per_row_block(65535);
        auto c0_id = next_id();
        auto c1_id = next_id();
        auto c2_id = next_id();
        {
            auto c0 = base_schema->add_column();
            c0->set_unique_id(c0_id);
            c0->set_name("c0");
            c0->set_type("INT");
            c0->set_is_key(true);
            c0->set_is_nullable(false);
        }
        {
            auto c1 = base_schema->add_column();
            c1->set_unique_id(c1_id);
            c1->set_name("c1");
            c1->set_type("INT");
            c1->set_is_key(true);
            c1->set_is_nullable(false);
        }
        {
            auto c2 = base_schema->add_column();
            c2->set_unique_id(c2_id);
            c2->set_name("c2");
            c2->set_type("INT");
            c2->set_is_key(false);
            c2->set_is_nullable(false);
        }

        _base_tablet_schema = TabletSchema::create(*base_schema);
        _base_schema = std::make_shared<VSchema>(ChunkHelper::convert_schema(*_base_tablet_schema));

        // new tablet
        _new_tablet_metadata = std::make_shared<TabletMetadata>();
        _new_tablet_metadata->set_id(next_id());
        _new_tablet_metadata->set_version(1);
        //
        //  | column | type | KEY | NULL |
        //  +--------+------+-----+------+
        //  |   c1   |  INT | YES |  NO  |
        //  |   c0   |  INT | YES |  NO  |
        //  |   c2   |  INT | NO  |  NO  |
        auto new_schema = _new_tablet_metadata->mutable_schema();
        new_schema->set_id(next_id());
        new_schema->set_num_short_key_columns(1);
        new_schema->set_keys_type(DUP_KEYS);
        new_schema->set_num_rows_per_row_block(65535);
        // c0 c1 c2 id should be same as base tablet schema
        {
            auto c1 = new_schema->add_column();
            c1->set_unique_id(c1_id);
            c1->set_name("c1");
            c1->set_type("INT");
            c1->set_is_key(false);
            c1->set_is_nullable(false);
        }
        {
            auto c0 = new_schema->add_column();
            c0->set_unique_id(c0_id);
            c0->set_name("c0");
            c0->set_type("INT");
            c0->set_is_key(true);
            c0->set_is_nullable(false);
        }
        {
            auto c2 = new_schema->add_column();
            c2->set_unique_id(next_id());
            c2->set_name("c2");
            c2->set_type("INT");
            c2->set_is_key(false);
            c2->set_is_nullable(false);
            c2->set_default_value("10");
        }

        _new_tablet_schema = TabletSchema::create(*new_schema);
        _new_schema = std::make_shared<VSchema>(ChunkHelper::convert_schema(*_new_tablet_schema));
    }

protected:
    constexpr static const char* const kTestGroupPath = "test_lake_sorted_schema_change";

    void SetUp() override {
        (void)ExecEnv::GetInstance()->lake_tablet_manager()->TEST_set_location_provider(_location_provider.get());
        (void)fs::remove_all(kTestGroupPath);
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kSegmentDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kMetadataDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kTxnLogDirectoryName)));
        CHECK_OK(_tablet_manager->put_tablet_metadata(*_base_tablet_metadata));
        CHECK_OK(_tablet_manager->put_tablet_metadata(*_new_tablet_metadata));
    }

    void TearDown() override {
        (void)ExecEnv::GetInstance()->lake_tablet_manager()->TEST_set_location_provider(_backup_location_provider);
        (void)fs::remove_all(kTestGroupPath);
    }

    std::unique_ptr<MemTracker> _mem_tracker;
    TabletManager* _tablet_manager;
    std::unique_ptr<FixedLocationProvider> _location_provider;
    LocationProvider* _backup_location_provider;

    std::shared_ptr<TabletMetadata> _base_tablet_metadata;
    std::shared_ptr<TabletSchema> _base_tablet_schema;
    std::shared_ptr<VSchema> _base_schema;

    std::shared_ptr<TabletMetadata> _new_tablet_metadata;
    std::shared_ptr<TabletSchema> _new_tablet_schema;
    std::shared_ptr<VSchema> _new_schema;

    int64_t _partition_id = 100;
};

TEST_F(SortedSchemaChangeTest, test_alter_key_order) {
    std::vector<int> k0{1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4};
    std::vector<int> k1{1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3};
    std::vector<int> v0{2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24};

    std::vector<int> k2{10, 10, 10, 11, 11, 11, 12, 12, 12, 13, 13, 13};
    std::vector<int> k3{1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3};
    std::vector<int> v1{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};

    auto ck0 = Int32Column::create();
    auto ck1 = Int32Column::create();
    auto cv0 = Int32Column::create();
    auto ck2 = Int32Column::create();
    auto ck3 = Int32Column::create();
    auto cv1 = Int32Column::create();
    ck0->append_numbers(k0.data(), k0.size() * sizeof(int));
    ck1->append_numbers(k1.data(), k1.size() * sizeof(int));
    cv0->append_numbers(v0.data(), v0.size() * sizeof(int));
    ck2->append_numbers(k2.data(), k2.size() * sizeof(int));
    ck3->append_numbers(k3.data(), k3.size() * sizeof(int));
    cv1->append_numbers(v1.data(), v1.size() * sizeof(int));

    VChunk chunk0({ck0, ck1, cv0}, _base_schema);
    VChunk chunk1({ck2, ck3, cv1}, _base_schema);

    auto indexes = std::vector<uint32_t>(k0.size());
    for (int i = 0; i < k0.size(); i++) {
        indexes[i] = i;
    }

    // write 2 rowsets
    int64_t version = 1;
    int64_t txn_id = 1000;
    auto base_tablet_id = _base_tablet_metadata->id();
    {
        auto delta_writer = DeltaWriter::create(base_tablet_id, txn_id, _partition_id, nullptr, _mem_tracker.get());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish());
        delta_writer->close();
        ASSERT_OK(_tablet_manager->publish_version(base_tablet_id, version, version + 1, &txn_id, 1));
        version++;
        txn_id++;
    }
    {
        auto delta_writer = DeltaWriter::create(base_tablet_id, txn_id, _partition_id, nullptr, _mem_tracker.get());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk1, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish());
        delta_writer->close();
        ASSERT_OK(_tablet_manager->publish_version(base_tablet_id, version, version + 1, &txn_id, 1));
        version++;
        txn_id++;
    }

    // do schema change
    auto new_tablet_id = _new_tablet_metadata->id();

    TAlterTabletReqV2 request;
    request.base_tablet_id = base_tablet_id;
    request.new_tablet_id = new_tablet_id;
    request.alter_version = version;
    request.txn_id = txn_id;

    SchemaChangeHandler handler;
    ASSERT_OK(handler.process_alter_tablet(request));
    ASSERT_OK(_tablet_manager->publish_version(new_tablet_id, 1, version + 1, &txn_id, 1));
    version++;
    txn_id++;

    // check new tablet data
    std::vector<int> rc1_0{1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3};
    std::vector<int> rc0_0{1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4};
    std::vector<int> rc2_0{2, 8, 14, 20, 4, 10, 16, 22, 6, 12, 18, 24};

    std::vector<int> rc1_1{1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3};
    std::vector<int> rc0_1{10, 11, 12, 13, 10, 11, 12, 13, 10, 11, 12, 13};
    std::vector<int> rc2_1{0, 3, 6, 9, 1, 4, 7, 10, 2, 5, 8, 11};

    ASSIGN_OR_ABORT(auto new_tablet, _tablet_manager->get_tablet(new_tablet_id));
    ASSIGN_OR_ABORT(auto reader, new_tablet.new_reader(version, *_new_schema));
    CHECK_OK(reader->prepare());
    CHECK_OK(reader->open(TabletReaderParams()));

    auto chunk = ChunkHelper::new_chunk(*_new_schema, 1024);

    CHECK_OK(reader->get_next(chunk.get()));
    for (int i = 0, sz = k0.size(); i < sz; i++) {
        EXPECT_EQ(rc1_0[i], chunk->get(i)[0].get_int32());
        EXPECT_EQ(rc0_0[i], chunk->get(i)[1].get_int32());
        EXPECT_EQ(rc2_0[i], chunk->get(i)[2].get_int32());
    }
    chunk->reset();

    CHECK_OK(reader->get_next(chunk.get()));
    for (int i = 0, sz = k0.size(); i < sz; i++) {
        EXPECT_EQ(rc1_1[i], chunk->get(i)[0].get_int32());
        EXPECT_EQ(rc0_1[i], chunk->get(i)[1].get_int32());
        EXPECT_EQ(rc2_1[i], chunk->get(i)[2].get_int32());
    }
    chunk->reset();

    auto st = reader->get_next(chunk.get());
    ASSERT_TRUE(st.is_end_of_file());
}

} // namespace starrocks::lake
