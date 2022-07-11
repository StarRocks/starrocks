// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/lake/horizontal_compaction_task.h"

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <random>

#include "column/chunk.h"
#include "column/datum_tuple.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "column/vectorized_fwd.h"
#include "common/logging.h"
#include "fs/fs_util.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "storage/chunk_helper.h"
#include "storage/lake/delta_writer.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/test_group_assigner.h"
#include "storage/lake/txn_log.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"

namespace starrocks::lake {

using namespace starrocks::vectorized;

using VSchema = starrocks::vectorized::Schema;
using VChunk = starrocks::vectorized::Chunk;

class DuplicateKeyHorizontalCompactionTest : public testing::Test {
public:
    DuplicateKeyHorizontalCompactionTest() {
        _tablet_manager = ExecEnv::GetInstance()->lake_tablet_manager();

        _parent_mem_tracker = std::make_unique<MemTracker>(-1);
        _mem_tracker = std::make_unique<MemTracker>(-1, "", _parent_mem_tracker.get());
        _group_assigner = std::make_unique<TestGroupAssigner>(kTestGroupPath);
        _backup_group_assigner = _tablet_manager->TEST_set_group_assigner(_group_assigner.get());

        _tablet_metadata = std::make_shared<TabletMetadata>();
        _tablet_metadata->set_id(next_id());
        _tablet_metadata->set_version(1);
        //
        //  | column | type | KEY | NULL |
        //  +--------+------+-----+------+
        //  |   c0   |  INT | YES |  NO  |
        //  |   c1   |  INT | NO  |  NO  |
        auto schema = _tablet_metadata->mutable_schema();
        schema->set_id(next_id());
        schema->set_num_short_key_columns(1);
        schema->set_keys_type(DUP_KEYS);
        schema->set_num_rows_per_row_block(65535);
        schema->set_compress_kind(COMPRESS_LZ4);
        auto c0 = schema->add_column();
        {
            c0->set_unique_id(next_id());
            c0->set_name("c0");
            c0->set_type("INT");
            c0->set_is_key(true);
            c0->set_is_nullable(false);
        }
        auto c1 = schema->add_column();
        {
            c1->set_unique_id(next_id());
            c1->set_name("c1");
            c1->set_type("INT");
            c1->set_is_key(false);
            c1->set_is_nullable(false);
        }

        _tablet_schema = TabletSchema::create(_mem_tracker.get(), *schema);
        _schema = std::make_shared<VSchema>(ChunkHelper::convert_schema(*_tablet_schema));
    }

protected:
    constexpr static const char* const kTestGroupPath = "test_lake_hcompaction_task";
    constexpr static const int kChunkSize = 12;

    void SetUp() override {
        (void)ExecEnv::GetInstance()->lake_tablet_manager()->TEST_set_group_assigner(_group_assigner.get());
        (void)fs::remove_all(kTestGroupPath);
        CHECK_OK(fs::create_directories(kTestGroupPath));
        CHECK_OK(_tablet_manager->put_tablet_metadata(kTestGroupPath, *_tablet_metadata));
    }

    void TearDown() override {
        ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(_tablet_metadata->id()));
        tablet.delete_txn_log(_txn_id);
        _txn_id++;
        (void)ExecEnv::GetInstance()->lake_tablet_manager()->TEST_set_group_assigner(_backup_group_assigner);
        (void)fs::remove_all(kTestGroupPath);
    }

    VChunk generate_data(int64_t chunk_size) {
        std::vector<int> v0(chunk_size);
        std::vector<int> v1(chunk_size);
        for (int i = 0; i < chunk_size; i++) {
            v0[i] = i;
        }
        auto rng = std::default_random_engine{};
        std::shuffle(v0.begin(), v0.end(), rng);
        for (int i = 0; i < chunk_size; i++) {
            v1[i] = v0[i] * 3;
        }

        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        c0->append_numbers(v0.data(), v0.size() * sizeof(int));
        c1->append_numbers(v1.data(), v1.size() * sizeof(int));
        return VChunk({c0, c1}, _schema);
    }

    int64_t read(int64_t version) {
        ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(_tablet_metadata->id()));
        ASSIGN_OR_ABORT(auto reader, tablet.new_reader(version, *_schema));
        CHECK_OK(reader->prepare());
        CHECK_OK(reader->open(TabletReaderParams()));
        auto chunk = ChunkHelper::new_chunk(*_schema, 128);
        int64_t ret = 0;
        while (true) {
            auto st = reader->get_next(chunk.get());
            if (st.is_end_of_file()) {
                break;
            }
            CHECK_OK(st);
            ret += chunk->num_rows();
            chunk->reset();
        }
        return ret;
    }

    TabletManager* _tablet_manager;
    std::unique_ptr<MemTracker> _parent_mem_tracker;
    std::unique_ptr<MemTracker> _mem_tracker;
    std::unique_ptr<TestGroupAssigner> _group_assigner;
    GroupAssigner* _backup_group_assigner;
    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<VSchema> _schema;
    int64_t _partition_id = 4560;
    int64_t _txn_id = 1230;
};

TEST_F(DuplicateKeyHorizontalCompactionTest, test1) {
    // Prepare data for writing
    auto chunk0 = generate_data(kChunkSize);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    for (int i = 0; i < 3; i++) {
        _txn_id++;
        auto delta_writer = DeltaWriter::create(tablet_id, _txn_id, _partition_id, nullptr, _mem_tracker.get());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish());
        delta_writer->close();
        // Publish version
        ASSERT_OK(_tablet_manager->publish_version(tablet_id, version, version + 1, &_txn_id, 1));
        version++;
    }
    ASSERT_EQ(kChunkSize * 3, read(version));

    ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(tablet_id));
    ASSIGN_OR_ABORT(auto t, _tablet_manager->get_tablet(tablet_id));
    _txn_id++;

    ASSIGN_OR_ABORT(auto task, _tablet_manager->compact(_tablet_metadata->id(), version, _txn_id));
    ASSERT_OK(task->execute());
    ASSERT_OK(_tablet_manager->publish_version(_tablet_metadata->id(), version, version + 1, &_txn_id, 1));
    version++;
    ASSERT_EQ(kChunkSize * 3, read(version));
}

class UniqueKeyHorizontalCompactionTest : public testing::Test {
public:
    UniqueKeyHorizontalCompactionTest() {
        _tablet_manager = ExecEnv::GetInstance()->lake_tablet_manager();

        _parent_mem_tracker = std::make_unique<MemTracker>(-1);
        _mem_tracker = std::make_unique<MemTracker>(-1, "", _parent_mem_tracker.get());
        _group_assigner = std::make_unique<TestGroupAssigner>(kTestGroupPath);
        _backup_group_assigner = _tablet_manager->TEST_set_group_assigner(_group_assigner.get());

        _tablet_metadata = std::make_shared<TabletMetadata>();
        _tablet_metadata->set_id(next_id());
        _tablet_metadata->set_version(1);
        //
        //  | column | type | KEY | NULL |
        //  +--------+------+-----+------+
        //  |   c0   |  INT | YES |  NO  |
        //  |   c1   |  INT | NO  |  NO  |
        auto schema = _tablet_metadata->mutable_schema();
        schema->set_id(next_id());
        schema->set_num_short_key_columns(1);
        schema->set_keys_type(UNIQUE_KEYS);
        schema->set_num_rows_per_row_block(65535);
        schema->set_compress_kind(COMPRESS_LZ4);
        auto c0 = schema->add_column();
        {
            c0->set_unique_id(next_id());
            c0->set_name("c0");
            c0->set_type("INT");
            c0->set_is_key(true);
            c0->set_is_nullable(false);
        }
        auto c1 = schema->add_column();
        {
            c1->set_unique_id(next_id());
            c1->set_name("c1");
            c1->set_type("INT");
            c1->set_is_key(false);
            c1->set_is_nullable(false);
            c1->set_aggregation("REPLACE");
        }

        _tablet_schema = TabletSchema::create(_mem_tracker.get(), *schema);
        _schema = std::make_shared<VSchema>(ChunkHelper::convert_schema(*_tablet_schema));
    }

protected:
    constexpr static const char* const kTestGroupPath = "test_lake_hcompaction_task_unique";
    constexpr static const int kChunkSize = 12;

    void SetUp() override {
        (void)ExecEnv::GetInstance()->lake_tablet_manager()->TEST_set_group_assigner(_group_assigner.get());
        (void)fs::remove_all(kTestGroupPath);
        CHECK_OK(fs::create_directories(kTestGroupPath));
        CHECK_OK(_tablet_manager->put_tablet_metadata(kTestGroupPath, *_tablet_metadata));
    }

    void TearDown() override {
        ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(_tablet_metadata->id()));
        tablet.delete_txn_log(_txn_id);
        _txn_id++;
        (void)ExecEnv::GetInstance()->lake_tablet_manager()->TEST_set_group_assigner(_backup_group_assigner);
        (void)fs::remove_all(kTestGroupPath);
    }

    VChunk generate_data(int64_t chunk_size) {
        std::vector<int> v0(chunk_size);
        std::vector<int> v1(chunk_size);
        for (int i = 0; i < chunk_size; i++) {
            v0[i] = i;
        }
        auto rng = std::default_random_engine{};
        std::shuffle(v0.begin(), v0.end(), rng);
        for (int i = 0; i < chunk_size; i++) {
            v1[i] = v0[i] * 3;
        }

        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        c0->append_numbers(v0.data(), v0.size() * sizeof(int));
        c1->append_numbers(v1.data(), v1.size() * sizeof(int));
        return VChunk({c0, c1}, _schema);
    }

    int64_t read(int64_t version) {
        ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(_tablet_metadata->id()));
        ASSIGN_OR_ABORT(auto reader, tablet.new_reader(version, *_schema));
        CHECK_OK(reader->prepare());
        CHECK_OK(reader->open(TabletReaderParams()));
        auto chunk = ChunkHelper::new_chunk(*_schema, 128);
        int64_t ret = 0;
        while (true) {
            auto st = reader->get_next(chunk.get());
            if (st.is_end_of_file()) {
                break;
            }
            CHECK_OK(st);
            ret += chunk->num_rows();
            chunk->reset();
        }
        return ret;
    }

    TabletManager* _tablet_manager;
    std::unique_ptr<MemTracker> _parent_mem_tracker;
    std::unique_ptr<MemTracker> _mem_tracker;
    std::unique_ptr<TestGroupAssigner> _group_assigner;
    GroupAssigner* _backup_group_assigner;
    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<VSchema> _schema;
    int64_t _partition_id = 4561;
    int64_t _txn_id = 1001;
};

TEST_F(UniqueKeyHorizontalCompactionTest, test1) {
    // Prepare data for writing
    auto chunk0 = generate_data(kChunkSize);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    for (int i = 0; i < 3; i++) {
        _txn_id++;
        auto delta_writer = DeltaWriter::create(tablet_id, _txn_id, _partition_id, nullptr, _mem_tracker.get());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish());
        delta_writer->close();
        // Publish version
        ASSERT_OK(_tablet_manager->publish_version(tablet_id, version, version + 1, &_txn_id, 1));
        version++;
    }
    ASSERT_EQ(kChunkSize, read(version));

    ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(tablet_id));
    ASSIGN_OR_ABORT(auto t, _tablet_manager->get_tablet(tablet_id));
    _txn_id++;

    ASSIGN_OR_ABORT(auto task, _tablet_manager->compact(_tablet_metadata->id(), version, _txn_id));
    ASSERT_OK(task->execute());
    ASSERT_OK(_tablet_manager->publish_version(_tablet_metadata->id(), version, version + 1, &_txn_id, 1));
    version++;
    ASSERT_EQ(kChunkSize, read(version));
}
} // namespace starrocks::lake
