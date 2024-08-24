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

#include <chrono>
#include <memory>
#include <string>
#include <thread>

#include "storage/chunk_helper.h"
#include "storage/horizontal_compaction_task.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_meta_manager.h"
#include "storage/tablet_updates.h"
#include "testutil/assert.h"

namespace starrocks {

class TabletSchemaGCTest : public testing::Test {
    TabletSharedPtr create_tablet(int64_t tablet_id, int32_t schema_hash, TKeysType::type keys_type) {
        srand(GetCurrentTimeMicros());
        TCreateTabletReq request;
        request.tablet_id = tablet_id;
        request.__set_version(1);
        request.__set_version_hash(0);
        request.tablet_schema.schema_hash = schema_hash;
        request.tablet_schema.short_key_column_count = 1;
        request.tablet_schema.keys_type = keys_type;
        request.tablet_schema.storage_type = TStorageType::COLUMN;
        request.tablet_schema.__set_id(rand());

        TColumn k1;
        k1.column_name = "k1";
        k1.__set_is_key(true);
        k1.column_type.type = TPrimitiveType::INT;
        request.tablet_schema.columns.push_back(k1);

        TColumn k2;
        k2.column_name = "v1";
        k2.__set_is_key(false);
        k2.column_type.type = TPrimitiveType::INT;
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

    void create_rowset(int min_version, int spec_version, std::vector<int32_t> rows_per_segment,
                       RowsetSharedPtr& rowset) {
        RowsetId rowset_id;
        rowset_id.init(2, ++_next_rowset_uid, 1, 1);
        RowsetWriterContext writer_context;
        writer_context.rowset_id = rowset_id;
        writer_context.tablet_id = _tablet->tablet_id();
        writer_context.tablet_schema_hash = _tablet->schema_hash();
        writer_context.partition_id = 0;
        writer_context.version = Version(min_version, spec_version);
        writer_context.rowset_path_prefix = _tablet->schema_hash_path();
        writer_context.rowset_state = VISIBLE;
        writer_context.tablet_schema = _tablet->tablet_schema();
        writer_context.writer_type = kHorizontal;

        std::unique_ptr<RowsetWriter> rowset_writer;
        ASSERT_OK(RowsetFactory::create_rowset_writer(writer_context, &rowset_writer));

        std::vector<std::unique_ptr<SegmentPB>> seg_infos;
        int32_t total_rows = 0;
        auto schema = ChunkHelper::convert_schema(_tablet->tablet_schema());
        for (int32_t num_rows : rows_per_segment) {
            std::unique_ptr<SegmentPB> segment;
            auto chunk = ChunkHelper::new_chunk(schema, num_rows);
            for (int i = total_rows; i < num_rows + total_rows; i++) {
                auto& cols = chunk->columns();
                cols[0]->append_datum(Datum(static_cast<int32_t>(i)));
                cols[1]->append_datum(Datum(static_cast<int32_t>(i + 1)));
                cols[2]->append_datum(Datum(static_cast<int32_t>(i + 2)));
            }
            ASSERT_OK(rowset_writer->flush_chunk(*chunk, segment.get()));
            total_rows += num_rows;
            seg_infos.emplace_back(std::move(segment));
        }

        rowset = rowset_writer->build().value();
    }

    void SetUp() override {
        srand(GetCurrentTimeMicros());
        _compaction_mem_tracker = std::make_unique<MemTracker>(-1);
        _next_rowset_uid = 0;
    }

    void TearDown() override {
        if (_tablet) {
            (void)StorageEngine::instance()->tablet_manager()->drop_tablet(_tablet->tablet_id());
            _tablet.reset();
        }
    }

protected:
    TabletSharedPtr _tablet;
    int64_t _next_rowset_uid;
    std::unique_ptr<MemTracker> _compaction_mem_tracker;
};

TEST_F(TabletSchemaGCTest, test_stale_schema_gc) {
    _tablet = create_tablet(rand(), rand(), TKeysType::DUP_KEYS);
    auto schema_id0 = _tablet->tablet_schema()->id();
    TabletSchemaPB schema_pb1;
    _tablet->tablet_schema()->to_schema_pb(&schema_pb1);
    schema_pb1.set_id(schema_pb1.id() + 1);
    schema_pb1.set_schema_version(schema_pb1.schema_version() + 1);
    auto tablet_schema1 = std::make_shared<TabletSchema>(schema_pb1);
    _tablet->update_max_version_schema(tablet_schema1);

    RowsetSharedPtr rowset1;
    create_rowset(2, 2, {1, 2, 3}, rowset1);
    ASSERT_TRUE(_tablet->insert_committed_rowset_schema(rowset1->rowset_id(), rowset1->tablet_schema()->id()));
    rowset1->rowset_meta()->set_tablet_schema_id();
    ASSERT_OK(_tablet->add_rowset(rowset1, false));
    auto schema_id1 = _tablet->tablet_schema()->id();

    TabletSchemaPB schema_pb2;
    _tablet->tablet_schema()->to_schema_pb(&schema_pb2);
    schema_pb2.set_id(schema_pb2.id() + 1);
    schema_pb2.set_schema_version(schema_pb2.schema_version() + 1);
    auto tablet_schema2 = std::make_shared<TabletSchema>(schema_pb2);
    _tablet->update_max_version_schema(tablet_schema2);
    auto schema_id2 = _tablet->tablet_schema()->id();

    RowsetSharedPtr rowset2;
    create_rowset(3, 3, {1, 2, 3}, rowset2);
    ASSERT_TRUE(_tablet->insert_committed_rowset_schema(rowset1->rowset_id(), rowset1->tablet_schema()->id()));
    rowset2->rowset_meta()->set_tablet_schema_id();
    ASSERT_OK(_tablet->add_inc_rowset(rowset2, 3));
    _tablet->tablet_meta()->delete_stale_schema();

    {
        ASSERT_TRUE(rowset1->rowset_meta()->has_tablet_schema_id());
        ASSERT_TRUE(rowset2->rowset_meta()->has_tablet_schema_id());
        ASSERT_TRUE(rowset1->rowset_meta()->tablet_schema()->id() == schema_id1);
        ASSERT_TRUE(rowset2->rowset_meta()->tablet_schema()->id() == schema_id2);
        const std::map<int64_t, TabletSchemaCSPtr> history_schema = _tablet->tablet_meta()->history_schema();
        ASSERT_TRUE(history_schema.size() == 2);
        ASSERT_TRUE(schema_id2 == _tablet->tablet_schema()->id());
        ASSERT_TRUE(history_schema.count(schema_id0) > 0);
        ASSERT_TRUE(history_schema.count(schema_id1) > 0);
    }

    rowset1->rowset_meta()->set_tablet_schema(_tablet->tablet_schema());
    rowset1->rowset_meta()->set_tablet_schema_id();
    _tablet->tablet_meta()->delete_stale_schema();
    {
        ASSERT_TRUE(rowset1->rowset_meta()->has_tablet_schema_id());
        ASSERT_TRUE(rowset2->rowset_meta()->has_tablet_schema_id());
        ASSERT_TRUE(rowset1->rowset_meta()->tablet_schema()->id() == schema_id2);
        ASSERT_TRUE(rowset2->rowset_meta()->tablet_schema()->id() == schema_id2);
        const std::map<int64_t, TabletSchemaCSPtr> history_schema = _tablet->tablet_meta()->history_schema();
        ASSERT_TRUE(history_schema.size() == 2);
        ASSERT_TRUE(schema_id2 == _tablet->tablet_schema()->id());
        ASSERT_TRUE(history_schema.count(schema_id0) > 0);
        ASSERT_TRUE(history_schema.count(schema_id1) > 0);
    }

    _tablet->erase_committed_rowset_schema(rowset1->rowset_id());
    _tablet->tablet_meta()->delete_stale_schema();
    {
        ASSERT_TRUE(rowset1->rowset_meta()->has_tablet_schema_id());
        ASSERT_TRUE(rowset2->rowset_meta()->has_tablet_schema_id());
        ASSERT_TRUE(rowset1->rowset_meta()->tablet_schema()->id() == schema_id2);
        ASSERT_TRUE(rowset2->rowset_meta()->tablet_schema()->id() == schema_id2);
        const std::map<int64_t, TabletSchemaCSPtr> history_schema = _tablet->tablet_meta()->history_schema();
        ASSERT_TRUE(history_schema.size() == 1);
        ASSERT_TRUE(schema_id2 == _tablet->tablet_schema()->id());
        ASSERT_TRUE(history_schema.count(schema_id0) > 0);
    }

    {
        TabletMetaPB tablet_meta_pb;
        _tablet->tablet_meta()->to_meta_pb(&tablet_meta_pb);
        TabletMeta new_meta;
        new_meta.init_from_pb(&tablet_meta_pb);
        ASSERT_TRUE(new_meta._history_schema.size() == 1);
        ASSERT_TRUE(new_meta._history_schema.count(schema_id0) > 0);
        ASSERT_TRUE(new_meta._schema->id() == schema_id2);
    }

    RowsetSharedPtr rowset3;
    create_rowset(0, 2, {1, 2, 3}, rowset3);
    std::vector<RowsetSharedPtr> input_rowsets;
    _tablet->capture_consistent_rowsets(Version(0, 2), &input_rowsets);
    ASSERT_TRUE(input_rowsets.size() == 2);
    _tablet->modify_rowsets_without_lock({rowset3}, input_rowsets, nullptr);

    input_rowsets.clear();
    _tablet->capture_consistent_rowsets(Version(0, 2), &input_rowsets);
    ASSERT_TRUE(input_rowsets.size() == 1);
    int64_t ori_sweep_time = config::tablet_rowset_stale_sweep_time_sec;
    config::tablet_rowset_stale_sweep_time_sec = 0;
    _tablet->delete_expired_stale_rowset();
    {
        const std::map<int64_t, TabletSchemaCSPtr> history_schema = _tablet->tablet_meta()->history_schema();
        ASSERT_TRUE(history_schema.size() == 0);
    }
    config::tablet_rowset_stale_sweep_time_sec = ori_sweep_time;
}

TEST_F(TabletSchemaGCTest, test_pk_stale_schema_gc) {
    _tablet = create_tablet(rand(), rand(), TKeysType::PRIMARY_KEYS);
    TabletSchemaPB schema_pb1;
    _tablet->tablet_schema()->to_schema_pb(&schema_pb1);
    schema_pb1.set_id(schema_pb1.id() + 1);
    schema_pb1.set_schema_version(schema_pb1.schema_version() + 1);
    auto tablet_schema1 = std::make_shared<TabletSchema>(schema_pb1);
    _tablet->update_max_version_schema(tablet_schema1);

    RowsetSharedPtr rowset1;
    create_rowset(0, 4, {1, 2, 3}, rowset1);
    rowset1->rowset_meta()->set_tablet_schema_id();
    ASSERT_OK(_tablet->rowset_commit(4, rowset1));
    auto schema_id1 = _tablet->tablet_schema()->id();

    TabletSchemaPB schema_pb2;
    _tablet->tablet_schema()->to_schema_pb(&schema_pb2);
    schema_pb2.set_id(schema_pb2.id() + 1);
    schema_pb2.set_schema_version(schema_pb2.schema_version() + 1);
    auto tablet_schema2 = std::make_shared<TabletSchema>(schema_pb2);
    _tablet->update_max_version_schema(tablet_schema2);
    auto schema_id2 = _tablet->tablet_schema()->id();

    RowsetSharedPtr rowset2;
    create_rowset(0, 2, {1, 2, 3}, rowset2);
    rowset2->rowset_meta()->set_tablet_schema_id();
    ASSERT_OK(_tablet->rowset_commit(2, rowset2));
    _tablet->tablet_meta()->delete_stale_schema();

    {
        ASSERT_TRUE(rowset1->rowset_meta()->has_tablet_schema_id());
        ASSERT_TRUE(rowset2->rowset_meta()->has_tablet_schema_id());
        ASSERT_TRUE(rowset1->rowset_meta()->tablet_schema()->id() == schema_id1);
        ASSERT_TRUE(rowset2->rowset_meta()->tablet_schema()->id() == schema_id2);
        const std::map<int64_t, TabletSchemaCSPtr> history_schema = _tablet->tablet_meta()->history_schema();
        ASSERT_TRUE(history_schema.size() == 1);
        ASSERT_TRUE(schema_id2 == _tablet->tablet_schema()->id());
        ASSERT_TRUE(history_schema.count(schema_id1) > 0);
    }

    rowset1->rowset_meta()->set_tablet_schema(_tablet->tablet_schema());
    rowset1->rowset_meta()->set_tablet_schema_id();
    _tablet->tablet_meta()->delete_stale_schema();
    {
        ASSERT_TRUE(rowset1->rowset_meta()->has_tablet_schema_id());
        ASSERT_TRUE(rowset2->rowset_meta()->has_tablet_schema_id());
        ASSERT_TRUE(rowset1->rowset_meta()->tablet_schema()->id() == schema_id2);
        ASSERT_TRUE(rowset2->rowset_meta()->tablet_schema()->id() == schema_id2);
        const std::map<int64_t, TabletSchemaCSPtr> history_schema = _tablet->tablet_meta()->history_schema();
        ASSERT_TRUE(history_schema.size() == 0);
        ASSERT_TRUE(schema_id2 == _tablet->tablet_schema()->id());
    }

    std::this_thread::sleep_for(std::chrono::seconds(1));
    ASSERT_EQ(_tablet->updates()->num_rowsets(), 1);
    ASSERT_EQ(_tablet->updates()->version_history_count(), 2);
    EditVersion latest_applied_verison;
    _tablet->updates()->get_latest_applied_version(&latest_applied_verison);
    ASSERT_EQ(latest_applied_verison.major_number(), 2);

    TabletSchemaPB schema_pb3;
    _tablet->tablet_schema()->to_schema_pb(&schema_pb3);
    schema_pb3.set_id(schema_pb3.id() + 1);
    schema_pb3.set_schema_version(schema_pb3.schema_version() + 1);
    auto tablet_schema3 = std::make_shared<TabletSchema>(schema_pb3);
    _tablet->update_max_version_schema(tablet_schema3);
    auto schema_id3 = _tablet->tablet_schema()->id();

    {
        const std::map<int64_t, TabletSchemaCSPtr> history_schema = _tablet->tablet_meta()->history_schema();
        ASSERT_TRUE(history_schema.size() == 1);
        ASSERT_TRUE(schema_id3 == _tablet->tablet_schema()->id());
        ASSERT_TRUE(history_schema.count(schema_id2) > 0);
    }

    RowsetSharedPtr rowset3;
    create_rowset(0, 3, {1, 2, 3}, rowset3);
    rowset3->rowset_meta()->set_tablet_schema_id();
    ASSERT_OK(_tablet->rowset_commit(3, rowset3));
    _tablet->tablet_meta()->delete_stale_schema();
    std::this_thread::sleep_for(std::chrono::seconds(1));
    _tablet->updates()->get_latest_applied_version(&latest_applied_verison);
    ASSERT_EQ(latest_applied_verison.major_number(), 4);

    int64_t expired_stale_sweep_endtime = UnixSeconds();
    ASSERT_TRUE(_tablet->updates()->compaction(_compaction_mem_tracker.get()).ok());
    // Wait until compaction applied.
    while (true) {
        std::vector<RowsetSharedPtr> rowsets;
        EditVersion full_version;
        ASSERT_TRUE(_tablet->updates()->get_applied_rowsets(4, &rowsets, &full_version).ok());
        if (full_version.minor_number() == 1) {
            break;
        }
        std::cerr << "waiting for compaction applied\n";
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    {
        ASSERT_EQ(_tablet->updates()->num_rowsets(), 1);
        ASSERT_TRUE(rowset1.use_count() == 2);
        ASSERT_TRUE(rowset2.use_count() == 2);
        ASSERT_TRUE(rowset3.use_count() == 2);
        rowset1.reset();
        rowset2.reset();
        rowset3.reset();
        _tablet->updates()->remove_expired_versions(expired_stale_sweep_endtime);
        const std::map<int64_t, TabletSchemaCSPtr> history_schema1 = _tablet->tablet_meta()->history_schema();
        ASSERT_TRUE(history_schema1.size() == 1);
        ASSERT_TRUE(schema_id3 == _tablet->tablet_schema()->id());
        ASSERT_TRUE(history_schema1.count(schema_id2) > 0);

        _tablet->updates()->remove_expired_versions(expired_stale_sweep_endtime);
        const std::map<int64_t, TabletSchemaCSPtr> history_schema2 = _tablet->tablet_meta()->history_schema();
        ASSERT_TRUE(history_schema2.size() == 0);
    }
}

} // namespace starrocks