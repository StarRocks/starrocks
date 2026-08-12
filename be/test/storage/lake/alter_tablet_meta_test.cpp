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

#include "agent/agent_task.h"
#include "base/failpoint/fail_point.h"
#include "base/testutil/id_generator.h"
#include "base/utility/defer_op.h"
#include "common/config_lake_fwd.h"
#include "common/config_primary_key_fwd.h"
#include "fs/fs_util.h"
#include "storage/chunk_helper.h"
#include "storage/lake/schema_change.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_writer.h"
#include "storage/lake/txn_log_applier.h"
#include "storage/lake/versioned_tablet.h"
#include "storage_primitive/flat_json_config.h"
#include "test_util.h"
#include "types/type_descriptor.h"

namespace starrocks::lake {

using namespace starrocks;

class AlterTabletMetaTest : public TestBase {
public:
    AlterTabletMetaTest() : TestBase(kTestDirectory) {
        _tablet_metadata = generate_simple_tablet_metadata(PRIMARY_KEYS);
        _tablet_schema = TabletSchema::create(_tablet_metadata->schema());
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
    }

    void SetUp() override {
        clear_and_init_test_dir();

        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override { remove_test_dir_ignore_error(); }

    void test_alter_update_tablet_schema(KeysType keys_type);

protected:
    constexpr static const char* const kTestDirectory = "test_alter_tablet_meta";

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
};

TEST_F(AlterTabletMetaTest, test_missing_txn_id) {
    lake::SchemaChangeHandler handler(_tablet_mgr.get());
    TUpdateTabletMetaInfoReq update_tablet_meta_req;

    TTabletMetaInfo tablet_meta_info;
    auto tablet_id = _tablet_metadata->id();
    tablet_meta_info.__set_tablet_id(tablet_id);
    tablet_meta_info.__set_meta_type(TTabletMetaType::ENABLE_PERSISTENT_INDEX);
    tablet_meta_info.__set_enable_persistent_index(true);

    update_tablet_meta_req.tabletMetaInfos.push_back(tablet_meta_info);
    auto status = handler.process_update_tablet_meta(update_tablet_meta_req);
    ASSERT_ERROR(status);
    ASSERT_EQ("txn_id not set in request", status.message());
}

// A compaction (or any writer created off a specific metadata version) must take flat_json from
// THAT version's metadata (_metadata), not the best-effort latest-metadata cache -- so the flatten
// decision is version-consistent and does not depend on cache warmth. Verifies
// VersionedTablet::new_writer_with_schema wires the versioned config onto the writer.
TEST_F(AlterTabletMetaTest, test_compaction_writer_uses_versioned_flat_json_config) {
    // Version WITH flat_json_config: the compaction writer must carry it.
    {
        auto md = std::make_shared<TabletMetadata>(*_tablet_metadata);
        md->mutable_flat_json_config()->set_flat_json_enable(true);
        md->mutable_flat_json_config()->set_flat_json_max_column_max(80);
        VersionedTablet tablet(_tablet_mgr.get(), md);
        ASSIGN_OR_ABORT(auto writer, tablet.new_writer_with_schema(kHorizontal, next_id(), 0, nullptr,
                                                                   /*is_compaction=*/true, _tablet_schema));
        ASSERT_NE(nullptr, writer->flat_json_config());
        ASSERT_TRUE(writer->flat_json_config()->is_flat_json_enabled());
        ASSERT_EQ(80, writer->flat_json_config()->get_flat_json_max_column_max());
    }
    // Version WITHOUT flat_json_config: writer leaves it null (falls back to global at write time).
    {
        auto md = std::make_shared<TabletMetadata>(*_tablet_metadata);
        md->clear_flat_json_config();
        VersionedTablet tablet(_tablet_mgr.get(), md);
        ASSIGN_OR_ABORT(auto writer, tablet.new_writer_with_schema(kHorizontal, next_id(), 0, nullptr,
                                                                   /*is_compaction=*/true, _tablet_schema));
        ASSERT_EQ(nullptr, writer->flat_json_config());
    }
}

// Spill-merged loads build their final segments through cloned writers
// (LoadSpillPipelineMergeIterator -> TabletWriter::clone()). The clone must inherit the
// plan-carried flat_json config; otherwise a spilled load silently falls back to the
// tablet-metadata cache / global config, recreating the nondeterminism this change removes.
TEST_F(AlterTabletMetaTest, test_clone_writer_propagates_flat_json_config) {
    auto md = std::make_shared<TabletMetadata>(*_tablet_metadata);
    md->mutable_flat_json_config()->set_flat_json_enable(true);
    md->mutable_flat_json_config()->set_flat_json_max_column_max(80);
    VersionedTablet tablet(_tablet_mgr.get(), md);
    ASSIGN_OR_ABORT(auto writer, tablet.new_writer_with_schema(kHorizontal, next_id(), 0, nullptr,
                                                               /*is_compaction=*/false, _tablet_schema));
    ASSERT_NE(nullptr, writer->flat_json_config());

    ASSIGN_OR_ABORT(auto cloned, writer->clone());
    ASSERT_NE(nullptr, cloned->flat_json_config());
    ASSERT_TRUE(cloned->flat_json_config()->is_flat_json_enabled());
    ASSERT_EQ(80, cloned->flat_json_config()->get_flat_json_max_column_max());
    cloned->close();
    writer->close();
}

TEST_F(AlterTabletMetaTest, test_alter_enable_persistent_index) {
    GTEST_SKIP() << "LOCAL persistent index is deprecated for shared-data primary-key tablets";
    lake::SchemaChangeHandler handler(_tablet_mgr.get());
    TUpdateTabletMetaInfoReq update_tablet_meta_req;
    int64_t txn_id = next_id();
    update_tablet_meta_req.__set_txn_id(txn_id);

    TTabletMetaInfo tablet_meta_info;
    auto tablet_id = _tablet_metadata->id();
    tablet_meta_info.__set_tablet_id(tablet_id);
    tablet_meta_info.__set_meta_type(TTabletMetaType::ENABLE_PERSISTENT_INDEX);
    tablet_meta_info.__set_enable_persistent_index(true);

    update_tablet_meta_req.tabletMetaInfos.push_back(tablet_meta_info);
    ASSERT_OK(handler.process_update_tablet_meta(update_tablet_meta_req));

    auto new_tablet_meta = publish_single_version(tablet_id, 2, txn_id);
    ASSERT_OK(new_tablet_meta.status());
    ASSERT_EQ(true, new_tablet_meta.value()->enable_persistent_index());

    txn_id = next_id();
    std::vector<int> k0{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22};
    std::vector<int> v0{2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 41, 44};
    auto c0 = Int32Column::create();
    auto c1 = Int32Column::create();
    c0->append_numbers(k0.data(), k0.size() * sizeof(int));
    c1->append_numbers(v0.data(), v0.size() * sizeof(int));
    Chunk chunk0({std::move(c0), std::move(c1)}, _schema);
    auto rowset_txn_meta = std::make_unique<RowsetTxnMetaPB>();
    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(_tablet_metadata->id()));
    std::shared_ptr<const TabletSchema> const_schema = _tablet_schema;
    ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, txn_id));
    ASSERT_OK(writer->open());
    // write segment #1
    ASSERT_OK(writer->write(chunk0));
    ASSERT_OK(writer->finish());
    // write txnlog
    auto txn_log = std::make_shared<TxnLog>();
    txn_log->set_tablet_id(_tablet_metadata->id());
    txn_log->set_txn_id(txn_id);
    auto op_write = txn_log->mutable_op_write();
    for (const auto& f : writer->segments()) {
        op_write->mutable_rowset()->add_segment_metas()->set_filename(f.path);
    }
    op_write->mutable_rowset()->set_num_rows(writer->num_rows());
    op_write->mutable_rowset()->set_data_size(writer->data_size());
    op_write->mutable_rowset()->set_overlapped(false);
    ASSERT_OK(_tablet_mgr->put_txn_log(txn_log));
    writer->close();
    ASSERT_OK(publish_single_version(_tablet_metadata->id(), 3, txn_id).status());
    auto data_dir = StorageEngine::instance()->get_persistent_index_store(tablet_id);
    ASSERT_TRUE(data_dir != nullptr);
    ASSERT_OK(FileSystem::Default()->path_exists(data_dir->get_persistent_index_path() + "/" +
                                                 std::to_string(tablet_id)));

    int64_t txn_id2 = next_id();
    TUpdateTabletMetaInfoReq update_tablet_meta_req2;
    update_tablet_meta_req2.__set_txn_id(txn_id2);

    TTabletMetaInfo tablet_meta_info2;
    tablet_meta_info2.__set_tablet_id(tablet_id);
    tablet_meta_info2.__set_meta_type(TTabletMetaType::ENABLE_PERSISTENT_INDEX);
    tablet_meta_info2.__set_enable_persistent_index(false);

    update_tablet_meta_req2.tabletMetaInfos.push_back(tablet_meta_info2);
    ASSERT_OK(handler.process_update_tablet_meta(update_tablet_meta_req2));

    auto new_tablet_meta2 = publish_single_version(tablet_id, 4, txn_id2);
    ASSERT_OK(new_tablet_meta2.status());
    ASSERT_EQ(false, new_tablet_meta2.value()->enable_persistent_index());
#ifdef USE_STAROS
    data_dir = StorageEngine::instance()->get_persistent_index_store(tablet_id);
    ASSERT_TRUE(data_dir != nullptr);
    ASSERT_ERROR(FileSystem::Default()->path_exists(data_dir->get_persistent_index_path() + "/" +
                                                    std::to_string(tablet_id)));
#endif
}

TEST_F(AlterTabletMetaTest, test_alter_enable_persistent_index_not_change) {
    lake::SchemaChangeHandler handler(_tablet_mgr.get());
    TUpdateTabletMetaInfoReq update_tablet_meta_req;
    int64_t txn_id = 1;
    update_tablet_meta_req.__set_txn_id(txn_id);

    TTabletMetaInfo tablet_meta_info;
    auto tablet_id = _tablet_metadata->id();
    tablet_meta_info.__set_tablet_id(tablet_id);
    tablet_meta_info.__set_meta_type(TTabletMetaType::ENABLE_PERSISTENT_INDEX);
    tablet_meta_info.__set_enable_persistent_index(true);

    update_tablet_meta_req.tabletMetaInfos.push_back(tablet_meta_info);
    ASSERT_OK(handler.process_update_tablet_meta(update_tablet_meta_req));

    auto new_tablet_meta = publish_single_version(tablet_id, 2, txn_id);
    ASSERT_OK(new_tablet_meta.status());
    ASSERT_EQ(true, new_tablet_meta.value()->enable_persistent_index());

    int64_t txn_id2 = txn_id + 1;
    TUpdateTabletMetaInfoReq update_tablet_meta_req2;
    update_tablet_meta_req2.__set_txn_id(txn_id2);

    // `enable_persistent_index` is still set to true
    TTabletMetaInfo tablet_meta_info2;
    tablet_meta_info2.__set_tablet_id(tablet_id);
    tablet_meta_info2.__set_meta_type(TTabletMetaType::ENABLE_PERSISTENT_INDEX);
    tablet_meta_info2.__set_enable_persistent_index(true);

    update_tablet_meta_req2.tabletMetaInfos.push_back(tablet_meta_info2);
    ASSERT_OK(handler.process_update_tablet_meta(update_tablet_meta_req2));

    auto new_tablet_meta2 = publish_single_version(tablet_id, 3, txn_id2);
    ASSERT_OK(new_tablet_meta2.status());
    ASSERT_EQ(true, new_tablet_meta2.value()->enable_persistent_index());
}

void AlterTabletMetaTest::test_alter_update_tablet_schema(KeysType keys_type) {
    std::shared_ptr<TabletMetadata> tablet_metadata = generate_simple_tablet_metadata(keys_type);
    auto rs1 = tablet_metadata->add_rowsets();
    rs1->set_id(next_id());

    // write new rowset
    {
        TxnLogPB log;
        log.set_tablet_id(tablet_metadata->id());
        auto op_write_meta = log.mutable_op_write();
        auto rs_meta = op_write_meta->mutable_rowset();
        rs_meta->set_id(next_id());
        rs_meta->set_num_rows(10);

        auto tablet_id = tablet_metadata->id();
        auto version = tablet_metadata->version() + 1;
        std::unique_ptr<TxnLogApplier> log_applier =
                new_txn_log_applier(Tablet(_tablet_mgr.get(), tablet_id), tablet_metadata, version, false, false);

        ASSERT_OK(log_applier->apply(log));
        ASSERT_TRUE(tablet_metadata->rowset_to_schema().size() == 0);
        ASSERT_TRUE(tablet_metadata->historical_schemas().size() == 0);
        ASSERT_TRUE(tablet_metadata->rowsets_size() == 2);
    }

    // update meta
    auto schema_id1 = tablet_metadata->schema().id();
    {
        TxnLogPB log;
        log.set_tablet_id(tablet_metadata->id());
        auto alter_metadata = log.mutable_op_alter_metadata();
        auto update_info = alter_metadata->add_metadata_update_infos();
        auto tablet_schema_pb = update_info->mutable_tablet_schema();
        tablet_schema_pb->CopyFrom(tablet_metadata->schema());
        tablet_schema_pb->set_id(next_id());
        tablet_schema_pb->set_schema_version(tablet_schema_pb->schema_version() + 1);

        auto tablet_id = tablet_metadata->id();
        auto version = tablet_metadata->version() + 1;
        std::unique_ptr<TxnLogApplier> log_applier =
                new_txn_log_applier(Tablet(_tablet_mgr.get(), tablet_id), tablet_metadata, version, false, false);

        ASSERT_OK(log_applier->apply(log));

        auto rowset_id0 = tablet_metadata->rowsets(0).id();
        auto rowset_id1 = tablet_metadata->rowsets(1).id();
        ASSERT_TRUE(tablet_metadata->rowset_to_schema().size() == 2);
        ASSERT_TRUE(tablet_metadata->rowset_to_schema().at(rowset_id0) == schema_id1);
        ASSERT_TRUE(tablet_metadata->rowset_to_schema().at(rowset_id1) == schema_id1);
        ASSERT_TRUE(tablet_metadata->historical_schemas().size() == 1);
        ASSERT_TRUE(tablet_metadata->historical_schemas().count(schema_id1) > 0);
    }

    // add rowset
    auto schema_id2 = tablet_metadata->schema().id();
    {
        TxnLogPB log;
        log.set_tablet_id(tablet_metadata->id());
        auto op_write_meta = log.mutable_op_write();
        auto rs_meta = op_write_meta->mutable_rowset();
        rs_meta->set_id(next_id());
        rs_meta->set_num_rows(10);

        auto tablet_id = tablet_metadata->id();
        auto version = tablet_metadata->version() + 1;
        std::unique_ptr<TxnLogApplier> log_applier =
                new_txn_log_applier(Tablet(_tablet_mgr.get(), tablet_id), tablet_metadata, version, false, false);

        ASSERT_OK(log_applier->apply(log));

        auto rowset_id0 = tablet_metadata->rowsets(0).id();
        auto rowset_id1 = tablet_metadata->rowsets(1).id();
        auto rowset_id2 = tablet_metadata->rowsets(2).id();
        ASSERT_TRUE(tablet_metadata->rowsets_size() == 3);
        ASSERT_TRUE(tablet_metadata->rowset_to_schema().size() == 3);
        ASSERT_TRUE(tablet_metadata->historical_schemas().size() == 2);
        ASSERT_TRUE(tablet_metadata->historical_schemas().count(schema_id1) > 0);
        ASSERT_TRUE(tablet_metadata->historical_schemas().count(schema_id2) > 0);
        ASSERT_TRUE(tablet_metadata->rowset_to_schema().at(rowset_id0) == schema_id1);
        ASSERT_TRUE(tablet_metadata->rowset_to_schema().at(rowset_id1) == schema_id1);
        ASSERT_TRUE(tablet_metadata->rowset_to_schema().at(rowset_id2) == schema_id2);
    }

    // update meta
    {
        TxnLogPB log;
        log.set_tablet_id(tablet_metadata->id());
        auto alter_metadata = log.mutable_op_alter_metadata();
        auto update_info = alter_metadata->add_metadata_update_infos();
        auto tablet_schema_pb = update_info->mutable_tablet_schema();
        tablet_schema_pb->CopyFrom(tablet_metadata->schema());
        tablet_schema_pb->set_id(next_id());
        tablet_schema_pb->set_schema_version(tablet_schema_pb->schema_version() + 1);

        auto tablet_id = tablet_metadata->id();
        auto version = tablet_metadata->version() + 1;
        std::unique_ptr<TxnLogApplier> log_applier =
                new_txn_log_applier(Tablet(_tablet_mgr.get(), tablet_id), tablet_metadata, version, false, false);

        ASSERT_OK(log_applier->apply(log));

        auto rowset_id0 = tablet_metadata->rowsets(0).id();
        auto rowset_id1 = tablet_metadata->rowsets(1).id();
        auto rowset_id2 = tablet_metadata->rowsets(2).id();
        ASSERT_TRUE(tablet_metadata->rowset_to_schema().size() == 3);
        ASSERT_TRUE(tablet_metadata->historical_schemas().size() == 2);
        ASSERT_TRUE(tablet_metadata->historical_schemas().count(schema_id1) > 0);
        ASSERT_TRUE(tablet_metadata->historical_schemas().count(schema_id2) > 0);
        ASSERT_TRUE(tablet_metadata->rowset_to_schema().at(rowset_id0) == schema_id1);
        ASSERT_TRUE(tablet_metadata->rowset_to_schema().at(rowset_id1) == schema_id1);
        ASSERT_TRUE(tablet_metadata->rowset_to_schema().at(rowset_id2) == schema_id2);
    }

    // compaction
    auto schema_id3 = tablet_metadata->schema().id();
    {
        TxnLogPB log;
        log.set_tablet_id(tablet_metadata->id());
        auto op_compaction_meta = log.mutable_op_compaction();
        if (keys_type == PRIMARY_KEYS) {
            op_compaction_meta->add_input_rowsets(tablet_metadata->rowsets(2).id());
            op_compaction_meta->add_input_rowsets(tablet_metadata->rowsets(1).id());
        } else {
            op_compaction_meta->add_input_rowsets(tablet_metadata->rowsets(1).id());
            op_compaction_meta->add_input_rowsets(tablet_metadata->rowsets(2).id());
        }
        auto rs_meta = op_compaction_meta->mutable_output_rowset();
        auto rs_id = next_id();
        rs_meta->set_id(rs_id);
        rs_meta->set_num_rows(10);
        rs_meta->add_segment_metas()->set_filename("segment1");

        auto tablet_id = tablet_metadata->id();
        auto version = tablet_metadata->version() + 1;
        std::unique_ptr<TxnLogApplier> log_applier =
                new_txn_log_applier(Tablet(_tablet_mgr.get(), tablet_id), tablet_metadata, version, false, false);

        ASSERT_OK(log_applier->apply(log));
        auto rowset_id0 = tablet_metadata->rowsets(0).id();
        auto rowset_id1 = tablet_metadata->rowsets(1).id();
        ASSERT_TRUE(tablet_metadata->rowsets_size() == 2);
        ASSERT_TRUE(tablet_metadata->rowset_to_schema().size() == 2);
        ASSERT_TRUE(tablet_metadata->historical_schemas().size() == 2);
        if (keys_type == PRIMARY_KEYS) {
            ASSERT_TRUE(tablet_metadata->rowset_to_schema().at(rowset_id0) == schema_id1);
            ASSERT_TRUE(tablet_metadata->rowset_to_schema().at(rowset_id1) == schema_id3);
            ASSERT_TRUE(tablet_metadata->historical_schemas().count(schema_id1) > 0);
            ASSERT_TRUE(tablet_metadata->historical_schemas().count(schema_id3) > 0);
        } else {
            ASSERT_TRUE(tablet_metadata->rowset_to_schema().at(rowset_id0) == schema_id1);
            ASSERT_TRUE(tablet_metadata->rowset_to_schema().at(rowset_id1) == schema_id2);
            ASSERT_TRUE(tablet_metadata->historical_schemas().count(schema_id1) > 0);
            ASSERT_TRUE(tablet_metadata->historical_schemas().count(schema_id2) > 0);
        }
    }

    // compaction one rowset
    {
        TxnLogPB log;
        log.set_tablet_id(tablet_metadata->id());
        auto op_compaction_meta = log.mutable_op_compaction();

        int32_t input_rowset_idx = 0;
        auto input_rs = tablet_metadata->mutable_rowsets(input_rowset_idx);
        input_rs->set_num_rows(0);
        op_compaction_meta->add_input_rowsets(input_rs->id());
        tablet_metadata->mutable_rowsets(0)->clear_segment_metas();
        tablet_metadata->mutable_rowsets(1)->clear_segment_metas();

        auto rs_meta = op_compaction_meta->mutable_output_rowset();
        rs_meta->set_id(next_id());
        rs_meta->set_num_rows(10);
        rs_meta->add_segment_metas()->set_filename("segment1");

        auto tablet_id = tablet_metadata->id();
        auto version = tablet_metadata->version() + 1;
        std::unique_ptr<TxnLogApplier> log_applier =
                new_txn_log_applier(Tablet(_tablet_mgr.get(), tablet_id), tablet_metadata, version, false, false);

        ASSERT_OK(log_applier->apply(log));
        auto rowset_id0 = tablet_metadata->rowsets(0).id();
        auto rowset_id1 = tablet_metadata->rowsets(1).id();
        ASSERT_TRUE(tablet_metadata->rowsets_size() == 2);
        ASSERT_TRUE(tablet_metadata->rowset_to_schema().size() == 2);
        if (keys_type == PRIMARY_KEYS) {
            ASSERT_TRUE(tablet_metadata->rowset_to_schema().at(rowset_id0) == schema_id3);
            ASSERT_TRUE(tablet_metadata->rowset_to_schema().at(rowset_id1) == schema_id3);
            ASSERT_TRUE(tablet_metadata->historical_schemas().size() == 1);
            ASSERT_TRUE(tablet_metadata->historical_schemas().count(schema_id3) > 0);
        } else {
            ASSERT_TRUE(tablet_metadata->rowset_to_schema().at(rowset_id0) == schema_id1);
            ASSERT_TRUE(tablet_metadata->rowset_to_schema().at(rowset_id1) == schema_id2);
            ASSERT_TRUE(tablet_metadata->historical_schemas().size() == 2);
            ASSERT_TRUE(tablet_metadata->historical_schemas().count(schema_id1) > 0);
            ASSERT_TRUE(tablet_metadata->historical_schemas().count(schema_id2) > 0);
        }
    }

    {
        TxnLogPB log;
        log.set_tablet_id(tablet_metadata->id());
        auto op_write_meta = log.mutable_op_write();
        auto rs_meta = op_write_meta->mutable_rowset();
        rs_meta->set_id(next_id());
        rs_meta->set_num_rows(10);

        tablet_metadata->mutable_rowsets(0)->clear_segment_metas();
        tablet_metadata->mutable_rowsets(1)->clear_segment_metas();
        tablet_metadata->mutable_rowsets(0)->set_num_rows(0);
        tablet_metadata->mutable_rowsets(1)->set_num_rows(0);

        auto tablet_id = tablet_metadata->id();
        auto version = tablet_metadata->version() + 1;
        std::unique_ptr<TxnLogApplier> log_applier =
                new_txn_log_applier(Tablet(_tablet_mgr.get(), tablet_id), tablet_metadata, version, false, false);

        ASSERT_OK(log_applier->apply(log));
        auto rowset_id0 = tablet_metadata->rowsets(0).id();
        auto rowset_id1 = tablet_metadata->rowsets(1).id();
        auto rowset_id2 = tablet_metadata->rowsets(2).id();
        ASSERT_TRUE(tablet_metadata->rowsets_size() == 3);
        ASSERT_TRUE(tablet_metadata->rowset_to_schema().size() == 3);
        if (keys_type == PRIMARY_KEYS) {
            ASSERT_TRUE(tablet_metadata->rowset_to_schema().at(rowset_id0) == schema_id3);
            ASSERT_TRUE(tablet_metadata->rowset_to_schema().at(rowset_id1) == schema_id3);
            ASSERT_TRUE(tablet_metadata->historical_schemas().count(schema_id3) > 0);
            ASSERT_TRUE(tablet_metadata->historical_schemas().size() == 1);
        } else {
            ASSERT_TRUE(tablet_metadata->rowset_to_schema().at(rowset_id0) == schema_id1);
            ASSERT_TRUE(tablet_metadata->rowset_to_schema().at(rowset_id1) == schema_id2);
            ASSERT_TRUE(tablet_metadata->historical_schemas().count(schema_id1) > 0);
            ASSERT_TRUE(tablet_metadata->historical_schemas().count(schema_id2) > 0);
            ASSERT_TRUE(tablet_metadata->historical_schemas().size() == 3);
        }
        ASSERT_TRUE(tablet_metadata->rowset_to_schema().at(rowset_id2) == schema_id3);
    }

    {
        TxnLogPB log;
        log.set_tablet_id(tablet_metadata->id());
        auto op_compaction_meta = log.mutable_op_compaction();
        op_compaction_meta->add_input_rowsets(tablet_metadata->rowsets(1).id());
        op_compaction_meta->add_input_rowsets(tablet_metadata->rowsets(2).id());
        auto rs_meta = op_compaction_meta->mutable_output_rowset();
        auto rs_id = next_id();
        rs_meta->set_id(rs_id);
        rs_meta->set_num_rows(10);
        rs_meta->add_segment_metas()->set_filename("segment1");

        tablet_metadata->mutable_rowsets(0)->clear_segment_metas();
        tablet_metadata->mutable_rowsets(1)->clear_segment_metas();
        tablet_metadata->mutable_rowsets(2)->clear_segment_metas();
        tablet_metadata->mutable_rowsets(0)->set_num_rows(0);
        tablet_metadata->mutable_rowsets(1)->set_num_rows(0);
        tablet_metadata->mutable_rowsets(2)->set_num_rows(0);

        auto tablet_id = tablet_metadata->id();
        auto version = tablet_metadata->version() + 1;
        std::unique_ptr<TxnLogApplier> log_applier =
                new_txn_log_applier(Tablet(_tablet_mgr.get(), tablet_id), tablet_metadata, version, false, false);

        ASSERT_OK(log_applier->apply(log));
        auto rowset_id0 = tablet_metadata->rowsets(0).id();
        auto rowset_id1 = tablet_metadata->rowsets(1).id();
        ASSERT_TRUE(tablet_metadata->rowsets_size() == 2);
        ASSERT_TRUE(tablet_metadata->rowset_to_schema().size() == 2);
        if (keys_type == PRIMARY_KEYS) {
            ASSERT_TRUE(tablet_metadata->historical_schemas().count(schema_id3) > 0);
            ASSERT_TRUE(tablet_metadata->rowset_to_schema().at(rowset_id0) == schema_id3);
            ASSERT_TRUE(tablet_metadata->rowset_to_schema().at(rowset_id1) == schema_id3);
            ASSERT_TRUE(tablet_metadata->historical_schemas().size() == 1);
        } else {
            ASSERT_TRUE(tablet_metadata->historical_schemas().count(schema_id1) > 0);
            ASSERT_TRUE(tablet_metadata->rowset_to_schema().at(rowset_id0) == schema_id1);
            ASSERT_TRUE(tablet_metadata->rowset_to_schema().at(rowset_id1) == schema_id3);
            ASSERT_TRUE(tablet_metadata->historical_schemas().size() == 2);
        }
    }
}

TEST_F(AlterTabletMetaTest, test_alter_non_pk_update_tablet_schema) {
    test_alter_update_tablet_schema(DUP_KEYS);
}

TEST_F(AlterTabletMetaTest, test_alter_pk_update_tablet_schema) {
    PFailPointTriggerMode trigger_mode;
    trigger_mode.set_mode(FailPointTriggerModeType::ENABLE);
    // enable hook_publish_primary_key_tablet
    auto fp = starrocks::failpoint::FailPointRegistry::GetInstance()->get("hook_publish_primary_key_tablet");
    fp->setMode(trigger_mode);

    fp = starrocks::failpoint::FailPointRegistry::GetInstance()->get("hook_publish_primary_key_tablet_compaction");
    fp->setMode(trigger_mode);

    test_alter_update_tablet_schema(PRIMARY_KEYS);

    // disable hook_publish_primary_key_tablet
    trigger_mode.set_mode(FailPointTriggerModeType::DISABLE);
    fp = starrocks::failpoint::FailPointRegistry::GetInstance()->get("hook_publish_primary_key_tablet");
    fp->setMode(trigger_mode);

    fp = starrocks::failpoint::FailPointRegistry::GetInstance()->get("hook_publish_primary_key_tablet_compaction");
    fp->setMode(trigger_mode);
}

TEST_F(AlterTabletMetaTest, test_alter_persistent_index_type) {
    lake::SchemaChangeHandler handler(_tablet_mgr.get());
    int version = 2;

    auto change_index_fn = [&](bool enable_persistent_index, TPersistentIndexType::type type) {
        TUpdateTabletMetaInfoReq update_tablet_meta_req;
        int64_t txn_id = next_id();
        update_tablet_meta_req.__set_txn_id(txn_id);

        TTabletMetaInfo tablet_meta_info;
        auto tablet_id = _tablet_metadata->id();
        tablet_meta_info.__set_tablet_id(tablet_id);
        tablet_meta_info.__set_meta_type(TTabletMetaType::ENABLE_PERSISTENT_INDEX);
        tablet_meta_info.__set_enable_persistent_index(enable_persistent_index);
        tablet_meta_info.__set_persistent_index_type(type);

        update_tablet_meta_req.tabletMetaInfos.push_back(tablet_meta_info);
        ASSERT_OK(handler.process_update_tablet_meta(update_tablet_meta_req));

        auto new_tablet_meta = publish_single_version(tablet_id, version++, txn_id);
        ASSERT_OK(new_tablet_meta.status());
        ASSERT_EQ(true, new_tablet_meta.value()->enable_persistent_index());
        ASSERT_TRUE(new_tablet_meta.value()->persistent_index_type() == (type == TPersistentIndexType::LOCAL)
                            ? PersistentIndexTypePB::LOCAL
                            : PersistentIndexTypePB::CLOUD_NATIVE);
    };

    auto write_data_fn = [&](bool rebuild_pindex) {
        int64_t txn_id = next_id();
        std::vector<int> k0{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22};
        std::vector<int> v0{2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 41, 44};
        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        c0->append_numbers(k0.data(), k0.size() * sizeof(int));
        c1->append_numbers(v0.data(), v0.size() * sizeof(int));
        Chunk chunk0({std::move(c0), std::move(c1)}, _schema);
        auto rowset_txn_meta = std::make_unique<RowsetTxnMetaPB>();
        ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(_tablet_metadata->id()));
        std::shared_ptr<const TabletSchema> const_schema = _tablet_schema;
        ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, txn_id));
        ASSERT_OK(writer->open());
        // write segment #1
        ASSERT_OK(writer->write(chunk0));
        ASSERT_OK(writer->finish());
        // write txnlog
        auto txn_log = std::make_shared<TxnLog>();
        txn_log->set_tablet_id(_tablet_metadata->id());
        txn_log->set_txn_id(txn_id);
        auto op_write = txn_log->mutable_op_write();
        for (const auto& f : writer->segments()) {
            op_write->mutable_rowset()->add_segment_metas()->set_filename(f.path);
        }
        op_write->mutable_rowset()->set_num_rows(writer->num_rows());
        op_write->mutable_rowset()->set_data_size(writer->data_size());
        op_write->mutable_rowset()->set_overlapped(false);
        ASSERT_OK(_tablet_mgr->put_txn_log(txn_log));
        writer->close();
        ASSERT_OK(publish_single_version(_tablet_metadata->id(), version++, txn_id, rebuild_pindex).status());
    };

    // 1. change to local index
    change_index_fn(true, TPersistentIndexType::LOCAL);
    ASSIGN_OR_ABORT(auto tablet_meta, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version - 1));
    ASSERT_EQ(true, tablet_meta->enable_persistent_index());
    ASSERT_TRUE(tablet_meta->persistent_index_type() == PersistentIndexTypePB::LOCAL);
    // 2. write data
    write_data_fn(false);
    // 3. change to cloud native index
    change_index_fn(true, TPersistentIndexType::CLOUD_NATIVE);
    ASSIGN_OR_ABORT(auto tablet_meta2, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version - 1));
    ASSERT_EQ(true, tablet_meta2->enable_persistent_index());
    ASSERT_TRUE(tablet_meta2->persistent_index_type() == PersistentIndexTypePB::CLOUD_NATIVE);
    // 4. generate sst files
    int64_t old_val = config::l0_max_mem_usage;
    config::l0_max_mem_usage = 1;
    for (int i = 0; i < 10; i++) {
        write_data_fn(false);
    }
    ASSIGN_OR_ABORT(auto tablet_meta3, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version - 1));
    ASSERT_TRUE(tablet_meta3->sstable_meta().sstables_size() > 0);

    ASSIGN_OR_ABORT(auto tablet_meta_d1, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version - 1));

    // 4. rebuild pindex
    { write_data_fn(true); }
    write_data_fn(false);
    config::l0_max_mem_usage = old_val;

    ASSIGN_OR_ABORT(auto tablet_meta_d2, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version - 1));

    // 5. change back to local
    change_index_fn(true, TPersistentIndexType::LOCAL);
    ASSIGN_OR_ABORT(auto tablet_meta4, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version - 1));
    ASSERT_EQ(true, tablet_meta4->enable_persistent_index());
    ASSERT_TRUE(tablet_meta4->persistent_index_type() == PersistentIndexTypePB::LOCAL);
    ASSERT_TRUE(tablet_meta4->sstable_meta().sstables_size() == 0);
    ASSERT_TRUE(tablet_meta4->orphan_files_size() > 0);
}

TEST_F(AlterTabletMetaTest, test_skip_load_pindex) {
    std::shared_ptr<TabletMetadata> tablet_metadata = generate_simple_tablet_metadata(PRIMARY_KEYS);
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*tablet_metadata));
    // write empty rowset
    {
        TxnLogPB log;
        log.set_tablet_id(tablet_metadata->id());
        auto op_write_meta = log.mutable_op_write();
        auto rs_meta = op_write_meta->mutable_rowset();
        rs_meta->set_id(next_id());
        rs_meta->set_num_rows(0);

        auto tablet_id = tablet_metadata->id();
        auto version = tablet_metadata->version() + 1;
        std::unique_ptr<TxnLogApplier> log_applier =
                new_txn_log_applier(Tablet(_tablet_mgr.get(), tablet_id), tablet_metadata, version, false, false);
        ASSERT_OK(log_applier->apply(log));
        ASSERT_TRUE(_tablet_mgr->update_mgr()->TEST_primary_index_refcnt(tablet_metadata->id(), 0));
    }

    {
        TxnLogPB log;
        log.set_tablet_id(tablet_metadata->id());
        auto op_compaction_meta = log.mutable_op_compaction();
        auto tablet_id = tablet_metadata->id();
        auto version = tablet_metadata->version() + 1;
        op_compaction_meta->set_compact_version(version);
        std::unique_ptr<TxnLogApplier> log_applier =
                new_txn_log_applier(Tablet(_tablet_mgr.get(), tablet_id), tablet_metadata, version, false, false);
        ASSERT_OK(log_applier->apply(log));
        ASSERT_TRUE(_tablet_mgr->update_mgr()->TEST_primary_index_refcnt(tablet_metadata->id(), 0));
    }
}

TEST_F(AlterTabletMetaTest, test_bundle_tablet_metadata) {
    lake::SchemaChangeHandler handler(_tablet_mgr.get());
    TUpdateTabletMetaInfoReq update_tablet_meta_req;
    int64_t txn_id = next_id();
    update_tablet_meta_req.__set_txn_id(txn_id);

    TTabletMetaInfo tablet_meta_info;
    auto tablet_id = _tablet_metadata->id();
    tablet_meta_info.__set_tablet_id(tablet_id);
    tablet_meta_info.__set_bundle_tablet_metadata(true);

    update_tablet_meta_req.tabletMetaInfos.push_back(tablet_meta_info);
    ASSERT_OK(handler.process_update_tablet_meta(update_tablet_meta_req));

    ASSIGN_OR_ABORT(auto txn_log, _tablet_mgr->get_txn_log(tablet_id, txn_id));
    ASSERT_TRUE(txn_log->has_op_alter_metadata());
    for (const auto& alter_meta : txn_log->op_alter_metadata().metadata_update_infos()) {
        ASSERT_TRUE(alter_meta.has_bundle_tablet_metadata());
        ASSERT_TRUE(alter_meta.bundle_tablet_metadata());
    }
    auto new_tablet_meta = publish_single_version(tablet_id, 2, txn_id);
    ASSERT_OK(new_tablet_meta.status());
}

TEST_F(AlterTabletMetaTest, test_compaction_strategy) {
    lake::SchemaChangeHandler handler(_tablet_mgr.get());
    TUpdateTabletMetaInfoReq update_tablet_meta_req;
    int64_t txn_id = next_id();
    update_tablet_meta_req.__set_txn_id(txn_id);

    TTabletMetaInfo tablet_meta_info;
    auto tablet_id = _tablet_metadata->id();
    tablet_meta_info.__set_tablet_id(tablet_id);
    tablet_meta_info.__set_compaction_strategy(TCompactionStrategy::REAL_TIME);

    update_tablet_meta_req.tabletMetaInfos.push_back(tablet_meta_info);
    ASSERT_OK(handler.process_update_tablet_meta(update_tablet_meta_req));

    ASSIGN_OR_ABORT(auto txn_log, _tablet_mgr->get_txn_log(tablet_id, txn_id));
    ASSERT_TRUE(txn_log->has_op_alter_metadata());
    auto new_tablet_meta = publish_single_version(tablet_id, 2, txn_id);
    ASSERT_OK(new_tablet_meta.status());
    ASSERT_EQ(CompactionStrategyPB::REAL_TIME, new_tablet_meta.value()->compaction_strategy());
}

namespace {

// Builds a DUP schema PB whose first `num_sort_keys` INT columns are the contiguous sort key
// (stable unique ids 100..) followed by a single INT value column.
TabletSchemaPB build_range_schema_pb(int num_sort_keys, int64_t schema_id) {
    TabletSchemaPB pb;
    pb.set_keys_type(DUP_KEYS);
    pb.set_id(schema_id);
    pb.set_schema_version(schema_id);
    pb.set_num_short_key_columns(1);
    for (int i = 0; i < num_sort_keys; ++i) {
        auto* c = pb.add_column();
        c->set_unique_id(100 + i);
        c->set_name("k" + std::to_string(i));
        c->set_type("INT");
        c->set_is_key(true);
        c->set_is_nullable(i == num_sort_keys - 1);
        c->set_aggregation("NONE");
        pb.add_sort_key_idxes(i);
    }
    auto* v = pb.add_column();
    v->set_unique_id(200);
    v->set_name("v");
    v->set_type("INT");
    v->set_is_key(false);
    v->set_is_nullable(true);
    v->set_aggregation("NONE");
    return pb;
}

void add_int_value(TuplePB* t, int32_t v) {
    auto* pb = t->add_values();
    TypeDescriptor td(TYPE_INT);
    pb->mutable_type()->CopyFrom(td.to_protobuf());
    pb->set_value(std::to_string(v));
    pb->set_variant_type(VariantTypePB::NORMAL_VALUE);
}

void add_null_value(TuplePB* t) {
    auto* pb = t->add_values();
    TypeDescriptor td(TYPE_INT);
    pb->mutable_type()->CopyFrom(td.to_protobuf());
    pb->set_variant_type(VariantTypePB::NULL_VALUE);
}

} // namespace

// apply_alter_meta_log with a range-carrying update installs the new tablet range and archives the
// pre-alter schema via the NON-CLEARING pattern, independent of `lake_enable_alter_struct`.
TEST_F(AlterTabletMetaTest, test_apply_range_alter_meta_non_clearing_archival) {
    auto saved = config::lake_enable_alter_struct;
    config::lake_enable_alter_struct = false;
    DeferOp restore([&]() { config::lake_enable_alter_struct = saved; });

    constexpr int64_t kOlderSchemaId = 400;
    constexpr int64_t kOldSchemaId = 500;
    constexpr int64_t kNewSchemaId = 501;
    constexpr uint32_t kRowset0 = 10;
    constexpr uint32_t kRowset1 = 11;
    constexpr uint32_t kRowset2 = 12;

    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(next_id());
    metadata->set_version(1);
    metadata->set_next_rowset_id(100);
    metadata->mutable_schema()->CopyFrom(build_range_schema_pb(1, kOldSchemaId));

    // Old range [(1), (2)).
    {
        auto* r = metadata->mutable_range();
        add_int_value(r->mutable_lower_bound(), 1);
        add_int_value(r->mutable_upper_bound(), 2);
        r->set_lower_bound_included(true);
        r->set_upper_bound_included(false);
    }

    // Three rowsets; only rowset0 is pre-mapped (to an even older schema).
    metadata->add_rowsets()->set_id(kRowset0);
    metadata->add_rowsets()->set_id(kRowset1);
    metadata->add_rowsets()->set_id(kRowset2);
    (*metadata->mutable_rowset_to_schema())[kRowset0] = kOlderSchemaId;
    (*metadata->mutable_historical_schemas())[kOlderSchemaId] = build_range_schema_pb(1, kOlderSchemaId);

    // Alter log: trailing sort-key ADD -> 2 sort keys, range [(1, NULL), (2, NULL)).
    TxnLogPB log;
    log.set_tablet_id(metadata->id());
    auto* upd = log.mutable_op_alter_metadata()->add_metadata_update_infos();
    upd->mutable_tablet_schema()->CopyFrom(build_range_schema_pb(2, kNewSchemaId));
    {
        auto* r = upd->mutable_tablet_range();
        add_int_value(r->mutable_lower_bound(), 1);
        add_null_value(r->mutable_lower_bound());
        add_int_value(r->mutable_upper_bound(), 2);
        add_null_value(r->mutable_upper_bound());
        r->set_lower_bound_included(true);
        r->set_upper_bound_included(false);
    }

    auto version = metadata->version() + 1;
    std::unique_ptr<TxnLogApplier> applier =
            new_txn_log_applier(Tablet(_tablet_mgr.get(), metadata->id()), metadata, version, false, false);
    ASSERT_OK(applier->apply(log));

    // New range installed with the trailing NULL sentinel.
    ASSERT_TRUE(metadata->has_range());
    ASSERT_EQ(2, metadata->range().lower_bound().values_size());
    ASSERT_EQ(2, metadata->range().upper_bound().values_size());
    ASSERT_EQ(VariantTypePB::NULL_VALUE, metadata->range().lower_bound().values(1).variant_type());
    ASSERT_EQ(VariantTypePB::NULL_VALUE, metadata->range().upper_bound().values(1).variant_type());

    // New schema installed.
    ASSERT_EQ(kNewSchemaId, metadata->schema().id());

    // Non-clearing archival: preexisting mapping/history preserved; only unmapped rowsets added.
    ASSERT_EQ(3, metadata->rowset_to_schema().size());
    ASSERT_EQ(kOlderSchemaId, metadata->rowset_to_schema().at(kRowset0)); // preserved
    ASSERT_EQ(kOldSchemaId, metadata->rowset_to_schema().at(kRowset1));   // newly mapped to old schema
    ASSERT_EQ(kOldSchemaId, metadata->rowset_to_schema().at(kRowset2));
    ASSERT_EQ(2, metadata->historical_schemas().size());
    ASSERT_TRUE(metadata->historical_schemas().count(kOlderSchemaId) > 0); // preserved, not cleared
    ASSERT_TRUE(metadata->historical_schemas().count(kOldSchemaId) > 0);   // pre-alter schema archived
}

// do_process_update_tablet_meta copies the thrift range into the proto update info (via structural
// validation) without logging raw boundary values.
TEST_F(AlterTabletMetaTest, test_do_process_update_tablet_meta_copies_range) {
    lake::SchemaChangeHandler handler(_tablet_mgr.get());

    // A DUP range tablet with a single INT sort key.
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(next_id());
    metadata->set_version(1);
    metadata->set_next_rowset_id(1);
    metadata->mutable_schema()->CopyFrom(build_range_schema_pb(1, 600));
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*metadata));
    auto tablet_id = metadata->id();

    int64_t txn_id = next_id();
    TUpdateTabletMetaInfoReq req;
    req.__set_txn_id(txn_id);

    TTabletMetaInfo info;
    info.__set_tablet_id(tablet_id);

    // New schema: 2 INT sort keys (trailing ADD).
    TTabletSchema t_schema;
    t_schema.__set_id(601);
    t_schema.__set_schema_version(2);
    t_schema.__set_short_key_column_count(1);
    t_schema.__set_keys_type(TKeysType::DUP_KEYS);
    for (int i = 0; i < 2; ++i) {
        TColumn c;
        c.__set_column_name("k" + std::to_string(i));
        c.column_type.__set_type(TPrimitiveType::INT);
        c.__set_col_unique_id(100 + i);
        c.__set_is_key(true);
        c.__set_is_allow_null(i == 1);
        t_schema.columns.push_back(c);
    }
    {
        TColumn v;
        v.__set_column_name("v");
        v.column_type.__set_type(TPrimitiveType::INT);
        v.__set_col_unique_id(200);
        v.__set_is_key(false);
        v.__set_is_allow_null(true);
        t_schema.columns.push_back(v);
    }
    t_schema.sort_key_idxes.push_back(0);
    t_schema.sort_key_idxes.push_back(1);
    t_schema.__isset.sort_key_idxes = true;
    info.__set_tablet_schema(t_schema);

    // Range [(1, NULL), (2, NULL)).
    TTabletRange t_range;
    TTypeDesc int_type;
    int_type.types.resize(1);
    int_type.__isset.types = true;
    int_type.types[0].type = TTypeNodeType::SCALAR;
    int_type.types[0].scalar_type.__set_type(TPrimitiveType::INT);
    int_type.types[0].__isset.scalar_type = true;
    auto make_variant = [&](std::optional<int32_t> v) {
        TVariant var;
        var.__set_type(int_type);
        if (v.has_value()) {
            var.__set_value(std::to_string(*v));
            var.__set_variant_type(TVariantType::NORMAL_VALUE);
        } else {
            var.__set_variant_type(TVariantType::NULL_VALUE);
        }
        return var;
    };
    TTuple lower;
    lower.values.push_back(make_variant(1));
    lower.values.push_back(make_variant(std::nullopt));
    TTuple upper;
    upper.values.push_back(make_variant(2));
    upper.values.push_back(make_variant(std::nullopt));
    t_range.__set_lower_bound(lower);
    t_range.__set_upper_bound(upper);
    t_range.__set_lower_bound_included(true);
    t_range.__set_upper_bound_included(false);
    info.__set_tablet_range(t_range);

    req.tabletMetaInfos.push_back(info);
    ASSERT_OK(handler.process_update_tablet_meta(req));

    // The generated txn log carries the range copied into the proto update info.
    ASSIGN_OR_ABORT(auto txn_log, _tablet_mgr->get_txn_log(tablet_id, txn_id));
    ASSERT_TRUE(txn_log->has_op_alter_metadata());
    ASSERT_EQ(1, txn_log->op_alter_metadata().metadata_update_infos_size());
    const auto& upd = txn_log->op_alter_metadata().metadata_update_infos(0);
    ASSERT_TRUE(upd.has_tablet_range());
    ASSERT_EQ(2, upd.tablet_range().lower_bound().values_size());
    ASSERT_EQ(2, upd.tablet_range().upper_bound().values_size());
    ASSERT_EQ("1", upd.tablet_range().lower_bound().values(0).value());
    ASSERT_EQ(VariantTypePB::NULL_VALUE, upd.tablet_range().lower_bound().values(1).variant_type());
    ASSERT_TRUE(upd.tablet_range().lower_bound_included());
    ASSERT_FALSE(upd.tablet_range().upper_bound_included());
}

TEST_F(AlterTabletMetaTest, test_alter_flat_json_config) {
    lake::SchemaChangeHandler handler(_tablet_mgr.get());
    TUpdateTabletMetaInfoReq update_tablet_meta_req;
    int64_t txn_id = next_id();
    update_tablet_meta_req.__set_txn_id(txn_id);

    TTabletMetaInfo tablet_meta_info;
    auto tablet_id = _tablet_metadata->id();
    tablet_meta_info.__set_tablet_id(tablet_id);
    TFlatJsonConfig flat_json_config;
    flat_json_config.__set_flat_json_enable(true);
    flat_json_config.__set_flat_json_null_factor(0.25);
    flat_json_config.__set_flat_json_sparsity_factor(0.75);
    flat_json_config.__set_flat_json_column_max(12);
    flat_json_config.__set_version(3);
    tablet_meta_info.__set_flat_json_config(flat_json_config);

    update_tablet_meta_req.tabletMetaInfos.push_back(tablet_meta_info);
    ASSERT_OK(handler.process_update_tablet_meta(update_tablet_meta_req));

    ASSIGN_OR_ABORT(auto txn_log, _tablet_mgr->get_txn_log(tablet_id, txn_id));
    ASSERT_TRUE(txn_log->has_op_alter_metadata());
    auto new_tablet_meta = publish_single_version(tablet_id, 2, txn_id);
    ASSERT_OK(new_tablet_meta.status());
    ASSERT_TRUE(new_tablet_meta.value()->has_flat_json_config());
    const auto& flat_json_config_pb = new_tablet_meta.value()->flat_json_config();
    ASSERT_TRUE(flat_json_config_pb.flat_json_enable());
    ASSERT_DOUBLE_EQ(0.25, flat_json_config_pb.flat_json_null_factor());
    ASSERT_DOUBLE_EQ(0.75, flat_json_config_pb.flat_json_sparsity_factor());
    ASSERT_EQ(12, flat_json_config_pb.flat_json_max_column_max());
    ASSERT_EQ(3, flat_json_config_pb.version());
}

} // namespace starrocks::lake
