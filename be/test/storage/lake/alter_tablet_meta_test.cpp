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
#include "base/utility/defer_op.h"
#include "base/testutil/id_generator.h"
#include "base/testutil/sync_point.h"
#include "common/config_json_flat_fwd.h"
#include "common/config_primary_key_fwd.h"
#include "fs/fs_util.h"
#include "storage/chunk_helper.h"
#include "storage/lake/schema_change.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/tablet_writer.h"
#include "storage/lake/txn_log_applier.h"
#include "test_util.h"

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

TEST_F(AlterTabletMetaTest, test_alter_enable_persistent_index) {
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

TEST_F(AlterTabletMetaTest, test_compaction_read_derive_uses_versioned_flat_json_config) {
    // (id INT KEY, j JSON) tablet with two flat segments: key 'a' appears in both segments,
    // key 'b' only in the first, so the source-level ratio of 'b' is 50%.
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(next_id());
    metadata->set_version(1);
    metadata->set_next_rowset_id(1);
    auto* schema_pb = metadata->mutable_schema();
    schema_pb->set_keys_type(DUP_KEYS);
    schema_pb->set_id(next_id());
    schema_pb->set_num_short_key_columns(1);
    schema_pb->set_num_rows_per_row_block(65535);
    {
        auto* c = schema_pb->add_column();
        c->set_unique_id(next_id());
        c->set_name("id");
        c->set_type("INT");
        c->set_is_key(true);
        c->set_is_nullable(false);
    }
    {
        auto* c = schema_pb->add_column();
        c->set_unique_id(next_id());
        c->set_name("j");
        c->set_type("JSON");
        c->set_is_key(false);
        c->set_is_nullable(true);
        c->set_aggregation("NONE");
    }
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*metadata));
    int64_t tablet_id = metadata->id();
    auto tablet_schema = TabletSchema::create(metadata->schema());
    auto schema = std::make_shared<Schema>(ChunkHelper::convert_schema(tablet_schema));

    auto write_rows = [&](int64_t new_version, bool with_b) {
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id));
        ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, txn_id));
        ASSERT_OK(writer->open());
        auto id_col = Int32Column::create();
        auto json_col = JsonColumn::create();
        auto null_col = NullColumn::create();
        for (int i = 0; i < 100; i++) {
            id_col->append(i);
            auto jv = with_b ? JsonValue::parse(R"({"a": 1, "b": 2})") : JsonValue::parse(R"({"a": 1})");
            ASSERT_TRUE(jv.ok());
            json_col->append(&jv.value());
            null_col->append(0);
        }
        auto j_col = NullableColumn::create(std::move(json_col), std::move(null_col));
        Chunk chunk({std::move(id_col), std::move(j_col)}, schema);
        ASSERT_OK(writer->write(chunk));
        ASSERT_OK(writer->finish());
        auto txn_log = std::make_shared<TxnLog>();
        txn_log->set_tablet_id(tablet_id);
        txn_log->set_txn_id(txn_id);
        auto* op_write = txn_log->mutable_op_write();
        for (const auto& f : writer->segments()) {
            op_write->mutable_rowset()->add_segment_metas()->set_filename(f.path);
        }
        op_write->mutable_rowset()->set_num_rows(writer->num_rows());
        op_write->mutable_rowset()->set_data_size(writer->data_size());
        op_write->mutable_rowset()->set_overlapped(false);
        ASSERT_OK(_tablet_mgr->put_txn_log(txn_log));
        writer->close();
        ASSERT_OK(publish_single_version(tablet_id, new_version, txn_id).status());
    };
    write_rows(2, true);
    write_rows(3, false);

    // v4 = v3 + a table-level flat_json config with a high sparsity factor: only keys present in
    // >= 80% of the sources should be derived — 'a' (2/2) qualifies, 'b' (1/2) does not.
    ASSIGN_OR_ABORT(auto v3, _tablet_mgr->get_tablet_metadata(tablet_id, 3));
    auto v4 = std::make_shared<TabletMetadata>(*v3);
    v4->set_version(4);
    auto* cfg = v4->mutable_flat_json_config();
    cfg->set_flat_json_enable(true);
    cfg->set_flat_json_null_factor(1);
    cfg->set_flat_json_sparsity_factor(0.8);
    cfg->set_flat_json_max_column_max(100);
    // Persist v4 while keeping the latest-metadata cache stale (still v3, which carries no
    // flat_json_config) — the state of a node whose cache no longer holds this tablet.
    SyncPoint::GetInstance()->SetCallBack("TabletManager::skip_cache_latest_metadata",
                                          [](void* arg) { *(bool*)arg = true; });
    SyncPoint::GetInstance()->EnableProcessing();
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(v4));
    SyncPoint::GetInstance()->ClearCallBack("TabletManager::skip_cache_latest_metadata");
    SyncPoint::GetInstance()->DisableProcessing();

    // A compaction-style read over v4 must derive flat_json paths from v4 itself, not from the
    // best-effort latest-metadata cache.
    TabletReader reader(_tablet_mgr.get(), v4, *schema);
    ASSERT_OK(reader.prepare());
    TabletReaderParams params;
    params.reader_type = ReaderType::READER_BASE_COMPACTION;
    params.chunk_size = 1024;
    std::vector<ColumnAccessPathPtr> access_paths;
    params.column_access_paths = &access_paths;
    ASSERT_OK(reader.open(params));
    reader.close();

    ASSERT_EQ(1, access_paths.size());
    std::vector<ColumnAccessPath*> leafs;
    access_paths[0]->get_all_leafs(&leafs);
    ASSERT_EQ(1, leafs.size());
    EXPECT_EQ("a", leafs[0]->path());
}

} // namespace starrocks::lake
