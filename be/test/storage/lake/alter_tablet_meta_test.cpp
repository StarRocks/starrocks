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
#include "fs/fs_util.h"
#include "storage/chunk_helper.h"
#include "storage/lake/schema_change.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_writer.h"
#include "test_util.h"
#include "testutil/id_generator.h"

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
    Chunk chunk0({c0, c1}, _schema);
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
    for (auto& f : writer->files()) {
        op_write->mutable_rowset()->add_segments(std::move(f.path));
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

} // namespace starrocks::lake
