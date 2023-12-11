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
#include "storage/lake/schema_change.h"
#include "storage/lake/tablet_manager.h"
#include "test_util.h"
#include "testutil/id_generator.h"

namespace starrocks::lake {

using namespace starrocks;

class AlterTabletMetaTest : public TestBase {
public:
    AlterTabletMetaTest() : TestBase(kTestDirectory) {
        _tablet_metadata = std::make_unique<TabletMetadata>();
        _tablet_metadata->set_id(next_id());
        _tablet_metadata->set_version(1);

        auto base_schema = _tablet_metadata->mutable_schema();
        base_schema->set_id(next_id());
        base_schema->set_keys_type(KeysType::PRIMARY_KEYS);
    }

    void SetUp() override {
        clear_and_init_test_dir();

        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override { remove_test_dir_ignore_error(); }

protected:
    constexpr static const char* const kTestDirectory = "test_alter_tablet_meta";

    std::unique_ptr<TabletMetadata> _tablet_metadata;
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
    ASSERT_EQ("txn_id not set in request", status.get_error_msg());
}

TEST_F(AlterTabletMetaTest, test_alter_enable_persistent_index) {
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

    TTabletMetaInfo tablet_meta_info2;
    tablet_meta_info2.__set_tablet_id(tablet_id);
    tablet_meta_info2.__set_meta_type(TTabletMetaType::ENABLE_PERSISTENT_INDEX);
    tablet_meta_info2.__set_enable_persistent_index(false);

    update_tablet_meta_req2.tabletMetaInfos.push_back(tablet_meta_info2);
    ASSERT_OK(handler.process_update_tablet_meta(update_tablet_meta_req2));

    auto new_tablet_meta2 = publish_single_version(tablet_id, 3, txn_id2);
    ASSERT_OK(new_tablet_meta2.status());
    ASSERT_EQ(false, new_tablet_meta2.value()->enable_persistent_index());
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

TEST_F(AlterTabletMetaTest, test_alter_not_persistent_index) {
    lake::SchemaChangeHandler handler(_tablet_mgr.get());
    TUpdateTabletMetaInfoReq update_tablet_meta_req;
    int64_t txn_id = 1;
    update_tablet_meta_req.__set_txn_id(txn_id);

    TTabletMetaInfo tablet_meta_info;
    auto tablet_id = _tablet_metadata->id();
    tablet_meta_info.__set_tablet_id(tablet_id);
    tablet_meta_info.__set_meta_type(TTabletMetaType::INMEMORY);

    update_tablet_meta_req.tabletMetaInfos.push_back(tablet_meta_info);
    ASSERT_ERROR(handler.process_update_tablet_meta(update_tablet_meta_req));
}

} // namespace starrocks::lake