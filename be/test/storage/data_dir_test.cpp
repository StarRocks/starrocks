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

#include "storage/data_dir.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <memory>

#include "gen_cpp/Types_types.h"
#include "gen_cpp/olap_file.pb.h"
#include "storage/olap_common.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/rowset/rowset_meta_manager.h"
#include "storage/tablet.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_meta.h"
#include "storage/tablet_meta_manager.h"
#include "storage/txn_manager.h"

namespace starrocks {

namespace sfs = std::filesystem;

class DataDirTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create TabletManager and TxnManager
        _tablet_manager = std::make_unique<TabletManager>(1);
        _txn_manager = std::make_unique<TxnManager>(1, 1, 1);
        sfs::path tmp = sfs::temp_directory_path();
        sfs::path dir = tmp / "data_dir_test";
        sfs::remove_all(dir);
        CHECK(sfs::create_directory(dir));
        _data_dir =
                std::make_unique<DataDir>(dir.string(), TStorageMedium::HDD, _tablet_manager.get(), _txn_manager.get());
        ASSERT_TRUE(_data_dir->init().ok());
    }

    void TearDown() override { _data_dir.reset(); }

    std::unique_ptr<DataDir> _data_dir;
    std::unique_ptr<TabletManager> _tablet_manager;
    std::unique_ptr<TxnManager> _txn_manager;
};

// NOLINTNEXTLINE
TEST_F(DataDirTest, test_load_committed_rowset_meta) {
    // Create tablet using TabletManager
    TCreateTabletReq create_tablet_req;
    create_tablet_req.tablet_id = 2001;
    create_tablet_req.__set_version(1);
    create_tablet_req.__set_version_hash(0);
    create_tablet_req.tablet_schema.__set_id(1);
    create_tablet_req.tablet_schema.__set_schema_hash(3001);
    create_tablet_req.tablet_schema.__set_keys_type(TKeysType::DUP_KEYS);
    create_tablet_req.tablet_schema.__set_short_key_column_count(1);
    create_tablet_req.tablet_schema.__set_storage_type(TStorageType::COLUMN);
    TColumn k0;
    k0.column_name = "c0";
    k0.__set_is_key(true);
    TColumnType ctype;
    ctype.__set_type(TPrimitiveType::INT);
    ctype.__set_len(4);
    k0.__set_column_type(ctype);
    create_tablet_req.tablet_schema.columns.push_back(k0);

    std::vector<DataDir*> data_dirs;
    data_dirs.push_back(_data_dir.get());
    Status create_st = _tablet_manager->create_tablet(create_tablet_req, data_dirs);
    ASSERT_TRUE(create_st.ok()) << create_st.to_string();

    // Get the created tablet
    TabletSharedPtr tablet = _tablet_manager->get_tablet(2001);
    ASSERT_TRUE(tablet != nullptr);

    // Create a rowset meta with COMMITTED state
    RowsetMetaPB rowset_meta_pb;
    rowset_meta_pb.set_rowset_id("001");
    rowset_meta_pb.set_rowset_seg_id(0);
    rowset_meta_pb.set_partition_id(1001);
    rowset_meta_pb.set_txn_id(5001);
    rowset_meta_pb.set_tablet_id(2001);
    rowset_meta_pb.set_tablet_schema_hash(3001);
    // Use the actual tablet uid from the created tablet
    rowset_meta_pb.mutable_tablet_uid()->set_lo(tablet->tablet_uid().lo);
    rowset_meta_pb.mutable_tablet_uid()->set_hi(tablet->tablet_uid().hi);
    rowset_meta_pb.set_rowset_state(RowsetStatePB::COMMITTED);
    rowset_meta_pb.set_start_version(1);
    rowset_meta_pb.set_end_version(1);
    rowset_meta_pb.set_num_rows(100);
    rowset_meta_pb.set_num_segments(1);
    rowset_meta_pb.set_data_disk_size(1024);
    rowset_meta_pb.set_empty(false);
    rowset_meta_pb.set_creation_time(time(nullptr));
    rowset_meta_pb.mutable_load_id()->set_hi(1);
    rowset_meta_pb.mutable_load_id()->set_lo(2);
    rowset_meta_pb.set_deprecated_rowset_id(0);

    // Copy tablet schema to rowset meta from the created tablet
    TabletSchemaPB schema_pb;
    tablet->tablet_schema()->to_schema_pb(&schema_pb);
    rowset_meta_pb.mutable_tablet_schema()->CopyFrom(schema_pb);

    // Save rowset meta to KVStore
    TabletUid tablet_uid = tablet->tablet_uid();
    ASSERT_TRUE(RowsetMetaManager::save(_data_dir->get_meta(), tablet_uid, rowset_meta_pb).ok());
    _tablet_manager->drop_tablet(2001, kKeepMetaAndFiles);
    _data_dir->load();
}

} // namespace starrocks
