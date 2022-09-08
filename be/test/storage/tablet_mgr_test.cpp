// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/olap/tablet_mgr_test.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
#include <filesystem>
#include <string>

#include "gtest/gtest.h"
#include "storage/kv_store.h"
#include "storage/storage_engine.h"
#include "storage/tablet_meta_manager.h"
#include "storage/txn_manager.h"
#include "util/file_utils.h"

#ifndef BE_TEST
#define BE_TEST
#endif

using std::string;

namespace starrocks {

static StorageEngine* k_engine = nullptr;

class TabletMgrTest : public testing::Test {
public:
    virtual void SetUp() {
        config::tablet_map_shard_size = 1;
        config::txn_map_shard_size = 1;
        config::txn_shard_size = 1;
        string test_engine_data_path = "./be/test/storage/test_data/tablet_mgr_test/data";
        _engine_data_path = "./be/test/storage/test_data/tablet_mgr_test/tmp";
        std::filesystem::remove_all(_engine_data_path);
        FileUtils::create_dir(_engine_data_path);
        FileUtils::create_dir(_engine_data_path + "/meta");

        std::vector<StorePath> paths;
        paths.emplace_back("_engine_data_path", -1);
        EngineOptions options;
        options.store_paths = paths;
        options.backend_uid = UniqueId::gen_uid();
        if (k_engine == nullptr) {
            k_engine = new StorageEngine(options);
        }

        _data_dir = new DataDir(_engine_data_path);
        _data_dir->init();
        string tmp_data_path = _engine_data_path + "/data";
        if (std::filesystem::exists(tmp_data_path)) {
            std::filesystem::remove_all(tmp_data_path);
        }
        copy_dir(test_engine_data_path, tmp_data_path);
        _tablet_id = 15007;
        _schema_hash = 368169781;
        _tablet_data_path = tmp_data_path + "/" + std::to_string(0) + "/" + std::to_string(_tablet_id) + "/" +
                            std::to_string(_schema_hash);
        _tablet_mgr.reset(new TabletManager(1));
    }

    virtual void TearDown() {
        delete _data_dir;
        if (std::filesystem::exists(_engine_data_path)) {
            ASSERT_TRUE(std::filesystem::remove_all(_engine_data_path));
        }
    }

protected:
    DataDir* _data_dir;
    std::string _engine_data_path;
    int64_t _tablet_id;
    int32_t _schema_hash;
    string _tablet_data_path;
    std::unique_ptr<TabletManager> _tablet_mgr;
};

TEST_F(TabletMgrTest, CreateTablet) {
    TColumnType col_type;
    col_type.__set_type(TPrimitiveType::SMALLINT);
    TColumn col1;
    col1.__set_column_name("col1");
    col1.__set_column_type(col_type);
    col1.__set_is_key(true);
    std::vector<TColumn> cols;
    cols.push_back(col1);
    TTabletSchema tablet_schema;
    tablet_schema.__set_short_key_column_count(1);
    tablet_schema.__set_schema_hash(3333);
    tablet_schema.__set_keys_type(TKeysType::AGG_KEYS);
    tablet_schema.__set_storage_type(TStorageType::COLUMN);
    tablet_schema.__set_columns(cols);
    TCreateTabletReq create_tablet_req;
    create_tablet_req.__set_tablet_schema(tablet_schema);
    create_tablet_req.__set_tablet_id(111);
    create_tablet_req.__set_version(2);
    create_tablet_req.__set_version_hash(3333);
    std::vector<DataDir*> data_dirs;
    data_dirs.push_back(_data_dir);
    tatus create_st = _tablet_mgr->create_tablet(create_tablet_req, data_dirs);
    ASSERT_TRUE(create_st.ok());
    TabletSharedPtr tablet = _tablet_mgr->get_tablet(111, 3333);
    ASSERT_TRUE(tablet != nullptr);
    // check dir exist
    bool dir_exist = FileUtils::check_exist(tablet->schema_hash_path());
    ASSERT_TRUE(dir_exist);
    // check meta has this tablet
    TabletMetaSharedPtr new_tablet_meta(new TabletMeta());
    Status check_meta_st = TabletMetaManager::get_meta(_data_dir, 111, 3333, new_tablet_meta);
    ASSERT_TRUE(check_meta_st.ok());

    // retry create should be successfully
    create_st = _tablet_mgr->create_tablet(create_tablet_req, data_dirs);
    ASSERT_TRUE(create_st.ok());

    // create tablet with different schema hash should be error
    tablet_schema.__set_schema_hash(4444);
    create_tablet_req.__set_tablet_schema(tablet_schema);
    create_st = _tablet_mgr->create_tablet(create_tablet_req, data_dirs);
    ASSERT_TRUE(create_st.is_already_exist());
}

TEST_F(TabletMgrTest, DropTablet) {
    TColumnType col_type;
    col_type.__set_type(TPrimitiveType::SMALLINT);
    TColumn col1;
    col1.__set_column_name("col1");
    col1.__set_column_type(col_type);
    col1.__set_is_key(true);
    std::vector<TColumn> cols;
    cols.push_back(col1);
    TTabletSchema tablet_schema;
    tablet_schema.__set_short_key_column_count(1);
    tablet_schema.__set_schema_hash(3333);
    tablet_schema.__set_keys_type(TKeysType::AGG_KEYS);
    tablet_schema.__set_storage_type(TStorageType::COLUMN);
    tablet_schema.__set_columns(cols);
    TCreateTabletReq create_tablet_req;
    create_tablet_req.__set_tablet_schema(tablet_schema);
    create_tablet_req.__set_tablet_id(111);
    create_tablet_req.__set_version(2);
    create_tablet_req.__set_version_hash(3333);
    std::vector<DataDir*> data_dirs;
    data_dirs.push_back(_data_dir);
    Status create_st = _tablet_mgr->create_tablet(create_tablet_req, data_dirs);
    ASSERT_TRUE(create_st.ok());
    TabletSharedPtr tablet = _tablet_mgr->get_tablet(111, 3333);
    ASSERT_TRUE(tablet != nullptr);

    // drop unexist tablet will be success
    Status drop_st = _tablet_mgr->drop_tablet(111, 4444, false);
    ASSERT_TRUE(drop_st.ok());
    tablet = _tablet_mgr->get_tablet(111, 3333);
    ASSERT_TRUE(tablet != nullptr);

    // drop exist tablet will be success
    drop_st = _tablet_mgr->drop_tablet(111, 3333, false);
    ASSERT_TRUE(drop_st.ok());
    tablet = _tablet_mgr->get_tablet(111, 3333);
    ASSERT_TRUE(tablet == nullptr);
    tablet = _tablet_mgr->get_tablet(111, 3333, true);
    ASSERT_TRUE(tablet != nullptr);

    // check dir exist
    std::string tablet_path = tablet->schema_hash_path();
    bool dir_exist = FileUtils::check_exist(tablet_path);
    ASSERT_TRUE(dir_exist);

    // do trash sweep, tablet will not be garbage collected
    // because tablet ptr referenced it
    Status trash_st = _tablet_mgr->start_trash_sweep();
    ASSERT_TRUE(trash_st.ok()) << trash_st.to_string();
    tablet = _tablet_mgr->get_tablet(111, 3333, true);
    ASSERT_TRUE(tablet != nullptr);
    dir_exist = FileUtils::check_exist(tablet_path);
    ASSERT_TRUE(dir_exist);

    // reset tablet ptr
    tablet.reset();
    trash_st = _tablet_mgr->start_trash_sweep();
    ASSERT_TRUE(trash_st.ok()) << trash_st.to_string();
    tablet = _tablet_mgr->get_tablet(111, 3333, true);
    ASSERT_TRUE(tablet == nullptr);
    dir_exist = FileUtils::check_exist(tablet_path);
    ASSERT_TRUE(!dir_exist);
}

TEST_F(TabletMgrTest, GetRowsetId) {
    // normal case
    {
        std::string path = _engine_data_path + "/data/0/15007/368169781";
        TTabletId tid;
        TSchemaHash schema_hash;
        ASSERT_TRUE(_tablet_mgr->get_tablet_id_and_schema_hash_from_path(path, &tid, &schema_hash));
        ASSERT_EQ(15007, tid);
        ASSERT_EQ(368169781, schema_hash);
    }
    {
        std::string path = _engine_data_path + "/data/0/15007/368169781/";
        TTabletId tid;
        TSchemaHash schema_hash;
        ASSERT_TRUE(_tablet_mgr->get_tablet_id_and_schema_hash_from_path(path, &tid, &schema_hash));
        ASSERT_EQ(15007, tid);
        ASSERT_EQ(368169781, schema_hash);
    }
    // normal case
    {
        std::string path =
                _engine_data_path + "/data/0/15007/368169781/020000000000000100000000000000020000000000000003_0_0.dat";
        TTabletId tid;
        TSchemaHash schema_hash;
        ASSERT_TRUE(_tablet_mgr->get_tablet_id_and_schema_hash_from_path(path, &tid, &schema_hash));
        ASSERT_EQ(15007, tid);
        ASSERT_EQ(368169781, schema_hash);

        RowsetId id;
        ASSERT_TRUE(_tablet_mgr->get_rowset_id_from_path(path, &id));
        EXPECT_EQ(2UL << 56 | 1, id.hi);
        ASSERT_EQ(2, id.mi);
        ASSERT_EQ(3, id.lo);
    }
    // empty tablet directory
    {
        std::string path = _engine_data_path + "/data/0/15007";
        TTabletId tid;
        TSchemaHash schema_hash;
        ASSERT_TRUE(_tablet_mgr->get_tablet_id_and_schema_hash_from_path(path, &tid, &schema_hash));
        ASSERT_EQ(15007, tid);
        ASSERT_EQ(0, schema_hash);

        RowsetId id;
        ASSERT_FALSE(_tablet_mgr->get_rowset_id_from_path(path, &id));
    }
    // empty tablet directory
    {
        std::string path = _engine_data_path + "/data/0/15007/";
        TTabletId tid;
        TSchemaHash schema_hash;
        ASSERT_TRUE(_tablet_mgr->get_tablet_id_and_schema_hash_from_path(path, &tid, &schema_hash));
        ASSERT_EQ(15007, tid);
        ASSERT_EQ(0, schema_hash);
    }
    // empty tablet directory
    {
        std::string path = _engine_data_path + "/data/0/15007abc";
        TTabletId tid;
        TSchemaHash schema_hash;
        ASSERT_FALSE(_tablet_mgr->get_tablet_id_and_schema_hash_from_path(path, &tid, &schema_hash));
    }
    // not match pattern
    {
        std::string path =
                _engine_data_path + "/data/0/15007/123abc/020000000000000100000000000000020000000000000003_0_0.dat";
        TTabletId tid;
        TSchemaHash schema_hash;
        ASSERT_FALSE(_tablet_mgr->get_tablet_id_and_schema_hash_from_path(path, &tid, &schema_hash));

        RowsetId id;
        ASSERT_FALSE(_tablet_mgr->get_rowset_id_from_path(path, &id));
    }
}

} // namespace starrocks
