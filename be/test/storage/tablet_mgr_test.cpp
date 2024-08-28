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
#include <memory>
#include <string>

#include "fs/fs_util.h"
#include "gtest/gtest.h"
#include "runtime/mem_tracker.h"
#include "storage/chunk_helper.h"
#include "storage/chunk_iterator.h"
#include "storage/compaction_manager.h"
#include "storage/kv_store.h"
#include "storage/olap_common.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/column_reader.h"
#include "storage/rowset/metadata_cache.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/rowset/segment_options.h"
#include "storage/rowset/segment_writer.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_meta_manager.h"
#include "storage/tablet_schema.h"
#include "storage/tablet_schema_helper.h"
#include "storage/txn_manager.h"

#ifndef BE_TEST
#define BE_TEST
#endif

using std::string;

namespace starrocks {

class TabletMgrTest : public testing::Test {
public:
    void SetUp() override {
        config::enable_event_based_compaction_framework = false;
        config::tablet_map_shard_size = 1;
        config::txn_map_shard_size = 1;
        config::txn_shard_size = 1;

        _engine_data_paths.resize(2);
        for (int i = 0; i < 2; i++) {
            _engine_data_paths[i] = fmt::format("./be/test/storage/test_data/tablet_mgr_test/tmp_{}", i);
            std::filesystem::remove_all(_engine_data_paths[i]);
            fs::create_directories(_engine_data_paths[i]);
            fs::create_directories(_engine_data_paths[i] + "/meta");

            std::vector<StorePath> paths;
            paths.emplace_back("_engine_data_path");

            auto data_dir = new DataDir(_engine_data_paths[i]);
            data_dir->init();
            _data_dirs.push_back(data_dir);
        }

        _tablet_id = 15007;
        _schema_hash = 368169781;
        _tablet_mgr = std::make_unique<TabletManager>(1);
    }

    void TearDown() override {
        for (int i = 0; i < 2; i++) {
            delete _data_dirs[i];
            if (std::filesystem::exists(_engine_data_paths[i])) {
                ASSERT_TRUE(std::filesystem::remove_all(_engine_data_paths[i]));
            }
        }
    }

    TCreateTabletReq get_create_tablet_request(int64_t tablet_id, int schema_hash) {
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
        tablet_schema.__set_schema_hash(schema_hash);
        tablet_schema.__set_keys_type(TKeysType::AGG_KEYS);
        tablet_schema.__set_storage_type(TStorageType::COLUMN);
        tablet_schema.__set_columns(cols);
        TCreateTabletReq create_tablet_req;
        create_tablet_req.__set_tablet_schema(tablet_schema);
        create_tablet_req.__set_tablet_id(tablet_id);
        create_tablet_req.__set_version(2);
        create_tablet_req.__set_version_hash(0);
        return create_tablet_req;
    }

protected:
    std::vector<DataDir*> _data_dirs;
    std::vector<std::string> _engine_data_paths;
    int64_t _tablet_id;
    int32_t _schema_hash;
    string _tablet_data_path;
    std::unique_ptr<TabletManager> _tablet_mgr;
};

TEST_F(TabletMgrTest, CreateTablet) {
    TCreateTabletReq create_tablet_req = get_create_tablet_request(111, 3333);
    std::vector<DataDir*> data_dirs;
    data_dirs.push_back(_data_dirs[0]);
    Status create_st = _tablet_mgr->create_tablet(create_tablet_req, data_dirs);
    ASSERT_TRUE(create_st.ok());
    TabletSharedPtr tablet = _tablet_mgr->get_tablet(111);
    ASSERT_TRUE(tablet != nullptr);
    // check dir exist
    bool dir_exist = fs::path_exist(tablet->schema_hash_path());
    ASSERT_TRUE(dir_exist);
    // check meta has this tablet
    TabletMetaSharedPtr new_tablet_meta(new TabletMeta());
    Status check_meta_st = TabletMetaManager::get_tablet_meta(_data_dirs[0], 111, 3333, new_tablet_meta.get());
    ASSERT_TRUE(check_meta_st.ok());

    // retry create should be successfully
    create_st = _tablet_mgr->create_tablet(create_tablet_req, data_dirs);
    ASSERT_TRUE(create_st.ok());

    // create tablet with different schema hash should return ok
    create_tablet_req = get_create_tablet_request(111, 4444);
    create_st = _tablet_mgr->create_tablet(create_tablet_req, data_dirs);
    ASSERT_TRUE(create_st.ok());
}

TEST_F(TabletMgrTest, DropTablet) {
    TCreateTabletReq create_tablet_req = get_create_tablet_request(111, 3333);
    std::vector<DataDir*> data_dirs;
    data_dirs.push_back(_data_dirs[0]);
    Status create_st = _tablet_mgr->create_tablet(create_tablet_req, data_dirs);
    ASSERT_TRUE(create_st.ok());
    TabletSharedPtr tablet = _tablet_mgr->get_tablet(111);
    ASSERT_TRUE(tablet != nullptr);

    // drop exist tablet will be success
    auto drop_st = _tablet_mgr->drop_tablet(111, kMoveFilesToTrash);
    ASSERT_TRUE(drop_st.ok());
    tablet = _tablet_mgr->get_tablet(111);
    ASSERT_TRUE(tablet == nullptr);
    tablet = _tablet_mgr->get_tablet(111, true);
    ASSERT_TRUE(tablet != nullptr);

    // check dir exist
    std::string tablet_path = tablet->schema_hash_path();
    bool dir_exist = fs::path_exist(tablet_path);
    ASSERT_TRUE(dir_exist);

    // do trash sweep, tablet will not be garbage collected
    // because tablet ptr referenced it
    Status trash_st = _tablet_mgr->start_trash_sweep();
    ASSERT_TRUE(trash_st.ok()) << trash_st.to_string();
    tablet = _tablet_mgr->get_tablet(111, true);
    ASSERT_TRUE(tablet != nullptr);
    dir_exist = fs::path_exist(tablet_path);
    ASSERT_TRUE(dir_exist);

    // reset tablet ptr
    tablet.reset();
    trash_st = _tablet_mgr->start_trash_sweep();
    ASSERT_TRUE(trash_st.ok()) << trash_st.to_string();
    tablet = _tablet_mgr->get_tablet(111, true);
    ASSERT_TRUE(tablet == nullptr);
    dir_exist = fs::path_exist(tablet_path);
    ASSERT_TRUE(!dir_exist);
}

TEST_F(TabletMgrTest, LoadExistTabletFromMeta) {
    {
        TCreateTabletReq create_tablet_req = get_create_tablet_request(111, 3333);
        std::vector<DataDir*> data_dirs;
        data_dirs.push_back(_data_dirs[0]);
        Status create_st = _tablet_mgr->create_tablet(create_tablet_req, data_dirs);
        ASSERT_TRUE(create_st.ok());
        TabletSharedPtr tablet = _tablet_mgr->get_tablet(111);
        ASSERT_TRUE(tablet != nullptr);
        std::string meta_str;
        ASSERT_TRUE(tablet->tablet_meta()->serialize(&meta_str).ok());
        Status st = _tablet_mgr->load_tablet_from_meta(_data_dirs[0], 111, tablet->schema_hash(), meta_str, false,
                                                       false, false, false);
        ASSERT_TRUE(st.code() == TStatusCode::INTERNAL_ERROR);
    }
    {
        // expect skip this tablet
        TCreateTabletReq create_tablet_req = get_create_tablet_request(111, 4444);
        std::vector<DataDir*> data_dirs;
        data_dirs.push_back(_data_dirs[1]);
        Status create_st = _tablet_mgr->create_tablet(create_tablet_req, data_dirs);
        ASSERT_TRUE(create_st.ok());
        TabletSharedPtr tablet = _tablet_mgr->get_tablet(111);
        ASSERT_TRUE(tablet != nullptr);
        std::string meta_str;
        ASSERT_TRUE(tablet->tablet_meta()->serialize(&meta_str).ok());
        Status st = _tablet_mgr->load_tablet_from_meta(_data_dirs[1], 111, tablet->schema_hash(), meta_str, false,
                                                       false, false, false);
        ASSERT_TRUE(st.is_already_exist());
    }
    // check tablet
    TabletSharedPtr tablet = _tablet_mgr->get_tablet(111);
    ASSERT_TRUE(tablet != nullptr);
    ASSERT_TRUE(tablet->schema_hash() == 3333);
}

TEST_F(TabletMgrTest, GetRowsetId) {
    // normal case
    {
        std::string path = _engine_data_paths[0] + "/data/0/15007/368169781";
        TTabletId tid;
        TSchemaHash schema_hash;
        ASSERT_TRUE(_tablet_mgr->get_tablet_id_and_schema_hash_from_path(path, &tid, &schema_hash));
        ASSERT_EQ(15007, tid);
        ASSERT_EQ(368169781, schema_hash);
    }
    {
        std::string path = _engine_data_paths[0] + "/data/0/15007/368169781/";
        TTabletId tid;
        TSchemaHash schema_hash;
        ASSERT_TRUE(_tablet_mgr->get_tablet_id_and_schema_hash_from_path(path, &tid, &schema_hash));
        ASSERT_EQ(15007, tid);
        ASSERT_EQ(368169781, schema_hash);
    }
    // normal case
    {
        std::string path = _engine_data_paths[0] +
                           "/data/0/15007/368169781/020000000000000100000000000000020000000000000003_0_0.dat";
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
        std::string path = _engine_data_paths[0] + "/data/0/15007";
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
        std::string path = _engine_data_paths[0] + "/data/0/15007/";
        TTabletId tid;
        TSchemaHash schema_hash;
        ASSERT_TRUE(_tablet_mgr->get_tablet_id_and_schema_hash_from_path(path, &tid, &schema_hash));
        ASSERT_EQ(15007, tid);
        ASSERT_EQ(0, schema_hash);
    }
    // empty tablet directory
    {
        std::string path = _engine_data_paths[0] + "/data/0/15007abc";
        TTabletId tid;
        TSchemaHash schema_hash;
        ASSERT_FALSE(_tablet_mgr->get_tablet_id_and_schema_hash_from_path(path, &tid, &schema_hash));
    }
    // not match pattern
    {
        std::string path =
                _engine_data_paths[0] + "/data/0/15007/123abc/020000000000000100000000000000020000000000000003_0_0.dat";
        TTabletId tid;
        TSchemaHash schema_hash;
        ASSERT_FALSE(_tablet_mgr->get_tablet_id_and_schema_hash_from_path(path, &tid, &schema_hash));

        RowsetId id;
        ASSERT_FALSE(_tablet_mgr->get_rowset_id_from_path(path, &id));
    }
}

TEST_F(TabletMgrTest, GetNextBatchTabletsTest) {
    std::vector<DataDir*> data_dirs;
    data_dirs.push_back(_data_dirs[0]);
    for (int i = 0; i < 20; i++) {
        TCreateTabletReq create_tablet_req = get_create_tablet_request(i, 3333);
        Status create_st = StorageEngine::instance()->tablet_manager()->create_tablet(create_tablet_req, data_dirs);
        ASSERT_TRUE(create_st.ok());
        TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(i);
        ASSERT_TRUE(tablet != nullptr);
    }

    int batch_count = 0;
    size_t batch_size = 10;
    size_t tablets_count = 0;
    std::vector<TabletSharedPtr> tablets;
    while (true) {
        tablets.clear();
        bool ret = StorageEngine::instance()->tablet_manager()->get_next_batch_tablets(batch_size, &tablets);
        tablets_count += tablets.size();
        batch_count++;
        if (ret) {
            break;
        }
    }
    ASSERT_GE(batch_count, 2);
    // because there maybe other tablets exists in storage engine
    ASSERT_GE(tablets_count, 20);

    StorageEngine::instance()->compaction_manager()->init_max_task_num(10);
    size_t num = StorageEngine::instance()->_compaction_check_one_round();
    ASSERT_GE(num, 20);
}

static void create_rowset_writer_context(RowsetWriterContext* rowset_writer_context,
                                         const std::string& schema_hash_path, const TabletSchemaCSPtr tablet_schema,
                                         int64_t start_ver, int64_t end_ver, int64_t rid) {
    RowsetId rowset_id;
    rowset_id.init(rid);
    rowset_writer_context->rowset_id = rowset_id;
    rowset_writer_context->tablet_id = 12347;
    rowset_writer_context->tablet_schema_hash = 1111;
    rowset_writer_context->partition_id = 10;
    rowset_writer_context->rowset_path_prefix = schema_hash_path;
    rowset_writer_context->rowset_state = VISIBLE;
    rowset_writer_context->tablet_schema = tablet_schema;
    rowset_writer_context->version.first = start_ver;
    rowset_writer_context->version.second = end_ver;
}

static void rowset_writer_add_rows(std::unique_ptr<RowsetWriter>& writer, const TabletSchemaCSPtr& tablet_schema) {
    std::vector<std::string> test_data;
    auto schema = ChunkHelper::convert_schema(tablet_schema);
    auto chunk = ChunkHelper::new_chunk(schema, 1024);
    for (size_t i = 0; i < 1024; ++i) {
        test_data.push_back("well" + std::to_string(i));
        auto& cols = chunk->columns();
        cols[0]->append_datum(Datum(static_cast<int32_t>(i)));
        Slice field_1(test_data[i]);
        cols[1]->append_datum(Datum(field_1));
        cols[2]->append_datum(Datum(static_cast<int32_t>(10000 + i)));
    }
    auto st = writer->add_chunk(*chunk);
    ASSERT_TRUE(st.ok()) << st.to_string() << ", version:" << writer->version();
}

static void set_default_create_tablet_request(TCreateTabletReq* request) {
    request->tablet_id = 12347;
    request->__set_version(1);
    request->tablet_schema.schema_hash = 1111;
    request->tablet_schema.short_key_column_count = 2;
    request->tablet_schema.keys_type = TKeysType::DUP_KEYS;
    request->tablet_schema.storage_type = TStorageType::COLUMN;

    TColumn k1;
    k1.column_name = "k1";
    k1.__set_is_key(true);
    k1.column_type.type = TPrimitiveType::INT;
    request->tablet_schema.columns.push_back(k1);

    TColumn k2;
    k2.column_name = "k2";
    k2.__set_is_key(true);
    k2.column_type.__set_len(64);
    k2.column_type.type = TPrimitiveType::VARCHAR;
    request->tablet_schema.columns.push_back(k2);

    TColumn v;
    v.column_name = "v1";
    v.__set_is_key(false);
    v.column_type.type = TPrimitiveType::INT;
    v.__set_aggregation_type(TAggregationType::SUM);
    request->tablet_schema.columns.push_back(v);
}

TEST_F(TabletMgrTest, RsVersionMapTest) {
    // create tablet first
    // create tablet 15007
    TCreateTabletReq request;
    set_default_create_tablet_request(&request);
    auto res = StorageEngine::instance()->create_tablet(request);
    ASSERT_TRUE(res.ok()) << res.to_string();
    TabletManager* tablet_manager = starrocks::StorageEngine::instance()->tablet_manager();
    TabletSharedPtr tablet = tablet_manager->get_tablet(12347);
    ASSERT_TRUE(tablet != nullptr);
    const auto& tablet_schema = tablet->tablet_schema();

    // create rowset <2, 2>, <3, 3>, <3, 4>, <4, 4>, <4, 5>, <5, 5>, <5, 6>
    std::vector<Version> ver_list;
    ver_list.emplace_back(2, 2);
    ver_list.emplace_back(3, 3);
    ver_list.emplace_back(4, 4);
    ver_list.emplace_back(3, 4);
    ver_list.emplace_back(5, 5);
    ver_list.emplace_back(4, 5);
    ver_list.emplace_back(5, 6);
    ver_list.emplace_back(2, 4);
    ver_list.emplace_back(3, 5);
    std::vector<Version> tmp_list;
    tablet->list_versions(&tmp_list);
    std::string debug = "";
    for (auto&& ver : tmp_list) {
        debug += "(" + std::to_string(ver.first) + "," + std::to_string(ver.second) + ")";
    }
    std::vector<RowsetSharedPtr> to_add;
    std::vector<RowsetSharedPtr> to_remove;
    std::vector<RowsetSharedPtr> to_replace;
    int64_t rid = 10000;
    for (auto&& ver : ver_list) {
        RowsetWriterContext rowset_writer_context;
        create_rowset_writer_context(&rowset_writer_context, tablet->schema_hash_path(), tablet_schema, ver.first,
                                     ver.second, rid++);
        std::unique_ptr<RowsetWriter> rowset_writer;
        ASSERT_TRUE(RowsetFactory::create_rowset_writer(rowset_writer_context, &rowset_writer).ok());

        rowset_writer_add_rows(rowset_writer, tablet_schema);
        rowset_writer->flush();
        RowsetSharedPtr src_rowset = *rowset_writer->build();
        to_add.push_back(std::move(src_rowset));
    }

    tablet->modify_rowsets_without_lock(to_add, to_remove, &to_replace);
    ASSERT_EQ(to_replace.size(), 0);
    tmp_list.clear();
    tablet->list_versions(&tmp_list);
    debug = "";
    for (auto&& ver : tmp_list) {
        debug += "(" + std::to_string(ver.first) + "," + std::to_string(ver.second) + ")";
    }
    // find version
    for (auto&& ver : ver_list) {
        RowsetSharedPtr p = tablet->get_rowset_by_version(ver);
        ASSERT_TRUE(p != nullptr) << debug << " not found: " << ver.first << ":" << ver.second;
        ASSERT_TRUE(p->start_version() == ver.first);
        ASSERT_TRUE(p->end_version() == ver.second);
    }
    // contain version
    ASSERT_TRUE(!tablet->contains_version(Version(4, 4)).ok()) << debug;
    ASSERT_TRUE(!tablet->contains_version(Version(4, 5)).ok()) << debug;
    ASSERT_TRUE(!tablet->contains_version(Version(6, 6)).ok()) << debug;

    // delete rowset
    for (int i = 0; i < 3; i++) {
        to_remove.push_back(to_add[i]);
    }
    to_add.clear();
    tablet->modify_rowsets_without_lock(to_add, to_remove, &to_replace);
    ASSERT_EQ(to_replace.size(), 0);

    // delete same rowset again
    tablet->modify_rowsets_without_lock(to_add, to_remove, &to_replace);
    ASSERT_EQ(to_replace.size(), 0);

    // replace stale rowset
    to_remove.clear();
    for (int i = 0; i < 3; i++) {
        RowsetWriterContext rowset_writer_context;
        create_rowset_writer_context(&rowset_writer_context, tablet->schema_hash_path(), tablet_schema,
                                     ver_list[i].first, ver_list[i].second, rid++);
        std::unique_ptr<RowsetWriter> rowset_writer;
        ASSERT_TRUE(RowsetFactory::create_rowset_writer(rowset_writer_context, &rowset_writer).ok());

        rowset_writer_add_rows(rowset_writer, tablet_schema);
        rowset_writer->flush();
        RowsetSharedPtr src_rowset = *rowset_writer->build();
        to_remove.push_back(std::move(src_rowset));
    }
    tablet->modify_rowsets_without_lock(to_add, to_remove, &to_replace);
    ASSERT_EQ(to_replace.size(), 3);
}

TEST_F(TabletMgrTest, RemoveTabletInDiskDisable) {
    TTabletId tablet_id = 4251234;
    TSchemaHash schema_hash = 3929134;
    TCreateTabletReq create_tablet_req = get_create_tablet_request(tablet_id, schema_hash);
    Status create_st = StorageEngine::instance()->create_tablet(create_tablet_req);
    std::vector<TabletInfo> tablet_info_vec;
    TabletInfo tablet_info(tablet_id, schema_hash, UniqueId::gen_uid());

    tablet_info_vec.push_back(tablet_info);
    StorageEngine::instance()->tablet_manager()->drop_tablets_on_error_root_path(tablet_info_vec);
}

} // namespace starrocks
