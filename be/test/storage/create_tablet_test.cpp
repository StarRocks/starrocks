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

#include "fs/fs_util.h"
#include "storage/storage_engine.h"
#include "storage/txn_manager.h"
#include "testutil/parallel_test.h"

namespace starrocks {

class CreateTableTest : public testing::Test {
public:
    ~CreateTableTest() override {
        if (_engine) {
            _engine->stop();
            delete _engine;
            _engine = nullptr;
        }
    }
    void SetUp() override {
        _default_storage_root_path = config::storage_root_path;
        config::storage_root_path = std::filesystem::current_path().string() + "/create_table";
        fs::remove_all(config::storage_root_path);
        std::vector<StorePath> paths;
        paths.emplace_back(config::storage_root_path);

        starrocks::EngineOptions options;
        options.store_paths = paths;
        if (_engine == nullptr) {
            Status s = starrocks::StorageEngine::open(options, &_engine);
            ASSERT_TRUE(s.ok()) << s.to_string();
        }
    }

    void TearDown() override {
        if (fs::path_exist(config::storage_root_path)) {
            ASSERT_TRUE(fs::remove_all(config::storage_root_path).ok());
        }
        config::storage_root_path = _default_storage_root_path;
    }

protected:
    StorageEngine* _engine = nullptr;
    std::string _default_storage_root_path;
};

PARALLEL_TEST(CreateTableTest, create_table_test_primary_key) {
    std::vector<int64_t> tablet_ids;
    std::vector<int32_t> schema_hashs;
    tablet_ids.push_back(10000);
    tablet_ids.push_back(10001);
    tablet_ids.push_back(10002);
    schema_hashs.push_back(20000);
    schema_hashs.push_back(20001);
    schema_hashs.push_back(20002);
    long txn_id = 30000;
    TCreateTableReq request;
    for (int i = 0; i < tablet_ids.size(); ++i) {
        TCreateTabletReq tablet_request;
        tablet_request.tablet_id = tablet_ids[i];
        tablet_request.__set_version(1);
        tablet_request.__set_version_hash(0);
        tablet_request.tablet_schema.schema_hash = schema_hashs[i];
        tablet_request.tablet_schema.short_key_column_count = 1;
        tablet_request.tablet_schema.keys_type = TKeysType::PRIMARY_KEYS;
        tablet_request.tablet_schema.storage_type = TStorageType::COLUMN;

        TColumn pk1;
        pk1.column_name = "pk1_bigint";
        pk1.__set_is_key(true);
        pk1.column_type.type = TPrimitiveType::BIGINT;
        tablet_request.tablet_schema.columns.push_back(pk1);
        TColumn pk2;
        pk2.column_name = "pk2_varchar";
        pk2.__set_is_key(true);
        pk2.column_type.type = TPrimitiveType::VARCHAR;
        pk2.column_type.len = 128;
        tablet_request.tablet_schema.columns.push_back(pk2);
        TColumn pk3;
        pk3.column_name = "pk3_int";
        pk3.__set_is_key(true);
        pk3.column_type.type = TPrimitiveType::INT;
        tablet_request.tablet_schema.columns.push_back(pk3);

        TColumn k2;
        k2.column_name = "v1";
        k2.__set_is_key(false);
        k2.column_type.type = TPrimitiveType::SMALLINT;
        tablet_request.tablet_schema.columns.push_back(k2);

        TColumn k3;
        k3.column_name = "v2";
        k3.__set_is_key(false);
        k3.column_type.type = TPrimitiveType::INT;
        tablet_request.tablet_schema.columns.push_back(k3);
        request.create_tablet_reqs.push_back(tablet_request);
    }
    Status st = StorageEngine::instance()->create_table(request, txn_id);
    CHECK(st.ok()) << st.to_string();
}

PARALLEL_TEST(CreateTableTest, create_table_test_primary_key_with_sort_key) {
    std::vector<int64_t> tablet_ids;
    std::vector<int32_t> schema_hashs;
    tablet_ids.push_back(100000);
    tablet_ids.push_back(100001);
    tablet_ids.push_back(100002);
    schema_hashs.push_back(200000);
    schema_hashs.push_back(200001);
    schema_hashs.push_back(200002);
    long txn_id = 300000;
    TCreateTableReq request;
    for (int i = 0; i < tablet_ids.size(); ++i) {
        TCreateTabletReq tablet_request;
        tablet_request.tablet_id = tablet_ids[i];
        tablet_request.__set_version(1);
        tablet_request.tablet_schema.schema_hash = schema_hashs[i];
        tablet_request.tablet_schema.short_key_column_count = 1;
        tablet_request.tablet_schema.keys_type = TKeysType::PRIMARY_KEYS;
        tablet_request.tablet_schema.storage_type = TStorageType::COLUMN;

        TColumn k1;
        k1.column_name = "pk";
        k1.__set_is_key(true);
        k1.column_type.type = TPrimitiveType::BIGINT;
        tablet_request.tablet_schema.columns.push_back(k1);

        TColumn k2;
        k2.column_name = "v1";
        k2.__set_is_key(false);
        k2.column_type.type = TPrimitiveType::SMALLINT;
        tablet_request.tablet_schema.columns.push_back(k2);

        TColumn k3;
        k3.column_name = "v2";
        k3.__set_is_key(false);
        k3.column_type.type = TPrimitiveType::INT;
        tablet_request.tablet_schema.columns.push_back(k3);
        request.create_tablet_reqs.push_back(tablet_request);
    }
    auto st = StorageEngine::instance()->create_table(request, txn_id);
    CHECK(st.ok()) << st.to_string();
}

PARALLEL_TEST(CreateTableTest, create_table_test_duplicate_key) {
    std::vector<int64_t> tablet_ids;
    std::vector<int32_t> schema_hashs;
    tablet_ids.push_back(1000000);
    tablet_ids.push_back(1000001);
    tablet_ids.push_back(1000002);
    schema_hashs.push_back(2000000);
    schema_hashs.push_back(2000001);
    schema_hashs.push_back(2000002);
    long txn_id = 3000000;
    TCreateTableReq request;
    for (int i = 0; i < tablet_ids.size(); ++i) {
        TCreateTabletReq tablet_request;
        tablet_request.tablet_id = tablet_ids[i];
        tablet_request.__set_version(1);
        tablet_request.tablet_schema.schema_hash = schema_hashs[i];
        tablet_request.tablet_schema.short_key_column_count = 1;
        tablet_request.tablet_schema.keys_type = TKeysType::DUP_KEYS;
        tablet_request.tablet_schema.storage_type = TStorageType::COLUMN;

        TColumn k1;
        k1.column_name = "pk";
        k1.__set_is_key(true);
        k1.column_type.type = TPrimitiveType::BIGINT;
        tablet_request.tablet_schema.columns.push_back(k1);

        TColumn k2;
        k2.column_name = "v1";
        k2.__set_is_key(false);
        k2.__set_is_allow_null(true);
        k2.column_type.type = TPrimitiveType::SMALLINT;
        tablet_request.tablet_schema.columns.push_back(k2);

        TColumn k3;
        k3.column_name = "v2";
        k3.__set_is_key(false);
        k3.__set_is_allow_null(true);
        k3.column_type.type = TPrimitiveType::INT;
        tablet_request.tablet_schema.columns.push_back(k3);
        request.create_tablet_reqs.push_back(tablet_request);
    }
    auto st = StorageEngine::instance()->create_table(request, txn_id);
    CHECK(st.ok()) << st.to_string();
}

PARALLEL_TEST(CreateTableTest, create_table_test_failed) {
    std::vector<int64_t> tablet_ids;
    std::vector<int32_t> schema_hashs;
    tablet_ids.push_back(10000000);
    tablet_ids.push_back(10000001);
    tablet_ids.push_back(10000002);
    schema_hashs.push_back(20000000);
    schema_hashs.push_back(20000001);
    schema_hashs.push_back(20000002);
    long txn_id = 30000000;

    TCreateTableReq request;

    for (int i = 0; i < tablet_ids.size(); ++i) {
        TCreateTabletReq tablet_request;
        tablet_request.tablet_id = tablet_ids[i];
        tablet_request.__set_version(1);
        tablet_request.__set_version_hash(0);
        tablet_request.tablet_schema.schema_hash = schema_hashs[i];
        tablet_request.tablet_schema.short_key_column_count = 1;
        tablet_request.tablet_schema.keys_type = TKeysType::PRIMARY_KEYS;
        tablet_request.tablet_schema.storage_type = TStorageType::COLUMN;

        TColumn k1;
        k1.column_name = "pk";
        k1.__set_is_key(true);
        k1.column_type.type = TPrimitiveType::BIGINT;
        tablet_request.tablet_schema.columns.push_back(k1);

        TColumn k2;
        k2.column_name = "v1";
        k2.__set_is_key(false);
        k2.column_type.type = TPrimitiveType::SMALLINT;
        tablet_request.tablet_schema.columns.push_back(k2);

        TColumn k3;
        k3.column_name = "v2";
        k3.__set_is_key(false);
        k3.column_type.type = TPrimitiveType::INT;
        tablet_request.tablet_schema.columns.push_back(k3);

        TColumn k4;
        k4.column_name = "v3";
        k4.__set_is_key(false);
        k4.column_type.type = TPrimitiveType::INT;
        k4.__set_default_value("1");
        tablet_request.tablet_schema.columns.push_back(k4);
        request.create_tablet_reqs.push_back(tablet_request);
    }
    auto st = StorageEngine::instance()->create_table(request, txn_id);
    CHECK(st.ok()) << st.to_string();
    TxnManager* txn_manager = StorageEngine::instance()->txn_manager();
    StatusOr<CreateTableTxn*> get_st = txn_manager->get_create_txn(txn_id);
    CHECK_EQ(get_st.value()->txn_state, TxnState::TXN_COMMITTED);

    // retry the create table request
    st = StorageEngine::instance()->create_table(request, txn_id);
    ASSERT_TRUE(st.is_already_exist());
}

} // namespace starrocks
