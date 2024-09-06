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
//   https://github.com/apache/incubator-doris/blob/master/be/test/olap/olap_meta_test.cpp

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

#include "storage/kv_store.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <sstream>
#include <string>

#include "fs/fs_util.h"
#include "storage/olap_define.h"

#ifndef BE_TEST
#define BE_TEST
#endif

using std::string;

namespace starrocks {

class KVStoreTest : public testing::Test {
public:
    void SetUp() override {
        _root_path = "./kv_store_test";
        fs::remove_all(_root_path);
        fs::create_directories(_root_path);

        _kv_store = new KVStore(_root_path);
        Status st = _kv_store->init();
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_TRUE(std::filesystem::exists(_root_path + "/meta"));
    }

    void TearDown() override {
        delete _kv_store;
        fs::remove_all(_root_path);
    }

private:
    std::string _root_path;
    KVStore* _kv_store;
};

TEST_F(KVStoreTest, TestGetRootPath) {
    std::string root_path = _kv_store->get_root_path();
    ASSERT_EQ("./kv_store_test", root_path);
}

TEST_F(KVStoreTest, TestPutAndGet) {
    // normal cases
    std::string key = "key";
    std::string value = "value";
    ASSERT_TRUE(_kv_store->put(META_COLUMN_FAMILY_INDEX, key, value).ok());
    std::string value_get;
    ASSERT_TRUE(_kv_store->get(META_COLUMN_FAMILY_INDEX, key, &value_get).ok());
    ASSERT_EQ(value, value_get);

    // abnormal cases
    ASSERT_TRUE(_kv_store->get(META_COLUMN_FAMILY_INDEX, "key_not_exist", &value_get).is_not_found());
}

TEST_F(KVStoreTest, TestRemove) {
    // normal cases
    std::string key = "key";
    std::string value = "value";
    ASSERT_TRUE(_kv_store->put(META_COLUMN_FAMILY_INDEX, key, value).ok());
    std::string value_get;
    ASSERT_TRUE(_kv_store->get(META_COLUMN_FAMILY_INDEX, key, &value_get).ok());
    ASSERT_EQ(value, value_get);
    ASSERT_TRUE(_kv_store->remove(META_COLUMN_FAMILY_INDEX, key).ok());
    ASSERT_TRUE(_kv_store->remove(META_COLUMN_FAMILY_INDEX, "key_not_exist").ok());
}

TEST_F(KVStoreTest, TestIterate) {
    // normal cases
    std::string key = "hdr_key";
    std::string value = "value";
    for (int i = 0; i < 10; i++) {
        std::stringstream ss;
        ss << key << "_" << i;
        ASSERT_TRUE(_kv_store->put(META_COLUMN_FAMILY_INDEX, ss.str(), value).ok());
    }
    bool error_flag = false;
    ASSERT_TRUE(_kv_store
                        ->iterate(META_COLUMN_FAMILY_INDEX, "hdr_",
                                  [&error_flag](std::string_view key, std::string_view value) -> bool {
                                      size_t pos = key.find_first_of("hdr_");
                                      if (pos != 0) {
                                          error_flag = true;
                                      }
                                      return true;
                                  })
                        .ok());
    ASSERT_EQ(false, error_flag);
}

TEST_F(KVStoreTest, TestOpDeleteRange) {
    // insert 10 keys
    for (int i = 0; i < 10; i++) {
        std::string key = fmt::format("key_{:016x}", i);
        std::string value = fmt::format("val_{:016x}", i);
        ASSERT_TRUE(_kv_store->put(META_COLUMN_FAMILY_INDEX, key, value).ok());
    }
    for (int i = 0; i < 10; i++) {
        std::string key = fmt::format("key_{:016x}", i);
        std::string value_get;
        ASSERT_TRUE(_kv_store->get(META_COLUMN_FAMILY_INDEX, key, &value_get).ok());
        ASSERT_TRUE(value_get == fmt::format("val_{:016x}", i));
    }
    // delete range from 0 ~ 9
    rocksdb::WriteBatch wb;
    ASSERT_TRUE(_kv_store
                        ->OptDeleteRange(META_COLUMN_FAMILY_INDEX, fmt::format("key_{:016x}", 0),
                                         fmt::format("key_{:016x}", 10), &wb)
                        .ok());
    ASSERT_TRUE(_kv_store->write_batch(&wb).ok());
    // check result
    for (int i = 0; i < 10; i++) {
        std::string key = fmt::format("key_{:016x}", i);
        std::string value_get;
        ASSERT_TRUE(_kv_store->get(META_COLUMN_FAMILY_INDEX, key, &value_get).is_not_found());
    }
}

} // namespace starrocks
