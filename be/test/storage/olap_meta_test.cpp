// This file is made available under Elastic License 2.0.
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

#include "storage/olap_meta.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <sstream>
#include <string>

#include "storage/olap_define.h"
#include "util/file_utils.h"

#ifndef BE_TEST
#define BE_TEST
#endif

using std::string;

namespace starrocks {

class OlapMetaTest : public testing::Test {
public:
    virtual void SetUp() {
        _root_path = "./ut_dir/olap_meta_test";
        FileUtils::remove_all(_root_path);
        FileUtils::create_dir(_root_path);

        _meta = new OlapMeta(_root_path);
        ASSERT_TRUE(_meta->init().ok());
        ASSERT_TRUE(std::filesystem::exists(_root_path + "/meta"));
    }

    virtual void TearDown() {
        delete _meta;
        FileUtils::remove_all(_root_path);
    }

private:
    std::string _root_path;
    OlapMeta* _meta;
};

TEST_F(OlapMetaTest, TestGetRootPath) {
    std::string root_path = _meta->get_root_path();
    ASSERT_EQ("./ut_dir/olap_meta_test", root_path);
}

TEST_F(OlapMetaTest, TestPutAndGet) {
    // normal cases
    std::string key = "key";
    std::string value = "value";
    ASSERT_TRUE(_meta->put(META_COLUMN_FAMILY_INDEX, key, value).ok());
    std::string value_get;
    ASSERT_TRUE(_meta->get(META_COLUMN_FAMILY_INDEX, key, &value_get).ok());
    ASSERT_EQ(value, value_get);

    // abnormal cases
    ASSERT_TRUE(_meta->get(META_COLUMN_FAMILY_INDEX, "key_not_exist", &value_get).is_not_found());
}

TEST_F(OlapMetaTest, TestRemove) {
    // normal cases
    std::string key = "key";
    std::string value = "value";
    ASSERT_TRUE(_meta->put(META_COLUMN_FAMILY_INDEX, key, value).ok());
    std::string value_get;
    ASSERT_TRUE(_meta->get(META_COLUMN_FAMILY_INDEX, key, &value_get).ok());
    ASSERT_EQ(value, value_get);
    ASSERT_TRUE(_meta->remove(META_COLUMN_FAMILY_INDEX, key).ok());
    ASSERT_TRUE(_meta->remove(META_COLUMN_FAMILY_INDEX, "key_not_exist").ok());
}

TEST_F(OlapMetaTest, TestIterate) {
    // normal cases
    std::string key = "hdr_key";
    std::string value = "value";
    for (int i = 0; i < 10; i++) {
        std::stringstream ss;
        ss << key << "_" << i;
        ASSERT_TRUE(_meta->put(META_COLUMN_FAMILY_INDEX, ss.str(), value).ok());
    }
    bool error_flag = false;
    ASSERT_TRUE(_meta->iterate(META_COLUMN_FAMILY_INDEX, "hdr_",
                               [&error_flag](const std::string_view& key, const std::string_view& value) -> bool {
                                   size_t pos = key.find_first_of("hdr_");
                                   if (pos != 0) {
                                       error_flag = true;
                                   }
                                   return true;
                               })
                        .ok());
    ASSERT_EQ(false, error_flag);
}

} // namespace starrocks
