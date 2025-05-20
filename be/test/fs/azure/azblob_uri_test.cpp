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

#include "fs/azure/azblob_uri.h"

#include <gtest/gtest.h>

namespace starrocks {

class AzBlobURITest : public ::testing::Test {};

TEST_F(AzBlobURITest, test_parse_virtual_host_style_uri) {
    std::string path = "https://account_name.blob.core.windows.net/container_name/blob_name";
    AzBlobURI uri;
    EXPECT_TRUE(uri.parse(path));
    EXPECT_FALSE(uri.is_path_style());
    EXPECT_STREQ(uri.scheme().c_str(), "https");
    EXPECT_STREQ(uri.account().c_str(), "account_name");
    EXPECT_STREQ(uri.container().c_str(), "container_name");
    EXPECT_STREQ(uri.blob_name().c_str(), "blob_name");
    EXPECT_STREQ(uri.get_account_uri().c_str(), "https://account_name.blob.core.windows.net");
    EXPECT_STREQ(uri.get_container_uri().c_str(), "https://account_name.blob.core.windows.net/container_name");
    EXPECT_STREQ(uri.get_blob_uri().c_str(), "https://account_name.blob.core.windows.net/container_name/blob_name");

    path = "https://account_name.blob.core.windows.net/container_name/dir_name/blob_name";
    EXPECT_TRUE(uri.parse(path));
    EXPECT_FALSE(uri.is_path_style());
    EXPECT_STREQ(uri.scheme().c_str(), "https");
    EXPECT_STREQ(uri.account().c_str(), "account_name");
    EXPECT_STREQ(uri.container().c_str(), "container_name");
    EXPECT_STREQ(uri.blob_name().c_str(), "dir_name/blob_name");
    EXPECT_STREQ(uri.get_account_uri().c_str(), "https://account_name.blob.core.windows.net");
    EXPECT_STREQ(uri.get_container_uri().c_str(), "https://account_name.blob.core.windows.net/container_name");
    EXPECT_STREQ(uri.get_blob_uri().c_str(),
                 "https://account_name.blob.core.windows.net/container_name/dir_name/blob_name");

    path = "https://account_name.blob.core.windows.net/container_name";
    EXPECT_TRUE(uri.parse(path));
    EXPECT_FALSE(uri.is_path_style());
    EXPECT_STREQ(uri.scheme().c_str(), "https");
    EXPECT_STREQ(uri.account().c_str(), "account_name");
    EXPECT_STREQ(uri.container().c_str(), "container_name");
    EXPECT_STREQ(uri.blob_name().c_str(), "");
    EXPECT_STREQ(uri.get_account_uri().c_str(), "https://account_name.blob.core.windows.net");
    EXPECT_STREQ(uri.get_container_uri().c_str(), "https://account_name.blob.core.windows.net/container_name");

    path = "https://account_name.blob.core.windows.net";
    EXPECT_TRUE(uri.parse(path));
    EXPECT_FALSE(uri.is_path_style());
    EXPECT_STREQ(uri.scheme().c_str(), "https");
    EXPECT_STREQ(uri.account().c_str(), "account_name");
    EXPECT_STREQ(uri.container().c_str(), "");
    EXPECT_STREQ(uri.blob_name().c_str(), "");
    EXPECT_STREQ(uri.get_account_uri().c_str(), "https://account_name.blob.core.windows.net");

    // Test invalid uri: missing 'scheme'
    path = "blob.core.windows.net";
    EXPECT_FALSE(uri.parse(path));

    // Test invalid host style uri: empty account
    path = "https://.blob.core.windows.net/container_name/blob_name";
    EXPECT_FALSE(uri.parse(path));
}

TEST_F(AzBlobURITest, test_parse_path_style_uri) {
    std::string path = "https://blob.core.windows.net/account_name/container_name/blob_name";
    AzBlobURI uri;
    uri.parse(path);
    EXPECT_TRUE(uri.is_path_style());
    EXPECT_STREQ(uri.scheme().c_str(), "https");
    EXPECT_STREQ(uri.account().c_str(), "account_name");
    EXPECT_STREQ(uri.container().c_str(), "container_name");
    EXPECT_STREQ(uri.blob_name().c_str(), "blob_name");
    EXPECT_STREQ(uri.get_account_uri().c_str(), "https://blob.core.windows.net/account_name");
    EXPECT_STREQ(uri.get_container_uri().c_str(), "https://blob.core.windows.net/account_name/container_name");
    EXPECT_STREQ(uri.get_blob_uri().c_str(), "https://blob.core.windows.net/account_name/container_name/blob_name");

    path = "https://blob.core.windows.net/account_name/container_name/dir_name/blob_name";
    EXPECT_TRUE(uri.parse(path));
    EXPECT_TRUE(uri.is_path_style());
    EXPECT_STREQ(uri.scheme().c_str(), "https");
    EXPECT_STREQ(uri.account().c_str(), "account_name");
    EXPECT_STREQ(uri.container().c_str(), "container_name");
    EXPECT_STREQ(uri.blob_name().c_str(), "dir_name/blob_name");
    EXPECT_STREQ(uri.get_account_uri().c_str(), "https://blob.core.windows.net/account_name");
    EXPECT_STREQ(uri.get_container_uri().c_str(), "https://blob.core.windows.net/account_name/container_name");
    EXPECT_STREQ(uri.get_blob_uri().c_str(),
                 "https://blob.core.windows.net/account_name/container_name/dir_name/blob_name");

    path = "https://blob.core.windows.net/account_name/container_name";
    EXPECT_TRUE(uri.parse(path));
    EXPECT_TRUE(uri.is_path_style());
    EXPECT_STREQ(uri.scheme().c_str(), "https");
    EXPECT_STREQ(uri.account().c_str(), "account_name");
    EXPECT_STREQ(uri.container().c_str(), "container_name");
    EXPECT_STREQ(uri.blob_name().c_str(), "");
    EXPECT_STREQ(uri.get_account_uri().c_str(), "https://blob.core.windows.net/account_name");
    EXPECT_STREQ(uri.get_container_uri().c_str(), "https://blob.core.windows.net/account_name/container_name");

    path = "https://blob.core.windows.net/account_name";
    EXPECT_TRUE(uri.parse(path));
    EXPECT_TRUE(uri.is_path_style());
    EXPECT_STREQ(uri.scheme().c_str(), "https");
    EXPECT_STREQ(uri.account().c_str(), "account_name");
    EXPECT_STREQ(uri.container().c_str(), "");
    EXPECT_STREQ(uri.blob_name().c_str(), "");
    EXPECT_STREQ(uri.get_account_uri().c_str(), "https://blob.core.windows.net/account_name");

    // Test invalid path style uri: missing 'account'
    path = "https://blob.core.windows.net";
    EXPECT_FALSE(uri.parse(path));
}

TEST_F(AzBlobURITest, test_parse_hdfs_style_uri) {
    std::string path = "wasbs://container_name@account_name.blob.core.windows.net/blob_name";
    AzBlobURI uri;
    EXPECT_TRUE(uri.parse(path));
    EXPECT_FALSE(uri.is_path_style());
    EXPECT_STREQ(uri.scheme().c_str(), "wasbs");
    EXPECT_STREQ(uri.account().c_str(), "account_name");
    EXPECT_STREQ(uri.container().c_str(), "container_name");
    EXPECT_STREQ(uri.blob_name().c_str(), "blob_name");
    EXPECT_STREQ(uri.get_account_uri().c_str(), "https://account_name.blob.core.windows.net");
    EXPECT_STREQ(uri.get_container_uri().c_str(), "https://account_name.blob.core.windows.net/container_name");
    EXPECT_STREQ(uri.get_blob_uri().c_str(), "https://account_name.blob.core.windows.net/container_name/blob_name");

    path = "wasbs://container_name@account_name.blob.core.windows.net/dir_name/blob_name";
    EXPECT_TRUE(uri.parse(path));
    EXPECT_FALSE(uri.is_path_style());
    EXPECT_STREQ(uri.scheme().c_str(), "wasbs");
    EXPECT_STREQ(uri.account().c_str(), "account_name");
    EXPECT_STREQ(uri.container().c_str(), "container_name");
    EXPECT_STREQ(uri.blob_name().c_str(), "dir_name/blob_name");
    EXPECT_STREQ(uri.get_account_uri().c_str(), "https://account_name.blob.core.windows.net");
    EXPECT_STREQ(uri.get_container_uri().c_str(), "https://account_name.blob.core.windows.net/container_name");
    EXPECT_STREQ(uri.get_blob_uri().c_str(),
                 "https://account_name.blob.core.windows.net/container_name/dir_name/blob_name");

    path = "wasbs://container_name@account_name.blob.core.windows.net/";
    EXPECT_TRUE(uri.parse(path));
    EXPECT_FALSE(uri.is_path_style());
    EXPECT_STREQ(uri.scheme().c_str(), "wasbs");
    EXPECT_STREQ(uri.account().c_str(), "account_name");
    EXPECT_STREQ(uri.container().c_str(), "container_name");
    EXPECT_STREQ(uri.blob_name().c_str(), "");
    EXPECT_STREQ(uri.get_account_uri().c_str(), "https://account_name.blob.core.windows.net");
    EXPECT_STREQ(uri.get_container_uri().c_str(), "https://account_name.blob.core.windows.net/container_name");

    // Test invalid hdfs style uri: empty container
    path = "wasbs://@account_name.blob.core.windows.net/blob_name";
    EXPECT_FALSE(uri.parse(path));
}

} // namespace starrocks
