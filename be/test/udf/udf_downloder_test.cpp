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


#include "udf/udf_downloder.h"

#include <gtest/gtest.h>

#include "fs/fs.h"
#include "fs/fs_util.h"

namespace starrocks {

class UDFDownloaderTest : public testing::Test {
public:
    std::unique_ptr<udf_downloder> downloader;

    void SetUp() override {
        downloader = std::make_unique<udf_downloder>();
    }
    void TearDown() override {
        downloader.reset();
    }
};

TEST_F(UDFDownloaderTest, TestDownloadFromCloudRegularStorageEngine) {

    std::string localPath = "/local/starrocks/plugins/java_udf/test.jar";

    Status status = downloader->download_remote_file_2_local("s3://test-bucket/starrocks/udf/test.jar",
            localPath, FSOptions{});

    EXPECT_FALSE(status.ok());
}

} // namespace starrocks