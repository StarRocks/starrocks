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

#include <cstdio>
#include <cstdlib>
#include <string>

#include "common/configbase.h"
#include "fs/azure/fs_azblob.h"
#include "fs/fs_posix.h"
#include "fs/fs_registry.h"
#include "fs/fs_s3.h"
#include "gtest/gtest.h"

namespace starrocks::fs {

namespace {

Status install_fs_test_providers() {
    static Status status = [] {
        static FileSystemProviderRegistry registry;
        RETURN_IF_ERROR(registry.register_provider(new_posix_file_system_provider()));
        RETURN_IF_ERROR(registry.register_provider(new_s3_file_system_provider()));
        RETURN_IF_ERROR(registry.register_provider(new_azblob_file_system_provider()));
        install_default_file_system_provider_registry(registry.freeze());
        return Status::OK();
    }();
    return status;
}

} // namespace

} // namespace starrocks::fs

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    if (getenv("STARROCKS_HOME") == nullptr) {
        fprintf(stderr, "you need set STARROCKS_HOME environment variable.\n");
        return -1;
    }
    std::string conffile = std::string(getenv("STARROCKS_HOME")) + "/conf/be_test.conf";
    if (!starrocks::config::init(conffile.c_str())) {
        fprintf(stderr, "error read config file.\n");
        return -1;
    }
    auto status = starrocks::fs::install_fs_test_providers();
    if (!status.ok()) {
        fprintf(stderr, "fail to install fs test providers: %s\n", status.to_string().c_str());
        return -1;
    }
    return RUN_ALL_TESTS();
}
