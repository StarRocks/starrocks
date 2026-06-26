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

#include "bootstrap/bootstrap.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "fs/fs_registry.h"

namespace starrocks {

TEST(BootstrapTest, RegistryBootstrapIsIdempotent) {
    ASSERT_OK(bootstrap::install_builtin_file_system_providers());
    ASSERT_OK(bootstrap::install_builtin_file_system_providers());
    ASSERT_OK(bootstrap::bootstrap_builtin_connectors());
    ASSERT_OK(bootstrap::bootstrap_builtin_connectors());

    ASSIGN_OR_ABORT(auto fs, fs::default_file_system_provider_registry().create_shared("posix://"));
    ASSERT_EQ(FileSystem::POSIX, fs->type());
}

} // namespace starrocks
