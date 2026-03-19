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

#include "io/core/io_error.h"

#include <gtest/gtest.h>

#include <cerrno>

namespace starrocks::io {

TEST(IOErrorTest, ErrorTypeMapping) {
    EXPECT_TRUE(io_error("read", ENOENT).is_not_found());
    EXPECT_TRUE(io_error("create", EEXIST).is_already_exist());
    EXPECT_TRUE(io_error("read", EIO).is_io_error());
}

} // namespace starrocks::io
