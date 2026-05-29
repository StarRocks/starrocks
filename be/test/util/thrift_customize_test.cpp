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

#include "gen_cpp/Types_types.h"

namespace starrocks {

class ThriftCustomizeTest : public testing::Test {
public:
    ThriftCustomizeTest() = default;
    ~ThriftCustomizeTest() override = default;
};

TEST_F(ThriftCustomizeTest, backend) {
    TBackend backend1;
    backend1.host = "127.0.0.1";
    backend1.be_port = 9090;
    backend1.http_port = 9091;

    TBackend backend2;
    backend2.host = "127.0.0.1";
    backend2.be_port = 9090;
    backend2.http_port = 9091;
    ASSERT_TRUE(backend1 == backend2);

    backend2.be_port = 9092;
    ASSERT_TRUE(backend1 < backend2);

    backend2.be_port = 9090;
    backend2.http_port = 9092;
    ASSERT_TRUE(backend1 < backend2);

    backend2.http_port = 9091;
    backend2.host = "127.0.0.2";
    ASSERT_TRUE(backend1 < backend2);
}

} // namespace starrocks