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

#include "agent/master_info.h"

#include "gtest/gtest.h"

namespace starrocks {

TEST(MasterInfoTest, basic_info_test) {
    {
        auto mode = get_master_run_mode();
        EXPECT_FALSE(mode.has_value());
    }

    TMasterInfo info;
    TNetworkAddress addr;
    addr.__set_hostname("127.0.0.1");
    info.__set_network_address(addr);
    info.__set_http_port(8030);
    info.__set_run_mode(TRunMode::SHARED_NOTHING);
    EXPECT_TRUE(update_master_info(info));

    {
        auto mode = get_master_run_mode();
        EXPECT_TRUE(mode.has_value());
        EXPECT_EQ(TRunMode::SHARED_NOTHING, mode.value());
    }
}

} // namespace starrocks
