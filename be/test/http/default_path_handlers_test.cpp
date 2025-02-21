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

#include "http/default_path_handlers.h"

#include <gtest/gtest.h>

#include "runtime/exec_env.h"

namespace starrocks {

class DefaultPathHandlersTest : public testing::Test {};

TEST_F(DefaultPathHandlersTest, mem_tracker) {
    WebPageHandler::ArgumentMap args;
    auto* mem_tracker = GlobalEnv::GetInstance()->process_mem_tracker();

    std::stringstream output1;
    MemTrackerWebPageHandler::handle(mem_tracker, args, &output1);
    ASSERT_TRUE(output1.str().find("<tr><td>1</td><td>process</td><td>") != std::string::npos);
    ASSERT_TRUE(output1.str().find("<tr><td>2</td><td>update</td>") != std::string::npos);

    std::stringstream output2;
    args["type"] = "metadata";
    args["upper_level"] = "4";
    MemTrackerWebPageHandler::handle(mem_tracker, args, &output2);
    ASSERT_TRUE(output2.str().find("<tr><td>1</td><td>process</td><td>") == std::string::npos);
    ASSERT_TRUE(output2.str().find("<tr><td>3</td><td>tablet_metadata</td><td>metadata</td>") != std::string::npos);
}
} // namespace starrocks
