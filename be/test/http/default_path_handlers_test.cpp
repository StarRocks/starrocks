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

#include "exec/exec_env.h"

namespace starrocks {

class DefaultPathHandlersTest : public testing::Test {};

TEST_F(DefaultPathHandlersTest, mem_tracker) {
    WebPageHandler::ArgumentMap args;
    const auto& runtime_env = *RuntimeEnv::GetInstance();
    auto* mem_tracker = runtime_env.process_mem_tracker();

    std::stringstream output1;
    MemTrackerWebPageHandler::handle(runtime_env, mem_tracker, args, &output1);
    ASSERT_TRUE(output1.str().find("<tr><td>1</td><td>process</td><td>") != std::string::npos);
    ASSERT_TRUE(output1.str().find("<tr><td>2</td><td>update</td>") != std::string::npos);

    std::stringstream output2;
    args["type"] = "metadata";
    args["upper_level"] = "4";
    MemTrackerWebPageHandler::handle(runtime_env, mem_tracker, args, &output2);
    ASSERT_TRUE(output2.str().find("<tr><td>1</td><td>process</td><td>") == std::string::npos);
    ASSERT_TRUE(output2.str().find("<tr><td>3</td><td>tablet_metadata</td><td>metadata</td>") != std::string::npos);
}

TEST_F(DefaultPathHandlersTest, jemalloc_stats_opts) {
    // Not asking is the page's default: omit the per-arena statistics.
    EXPECT_EQ("a", parse_jemalloc_stats_opts(std::nullopt).value());

    // Every character this page accepts, and the combination /memz is most useful with:
    // per-arena without the bin/large/mutex tables.
    EXPECT_EQ("gmdablxeh", parse_jemalloc_stats_opts("gmdablxeh").value());
    EXPECT_EQ("blx", parse_jemalloc_stats_opts("blx").value());

    // An empty string is a real request: omit nothing.
    EXPECT_EQ("", parse_jemalloc_stats_opts("").value());

    // jemalloc ignores what it does not recognise, so an unknown character has to be rejected
    // here or a typo looks like it took effect.
    EXPECT_FALSE(parse_jemalloc_stats_opts("z").has_value());
    EXPECT_FALSE(parse_jemalloc_stats_opts("blz").has_value());
    EXPECT_FALSE(parse_jemalloc_stats_opts("A").has_value()) << "the set is case sensitive";
    EXPECT_FALSE(parse_jemalloc_stats_opts(" a").has_value());

    // malloc_stats_print() understands 'J', but it switches the report to JSON and this page
    // always wraps its output in HTML, so accepting it would advertise an interface /memz
    // cannot honour.
    EXPECT_FALSE(parse_jemalloc_stats_opts("J").has_value());
    EXPECT_FALSE(parse_jemalloc_stats_opts("Jgmdablxeh").has_value());
}
} // namespace starrocks
