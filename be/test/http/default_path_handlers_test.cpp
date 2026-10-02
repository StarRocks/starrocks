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

<<<<<<< HEAD
#include "runtime/exec_env.h"
=======
#include "base/utility/defer_op.h"
#include "common/config_object_storage_fwd.h"
#include "exec/exec_env.h"
>>>>>>> c9fa825 ([BugFix] Mask object storage credentials in BE /varz and information_schema.be_configs (#80040))

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
<<<<<<< HEAD
=======

TEST_F(DefaultPathHandlersTest, mem_tracker_with_non_numeric_upper_level) {
    WebPageHandler::ArgumentMap args;
    args["upper_level"] = "abc";
    const auto& runtime_env = *RuntimeEnv::GetInstance();
    auto* mem_tracker = runtime_env.process_mem_tracker();

    std::stringstream output;
    MemTrackerWebPageHandler::handle(runtime_env, mem_tracker, args, &output);

    ASSERT_TRUE(output.str().find("Invalid upper_level") != std::string::npos);
    // The page still renders, with the default two levels.
    ASSERT_TRUE(output.str().find("<tr><td>1</td><td>process</td><td>") != std::string::npos);
    ASSERT_TRUE(output.str().find("<tr><td>2</td><td>update</td>") != std::string::npos);
}

TEST_F(DefaultPathHandlersTest, mem_tracker_with_negative_upper_level) {
    WebPageHandler::ArgumentMap args;
    args["upper_level"] = "-1";
    const auto& runtime_env = *RuntimeEnv::GetInstance();
    auto* mem_tracker = runtime_env.process_mem_tracker();

    std::stringstream output;
    MemTrackerWebPageHandler::handle(runtime_env, mem_tracker, args, &output);

    ASSERT_TRUE(output.str().find("Invalid upper_level") != std::string::npos);
    ASSERT_TRUE(output.str().find("<tr><td>1</td><td>process</td><td>") != std::string::npos);
    ASSERT_TRUE(output.str().find("<tr><td>2</td><td>update</td>") != std::string::npos);
}

TEST_F(DefaultPathHandlersTest, mem_tracker_does_not_echo_upper_level) {
    WebPageHandler::ArgumentMap args;
    args["upper_level"] = "<script>alert(1)</script>";
    const auto& runtime_env = *RuntimeEnv::GetInstance();
    auto* mem_tracker = runtime_env.process_mem_tracker();

    std::stringstream output;
    MemTrackerWebPageHandler::handle(runtime_env, mem_tracker, args, &output);

    ASSERT_TRUE(output.str().find("Invalid upper_level") != std::string::npos);
    ASSERT_TRUE(output.str().find("<script>") == std::string::npos);
}

TEST_F(DefaultPathHandlersTest, config_handler_masks_object_storage_credentials) {
    std::string old_ak = config::object_storage_access_key_id;
    std::string old_sk = config::object_storage_secret_access_key;
    DeferOp restore([&]() {
        config::object_storage_access_key_id = old_ak;
        config::object_storage_secret_access_key = old_sk;
    });

    config::object_storage_access_key_id = "AKIDEXAMPLEACCESSKEY";
    config::object_storage_secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    WebPageHandler::ArgumentMap args;
    std::stringstream output;
    config_handler(args, &output);
    const std::string page = output.str();

    EXPECT_EQ(std::string::npos, page.find("AKIDEXAMPLEACCESSKEY"));
    EXPECT_EQ(std::string::npos, page.find("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"));
    const std::string mask(config::kSensitiveConfigMask);
    EXPECT_NE(std::string::npos, page.find("\nobject_storage_access_key_id=" + mask + "\n"));
    EXPECT_NE(std::string::npos, page.find("\nobject_storage_secret_access_key=" + mask + "\n"));
    // Non-credential configs are still printed as is.
    EXPECT_NE(std::string::npos, page.find("\nobject_storage_endpoint=" + config::object_storage_endpoint + "\n"));
}
>>>>>>> c9fa825 ([BugFix] Mask object storage credentials in BE /varz and information_schema.be_configs (#80040))
} // namespace starrocks
