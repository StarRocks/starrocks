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

#include <filesystem>
#include <fstream>

#include "base/testutil/assert.h"
#include "fmt/format.h"
#include "platform/python/env.h"

namespace starrocks {

class PythonEnvRegistryTest : public testing::Test {
public:
    void SetUp() override {
        _test_dir = std::filesystem::current_path() / "test_python_env_registry";
        std::filesystem::remove_all(_test_dir);
        std::filesystem::create_directories(_test_dir);
    }

    void TearDown() override { std::filesystem::remove_all(_test_dir); }

protected:
    std::filesystem::path _test_dir;
};

TEST_F(PythonEnvRegistryTest, EmptyRegistryReturnsError) {
    PythonEnvRegistry registry;
    auto env = registry.getDefault();
    ASSERT_FALSE(env.ok());
    EXPECT_TRUE(env.status().is_internal_error()) << env.status();
    EXPECT_EQ("not found avaliable env", env.status().message());
}

TEST_F(PythonEnvRegistryTest, RejectsNonDirectoryEnv) {
    PythonEnvRegistry registry;
    auto env_path = (_test_dir / "missing").string();

    auto status = registry.init({env_path});

    ASSERT_TRUE(status.is_invalid_argument()) << status;
    EXPECT_EQ(fmt::format("unsupported python env: {} not a directory", env_path), status.message());
}

TEST_F(PythonEnvRegistryTest, RejectsEnvWithoutPythonBinary) {
    PythonEnvRegistry registry;
    auto env_path = _test_dir / "python_env";
    std::filesystem::create_directories(env_path / "bin");

    auto status = registry.init({env_path.string()});

    ASSERT_TRUE(status.is_invalid_argument()) << status;
    EXPECT_EQ(fmt::format("unsupported python env: {} not found python", env_path.string()), status.message());
}

TEST_F(PythonEnvRegistryTest, AcceptsValidEnvAndReturnsPythonPath) {
    PythonEnvRegistry registry;
    auto env_path = _test_dir / "python_env";
    std::filesystem::create_directories(env_path / "bin");
    std::ofstream(env_path / "bin/python3").close();

    ASSERT_OK(registry.init({env_path.string()}));
    auto env = registry.getDefault();
    ASSERT_OK(env.status());
    EXPECT_EQ(env_path.string(), env->home);
    EXPECT_EQ((env_path / "bin/python3").string(), env->get_python_path());
}

} // namespace starrocks
