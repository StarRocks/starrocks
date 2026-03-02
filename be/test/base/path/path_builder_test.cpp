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

#include "base/path/path_builder.h"

#include <gtest/gtest.h>

#include <cstdlib>
#include <optional>
#include <string>

namespace starrocks {
namespace {

class ScopedEnvVar {
public:
    explicit ScopedEnvVar(const char* name) : name_(name) {
        const char* value = std::getenv(name);
        if (value != nullptr) {
            old_value_ = value;
        }
    }

    void set(const char* value) { setenv(name_.c_str(), value, 1); }

    void unset() { unsetenv(name_.c_str()); }

    ~ScopedEnvVar() {
        if (old_value_.has_value()) {
            setenv(name_.c_str(), old_value_->c_str(), 1);
        } else {
            unsetenv(name_.c_str());
        }
        PathBuilder::reset_starrocks_home_for_test();
    }

private:
    std::string name_;
    std::optional<std::string> old_value_;
};

} // namespace

TEST(PathBuilderTest, HandlesMissingStarrocksHome) {
    ScopedEnvVar env("STARROCKS_HOME");
    env.unset();
    PathBuilder::reset_starrocks_home_for_test();

    std::string full_path;
    PathBuilder::get_full_path("be/test", &full_path);
    EXPECT_EQ(full_path, "be/test");
}

TEST(PathBuilderTest, UsesStarrocksHomeWhenSet) {
    ScopedEnvVar env("STARROCKS_HOME");
    env.set("/tmp/starrocks-home");
    PathBuilder::reset_starrocks_home_for_test();

    std::string full_path;
    PathBuilder::get_full_path("be/test", &full_path);
    EXPECT_EQ(full_path, "/tmp/starrocks-home/be/test");
}

} // namespace starrocks
