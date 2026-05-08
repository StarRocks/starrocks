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

#include "jvm/jvm_runtime.h"

#include <cstdlib>
#include <optional>
#include <string>

#include "gtest/gtest.h"

extern "C" JNIEnv* getJNIEnv(void) {
    return nullptr;
}

namespace starrocks {

class ScopedEnvVar {
public:
    explicit ScopedEnvVar(const char* name) : _name(name) {
        const char* value = std::getenv(name);
        if (value != nullptr) {
            _old_value = value;
        }
    }

    ~ScopedEnvVar() {
        if (_old_value.has_value()) {
            setenv(_name.c_str(), _old_value->c_str(), 1);
        } else {
            unsetenv(_name.c_str());
        }
    }

    void set(const char* value) { setenv(_name.c_str(), value, 1); }

    void unset() { unsetenv(_name.c_str()); }

private:
    std::string _name;
    std::optional<std::string> _old_value;
};

TEST(JvmRuntimeTest, detect_java_runtime_requires_java_home) {
    ScopedEnvVar java_home("JAVA_HOME");
    java_home.unset();

    Status status = detect_java_runtime();

    ASSERT_FALSE(status.ok());
    ASSERT_NE(std::string::npos, status.to_string().find("JAVA_HOME"));
}

TEST(JvmRuntimeTest, detect_java_runtime_reports_null_jni_env) {
    ScopedEnvVar java_home("JAVA_HOME");
    java_home.set("/tmp");

    Status status = detect_java_runtime();

    ASSERT_FALSE(status.ok());
    ASSERT_NE(std::string::npos, status.to_string().find("JNIEnv"));
}

} // namespace starrocks
