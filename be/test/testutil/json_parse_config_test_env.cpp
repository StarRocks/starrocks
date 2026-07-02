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

#include "common/config_types_fwd.h"

namespace starrocks {
namespace {

// JsonValue::parse_json_or_string() rejects a document once its array/object nesting exceeds
// config::json_parse_max_nesting_depth. That config is a zero-initialized global whose 10000 default
// is only applied by config::init(). Lightweight unit-test binaries (types_test, column_test,
// expr_core_test, ...) link gtest_main and never call config::init(), so the limit would read 0 and
// every nested document would be rejected. Restore it to the configured default before any test runs.
//
// Scoped to this single config on purpose: a full config::init() here would also flip every other
// config to its production default (e.g. enable_json_flat), changing behavior these binaries have
// always exercised with zero-initialized config, and would clobber be_test.conf overrides if this
// file were ever linked into the full test binary.
class JsonParseConfigTestEnv : public ::testing::Environment {
public:
    void SetUp() override {
        // Keep in sync with the CONF_mInt32 default in be/src/common/config.h.
        config::json_parse_max_nesting_depth = 10000;
    }
};

[[maybe_unused]] const ::testing::Environment* const kJsonParseConfigTestEnv =
        ::testing::AddGlobalTestEnvironment(new JsonParseConfigTestEnv());

} // namespace
} // namespace starrocks
