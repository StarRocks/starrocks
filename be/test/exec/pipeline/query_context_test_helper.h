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

#pragma once

#include <memory>

#include "exec/pipeline/primitives/query_lifecycle.h"
#include "exec/pipeline/query_context.h"

namespace starrocks::pipeline {

class TestQueryLifecycle final : public QueryLifecycle {
public:
    void on_fragment_finished(FragmentContextPtr fragment_ctx) override { (void)fragment_ctx; }
};

inline QueryLifecycle* test_query_lifecycle() {
    static TestQueryLifecycle lifecycle;
    return &lifecycle;
}

inline std::shared_ptr<QueryContext> make_test_query_context() {
    return std::make_shared<QueryContext>(test_query_lifecycle());
}

} // namespace starrocks::pipeline
