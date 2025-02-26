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

#include "runtime/diagnose_daemon.h"

#include <gtest/gtest.h>

#include "testutil/assert.h"

namespace starrocks {

class DiagnoseDaemonTest : public testing::Test {
public:
    void SetUp() override {
        _daemon = std::make_unique<DiagnoseDaemon>();
        ASSERT_OK(_daemon->init());
    }

    void TearDown() override {
        if (_daemon) {
            _daemon->stop();
            _daemon.reset();
        }
    }

protected:
    std::unique_ptr<DiagnoseDaemon> _daemon;
};

TEST_F(DiagnoseDaemonTest, test_stack_trace) {
    DiagnoseRequest request1;
    request1.type = DiagnoseType::STACK_TRACE;
    request1.context = "trace1";
    ASSERT_OK(_daemon->diagnose(request1));

    DiagnoseRequest request2;
    request2.type = DiagnoseType::STACK_TRACE;
    request2.context = "trace2";
    ASSERT_OK(_daemon->diagnose(request2));
    _daemon->thread_pool()->wait();
}

} // namespace starrocks