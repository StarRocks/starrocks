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

<<<<<<< HEAD:fe/fe-core/src/main/java/com/starrocks/sql/ast/SetListItem.java
package com.starrocks.sql.ast;

import com.starrocks.analysis.ParseNode;
import com.starrocks.sql.parser.NodePosition;
=======
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>

#include <atomic>

#include "google/protobuf/stubs/callback.h"
#include "util/countdown_latch.h"
>>>>>>> 5b7877e008 ([BugFix] Prevent reopening of aborted load channels (#66793)):be/test/service/brpc_service_test_util.h

public class SetListItem implements ParseNode {

    protected final NodePosition pos;

    public SetListItem(NodePosition pos) {
        this.pos = pos;
    }

<<<<<<< HEAD:fe/fe-core/src/main/java/com/starrocks/sql/ast/SetListItem.java
    @Override
    public NodePosition getPos() {
        return pos;
    }
}
=======
    bool has_run() { return _run.load(); }

private:
    std::atomic_bool _run = false;
};

class MockCountDownClosure : public MockClosure {
public:
    using BThreadCountDownLatch = GenericCountDownLatch<bthread::Mutex, bthread::ConditionVariable>;
    MockCountDownClosure() : _latch(1) {}
    ~MockCountDownClosure() override = default;

    void wait() { _latch.wait(); }

    void Run() override {
        MockClosure::Run();
        _latch.count_down();
    }

private:
    BThreadCountDownLatch _latch;
};

} // namespace starrocks
>>>>>>> 5b7877e008 ([BugFix] Prevent reopening of aborted load channels (#66793)):be/test/service/brpc_service_test_util.h
