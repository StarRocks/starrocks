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

#include "exec/write_combined_txn_log.h"

#include <gtest/gtest.h>

#include <map>

#include "testutil/assert.h"
#include "util/failpoint/fail_point.h"

namespace starrocks::lake {

class WriteCombinedTxnLogTest : public testing::Test {
public:
    WriteCombinedTxnLogTest() {}
};

TEST_F(WriteCombinedTxnLogTest, test_write_combined_txn_log_parallel) {
    std::map<int64_t, CombinedTxnLogPB> txn_log_map;
    size_t N = 2;
    for (int64_t i = 0; i < N; i++) {
        CombinedTxnLogPB combinde_txn_log_pb;
        txn_log_map.insert(std::make_pair(i, std::move(combinde_txn_log_pb)));
    }
    PFailPointTriggerMode trigger_mode;
    trigger_mode.set_mode(FailPointTriggerModeType::ENABLE);
    auto fp = starrocks::failpoint::FailPointRegistry::GetInstance()->get("put_combined_txn_log_success");
    fp->setMode(trigger_mode);
    ASSERT_TRUE(write_combined_txn_log_parallel(txn_log_map).ok());
    trigger_mode.set_mode(FailPointTriggerModeType::DISABLE);
    fp->setMode(trigger_mode);

    trigger_mode.set_mode(FailPointTriggerModeType::ENABLE);
    fp = starrocks::failpoint::FailPointRegistry::GetInstance()->get("put_combined_txn_log_fail");
    fp->setMode(trigger_mode);
    ASSERT_FALSE(write_combined_txn_log_parallel(txn_log_map).ok());
    trigger_mode.set_mode(FailPointTriggerModeType::DISABLE);
    fp->setMode(trigger_mode);
}

} // namespace starrocks::lake