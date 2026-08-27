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
#include <set>

#include "gen_cpp/lake_types.pb.h"
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

namespace {
CombinedTxnLogPB make_logs(int64_t partition_id, const std::vector<int64_t>& tablet_ids) {
    CombinedTxnLogPB logs;
    for (int64_t tablet_id : tablet_ids) {
        auto* log = logs.add_txn_logs();
        log->set_tablet_id(tablet_id);
        log->set_txn_id(1000);
        log->set_partition_id(partition_id);
    }
    return logs;
}
} // namespace

// A combined txn log missing an entry makes the transaction permanently unpublishable once it
// commits: publish resolves every tablet inside that object and has no per-tablet fallback. The
// write must be refused instead of leaving a partial object behind.
TEST_F(WriteCombinedTxnLogTest, test_refuse_incomplete_combined_txn_log) {
    // The partition owns tablets {1, 2, 3}; only two of them made it into the collected log.
    std::map<int64_t, CombinedTxnLogPB> txn_log_map;
    txn_log_map.emplace(100, make_logs(100, {1, 2}));
    ExpectedTabletsByPartition expected;
    expected.emplace(100, std::set<int64_t>{1, 2, 3});

    auto st = write_combined_txn_log_parallel(txn_log_map, expected);
    ASSERT_FALSE(st.ok());
    // The parallel path must hand back the rejection itself, not a re-wrapped IOError: a caller
    // that retries on IO errors would otherwise keep retrying an invariant violation.
    ASSERT_TRUE(st.is_internal_error()) << st.to_string();
    const auto msg = st.to_string();
    ASSERT_NE(msg.find("refuse to write incomplete combined txn log"), std::string::npos) << msg;
    ASSERT_NE(msg.find("missing 1 of 3"), std::string::npos) << msg;
    ASSERT_NE(msg.find('3'), std::string::npos) << msg;

    // The single-partition entry point must reject it too.
    auto single = write_combined_txn_log(make_logs(100, {1, 2}), std::set<int64_t>{1, 2, 3});
    ASSERT_FALSE(single.ok());
    ASSERT_NE(single.to_string().find("refuse to write incomplete combined txn log"), std::string::npos)
            << single.to_string();
}

// A complete log, and the legacy no-expectation call, must both get past the coverage check.
// The failpoint stands in for the object-store write that follows it.
TEST_F(WriteCombinedTxnLogTest, test_complete_combined_txn_log_passes_coverage) {
    std::map<int64_t, CombinedTxnLogPB> txn_log_map;
    txn_log_map.emplace(100, make_logs(100, {1, 2, 3}));
    ExpectedTabletsByPartition expected;
    expected.emplace(100, std::set<int64_t>{1, 2, 3});

    PFailPointTriggerMode trigger_mode;
    trigger_mode.set_mode(FailPointTriggerModeType::ENABLE);
    auto fp = starrocks::failpoint::FailPointRegistry::GetInstance()->get("put_combined_txn_log_success");
    fp->setMode(trigger_mode);

    ASSERT_TRUE(write_combined_txn_log_parallel(txn_log_map, expected).ok());
    // No expectation supplied -> unchanged behaviour for callers that have no independent source.
    ASSERT_TRUE(write_combined_txn_log_parallel(txn_log_map).ok());

    trigger_mode.set_mode(FailPointTriggerModeType::DISABLE);
    fp->setMode(trigger_mode);
}

} // namespace starrocks::lake
