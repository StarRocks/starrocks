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

#include "runtime/remote_scan_token_mgr.h"

#include <gtest/gtest.h>

#include "base/time/time.h"
#include "base/uid_util.h"
#include "exec/exec_env.h"
#include "exec/pipeline/sink/remote_scan_result_sink_operator.h"
#include "runtime/remote_chunk_queue_mgr.h"
#include "runtime/runtime_state.h"

namespace starrocks {

class RemoteScanTokenMgrTest : public testing::Test {
protected:
    static TUniqueId fragment_id(int64_t hi, int64_t lo) {
        TUniqueId id;
        id.hi = hi;
        id.lo = lo;
        return id;
    }
};

TEST_F(RemoteScanTokenMgrTest, register_and_lookup_token) {
    RemoteScanTokenMgr mgr;
    TUniqueId expected = fragment_id(10, 20);

    ASSERT_TRUE(mgr.register_token("token-1", expected, TStarRocksScanTransport::STARROCKS_BRPC_CHUNK, 0).ok());
    ASSERT_EQ(1, mgr.size());

    TUniqueId actual;
    ASSERT_TRUE(mgr.lookup("token-1", TStarRocksScanTransport::STARROCKS_BRPC_CHUNK, &actual).ok());
    ASSERT_EQ(expected.hi, actual.hi);
    ASSERT_EQ(expected.lo, actual.lo);
}

TEST_F(RemoteScanTokenMgrTest, reject_empty_or_unknown_token) {
    RemoteScanTokenMgr mgr;
    TUniqueId actual;

    ASSERT_TRUE(mgr.register_token("", fragment_id(1, 1), TStarRocksScanTransport::STARROCKS_ARROW_FLIGHT, 0)
                        .is_invalid_argument());
    ASSERT_TRUE(mgr.lookup("", TStarRocksScanTransport::STARROCKS_ARROW_FLIGHT, &actual).is_invalid_argument());
    ASSERT_TRUE(mgr.lookup("missing", TStarRocksScanTransport::STARROCKS_ARROW_FLIGHT, &actual).is_not_found());
}

TEST_F(RemoteScanTokenMgrTest, reject_transport_mismatch) {
    RemoteScanTokenMgr mgr;
    auto transport = TStarRocksScanTransport::STARROCKS_ARROW_FLIGHT;
    ASSERT_TRUE(mgr.register_token("token-1", fragment_id(1, 2), transport, 0).ok());

    TUniqueId actual;
    ASSERT_TRUE(mgr.lookup("token-1", TStarRocksScanTransport::STARROCKS_BRPC_CHUNK, &actual).is_invalid_argument());
    ASSERT_EQ(1, mgr.size());
}

TEST_F(RemoteScanTokenMgrTest, expired_token_lookup_reports_not_found_without_erasing) {
    RemoteScanTokenMgr mgr;
    auto transport = TStarRocksScanTransport::STARROCKS_ARROW_FLIGHT;
    ASSERT_TRUE(mgr.register_token("token-1", fragment_id(1, 2), transport, 1).ok());

    // Lookup reports expiry but must NOT erase the token: the background sweep reaps the token
    // together with its queue, so erasing on lookup would orphan the queue and leak its data.
    TUniqueId actual;
    ASSERT_TRUE(mgr.lookup("token-1", TStarRocksScanTransport::STARROCKS_ARROW_FLIGHT, &actual).is_not_found());
    ASSERT_EQ(1, mgr.size());

    // The active cleanup sweep is what actually removes the expired token.
    ASSERT_EQ(1, mgr.cleanup_expired_tokens(UnixMillis()).size());
    ASSERT_EQ(0, mgr.size());
}

TEST_F(RemoteScanTokenMgrTest, expired_token_is_removed_by_active_cleanup) {
    RemoteScanTokenMgr mgr;
    auto transport = TStarRocksScanTransport::STARROCKS_BRPC_CHUNK;
    TUniqueId expected = fragment_id(7, 8);
    ASSERT_TRUE(mgr.register_token("token-1", expected, transport, 100).ok());

    auto expired_tokens = mgr.cleanup_expired_tokens(101);
    ASSERT_EQ(1, expired_tokens.size());
    EXPECT_EQ("token-1", expired_tokens[0].scan_token);
    EXPECT_EQ(expected.hi, expired_tokens[0].fragment_instance_id.hi);
    EXPECT_EQ(expected.lo, expired_tokens[0].fragment_instance_id.lo);
    EXPECT_EQ(transport, expired_tokens[0].transport);
    EXPECT_EQ(0, mgr.size());

    TUniqueId actual;
    ASSERT_TRUE(mgr.lookup("token-1", transport, &actual).is_not_found());
}

TEST_F(RemoteScanTokenMgrTest, remove_token) {
    RemoteScanTokenMgr mgr;
    auto transport = TStarRocksScanTransport::STARROCKS_ARROW_FLIGHT;
    ASSERT_TRUE(mgr.register_token("token-1", fragment_id(1, 2), transport, 0).ok());
    ASSERT_TRUE(mgr.remove("token-1").ok());
    ASSERT_EQ(0, mgr.size());

    TUniqueId actual;
    ASSERT_TRUE(mgr.lookup("token-1", TStarRocksScanTransport::STARROCKS_ARROW_FLIGHT, &actual).is_not_found());
}

TEST_F(RemoteScanTokenMgrTest, result_sink_factory_close_keeps_token_for_consumer_fetch) {
    auto* exec_env = ExecEnv::GetInstance();
    TUniqueId query_id = fragment_id(100, 200);
    TUniqueId fragment_instance_id = fragment_id(100, 201);
    TQueryOptions query_options;
    TQueryGlobals query_globals;
    RuntimeState state(query_id, fragment_instance_id, query_options, query_globals,
                       &exec_env->query_execution_services(), exec_env);

    TRemoteScanResultSink sink;
    sink.__set_transport(TStarRocksScanTransport::STARROCKS_BRPC_CHUNK);
    sink.__set_scan_token("close-keeps-token");
    sink.__set_expire_ms(UnixMillis() + 60000);
    pipeline::RemoteScanResultSinkOperatorFactory factory(1, RowDescriptor(), {}, sink);
    ASSERT_TRUE(factory.prepare(&state).ok());

    std::string instance_token = sink.scan_token + ":" + print_id(fragment_instance_id);
    state.set_is_cancelled(true);
    factory.close(&state);

    TUniqueId actual;
    ASSERT_TRUE(exec_env->remote_scan_token_mgr()
                        ->lookup(instance_token, TStarRocksScanTransport::STARROCKS_BRPC_CHUNK, &actual)
                        .ok());
    EXPECT_EQ(fragment_instance_id.hi, actual.hi);
    EXPECT_EQ(fragment_instance_id.lo, actual.lo);

    ASSERT_TRUE(exec_env->remote_scan_token_mgr()->remove(instance_token).ok());
    ASSERT_TRUE(exec_env->remote_chunk_queue_mgr()->cancel(fragment_instance_id).ok());
}

TEST_F(RemoteScanTokenMgrTest, finished_result_sink_cancel_keeps_token_until_consumer_fetches_eos) {
    auto* exec_env = ExecEnv::GetInstance();
    TUniqueId query_id = fragment_id(110, 210);
    TUniqueId fragment_instance_id = fragment_id(110, 211);
    TQueryOptions query_options;
    TQueryGlobals query_globals;
    RuntimeState state(query_id, fragment_instance_id, query_options, query_globals,
                       &exec_env->query_execution_services(), exec_env);

    TRemoteScanResultSink sink;
    sink.__set_transport(TStarRocksScanTransport::STARROCKS_BRPC_CHUNK);
    sink.__set_scan_token("finished-cancel-keeps-token");
    sink.__set_expire_ms(UnixMillis() + 60000);
    pipeline::RemoteScanResultSinkOperatorFactory factory(1, RowDescriptor(), {}, sink);
    ASSERT_TRUE(factory.prepare(&state).ok());

    auto first_operator = factory.create(1, 0);
    ASSERT_TRUE(first_operator->prepare(&state).ok());

    std::string instance_token = sink.scan_token + ":" + print_id(fragment_instance_id);
    ASSERT_TRUE(first_operator->set_finishing(&state).ok());
    ASSERT_FALSE(first_operator->pending_finish());
    ASSERT_TRUE(first_operator->set_cancelled(&state).ok());

    TUniqueId actual;
    ASSERT_TRUE(exec_env->remote_scan_token_mgr()
                        ->lookup(instance_token, TStarRocksScanTransport::STARROCKS_BRPC_CHUNK, &actual)
                        .ok());
    EXPECT_EQ(fragment_instance_id.hi, actual.hi);
    EXPECT_EQ(fragment_instance_id.lo, actual.lo);

    ChunkPB chunk;
    bool eos = false;
    Status fetch_status = exec_env->remote_chunk_queue_mgr()->fetch_chunk(fragment_instance_id, 0, &chunk, &eos);
    ASSERT_TRUE(fetch_status.ok()) << fetch_status;
    EXPECT_TRUE(eos);

    ASSERT_TRUE(exec_env->remote_scan_token_mgr()->remove(instance_token).ok());
}

TEST_F(RemoteScanTokenMgrTest, cancelled_before_finish_publishes_failure_not_clean_eos) {
    auto* exec_env = ExecEnv::GetInstance();
    TUniqueId query_id = fragment_id(120, 220);
    TUniqueId fragment_instance_id = fragment_id(120, 221);
    TQueryOptions query_options;
    TQueryGlobals query_globals;
    RuntimeState state(query_id, fragment_instance_id, query_options, query_globals,
                       &exec_env->query_execution_services(), exec_env);

    TRemoteScanResultSink sink;
    sink.__set_transport(TStarRocksScanTransport::STARROCKS_BRPC_CHUNK);
    sink.__set_scan_token("cancel-before-finish");
    sink.__set_expire_ms(UnixMillis() + 60000);
    pipeline::RemoteScanResultSinkOperatorFactory factory(1, RowDescriptor(), {}, sink);
    ASSERT_TRUE(factory.prepare(&state).ok());

    auto first_operator = factory.create(1, 0);
    ASSERT_TRUE(first_operator->prepare(&state).ok());

    // The driver cancel path always runs set_finishing before set_cancelled.
    // With the query cancelled mid-execution the input has not genuinely ended,
    // so the consumer must observe a failure instead of a clean EOS over the
    // (possibly truncated) data.
    state.set_is_cancelled(true);
    ASSERT_TRUE(first_operator->set_finishing(&state).ok());
    ASSERT_TRUE(first_operator->set_cancelled(&state).ok());

    ChunkPB chunk;
    bool eos = false;
    Status fetch_status = exec_env->remote_chunk_queue_mgr()->fetch_chunk(fragment_instance_id, 0, &chunk, &eos);
    ASSERT_FALSE(fetch_status.ok());

    // The cancel path owns the token cleanup; the token must be gone so a
    // consumer retry fails fast instead of waiting for expiry.
    TUniqueId actual;
    std::string instance_token = sink.scan_token + ":" + print_id(fragment_instance_id);
    ASSERT_TRUE(exec_env->remote_scan_token_mgr()
                        ->lookup(instance_token, TStarRocksScanTransport::STARROCKS_BRPC_CHUNK, &actual)
                        .is_not_found());
}

} // namespace starrocks
