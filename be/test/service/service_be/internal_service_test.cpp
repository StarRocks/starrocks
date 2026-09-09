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

#include "service/service_be/internal_service.h"

#include <brpc/controller.h>
#include <gtest/gtest.h>

#include <atomic>
#include <memory>

#include "base/testutil/assert.h"
#include "base/testutil/sync_point.h"
#include "base/uid_util.h"
#include "base/utility/defer_op.h"
#include "cache/datacache.h"
#include "cache/disk_cache/test_cache_utils.h"
#include "common/process_exit.h"
#include "compute_env/load/stream_load_metrics.h"
#include "data_sink/tablet/tablet_sink_index_channel.h"
#include "data_workflows/load/tablet_writer/load_channel_mgr.h"
#include "exec/exec_env.h"
#include "exec/runtime/query_context_manager.h"
#include "orchestration/orchestration_env.h"
#include "platform/platform_env.h"
#include "runtime/runtime_env.h"
#include "service/brpc_service_test_util.h"

namespace starrocks {

class InternalServiceTest : public testing::Test {
public:
    void SetUp() override {
        _load_channel_mgr = std::make_unique<LoadChannelMgr>(nullptr, RuntimeEnv::GetInstance()->diagnose_daemon(),
                                                             PlatformEnv::GetInstance()->brpc_stub_cache());
        ASSERT_OK(_load_channel_mgr->init(RuntimeEnv::GetInstance()->load_mem_tracker()));
    }

    void TearDown() override {
        if (_load_channel_mgr != nullptr) {
            _load_channel_mgr->close();
        }
    }

protected:
    std::unique_ptr<LoadChannelMgr> _load_channel_mgr;
};

TEST_F(InternalServiceTest, test_get_info_timeout_invalid) {
    BackendInternalServiceImpl<PInternalService> service(ExecEnv::GetInstance(), nullptr, _load_channel_mgr.get());
    PProxyRequest request;
    PProxyResult response;
    service._get_info_impl(&request, &response, nullptr, -10);
    auto st = Status(response.status());
    ASSERT_TRUE(st.is_time_out());
}

TEST_F(InternalServiceTest, test_submit_mv_maintenance_task_not_supported) {
    BackendInternalServiceImpl<PInternalService> service(ExecEnv::GetInstance(), nullptr, _load_channel_mgr.get());
    PMVMaintenanceTaskRequest request;
    PMVMaintenanceTaskResult response;
    brpc::Controller cntl;
    MockClosure closure;

    service.submit_mv_maintenance_task(&cntl, &request, &response, &closure);

    auto st = Status(response.status());
    ASSERT_TRUE(st.is_not_supported());
    ASSERT_TRUE(st.message().find("Legacy incremental MV maintenance is no longer supported") != std::string::npos);
}

TEST_F(InternalServiceTest, test_tablet_writer_add_chunks_via_http) {
    BackendInternalServiceImpl<PInternalService> service(ExecEnv::GetInstance(), nullptr, _load_channel_mgr.get());
    {
        PHttpRequest request;
        PTabletWriterAddBatchResult response;
        brpc::Controller cntl;
        MockClosure closure;
        service.tablet_writer_add_chunks_via_http(&cntl, &request, &response, &closure);
        auto st = Status(response.status());
        ASSERT_FALSE(st.ok());
    }
    {
        brpc::Controller cntl;
        PTabletWriterAddChunksRequest req;
        auto* r = req.add_requests();
        r->set_txn_id(1000);
        r->set_index_id(2000);
        r->set_sender_id(3000);
        serialize_to_iobuf<PTabletWriterAddChunksRequest>(req, &cntl.request_attachment());
        PHttpRequest request;
        PTabletWriterAddBatchResult response;
        MockClosure closure;
        service.tablet_writer_add_chunks_via_http(&cntl, &request, &response, &closure);
        auto st = Status(response.status());
        ASSERT_FALSE(st.ok());
        ASSERT_TRUE(response.status().error_msgs().at(0).find("no associated load channel") != std::string::npos);
    }
    {
        PHttpRequest request;
        PTabletWriterAddBatchResult response;
        brpc::Controller cntl;
        MockClosure closure;
        service.PInternalServiceImplBase::tablet_writer_add_chunks_via_http(&cntl, &request, &response, &closure);
        auto st = Status(response.status());
        ASSERT_TRUE(st.is_not_supported());
    }
}

TEST_F(InternalServiceTest, test_tablet_writer_add_chunk_via_http) {
    BackendInternalServiceImpl<PInternalService> service(ExecEnv::GetInstance(), nullptr, _load_channel_mgr.get());
    {
        PHttpRequest request;
        PTabletWriterAddBatchResult response;
        brpc::Controller cntl;
        MockClosure closure;
        service.tablet_writer_add_chunk_via_http(&cntl, &request, &response, &closure);
        auto st = Status(response.status());
        ASSERT_FALSE(st.ok());
    }
    {
        PHttpRequest request;
        PTabletWriterAddBatchResult response;
        brpc::Controller cntl;
        size_t request_size = 123; // fake
        cntl.request_attachment().append(&request_size, sizeof(request_size));
        MockClosure closure;
        service.tablet_writer_add_chunk_via_http(&cntl, &request, &response, &closure);
        auto st = Status(response.status());
        ASSERT_FALSE(st.ok());
    }
    {
        brpc::Controller cntl;
        PTabletWriterAddChunksRequest req;
        auto* r = req.add_requests();
        r->set_txn_id(1000);
        r->set_index_id(2000);
        r->set_sender_id(3000);
        serialize_to_iobuf<PTabletWriterAddChunksRequest>(req, &cntl.request_attachment());
        PHttpRequest request;
        PTabletWriterAddBatchResult response;
        MockClosure closure;
        service.tablet_writer_add_chunk_via_http(&cntl, &request, &response, &closure);
        auto st = Status(response.status());
        ASSERT_FALSE(st.ok());
    }
    {
        brpc::Controller cntl;
        PTabletWriterAddChunkRequest req;
        req.set_txn_id(1000);
        req.set_index_id(2000);
        req.set_sender_id(3000);
        serialize_to_iobuf<PTabletWriterAddChunkRequest>(req, &cntl.request_attachment());
        PHttpRequest request;
        PTabletWriterAddBatchResult response;
        MockClosure closure;
        service.tablet_writer_add_chunk_via_http(&cntl, &request, &response, &closure);
        auto st = Status(response.status());
        ASSERT_FALSE(st.ok());
        ASSERT_TRUE(response.status().error_msgs().at(0).find("no associated load channel") != std::string::npos);
    }
    {
        PHttpRequest request;
        PTabletWriterAddBatchResult response;
        brpc::Controller cntl;
        MockClosure closure;
        service.PInternalServiceImplBase::tablet_writer_add_chunk_via_http(&cntl, &request, &response, &closure);
        auto st = Status(response.status());
        ASSERT_TRUE(st.is_not_supported());
    }
}

TEST_F(InternalServiceTest, test_load_diagnose) {
    BackendInternalServiceImpl<PInternalService> service(ExecEnv::GetInstance(), nullptr, _load_channel_mgr.get());
    PLoadDiagnoseRequest request;
    request.set_txn_id(1);
    request.mutable_id()->set_hi(0);
    request.mutable_id()->set_lo(0);
    request.set_profile(true);
    request.set_stack_trace(true);
    PLoadDiagnoseResult response;
    brpc::Controller cntl;
    MockClosure closure;
    service.load_diagnose(&cntl, &request, &response, &closure);
    ASSERT_TRUE(response.has_profile_status());
    auto st = Status(response.profile_status());
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.message().find("can't find the load channel") != std::string::npos);
    ASSERT_TRUE(response.has_stack_trace_status());
    st = Status(response.stack_trace_status());
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.message().find("can't find the load channel") != std::string::npos);
}

#ifdef WITH_STARCACHE
TEST_F(InternalServiceTest, test_fetch_datacache_via_brpc) {
    BackendInternalServiceImpl<PInternalService> service(ExecEnv::GetInstance(), nullptr, _load_channel_mgr.get());

    PFetchDataCacheRequest request;
    PFetchDataCacheResponse response;
    request.set_request_id(0);
    request.set_cache_key("test_file");
    request.set_offset(0);
    request.set_size(1024);

    {
        brpc::Controller cntl;
        MockClosure closure;
        service._fetch_datacache(&cntl, &request, &response, &closure);
        auto st = Status(response.status());
        ASSERT_FALSE(st.ok());
    }

    std::shared_ptr<BlockCache> cache;
    {
        DiskCacheOptions options = TestCacheUtils::create_simple_options(256 * KB, 0, 20 * MB);
        options.inline_item_count_limit = 1000;
        cache = TestCacheUtils::create_cache(options);

        const size_t cache_size = 1024;
        const std::string cache_key = "test_file";
        std::string value(cache_size, 'a');
        Status st = cache->write(cache_key, 0, cache_size, value.c_str());
        ASSERT_TRUE(st.ok());

        DataCache* cache_env = DataCache::GetInstance();
        cache_env->set_local_disk_cache(cache->local_cache());
        cache_env->set_block_cache(cache);
    }

    {
        brpc::Controller cntl;
        MockClosure closure;
        service.fetch_datacache(&cntl, &request, &response, &closure);
        for (int retry = 3; retry > 0; --retry) {
            if (closure.has_run()) {
                break;
            }
            sleep(1);
        }
        auto st = Status(response.status());
        // Read cache data.
        ASSERT_TRUE(st.ok()) << st.message();

        IOBuffer buffer;
        cntl.response_attachment().swap(buffer.raw_buf());
        std::string target_value(1024, 'a');
        ASSERT_EQ(buffer.const_raw_buf().to_string(), target_value);
    }
}
#endif

TEST_F(InternalServiceTest, test_get_load_replica_status) {
    BackendInternalServiceImpl<PInternalService> service(ExecEnv::GetInstance(), nullptr, _load_channel_mgr.get());
    PLoadReplicaStatusRequest request;
    request.mutable_load_id()->set_hi(0);
    request.mutable_load_id()->set_lo(0);
    request.set_txn_id(1);
    request.set_sink_id(1);
    request.set_node_id(1);
    request.add_tablet_ids(1);
    PLoadReplicaStatusResult response;
    brpc::Controller cntl;
    MockClosure closure;
    service.get_load_replica_status(&cntl, &request, &response, &closure);
    ASSERT_EQ(1, response.replica_statuses_size());
}

extern std::atomic<bool> k_starrocks_exit;
extern std::atomic<bool> k_starrocks_quick_exit;
extern std::atomic<bool> k_starrocks_force_reject;

TEST_F(InternalServiceTest, test_short_circuit_rejected_while_shutting_down) {
    // Verify rejection and guard cleanup for a short-circuit RPC.
    k_starrocks_exit.store(true);
    k_starrocks_force_reject.store(true);
    orchestration::OrchestrationEnv orchestration_env;

    BackendInternalServiceImpl<PInternalService> service(ExecEnv::GetInstance(), &orchestration_env,
                                                         _load_channel_mgr.get());

    PExecShortCircuitRequest request;
    PExecShortCircuitResult response;
    brpc::Controller cntl;
    MockClosure closure;

    service.exec_short_circuit(&cntl, &request, &response, &closure);

    ASSERT_TRUE(cntl.Failed());
    ASSERT_EQ(brpc::EINTERNAL, cntl.ErrorCode());
    // ErrorText includes brpc's error-code prefix.
    ASSERT_EQ("[E2001]BE is shutting down", cntl.ErrorText());

    k_starrocks_exit.store(false);
    k_starrocks_force_reject.store(false);
}

TEST_F(InternalServiceTest, test_exec_plan_fragment_rejected_while_shutting_down) {
    // Verify rejection and guard cleanup for a fragment-prep RPC.
    k_starrocks_exit.store(true);
    k_starrocks_force_reject.store(true);

    orchestration::OrchestrationEnv orchestration_env;
    BackendInternalServiceImpl<PInternalService> service(ExecEnv::GetInstance(), &orchestration_env,
                                                         _load_channel_mgr.get());

    PExecPlanFragmentRequest request;
    PExecPlanFragmentResult response;
    brpc::Controller cntl;
    MockClosure closure;

    // Rejection moved to the public entry (admission gate): force_reject makes it SetFailed
    // and restore the inflight count, instead of the private worker which no longer rejects.
    service.exec_plan_fragment(&cntl, &request, &response, &closure);

    ASSERT_TRUE(cntl.Failed());
    ASSERT_EQ(brpc::EINTERNAL, cntl.ErrorCode());
    ASSERT_EQ("[E2001]BE is shutting down", cntl.ErrorText());

    k_starrocks_exit.store(false);
    k_starrocks_force_reject.store(false);
}

TEST_F(InternalServiceTest, test_exec_batch_plan_fragments_rejected_while_shutting_down) {
    k_starrocks_exit.store(true);
    k_starrocks_force_reject.store(true);

    orchestration::OrchestrationEnv orchestration_env;
    BackendInternalServiceImpl<PInternalService> service(ExecEnv::GetInstance(), &orchestration_env,
                                                         _load_channel_mgr.get());

    PExecBatchPlanFragmentsRequest request;
    PExecBatchPlanFragmentsResult response;
    brpc::Controller cntl;
    MockClosure closure;

    // Rejection moved to the public entry (admission gate), same as exec_plan_fragment.
    service.exec_batch_plan_fragments(&cntl, &request, &response, &closure);

    ASSERT_TRUE(cntl.Failed());
    ASSERT_EQ(brpc::EINTERNAL, cntl.ErrorCode());
    ASSERT_EQ("[E2001]BE is shutting down", cntl.ErrorText());

    k_starrocks_exit.store(false);
    k_starrocks_force_reject.store(false);
}

TEST_F(InternalServiceTest, test_drain_resample_observes_successor_after_predecessor_release) {
    // Drain reads shutdown_work first, then registries. Hold one predecessor, then at
    // before_query_read publish this test's query and drop the predecessor so the
    // re-sample must still see the successor (never a false zero).
    // before+1 / before assumes this fixture is serial: no sibling test mutates
    // shutdown_work during the sample. Not a process-wide invariant.
    orchestration::OrchestrationEnv orchestration_env;
    orchestration_env.set_exec_env_for_test(ExecEnv::GetInstance());

    TUniqueId query_id = UniqueId::gen_uid().to_thrift();
    std::atomic<bool> registered_query{false};

    const size_t before = shutdown_work_inflight();
    std::atomic<bool> owns_shutdown_work{true};
    inc_shutdown_work();
    ASSERT_EQ(before + 1, shutdown_work_inflight());

    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack("OrchestrationEnv::_get_running_fragments_count:before_query_read",
                                          [&](void* arg) {
                                              auto st = ExecEnv::GetInstance()->query_context_mgr()->get_or_register(
                                                      query_id,
                                                      /*return_error_if_not_exist=*/false);
                                              ASSERT_OK(st);
                                              registered_query.store(true);
                                              if (owns_shutdown_work.exchange(false)) {
                                                  dec_shutdown_work();
                                              }
                                          });
    DeferOp clear_sync_point([&] {
        SyncPoint::GetInstance()->ClearCallBack("OrchestrationEnv::_get_running_fragments_count:before_query_read");
        SyncPoint::GetInstance()->DisableProcessing();
        if (registered_query.load()) {
            ExecEnv::GetInstance()->query_context_mgr()->remove(query_id);
        }
        if (owns_shutdown_work.exchange(false)) {
            dec_shutdown_work();
        }
    });

    size_t count = orchestration_env.get_running_fragments_count_for_test();
    ASSERT_GT(count, 0);
    ASSERT_FALSE(owns_shutdown_work.load());
    ASSERT_EQ(before, shutdown_work_inflight());
}

TEST_F(InternalServiceTest, test_quick_exit_still_counts_pipeline_query) {
    ASSERT_NE(nullptr, ExecEnv::GetInstance()->query_context_mgr());
    orchestration::OrchestrationEnv orchestration_env;
    orchestration_env.set_exec_env_for_test(ExecEnv::GetInstance());

    TUniqueId query_id = UniqueId::gen_uid().to_thrift();
    ASSERT_OK(ExecEnv::GetInstance()->query_context_mgr()->get_or_register(query_id, false));
    auto* metrics = StreamLoadMetrics::instance();
    const auto stream_before = metrics->streaming_load_current_processing.value();
    metrics->streaming_load_current_processing.increment(1);
    inc_shutdown_work();
    DeferOp restore([&] {
        ExecEnv::GetInstance()->query_context_mgr()->remove(query_id);
        metrics->streaming_load_current_processing.set_value(stream_before);
        dec_shutdown_work();
        k_starrocks_quick_exit.store(false);
    });

    ASSERT_TRUE(set_process_quick_exit());
    EXPECT_GE(orchestration_env.get_running_fragments_count_for_test(), 1);

    k_starrocks_quick_exit.store(false);
    EXPECT_GE(orchestration_env.get_running_fragments_count_for_test(), 2);
}

} // namespace starrocks
