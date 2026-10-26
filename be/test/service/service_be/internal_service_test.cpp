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
#include <chrono>
#include <future>
#include <memory>
#include <mutex>
#include <string>

#include "base/testutil/assert.h"
#include "base/utility/defer_op.h"
#include "cache/datacache.h"
#include "cache/disk_cache/test_cache_utils.h"
#include "common/config_ingest_fwd.h"
#include "data_sink/tablet/tablet_sink_index_channel.h"
#include "data_workflows/load/tablet_writer/load_channel_mgr.h"
#include "data_workflows/load/tablet_writer/local_tablets_channel.h"
#include "exec/exec_env.h"
#include "platform/platform_env.h"
#include "runtime/runtime_env.h"
#include "service/brpc_service_test_util.h"

namespace starrocks {

namespace {

class AddSegmentCountingClosure : public google::protobuf::Closure {
public:
    void Run() override { calls.fetch_add(1); }
    std::atomic<int> calls{0};
};

} // namespace

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

// Cover both lock stages through the real BRPC handler -> manager -> channel chain.
class AddSegmentLockTimeoutServiceTest : public InternalServiceTest, public testing::WithParamInterface<bool> {};

TEST_P(AddSegmentLockTimeoutServiceTest, channel_lock_timeout_completes_rpc_once) {
    const bool hold_tablets_lock = GetParam();
    SCOPED_TRACE(hold_tablets_lock ? "R" : "M");
    const auto previous_timeout = config::tablet_writer_add_segment_lock_timeout_ms;
    DeferOp restore([&] { config::tablet_writer_add_segment_lock_timeout_ms = previous_timeout; });
    config::tablet_writer_add_segment_lock_timeout_ms = 20;

    PTabletWriterAddSegmentRequest request;
    request.mutable_id()->set_hi(41);
    request.mutable_id()->set_lo(42);
    request.set_txn_id(43);
    request.set_index_id(44);
    request.set_sink_id(45);
    request.set_tablet_id(46);
    request.set_eos(false);
    request.mutable_segment()->set_segment_id(0);
    request.mutable_segment()->set_data_size(0);
    const UniqueId load_id(request.id());
    auto* runtime = RuntimeEnv::GetInstance();
    auto* cache = PlatformEnv::GetInstance()->brpc_stub_cache();
    auto channel = std::make_shared<LoadChannel>(
            _load_channel_mgr.get(), nullptr, runtime->diagnose_daemon(), cache, load_id, request.txn_id(), "", 60,
            std::make_unique<MemTracker>(-1, "segment_lock_test", runtime->load_mem_tracker()));
    const TabletsChannelKey key(request.id(), request.sink_id(), request.index_id());
    auto tablets_channel =
            new_local_tablets_channel(channel.get(), key, channel->mem_tracker(), channel->_root_profile.get(), cache);
    channel->_tablets_channels.emplace(key, tablets_channel);
    {
        std::lock_guard lock(_load_channel_mgr->_lock);
        _load_channel_mgr->_load_channels.emplace(load_id, channel);
    }
    BackendInternalServiceImpl<PInternalService> service(ExecEnv::GetInstance(), nullptr, _load_channel_mgr.get());
    brpc::Controller cntl;
    PTabletWriterAddSegmentResult response;
    AddSegmentCountingClosure done;
    std::unique_lock m_holder(channel->_lock, std::defer_lock);
    std::unique_lock r_holder(tablets_channel->_rw_mtx, std::defer_lock);
    if (hold_tablets_lock) {
        r_holder.lock();
    } else {
        m_holder.lock();
    }
    auto call = std::async(std::launch::async,
                           [&] { service.tablet_writer_add_segment(&cntl, &request, &response, &done); });
    const auto ready = call.wait_for(std::chrono::seconds(5));
    if (m_holder.owns_lock()) m_holder.unlock();
    if (r_holder.owns_lock()) r_holder.unlock();
    call.get();
    ASSERT_EQ(std::future_status::ready, ready);
    ASSERT_EQ(TStatusCode::TIMEOUT, response.status().status_code());
    ASSERT_EQ(1, response.status().error_msgs_size());
    const auto& message = response.status().error_msgs(0);
    EXPECT_NE(std::string::npos,
              message.find(hold_tablets_lock ? "LocalTabletsChannel::_rw_mtx" : "LoadChannel::_lock"));
    EXPECT_NE(std::string::npos, message.find("txn_id: 43"));
    EXPECT_EQ(1, done.calls.load());
    EXPECT_EQ(1, channel->_num_segment);
    EXPECT_EQ(0, response.tablet_vec_size());
    EXPECT_EQ(0, response.failed_tablet_vec_size());
    std::unique_lock m_probe(channel->_lock, std::try_to_lock);
    EXPECT_TRUE(m_probe.owns_lock());
    std::unique_lock r_probe(tablets_channel->_rw_mtx, std::try_to_lock);
    EXPECT_TRUE(r_probe.owns_lock());
}

INSTANTIATE_TEST_SUITE_P(BrpcAtMAndR, AddSegmentLockTimeoutServiceTest, testing::Bool());

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

} // namespace starrocks
