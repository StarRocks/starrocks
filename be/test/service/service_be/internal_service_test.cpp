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

#include <memory>

#include "common/utils.h"
#include "exec/tablet_sink_index_channel.h"
#include "runtime/exec_env.h"
#include "service/brpc_service_test_util.h"

namespace starrocks {

class InternalServiceTest : public testing::Test {};

TEST_F(InternalServiceTest, test_get_info_timeout_invalid) {
    BackendInternalServiceImpl<PInternalService> service(ExecEnv::GetInstance());
    PProxyRequest request;
    PProxyResult response;
    service._get_info_impl(&request, &response, nullptr, -10);
    auto st = Status(response.status());
    ASSERT_TRUE(st.is_time_out());
}

TEST_F(InternalServiceTest, test_tablet_writer_add_chunks_via_http) {
    BackendInternalServiceImpl<PInternalService> service(ExecEnv::GetInstance());
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
    BackendInternalServiceImpl<PInternalService> service(ExecEnv::GetInstance());
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
    BackendInternalServiceImpl<PInternalService> service(ExecEnv::GetInstance());
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

TEST_F(InternalServiceTest, test_fetch_datacache_via_brpc) {
    BackendInternalServiceImpl<PInternalService> service(ExecEnv::GetInstance());

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

    std::shared_ptr<BlockCache> cache(new BlockCache);
    {
        CacheOptions options;
        options.mem_space_size = 20 * 1024 * 1024;
        options.block_size = 256 * 1024 * 1024;
        options.max_concurrent_inserts = 100000;
        options.max_flying_memory_mb = 100;
        options.engine = "starcache";
        options.inline_item_count_limit = 1000;
        Status status = cache->init(options);
        ASSERT_TRUE(status.ok());

        const size_t cache_size = 1024;
        const std::string cache_key = "test_file";
        std::string value(cache_size, 'a');
        Status st = cache->write(cache_key, 0, cache_size, value.c_str());
        ASSERT_TRUE(st.ok());

        CacheEnv* cache_env = CacheEnv::GetInstance();
        cache_env->_block_cache = cache;
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

} // namespace starrocks
