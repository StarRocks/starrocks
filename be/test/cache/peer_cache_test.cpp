// Copyright 2020-present StarRocks, Inc. All rights reserved.
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

#include <fmt/format.h>
#include <gtest/gtest.h>

#include "cache/disk_cache/io_buffer.h"
#include "cache/disk_cache/local_disk_cache_engine.h"
#include "cache/peer_cache_engine.h"
#include "common/brpc/brpc_stub_cache.h"
#include "common/bthread_timer.h"
#include "common/statusor.h"

namespace starrocks {

class PeerCacheTest : public ::testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(PeerCacheTest, unsupported_op) {
    PeerCacheEngine peer_cache;
    IOBuffer buf;
    Status st = peer_cache.write("test_file", buf, nullptr);
    ASSERT_TRUE(st.is_not_supported());

    st = peer_cache.remove("test_file");
    ASSERT_TRUE(st.is_not_supported());
}

TEST_F(PeerCacheTest, io_adaptor) {
    PeerCacheEngine peer_cache;
    RemoteCacheOptions options;
    options.skip_read_factor = 1.0;
    Status st = peer_cache.init(options);
    ASSERT_TRUE(st.ok());

    for (size_t i = 0; i < 100; ++i) {
        peer_cache.record_read_cache(1024, 10000);
        peer_cache.record_read_remote(1024, 1000);
    }

    DiskCacheReadOptions read_options;
    read_options.use_adaptor = true;
    IOBuffer buf;
    st = peer_cache.read("test_key", 0, 100, &buf, &read_options);
    ASSERT_TRUE(st.is_resource_busy());
}

// When the peer node's host cannot be resolved (e.g. it is restarting during a rolling upgrade),
// BrpcStubCache::get_stub() returns nullptr. PeerCacheEngine::read must return an error instead of
// dereferencing the null stub, so the caller can fall back to remote storage. Before the guard was
// added this path crashed with SIGSEGV.
TEST_F(PeerCacheTest, read_returns_error_when_peer_stub_unavailable) {
    BthreadTimer timer;
    ASSERT_TRUE(timer.start().ok());
    BrpcStubCache stub_cache(&timer);

    PeerCacheEngine peer_cache;
    RemoteCacheOptions options;
    options.skip_read_factor = 1.0;
    ASSERT_TRUE(peer_cache.init(options).ok());
    // Inject the stub cache into the engine under test so read() exercises the get_stub()-returns-null
    // guard rather than the early-out for a missing stub cache.
    peer_cache.set_stub_cache(&stub_cache);

    DiskCacheReadOptions read_options;
    read_options.use_adaptor = false;
    // An unresolvable host makes BrpcStubCache::get_stub() return nullptr.
    read_options.remote_host = "invalid.cm.invalid";
    read_options.remote_port = 8060;

    IOBuffer buf;
    Status st = peer_cache.read("test_key", 0, 100, &buf, &read_options);
    ASSERT_TRUE(st.is_internal_error());
}

} // namespace starrocks
