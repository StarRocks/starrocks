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

} // namespace starrocks
