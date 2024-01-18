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

#include "runtime/data_stream_mgr.h"

#include <gtest/gtest.h>

namespace starrocks {

TEST(DataStreamMgr, pass_through_buffer_test) {
    auto mgr = std::make_unique<DataStreamMgr>();

    TUniqueId query_id;
    query_id.lo = 1121;
    query_id.hi = 2023;

    // register the same query_id twice
    mgr->prepare_pass_through_chunk_buffer(query_id);
    mgr->prepare_pass_through_chunk_buffer(query_id);

    // unregister one
    mgr->destroy_pass_through_chunk_buffer(query_id);
    // close with no issue
    mgr->close();
    // unregister for the second
    mgr->destroy_pass_through_chunk_buffer(query_id);
    // no issue, all resources released
    mgr.reset();
}

} // namespace starrocks
