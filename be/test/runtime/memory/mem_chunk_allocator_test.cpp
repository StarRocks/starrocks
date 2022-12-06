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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/runtime/memory/chunk_allocator_test.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "runtime/memory/mem_chunk_allocator.h"

#include <gtest/gtest.h>

#include "common/config.h"
#include "runtime/memory/mem_chunk.h"

namespace starrocks {

TEST(MemChunkAllocatorTest, Normal) {
    config::use_mmap_allocate_chunk = true;
    for (size_t size = 4096; size <= 1024 * 1024; size <<= 1) {
        MemChunk chunk;
        ASSERT_TRUE(MemChunkAllocator::instance()->allocate(size, &chunk));
        ASSERT_NE(nullptr, chunk.data);
        ASSERT_EQ(size, chunk.size);
        MemChunkAllocator::instance()->free(chunk);
    }
}
} // namespace starrocks
