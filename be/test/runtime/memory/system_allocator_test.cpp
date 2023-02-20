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
//   https://github.com/apache/incubator-doris/blob/master/be/test/runtime/memory/system_allocator_test.cpp

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

#include "runtime/memory/system_allocator.h"

#include <gtest/gtest.h>

#include <memory>

#include "common/config.h"
#include "runtime/mem_tracker.h"

namespace starrocks {

template <bool use_mmap>
void test_normal() {
    std::unique_ptr<MemTracker> mem_tracker = std::make_unique<MemTracker>(-1);
    config::use_mmap_allocate_chunk = use_mmap;
    {
        auto ptr = SystemAllocator::allocate(mem_tracker.get(), 4096);
        ASSERT_NE(nullptr, ptr);
        ASSERT_EQ(0, (uint64_t)ptr % 4096);
        SystemAllocator::free(mem_tracker.get(), ptr, 4096);
    }
    {
        auto ptr = SystemAllocator::allocate(mem_tracker.get(), 100);
        ASSERT_NE(nullptr, ptr);
        ASSERT_EQ(0, (uint64_t)ptr % 4096);
        SystemAllocator::free(mem_tracker.get(), ptr, 100);
    }
}

TEST(SystemAllocatorTest, TestNormal) {
    test_normal<true>();
    test_normal<false>();
}

} // namespace starrocks
