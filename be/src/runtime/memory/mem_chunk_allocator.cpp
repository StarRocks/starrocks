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
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/memory/chunk_allocator.cpp

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

#include "runtime/current_thread.h"
#include "runtime/memory/mem_chunk.h"
#include "runtime/memory/system_allocator.h"
#include "util/failpoint/fail_point.h"
#include "util/runtime_profile.h"
#include "util/starrocks_metrics.h"

namespace starrocks {

static IntCounter system_alloc_count(MetricUnit::NOUNIT);
static IntCounter system_free_count(MetricUnit::NOUNIT);
static IntCounter system_alloc_cost_ns(MetricUnit::NANOSECONDS);
static IntCounter system_free_cost_ns(MetricUnit::NANOSECONDS);

void MemChunkAllocator::init_metrics() {
#define REGISTER_METIRC_WITH_NAME(name, metric) StarRocksMetrics::instance()->metrics()->register_metric(#name, &metric)

#define REGISTER_METIRC_WITH_PREFIX(prefix, name) REGISTER_METIRC_WITH_NAME(prefix##name, name)

#define REGISTER_METIRC(name) REGISTER_METIRC_WITH_PREFIX(chunk_pool_, name)

    REGISTER_METIRC(system_alloc_count);
    REGISTER_METIRC(system_free_count);
    REGISTER_METIRC(system_alloc_cost_ns);
    REGISTER_METIRC(system_free_cost_ns);
}

DEFINE_FAIL_POINT(mem_chunk_allocator_allocate_fail);

bool MemChunkAllocator::allocate(size_t size, MemChunk* chunk) {
    FAIL_POINT_TRIGGER_RETURN(random_error, false);
    FAIL_POINT_TRIGGER_RETURN(mem_chunk_allocator_allocate_fail, false);

    chunk->size = size;

    int64_t cost_ns = 0;
    {
        SCOPED_RAW_TIMER(&cost_ns);
        // allocate from system allocator
        chunk->data = SystemAllocator::allocate(tls_thread_status.mem_tracker(), size);
    }
    system_alloc_count.increment(1);
    system_alloc_cost_ns.increment(cost_ns);
    if (chunk->data == nullptr) {
        return false;
    } else {
        return true;
    }
}

void MemChunkAllocator::free(const MemChunk& chunk) {
    int64_t cost_ns = 0;
    {
        SCOPED_RAW_TIMER(&cost_ns);
        SystemAllocator::free(tls_thread_status.mem_tracker(), chunk.data, chunk.size);
    }
    system_free_count.increment(1);
    system_free_cost_ns.increment(cost_ns);
}

} // namespace starrocks
