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

// This file provides stubs for symbols that became undefined after deleting symbol_provider.cpp
#include <string>
#include <vector>
#include <cstdint>

// Forward declarations to keep this self-contained
namespace starrocks {
    class StorePath {};
    class Status {
    public:
        static Status NotSupported(const std::string& msg);
    };

    class SchemaBeDataCacheMetricsScanner {
    public:
        SchemaBeDataCacheMetricsScanner();
    };

    class SchemaBeCloudNativeCompactionsScanner {
    public:
        SchemaBeCloudNativeCompactionsScanner();
    };

    namespace lake {
        class TabletManager {
        public:
            ~TabletManager();
        };
    } // namespace lake

    namespace pipeline {
        class SpillableHashJoinBuildOperatorFactory {
        public:
            virtual ~SpillableHashJoinBuildOperatorFactory();
        };
        class SpillableHashJoinProbeOperatorFactory {
        public:
            virtual ~SpillableHashJoinProbeOperatorFactory();
        };
        class SpillablePartitionSortSinkOperatorFactory {
        public:
            virtual ~SpillablePartitionSortSinkOperatorFactory();
        };
    } // namespace pipeline

    // Declaration from be/src/service/mem_hook.h
    int64_t set_large_memory_alloc_failure_threshold(int64_t);

} // namespace starrocks

// Declaration from be/src/common/prof/heap_prof.cpp
std::string exec(const std::string& cmd);


// Implementations
starrocks::SchemaBeDataCacheMetricsScanner::SchemaBeDataCacheMetricsScanner() {}
starrocks::SchemaBeCloudNativeCompactionsScanner::SchemaBeCloudNativeCompactionsScanner() {}

int64_t starrocks::set_large_memory_alloc_failure_threshold(int64_t) { return 0; }

std::string exec(const std::string& cmd) { return ""; }

starrocks::lake::TabletManager::~TabletManager() {}

// vtable implementations
starrocks::pipeline::SpillableHashJoinBuildOperatorFactory::~SpillableHashJoinBuildOperatorFactory() = default;
starrocks::pipeline::SpillableHashJoinProbeOperatorFactory::~SpillableHashJoinProbeOperatorFactory() = default;
starrocks::pipeline::SpillablePartitionSortSinkOperatorFactory::~SpillablePartitionSortSinkOperatorFactory() = default;

// __real___cxa_throw stub
#include <stdlib.h>
extern "C" {
    void __real___cxa_throw(void* thrown_exception, void* tinfo, void (*dest)(void*)) {
        abort();
    }
}