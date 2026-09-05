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

#pragma once

#include "common/runtime_profile.h"

namespace starrocks::connector {

// A structure to hold common profiling information for connector sink.
// To add some specific profile metrics for different connector sinks (such as Hive, Iceberg, etc.) later,
// we can inherit the current class to implement different connector sink profiles.
struct ConnectorSinkProfile {
    // Runtime profile instance
    RuntimeProfile* runtime_profile = nullptr;

    // Counter metrics
    RuntimeProfile::Counter* partition_writer_counter = nullptr;
    RuntimeProfile::Counter* write_file_counter = nullptr;
    RuntimeProfile::Counter* write_file_record_counter = nullptr;
    RuntimeProfile::Counter* write_file_bytes = nullptr;

    // Timer metrics
    RuntimeProfile::Counter* write_file_timer = nullptr;
    RuntimeProfile::Counter* commit_file_timer = nullptr;
    RuntimeProfile::Counter* sort_timer = nullptr;
    RuntimeProfile::Counter* spill_chunk_timer = nullptr;
    RuntimeProfile::Counter* merge_blocks_timer = nullptr;

    // Memory usage peak metrics
    RuntimeProfile::Counter* spilling_bytes_usage_peak = nullptr;
};

} // namespace starrocks::connector
