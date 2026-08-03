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

#include <string>

#include "common/status.h"
#include "gen_cpp/InternalService_types.h"

namespace starrocks {

class RejectedRecordWriter;
class RuntimeState;

class LoadPathStateHelper {
public:
    static Status create_error_log_file(RuntimeState* state);
    static void append_error_msg_to_file(RuntimeState* state, const std::string& line, const std::string& error_msg,
                                         bool is_summary = false);
    static void append_rejected_record_to_file(RuntimeState* state, const std::string& record,
                                               const std::string& error_msg, const std::string& source);

    // Build the source_info JSON to record on a rejected row. Routine load
    // tasks pre-populate `query_options.routine_load_source_info` with a
    // kafka anchor (topic + partitions + begin_offsets); when set we
    // forward it as-is. Otherwise wrap the legacy free-form `source`
    // string (typically a file path) in `{"source": "<source>"}`. Returns
    // an empty string when no source info is available so callers can
    // pass it through unchanged.
    static std::string build_rejected_record_source_info(const TQueryOptions& query_options, const std::string& source);

    // Return the per-fragment RejectedRecordWriter, constructing it on
    // first call under the RuntimeState rejected-record lock. Returns
    // nullptr only when rejected-record logging is disabled for this state.
    static RejectedRecordWriter* rejected_record_writer(RuntimeState* state);
};

} // namespace starrocks
