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

#include <deque>
#include <memory>
#include <string>

#include "common/status.h"

namespace starrocks {

struct GrepLogEntry {
    int64_t timestamp{0};
    char level{'I'}; // I, W, E, F
    uint64_t thread_id{0};
    std::string log;
};

std::vector<std::string> list_log_files(char level);

/**
 * Grep log file.
 * @param start_ts start timestamp, seconds since epoch
 * @param end_ts end timestamp, seconds since epoch
 * @param level log level, 'I', 'W', 'E', 'F'
 * @param pattern grep pattern, can be empty(means match all)
 * @param limit max number of entries
 * @param entries output entries
 * @return Status
 */
Status grep_log(int64_t start_ts, int64_t end_ts, char level, const std::string& pattern, size_t limit,
                std::deque<GrepLogEntry>& entries);

/**
 * Grep log file and return all line as whole string, parameters are same as grep_log
 * @return log string
 */
std::string grep_log_as_string(int64_t start_ts, int64_t end_ts, char level, const std::string& pattern, size_t limit);

} // namespace starrocks
