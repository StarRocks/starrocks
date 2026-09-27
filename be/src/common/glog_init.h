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
#include "gutil/walltime.h"

namespace starrocks {

// glog doesn't allow multiple invocations of InitGoogleLogging. This method conditionally
// calls InitGoogleLogging only if it hasn't been called before.
bool init_glog(const char* basename, bool install_signal_handler = false);

// Shuts down the google logging library. Call before exit to ensure that log files are
// flushed. May only be called once.
void shutdown_logging();

// Format a timestamp in the same format as used by GLog.
std::string FormatTimestampForLog(MicrosecondsInt64 micros_since_epoch);

// Applies the current sys_log_level to the running process. Returns an error if it names no known
// severity, which config validation should already have ruled out.
Status update_logging();

// Reports every config value that had to be replaced by its default, on stderr as well as through
// glog, and clears them so each is reported once. init_glog calls this as soon as logging is up;
// anything that sets up glog without init_glog has to call it too, or the replacement is silent.
void report_config_fallbacks();

} // namespace starrocks
