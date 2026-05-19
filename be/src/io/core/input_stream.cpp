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

#include "io/core/input_stream.h"

namespace starrocks::io {

// Out-of-line so gcov can attribute coverage to a concrete `.cpp` line: header-only
// inline definitions get inlined at every call site and the resulting `.gcda` data
// is keyed off the call site, leaving the header line counted as 0/0.
IoStatsSnapshot InputStream::get_io_stats_snapshot() const {
    return {};
}

IoStatsSnapshot InputStreamWrapper::get_io_stats_snapshot() const {
    return _impl->get_io_stats_snapshot();
}

} // namespace starrocks::io
