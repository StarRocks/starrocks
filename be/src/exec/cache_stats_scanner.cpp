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

#include "exec/cache_stats_scanner.h"

#include "base/string/string_parser.hpp"

namespace starrocks {

CacheStatsScanner::CacheStatsScanner(const TupleDescriptor* tuple_desc) {
    (void)tuple_desc;
}

Status CacheStatsScanner::init(RuntimeState* state, const TInternalScanRange& scan_range) {
    (void)state;
    _tablet_id = scan_range.tablet_id;
    if (!scan_range.version.empty()) {
        StringParser::ParseResult result = StringParser::PARSE_SUCCESS;
        _version = StringParser::string_to_int<int64_t>(scan_range.version.data(),
                                                        static_cast<int>(scan_range.version.size()), &result);
        if (result != StringParser::PARSE_SUCCESS) {
            return Status::InvalidArgument("Invalid cache stats scan range version: " + scan_range.version);
        }
    }
    return Status::OK();
}

Status CacheStatsScanner::open(RuntimeState* state) {
    (void)state;
    return Status::OK();
}

void CacheStatsScanner::close(RuntimeState* state) {
    (void)state;
}

Status CacheStatsScanner::get_chunk(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    (void)state;
    (void)chunk;
    *eos = false;
    return Status::NotSupported("_CACHE_STATS_ is not implemented in BE");
}

} // namespace starrocks
