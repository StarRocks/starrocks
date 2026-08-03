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

#include <cctz/civil_time.h>
#include <cctz/time_zone.h>

#include <cstdint>

#include "exec/schema_scanner.h"
#include "gen_cpp/FrontendService_types.h"

namespace starrocks {

class Column;

class SchemaLoadsScanner : public SchemaScanner {
public:
    SchemaLoadsScanner();
    ~SchemaLoadsScanner() override;

    Status start(RuntimeState* state) override;
    Status get_next(ChunkPtr* chunk, bool* eos) override;

    // Convert a session-zone DATETIME literal (extracted from a predicate) to
    // the UTC epoch ms FE compares against. Returns a second-aligned value
    // (information_schema.loads is second-precision today); for DST-ambiguous
    // civil times the result widens the bound via min/max of
    // cctz::civil_lookup.pre/post so the FE prefilter never drops a row that
    // BE's post-filter would have kept. Public so the anonymous-namespace
    // predicate-extraction helper and the unit test can call it.
    static int64_t literal_to_epoch_ms(const cctz::time_zone& session_tz, cctz::civil_second cs, bool is_lower_bound);

private:
    Status fill_chunk(ChunkPtr* chunk);

    // Fill a DATETIME column from a UTC-epoch-ms thrift field, converting to the
    // session zone via from_unixtime() so the materialized wall-clock matches what
    // the user expects in their session. Returns true when the ms field was set
    // (and the column was populated, either with a value or a NULL); the caller
    // should then skip its legacy string-parsing fallback.
    bool _fill_datetime_column_from_ms(Column* column, bool is_set, int64_t epoch_ms) const;

    int _cur_idx = 0;
    TGetLoadsResult _result;
    static SchemaScanner::ColumnDesc _s_tbls_columns[];
};

} // namespace starrocks
