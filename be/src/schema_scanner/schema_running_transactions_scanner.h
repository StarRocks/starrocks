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

#include <cstdint>

#include "exec/schema_scanner.h"
#include "gen_cpp/FrontendService_types.h"

namespace starrocks {

class Column;

// BE scanner backing information_schema.running_transactions. It issues the getRunningTransactions thrift
// RPC to the FE leader (SchemaHelper::get_running_transactions) and copies the returned TRunningTxnInfo
// rows into the output chunk. Row production lives on the FE; this scanner is the correctness path so the
// view never falls back to SchemaDummyScanner (which would silently return zero rows).
class SchemaRunningTransactionsScanner : public SchemaScanner {
public:
    SchemaRunningTransactionsScanner();
    ~SchemaRunningTransactionsScanner() override;

    Status start(RuntimeState* state) override;
    Status get_next(ChunkPtr* chunk, bool* eos) override;

private:
    Status fill_chunk(ChunkPtr* chunk);

    // Render a UTC-epoch-ms thrift field into a DATETIME column in the session time zone. Unset or
    // epoch_ms <= 0 (a running txn has no publish/finish time yet) appends NULL. Unlike loads' same-named
    // helper there is no legacy string fallback, so this always populates the column and returns nothing.
    void _fill_datetime_column_from_ms(Column* column, bool is_set, int64_t epoch_ms) const;

    int _cur_idx = 0;
    TGetRunningTxnsResult _result;
    static SchemaScanner::ColumnDesc _s_tbls_columns[];
};

} // namespace starrocks
