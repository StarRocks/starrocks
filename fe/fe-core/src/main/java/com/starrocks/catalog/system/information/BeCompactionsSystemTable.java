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
package com.starrocks.catalog.system.information;

import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.SystemId;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.thrift.TSchemaTableType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;

import static com.starrocks.catalog.system.SystemTable.builder;

public class BeCompactionsSystemTable {
    private static final String NAME = "be_compactions";

    public static SystemTable create() {
        return new SystemTable(SystemId.BE_COMPACTIONS_ID,
                NAME,
                Table.TableType.SCHEMA,
                builder()
                        .column("BE_ID", IntegerType.BIGINT)
                        .column("CANDIDATES_NUM", IntegerType.BIGINT)
                        .column("BASE_COMPACTION_CONCURRENCY", IntegerType.BIGINT)
                        .column("CUMULATIVE_COMPACTION_CONCURRENCY", IntegerType.BIGINT)
                        .column("LATEST_COMPACTION_SCORE", FloatType.DOUBLE)
                        .column("CANDIDATE_MAX_SCORE", FloatType.DOUBLE)
                        .column("MANUAL_COMPACTION_CONCURRENCY", IntegerType.BIGINT)
                        .column("MANUAL_COMPACTION_CANDIDATES_NUM", IntegerType.BIGINT)
                        .build(), TSchemaTableType.SCH_BE_COMPACTIONS);
    }
}
