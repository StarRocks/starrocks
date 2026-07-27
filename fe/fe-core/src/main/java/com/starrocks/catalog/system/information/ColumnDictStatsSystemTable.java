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
import com.starrocks.type.BooleanType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.TypeFactory;

import static com.starrocks.catalog.system.SystemTable.NAME_CHAR_LEN;
import static com.starrocks.catalog.system.SystemTable.builder;

// E4 (column-level shared ZSTD dictionary) observability.
//
// One row per (tablet, segment, column / flat-JSON sub-column). Because the BE
// scanner opens per-segment footers to fill the dictionary/encoding/size
// columns, this table REQUIRES a TABLE_NAME (optionally with TABLE_SCHEMA) or a
// TABLET_ID equality predicate; a full-database scan is rejected by the BE
// scanner. See SchemaColumnDictStatsScanner (BE) for the enforcement.
//
// NOTE: every column is created via builder().column(name, type), which marks
// the column NULLABLE. The footer-derived columns (SEGMENT_ID, ENCODING,
// COMPRESSION, HAS_SHARED_DICT, SHARED_DICT_SIZE, DATA_SIZE, UNCOMPRESSED_SIZE,
// COMPRESSION_RATIO) are emitted as NULL by the BE when the segment footer
// cannot be read (e.g. shared-data / lake tablets, primary-key tablets, or
// encrypted / bundled segments) so that no subtly-wrong value is ever surfaced.
public class ColumnDictStatsSystemTable {
    public static final String NAME = "column_dict_stats";

    public static SystemTable create() {
        return new SystemTable(SystemId.COLUMN_DICT_STATS_ID,
                NAME,
                Table.TableType.SCHEMA,
                builder()
                        .column("TABLE_SCHEMA", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("TABLE_NAME", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("PARTITION_ID", IntegerType.BIGINT)
                        .column("TABLET_ID", IntegerType.BIGINT)
                        .column("SEGMENT_ID", IntegerType.BIGINT)
                        .column("COLUMN_NAME", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("ENCODING", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("COMPRESSION", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("USE_SHARED_DICT", BooleanType.BOOLEAN)
                        .column("HAS_SHARED_DICT", BooleanType.BOOLEAN)
                        .column("SHARED_DICT_SIZE", IntegerType.BIGINT)
                        .column("DATA_SIZE", IntegerType.BIGINT)
                        .column("UNCOMPRESSED_SIZE", IntegerType.BIGINT)
                        .column("COMPRESSION_RATIO", FloatType.DOUBLE)
                        .build(), TSchemaTableType.SCH_COLUMN_DICT_STATS);
    }
}
