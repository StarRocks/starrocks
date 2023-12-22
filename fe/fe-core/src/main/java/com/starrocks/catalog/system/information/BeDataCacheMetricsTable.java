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

import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.SystemId;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.thrift.TSchemaTableType;

import static com.starrocks.catalog.system.SystemTable.MAX_FIELD_VARCHAR_LENGTH;
import static com.starrocks.catalog.system.SystemTable.NAME_CHAR_LEN;

public class BeDataCacheMetricsTable {

    public static SystemTable create() {
        return new SystemTable(SystemId.BE_DATACACHE_METRICS, "be_datacache_metrics", Table.TableType.SCHEMA,
                SystemTable.builder()
                        .column("BE_ID", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("STATUS", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("DISK_QUOTA_BYTES", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("DISK_USED_BYTES", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("MEM_QUOTA_BYTES", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("MEM_USED_BYTES", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("META_USED_BYTES", ScalarType.createType(PrimitiveType.BIGINT))
                        // TODO: Better to use struct type.
                        .column("DIR_SPACES", ScalarType.createVarcharType(MAX_FIELD_VARCHAR_LENGTH))
                        .build(),
                TSchemaTableType.SCH_BE_DATACACHE_METRICS);
    }
}
