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
import com.starrocks.type.ArrayType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.MapType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.TypeFactory;

import java.util.ArrayList;

import static com.starrocks.catalog.system.SystemTable.MAX_FIELD_VARCHAR_LENGTH;
import static com.starrocks.catalog.system.SystemTable.NAME_CHAR_LEN;

public class BeDataCacheMetricsTable {
    private static final String NAME = "be_datacache_metrics";

    public static SystemTable create() {
        ArrayList<StructField> dirSpacesFields = new ArrayList<>();
        dirSpacesFields.add(new StructField("path", TypeFactory.createVarcharType(MAX_FIELD_VARCHAR_LENGTH)));
        dirSpacesFields.add(new StructField("quota_bytes", IntegerType.BIGINT));
        StructType dirSpacesType = new StructType(dirSpacesFields);
        ArrayType dirSpacesArrayType = new ArrayType(dirSpacesType);

        MapType usedBytesDetailType =
                new MapType(IntegerType.INT,
                        IntegerType.BIGINT);

        return new SystemTable(SystemId.BE_DATACACHE_METRICS, NAME, Table.TableType.SCHEMA,
                SystemTable.builder()
                        .column("BE_ID", IntegerType.BIGINT)
                        .column("STATUS", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("DISK_QUOTA_BYTES", IntegerType.BIGINT)
                        .column("DISK_USED_BYTES", IntegerType.BIGINT)
                        .column("MEM_QUOTA_BYTES", IntegerType.BIGINT)
                        .column("MEM_USED_BYTES", IntegerType.BIGINT)
                        .column("META_USED_BYTES", IntegerType.BIGINT)
                        .column("DIR_SPACES", dirSpacesArrayType)
                        .column("USED_BYTES_DETAIL", usedBytesDetailType)
                        .build(),
                TSchemaTableType.SCH_BE_DATACACHE_METRICS);
    }
}
