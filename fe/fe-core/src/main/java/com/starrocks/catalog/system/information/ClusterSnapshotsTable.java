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

import static com.starrocks.catalog.system.SystemTable.NAME_CHAR_LEN;
import static com.starrocks.catalog.system.SystemTable.builder;

public class ClusterSnapshotsTable {
    public static final String NAME = "cluster_snapshots";

    public static SystemTable create() {
        return new SystemTable(SystemId.CLUSTER_SNAPSHOTS_ID,
                NAME,
                Table.TableType.SCHEMA,
                builder()
                        .column("SNAPSHOT_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("SNAPSHOT_TYPE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("CREATED_TIME", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("FINISHED_TIME", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("FE_JOURNAL_ID", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("STARMGR_JOURNAL_ID", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("PROPERTIES", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("STORAGE_VOLUME", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("STORAGE_PATH", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .build(), TSchemaTableType.SCH_CLUSTER_SNAPSHOTS);
    }
}
