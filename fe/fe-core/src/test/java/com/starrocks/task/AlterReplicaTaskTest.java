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


package com.starrocks.task;

import com.starrocks.alter.AlterJobV2;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.SchemaInfo;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.thrift.TAlterTabletReqV2;
import com.starrocks.thrift.TColumn;
import com.starrocks.thrift.TStorageType;
import com.starrocks.thrift.TTabletSchema;
import com.starrocks.thrift.TTabletType;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AlterReplicaTaskTest {

    @Test
    public void testAlterLocalTablet() {
        AlterReplicaTask task = AlterReplicaTask.alterLocalTablet(1, 2, 3, 4, 5, 6,
                7, 8, 9, 10, 11, 12, null, Collections.emptyList());

        Assertions.assertEquals(1, task.getBackendId());
        Assertions.assertEquals(2, task.getDbId());
        Assertions.assertEquals(3, task.getTableId());
        Assertions.assertEquals(4, task.getPartitionId());
        Assertions.assertEquals(5, task.getIndexId());
        Assertions.assertEquals(6, task.getTabletId());
        Assertions.assertEquals(7, task.getBaseTabletId());
        Assertions.assertEquals(8, task.getNewReplicaId());
        Assertions.assertEquals(9, task.getNewSchemaHash());
        Assertions.assertEquals(10, task.getBaseSchemaHash());
        Assertions.assertEquals(11, task.getVersion());
        Assertions.assertEquals(12, task.getJobId());
        Assertions.assertEquals(AlterJobV2.JobType.SCHEMA_CHANGE, task.getJobType());

        TAlterTabletReqV2 request = task.toThrift();
        Assertions.assertEquals(7, request.base_tablet_id);
        Assertions.assertEquals(6, request.new_tablet_id);
        Assertions.assertEquals(10, request.base_schema_hash);
        Assertions.assertEquals(9, request.new_schema_hash);
        Assertions.assertEquals(11, request.alter_version);
        Assertions.assertFalse(request.isSetMaterialized_view_params());
        Assertions.assertEquals(TTabletType.TABLET_TYPE_DISK, request.tablet_type);
    }

    @Test
    public void testRollupLocalTablet() {
        AlterReplicaTask task = AlterReplicaTask.rollupLocalTablet(1, 2, 3, 4, 5, 6,
                7, 8, 9, 10, 11, 12, null, null);

        Assertions.assertEquals(1, task.getBackendId());
        Assertions.assertEquals(2, task.getDbId());
        Assertions.assertEquals(3, task.getTableId());
        Assertions.assertEquals(4, task.getPartitionId());
        Assertions.assertEquals(5, task.getIndexId());
        Assertions.assertEquals(6, task.getTabletId());
        Assertions.assertEquals(7, task.getBaseTabletId());
        Assertions.assertEquals(8, task.getNewReplicaId());
        Assertions.assertEquals(9, task.getNewSchemaHash());
        Assertions.assertEquals(10, task.getBaseSchemaHash());
        Assertions.assertEquals(11, task.getVersion());
        Assertions.assertEquals(12, task.getJobId());
        Assertions.assertEquals(AlterJobV2.JobType.ROLLUP, task.getJobType());

        TAlterTabletReqV2 request = task.toThrift();
        Assertions.assertEquals(7, request.base_tablet_id);
        Assertions.assertEquals(6, request.new_tablet_id);
        Assertions.assertEquals(10, request.base_schema_hash);
        Assertions.assertEquals(9, request.new_schema_hash);
        Assertions.assertEquals(11, request.alter_version);
        Assertions.assertEquals(TTabletType.TABLET_TYPE_DISK, request.tablet_type);
    }

    private TTabletSchema createTestTabletSchema(long schemaId, List<Column> columns) {
        return SchemaInfo.newBuilder()
                .setId(schemaId)
                .setKeysType(KeysType.DUP_KEYS)
                .setShortKeyColumnCount((short) 1)
                .setSchemaHash(0)
                .setStorageType(TStorageType.COLUMN)
                .addColumns(columns)
                .build()
                .toTabletSchema();
    }

    @Test
    public void testAlterLakeTablet() {
        // Create test columns
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("k1", IntegerType.INT, false, null, "", ""));
        columns.add(new Column("v1", IntegerType.BIGINT, false, null, "", ""));

        // Create TTabletSchema
        TTabletSchema baseTabletReadSchema = createTestTabletSchema(10001L, columns);

        // Create AlterReplicaTask with baseTabletReadSchema
        AlterReplicaTask task = AlterReplicaTask.alterLakeTablet(100, 200, 300, 400, 500, 600,
                700, 800, 900, 1000, null, baseTabletReadSchema);

        // Verify field initialization
        Assertions.assertEquals(100, task.getBackendId());
        Assertions.assertEquals(200, task.getDbId());
        Assertions.assertEquals(300, task.getTableId());
        Assertions.assertEquals(400, task.getPartitionId());
        Assertions.assertEquals(500, task.getIndexId());
        Assertions.assertEquals(600, task.getTabletId());
        Assertions.assertEquals(700, task.getBaseTabletId());
        Assertions.assertEquals(800, task.getVersion());
        Assertions.assertEquals(900, task.getJobId());
        Assertions.assertEquals(AlterJobV2.JobType.SCHEMA_CHANGE, task.getJobType());

        // Verify LakeTablet constructor sets replica/schema hash to -1
        Assertions.assertEquals(-1, task.getNewReplicaId());
        Assertions.assertEquals(-1, task.getNewSchemaHash());
        Assertions.assertEquals(-1, task.getBaseSchemaHash());

        // Verify toThrift() sets base_tablet_read_schema and preserves other fields
        TAlterTabletReqV2 request = task.toThrift();
        
        // Verify other fields are preserved
        Assertions.assertEquals(700, request.base_tablet_id);
        Assertions.assertEquals(600, request.new_tablet_id);
        Assertions.assertEquals(800, request.alter_version);
        Assertions.assertEquals(1000, request.txn_id);
        Assertions.assertEquals(900, request.job_id);
        Assertions.assertEquals(TTabletType.TABLET_TYPE_LAKE, request.tablet_type);
        Assertions.assertEquals(com.starrocks.thrift.TAlterJobType.SCHEMA_CHANGE, request.alter_job_type);

        // Verify baseTabletReadSchema is set and initialized correctly (TC-3, TC-5)
        Assertions.assertTrue(request.isSetBase_tablet_read_schema());
        Assertions.assertNotNull(request.getBase_tablet_read_schema());
        TTabletSchema schema = request.getBase_tablet_read_schema();
        
        // Verify schema properties
        Assertions.assertEquals(10001L, schema.getId());
        Assertions.assertNotNull(schema.getKeys_type());
        Assertions.assertEquals(1, schema.getShort_key_column_count());
        Assertions.assertEquals(0, schema.getSchema_hash());
        Assertions.assertEquals(TStorageType.COLUMN, schema.getStorage_type());
        Assertions.assertEquals(2, schema.getColumns().size());
        List<TColumn> tColumns = schema.getColumns();
        Assertions.assertEquals("k1", tColumns.get(0).getColumn_name());
        Assertions.assertEquals("v1", tColumns.get(1).getColumn_name());

        // Verify tablet type is set to LAKE
        Assertions.assertEquals(TTabletType.TABLET_TYPE_LAKE, request.tablet_type);
    }

    @Test
    public void testRollupLakeTablet() {
        // Create test columns
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("k1", IntegerType.INT, false, null, "", ""));
        columns.add(new Column("v1", IntegerType.BIGINT, false, null, "", ""));

        // Create TTabletSchema
        TTabletSchema baseTabletReadSchema = createTestTabletSchema(10002L, columns);

        // Create RollupJobV2Params (can be null for this test)
        AlterReplicaTask.RollupJobV2Params rollupJobV2Params = null;

        // Create AlterReplicaTask with baseTabletReadSchema
        AlterReplicaTask task = AlterReplicaTask.rollupLakeTablet(100, 200, 300, 400, 500, 600,
                700, 800, 900, rollupJobV2Params, baseTabletReadSchema, 1000);

        // Verify field initialization
        Assertions.assertEquals(100, task.getBackendId());
        Assertions.assertEquals(200, task.getDbId());
        Assertions.assertEquals(300, task.getTableId());
        Assertions.assertEquals(400, task.getPartitionId());
        Assertions.assertEquals(500, task.getIndexId());
        Assertions.assertEquals(600, task.getTabletId());
        Assertions.assertEquals(700, task.getBaseTabletId());
        Assertions.assertEquals(800, task.getVersion());
        Assertions.assertEquals(900, task.getJobId());
        Assertions.assertEquals(AlterJobV2.JobType.ROLLUP, task.getJobType());

        // Verify LakeTablet constructor sets replica/schema hash to -1
        Assertions.assertEquals(-1, task.getNewReplicaId());
        Assertions.assertEquals(-1, task.getNewSchemaHash());
        Assertions.assertEquals(-1, task.getBaseSchemaHash());

        // Verify toThrift() sets base_tablet_read_schema and preserves other fields
        TAlterTabletReqV2 request = task.toThrift();
        
        // Verify other fields are preserved
        Assertions.assertEquals(700, request.base_tablet_id);
        Assertions.assertEquals(600, request.new_tablet_id);
        Assertions.assertEquals(800, request.alter_version);
        Assertions.assertEquals(1000, request.txn_id);
        Assertions.assertEquals(900, request.job_id);
        Assertions.assertEquals(TTabletType.TABLET_TYPE_LAKE, request.tablet_type);
        Assertions.assertEquals(com.starrocks.thrift.TAlterJobType.ROLLUP, request.alter_job_type);

        // Verify baseTabletReadSchema is set and initialized correctly
        Assertions.assertTrue(request.isSetBase_tablet_read_schema());
        Assertions.assertNotNull(request.getBase_tablet_read_schema());
        TTabletSchema schema = request.getBase_tablet_read_schema();
        
        // Verify schema properties
        Assertions.assertEquals(10002L, schema.getId());
        Assertions.assertNotNull(schema.getKeys_type());
        Assertions.assertEquals(1, schema.getShort_key_column_count());
        Assertions.assertEquals(0, schema.getSchema_hash());
        Assertions.assertEquals(TStorageType.COLUMN, schema.getStorage_type());
        Assertions.assertEquals(2, schema.getColumns().size());
        List<TColumn> tColumns = schema.getColumns();
        Assertions.assertEquals("k1", tColumns.get(0).getColumn_name());
        Assertions.assertEquals("v1", tColumns.get(1).getColumn_name());

        // Verify tablet type is set to LAKE
        Assertions.assertEquals(TTabletType.TABLET_TYPE_LAKE, request.tablet_type);
    }

}
