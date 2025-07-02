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
import com.starrocks.thrift.TAlterTabletReqV2;
import com.starrocks.thrift.TTabletType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

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
    public void testAlterLakeTablet() {
        AlterReplicaTask task = AlterReplicaTask.alterLakeTablet(1, 2, 3, 4, 5, 6,
                7, 8, 9, 10, null);

        Assertions.assertEquals(1, task.getBackendId());
        Assertions.assertEquals(2, task.getDbId());
        Assertions.assertEquals(3, task.getTableId());
        Assertions.assertEquals(4, task.getPartitionId());
        Assertions.assertEquals(5, task.getIndexId());
        Assertions.assertEquals(6, task.getTabletId());
        Assertions.assertEquals(7, task.getBaseTabletId());
        Assertions.assertEquals(8, task.getVersion());
        Assertions.assertEquals(9, task.getJobId());
        Assertions.assertEquals(AlterJobV2.JobType.SCHEMA_CHANGE, task.getJobType());

        TAlterTabletReqV2 request = task.toThrift();
        Assertions.assertEquals(7, request.base_tablet_id);
        Assertions.assertEquals(6, request.new_tablet_id);
        Assertions.assertEquals(8, request.alter_version);
        Assertions.assertEquals(10, request.txn_id);
        Assertions.assertFalse(request.isSetMaterialized_view_params());
        Assertions.assertEquals(TTabletType.TABLET_TYPE_LAKE, request.tablet_type);
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
}
