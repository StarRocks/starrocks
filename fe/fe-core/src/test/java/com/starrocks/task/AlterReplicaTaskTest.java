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
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

public class AlterReplicaTaskTest {

    @Test
    public void testAlterLocalTablet() {
        AlterReplicaTask task = AlterReplicaTask.alterLocalTablet(1, 2, 3, 4, 5, 6,
                7, 8, 9, 10, 11, 12, null);

        Assert.assertEquals(1, task.getBackendId());
        Assert.assertEquals(2, task.getDbId());
        Assert.assertEquals(3, task.getTableId());
        Assert.assertEquals(4, task.getPartitionId());
        Assert.assertEquals(5, task.getIndexId());
        Assert.assertEquals(6, task.getTabletId());
        Assert.assertEquals(7, task.getBaseTabletId());
        Assert.assertEquals(8, task.getNewReplicaId());
        Assert.assertEquals(9, task.getNewSchemaHash());
        Assert.assertEquals(10, task.getBaseSchemaHash());
        Assert.assertEquals(11, task.getVersion());
        Assert.assertEquals(12, task.getJobId());
        Assert.assertEquals(AlterJobV2.JobType.SCHEMA_CHANGE, task.getJobType());

        TAlterTabletReqV2 request = task.toThrift();
        Assert.assertEquals(7, request.base_tablet_id);
        Assert.assertEquals(6, request.new_tablet_id);
        Assert.assertEquals(10, request.base_schema_hash);
        Assert.assertEquals(9, request.new_schema_hash);
        Assert.assertEquals(11, request.alter_version);
        Assert.assertFalse(request.isSetMaterialized_view_params());
        Assert.assertEquals(TTabletType.TABLET_TYPE_DISK, request.tablet_type);
    }

    @Test
    public void testAlterLakeTablet() {
        AlterReplicaTask task = AlterReplicaTask.alterLakeTablet(1, 2, 3, 4, 5, 6,
                7, 8, 9, 10);

        Assert.assertEquals(1, task.getBackendId());
        Assert.assertEquals(2, task.getDbId());
        Assert.assertEquals(3, task.getTableId());
        Assert.assertEquals(4, task.getPartitionId());
        Assert.assertEquals(5, task.getIndexId());
        Assert.assertEquals(6, task.getTabletId());
        Assert.assertEquals(7, task.getBaseTabletId());
        Assert.assertEquals(8, task.getVersion());
        Assert.assertEquals(9, task.getJobId());
        Assert.assertEquals(AlterJobV2.JobType.SCHEMA_CHANGE, task.getJobType());

        TAlterTabletReqV2 request = task.toThrift();
        Assert.assertEquals(7, request.base_tablet_id);
        Assert.assertEquals(6, request.new_tablet_id);
        Assert.assertEquals(8, request.alter_version);
        Assert.assertEquals(10, request.txn_id);
        Assert.assertFalse(request.isSetMaterialized_view_params());
        Assert.assertEquals(TTabletType.TABLET_TYPE_LAKE, request.tablet_type);
    }

    @Test
    public void testRollupLocalTablet() {
        AlterReplicaTask task = AlterReplicaTask.rollupLocalTablet(1, 2, 3, 4, 5, 6,
                7, 8, 9, 10, 11, 12, new HashMap<>());

        Assert.assertEquals(1, task.getBackendId());
        Assert.assertEquals(2, task.getDbId());
        Assert.assertEquals(3, task.getTableId());
        Assert.assertEquals(4, task.getPartitionId());
        Assert.assertEquals(5, task.getIndexId());
        Assert.assertEquals(6, task.getTabletId());
        Assert.assertEquals(7, task.getBaseTabletId());
        Assert.assertEquals(8, task.getNewReplicaId());
        Assert.assertEquals(9, task.getNewSchemaHash());
        Assert.assertEquals(10, task.getBaseSchemaHash());
        Assert.assertEquals(11, task.getVersion());
        Assert.assertEquals(12, task.getJobId());
        Assert.assertEquals(AlterJobV2.JobType.ROLLUP, task.getJobType());

        TAlterTabletReqV2 request = task.toThrift();
        Assert.assertEquals(7, request.base_tablet_id);
        Assert.assertEquals(6, request.new_tablet_id);
        Assert.assertEquals(10, request.base_schema_hash);
        Assert.assertEquals(9, request.new_schema_hash);
        Assert.assertEquals(11, request.alter_version);
        Assert.assertEquals(TTabletType.TABLET_TYPE_DISK, request.tablet_type);
    }
}
