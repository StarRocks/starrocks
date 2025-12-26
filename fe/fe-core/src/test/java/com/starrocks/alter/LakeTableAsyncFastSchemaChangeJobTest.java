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

package com.starrocks.alter;

import com.starrocks.catalog.Partition;
import com.starrocks.catalog.SchemaInfo;
import com.starrocks.lake.LakeTable;
import com.starrocks.rpc.ThriftConnectionPool;
import com.starrocks.thrift.TAgentTaskRequest;
import com.starrocks.thrift.TCompressionType;
import com.starrocks.thrift.TTabletMetaInfo;
import com.starrocks.thrift.TTabletSchema;
import com.starrocks.thrift.TTaskType;
import com.starrocks.thrift.TUpdateTabletMetaInfoReq;
import com.starrocks.utframe.MockGenericPool;
import com.starrocks.utframe.MockedBackend;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class LakeTableAsyncFastSchemaChangeJobTest extends LakeFastSchemaChangeTestBase {

    @Override
    public boolean isFastSchemaEvolutionV2() {
        return false;
    }

    @Test
    public void testCompressionSettings() throws Exception {
        // Create a table with zstd compression and level 9
        LakeTable table = createTable(connectContext,
                "CREATE TABLE t_compression_test(c0 INT) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) " +
                        "BUCKETS 1 PROPERTIES('cloud_native_fast_schema_evolution_v2'='false', 'compression'='zstd(9)')");

        Assertions.assertEquals(TCompressionType.ZSTD, table.getCompressionType());
        Assertions.assertEquals(9, table.getCompressionLevel());

        List<MockedBackend.MockBeThriftClient> thriftClients = ((MockGenericPool<?>) ThriftConnectionPool.backendPool)
                .getAllBackends().stream().map(MockedBackend::getMockBeThriftClient).toList();
        Assertions.assertFalse(thriftClients.isEmpty());
        thriftClients.forEach(client -> client.setCaptureAgentTask(true));
        try {
            String alterSql = "ALTER TABLE t_compression_test ADD COLUMN c1 BIGINT";
            AlterJobV2 alterJob = executeAlterAndWaitFinish(table, alterSql, true);
            Assertions.assertInstanceOf(LakeTableAsyncFastSchemaChangeJob.class, alterJob);
            LakeTableAsyncFastSchemaChangeJob job = (LakeTableAsyncFastSchemaChangeJob) alterJob;
            List<SchemaInfo> schemaInfos = job.getSchemaInfoList();
            Assertions.assertEquals(1, schemaInfos.size());
            Assertions.assertEquals(TCompressionType.ZSTD, schemaInfos.get(0).getCompressionType());
            Assertions.assertEquals(9, schemaInfos.get(0).getCompressionLevel());

            // Get all tablet IDs from the table
            Set<Long> tableTabletIds = new HashSet<>();
            for (Partition partition : table.getPartitions()) {
                for (com.starrocks.catalog.PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                    com.starrocks.catalog.MaterializedIndex index = physicalPartition.getIndex(table.getBaseIndexMetaId());
                    if (index != null) {
                        for (com.starrocks.catalog.Tablet tablet : index.getTablets()) {
                            tableTabletIds.add(tablet.getId());
                        }
                    }
                }
            }
            Assertions.assertEquals(1, tableTabletIds.size());

            // 1. get all TAgentTask by using MockBeThriftClient::getCapturedAgentTasks
            List<TAgentTaskRequest> allTasks = thriftClients.stream()
                    .flatMap(client -> client.getCapturedAgentTasks().stream())
                    .toList();

            // 2. get all tasks related to fast schema evolution for table t_compression_test
            // Fast schema evolution uses UPDATE_TABLET_META_INFO task type
            List<TAgentTaskRequest> fastSchemaEvolutionTasks = allTasks.stream()
                    .filter(task -> task.getTask_type() == TTaskType.UPDATE_TABLET_META_INFO)
                    .filter(task -> task.isSetUpdate_tablet_meta_info_req())
                    .toList();

            // 3. get TTabletSchema from agent task, filter by tablet IDs belonging to the table
            List<TTabletSchema> tabletSchemas = fastSchemaEvolutionTasks.stream()
                    .map(TAgentTaskRequest::getUpdate_tablet_meta_info_req)
                    .map(TUpdateTabletMetaInfoReq::getTabletMetaInfos)
                    .flatMap(List::stream)
                    .filter(metaInfo -> tableTabletIds.contains(metaInfo.getTablet_id()))
                    .filter(TTabletMetaInfo::isSetTablet_schema)
                    .map(TTabletMetaInfo::getTablet_schema)
                    .toList();

            Assertions.assertEquals(1, tabletSchemas.size(),
                    "There should be exactly one TTabletSchema for fast schema evolution");

            // 4. verify the compression type and level in TTabletSchema is correct
            TTabletSchema tabletSchema = tabletSchemas.get(0);
            Assertions.assertEquals(TCompressionType.ZSTD, tabletSchema.getCompression_type());
            Assertions.assertEquals(9, tabletSchema.getCompression_level());
        } finally {
            thriftClients.forEach(client -> client.setCaptureAgentTask(false));
            thriftClients.forEach(MockedBackend.MockBeThriftClient::clearCapturedAgentTasks);
        }
    }
}
