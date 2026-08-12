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

package com.starrocks.server;

import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.DdlException;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LakeColocateShardMetaGroupTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(true, RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test_colo_mg").useDatabase("test_colo_mg");
    }

    @Test
    public void testNewPartitionShardsJoinMetaGroupAtCreation() throws Exception {
        List<Long> capturedMetaGroupIds = new ArrayList<>();
        new MockUp<StarOSAgent>() {
            @Mock
            public List<Long> createShards(Invocation invocation, int numShards, FilePathInfo pathInfo,
                                           FileCacheInfo cacheInfo, List<Long> groupIds, List<Long> matchShardIds,
                                           Map<String, String> properties, long metaGroupId,
                                           ComputeResource computeResource) throws DdlException {
                capturedMetaGroupIds.add(metaGroupId);
                return invocation.proceed(numShards, pathInfo, cacheInfo, groupIds, matchShardIds,
                        properties, metaGroupId, computeResource);
            }
        };

        starRocksAssert.withTable("CREATE TABLE test_colo_mg.cm1 (k1 int, k2 int)"
                + " PARTITION BY RANGE(k1) (PARTITION p1 VALUES LESS THAN ('10'))"
                + " DISTRIBUTED BY HASH(k2) BUCKETS 3"
                + " PROPERTIES('colocate_with' = 'cg_mg_ut')");
        OlapTable table = (OlapTable) starRocksAssert.getTable("test_colo_mg", "cm1");
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        Assertions.assertTrue(colocateTableIndex.isMetaGroupColocateTable(table.getId()));
        long grpId = colocateTableIndex.getGroup(table.getId()).grpId;

        // A new partition of the colocate table: its shards must be created with the meta group,
        // so the very first placement already honors the colocation constraint.
        capturedMetaGroupIds.clear();
        starRocksAssert.ddl("ALTER TABLE test_colo_mg.cm1 ADD PARTITION p2 VALUES LESS THAN ('20')");
        Assertions.assertEquals(List.of(grpId), capturedMetaGroupIds);

        // Control: a non-colocate table's new partition carries no meta group.
        starRocksAssert.withTable("CREATE TABLE test_colo_mg.cm2 (k1 int, k2 int)"
                + " PARTITION BY RANGE(k1) (PARTITION p1 VALUES LESS THAN ('10'))"
                + " DISTRIBUTED BY HASH(k2) BUCKETS 3");
        capturedMetaGroupIds.clear();
        starRocksAssert.ddl("ALTER TABLE test_colo_mg.cm2 ADD PARTITION p2 VALUES LESS THAN ('20')");
        Assertions.assertEquals(List.of(0L), capturedMetaGroupIds);
    }
}
