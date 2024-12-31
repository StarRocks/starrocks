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

package com.starrocks.load.batchwrite;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Tablet;
import com.starrocks.qe.ConnectContext;
import com.starrocks.schema.MTable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TTabletCommitInfo;
import com.starrocks.transaction.TransactionStatus;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public abstract class BatchWriteTestBase {

    protected static final String DB_NAME_1 = "batch_write_load_db_1";
    protected static final String TABLE_NAME_1_1 = "batch_write_load_tbl_1_1";
    protected static final String TABLE_NAME_1_2 = "batch_write_load_tbl_1_2";
    protected static final String DB_NAME_2 = "batch_write_load_db_2";
    protected static final String TABLE_NAME_2_1 = "batch_write_load_tbl_2_1";
    protected static final String TABLE_NAME_2_2 = "batch_write_load_tbl_2_2";

    protected static ConnectContext connectContext;
    protected static StarRocksAssert starRocksAssert;
    protected static Database DATABASE_1;
    protected static OlapTable TABLE_1_1;
    protected static OlapTable TABLE_1_2;
    protected static Database DATABASE_2;
    protected static OlapTable TABLE_2_1;
    protected static OlapTable TABLE_2_2;

    protected static List<ComputeNode> allNodes;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
        UtFrameUtils.addMockBackend(10004);
        UtFrameUtils.addMockBackend(10005);
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase(DB_NAME_1)
                .useDatabase(DB_NAME_1)
                .withTable(new MTable(TABLE_NAME_1_1, Arrays.asList("c0 INT", "c1 STRING")))
                .withTable(new MTable(TABLE_NAME_1_2, Arrays.asList("c0 INT")));
        starRocksAssert.withDatabase(DB_NAME_2)
                .useDatabase(DB_NAME_2)
                .withTable(new MTable(TABLE_NAME_2_1, Arrays.asList("c0 INT", "c1 STRING")))
                .withTable(new MTable(TABLE_NAME_2_2, Arrays.asList("c0 INT")));

        DATABASE_1 = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME_1);
        TABLE_1_1 = (OlapTable) DATABASE_1.getTable(TABLE_NAME_1_1);
        TABLE_1_2 = (OlapTable) DATABASE_1.getTable(TABLE_NAME_1_2);

        DATABASE_2 = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME_2);
        TABLE_2_1 = (OlapTable) DATABASE_2.getTable(TABLE_NAME_2_1);
        TABLE_2_2 = (OlapTable) DATABASE_2.getTable(TABLE_NAME_2_2);

        allNodes = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getAvailableBackends()
                .stream().map(node -> (ComputeNode) node).collect(Collectors.toList());
        Collections.shuffle(allNodes);
    }

    protected List<TTabletCommitInfo> buildCommitInfos() {
        List<TTabletCommitInfo> commitInfos = new ArrayList<>();
        for (Partition partition : TABLE_1_1.getPartitions()) {
            List<MaterializedIndex> materializedIndices =
                    partition.getDefaultPhysicalPartition().getMaterializedIndices(MaterializedIndex.IndexExtState.ALL);
            for (MaterializedIndex index : materializedIndices) {
                for (Tablet tablet : index.getTablets()) {
                    for (long backendId : tablet.getBackendIds()) {
                        commitInfos.add(new TTabletCommitInfo(tablet.getId(), backendId));
                    }
                }
            }
        }
        return commitInfos;
    }

    protected TransactionStatus getTxnStatus(String label) {
        return GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                .getLabelStatus(DATABASE_1.getId(), label).getStatus();
    }
}
