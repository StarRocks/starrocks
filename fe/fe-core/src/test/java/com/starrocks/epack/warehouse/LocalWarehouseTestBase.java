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

package com.starrocks.epack.warehouse;

import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.util.StringUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.Warehouse;
import org.junit.Assert;

import java.util.concurrent.ThreadLocalRandom;

public class LocalWarehouseTestBase {
    protected static ConnectContext connectContext;
    protected static StarRocksAssert starRocksAssert;

    public static void setupBeforeClass() {
        // create connect context
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
    }

    static String randomCNGroupName() {
        return "cg_" + StringUtils.generateRandomString(16);
    }

    static String randomWarehouseName() {
        return "wh_" + StringUtils.generateRandomString(16);
    }

    static String randomNodeAddress() {
        return "127.0.0.1:" + ThreadLocalRandom.current().nextInt(10000, 60000);
    }

    static Cluster getClusterByName(String warehouseName, String cnGroupName) {
        LocalWarehouse wh =
                (LocalWarehouse) GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouse(warehouseName);
        return wh.getCluster(cnGroupName);
    }

    static Warehouse ensureWarehouseCreated(String warehouseName) {
        String sql = "CREATE WAREHOUSE " + warehouseName;
        ExceptionChecker.expectThrowsNoException(() -> starRocksAssert.ddl(sql));
        Warehouse wh = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouseAllowNull(warehouseName);
        Assert.assertNotNull(wh);
        return wh;
    }

    static Cluster ensureCnGroupCreated(String warehouseName, String cnGroupName) {
        String sql = "ALTER WAREHOUSE " + warehouseName + " ADD CNGROUP " + cnGroupName;
        ExceptionChecker.expectThrowsNoException(() -> starRocksAssert.ddl(sql));
        Cluster cluster = getClusterByName(warehouseName, cnGroupName);
        Assert.assertNotNull(cluster);
        return cluster;
    }

    static void ensureCnGroupDropped(String warehouseName, String cnGroupName) {
        String sql = "ALTER WAREHOUSE " + warehouseName + " DROP CNGROUP IF EXISTS " + cnGroupName + " FORCE";
        ExceptionChecker.expectThrowsNoException(() -> starRocksAssert.ddl(sql));
        Cluster cluster = getClusterByName(warehouseName, cnGroupName);
        Assert.assertNull(cluster);
    }

    static void ensureWarehouseDropped(String warehouseName) {
        String sql = "DROP WAREHOUSE IF EXISTS " + warehouseName;
        ExceptionChecker.expectThrowsNoException(() -> starRocksAssert.ddl(sql));
        Warehouse wh = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouseAllowNull(warehouseName);
        Assert.assertNull(wh);
    }

    static Backend getBackendNode(String nodeAddress) {
        String[] hostPort = nodeAddress.split(":");
        String host = hostPort[0];
        int port = Integer.parseInt(hostPort[1]);
        return GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendWithHeartbeatPort(host, port);
    }

    static ComputeNode getComputeNode(String nodeAddress) {
        String[] hostPort = nodeAddress.split(":");
        String host = hostPort[0];
        int port = Integer.parseInt(hostPort[1]);
        return GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                .getComputeNodeWithHeartbeatPort(host, port);
    }
}
