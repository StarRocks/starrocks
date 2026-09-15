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

package com.starrocks.qe.scheduler;

import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.connector.iceberg.IcebergMORParams;
import com.starrocks.connector.iceberg.IcebergTableMORParams;
import com.starrocks.connector.iceberg.IcebergUtil;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.gcp.GCPCloudConfiguration;
import com.starrocks.credential.gcp.GCPCloudConfigurationProvider;
import com.starrocks.credential.gcp.GCPCloudCredential;
import com.starrocks.planner.IcebergScanNode;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.ScanNode;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.planner.TupleId;
import com.starrocks.proto.PExecPlanFragmentResult;
import com.starrocks.proto.StatusPB;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.qe.scheduler.dag.FragmentInstanceExecState;
import com.starrocks.qe.scheduler.slot.DeployState;
import com.starrocks.rpc.BackendServiceClient;
import com.starrocks.server.MetadataMgr;
import com.starrocks.thrift.TCloudConfiguration;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStatusCode;
import mockit.Mock;
import mockit.MockUp;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Covers the coordinator side of mid-query vended credential refresh: the refresh-only timer tick
 * and the delivery-round attach path. The scheduler harness only mocks a hive catalog, so an
 * IcebergScanNode carrying a near-expiry vended credential is injected into the scan fragment.
 */
public class VendedCredentialRefreshSchedulerTest extends SchedulerConnectorTestBase {

    private IcebergTable icebergTable;
    private CloudConfiguration freshConfig;

    private static CloudConfiguration vendedConfig(String token, long expiresAtMs) {
        GCPCloudCredential credential = new GCPCloudCredential("", false, "", "", "", "",
                token, String.valueOf(expiresAtMs));
        return new GCPCloudConfiguration(credential);
    }

    private IcebergScanNode newIcebergNode(int planNodeId, String token, long expiryMs) {
        new MockUp<IcebergTable>() {
            @Mock
            public String getCatalogName() {
                return null;
            }
        };
        icebergTable = new IcebergTable();
        TupleDescriptor desc = new TupleDescriptor(new TupleId(planNodeId));
        desc.setTable(icebergTable);
        IcebergScanNode node = new IcebergScanNode(new PlanNodeId(planNodeId), desc, "IcebergScanNode",
                IcebergTableMORParams.EMPTY, IcebergMORParams.EMPTY, null);

        new MockUp<IcebergTable>() {
            @Mock
            public String getCatalogName() {
                return "cat";
            }

            @Mock
            public String getCatalogDBName() {
                return "db";
            }

            @Mock
            public String getCatalogTableName() {
                return "tbl";
            }
        };
        new MockUp<MetadataMgr>() {
            @Mock
            public void refreshTable(String catalogName, String srDbName, Table table,
                                     List<String> partitionNames, boolean onlyCachedPartitions) {
                // no-op: the reload only needs to be observed, not performed
            }

            @Mock
            public Table getTable(ConnectContext context, String catalogName, String dbName, String tblName) {
                return icebergTable;
            }
        };
        new MockUp<IcebergUtil>() {
            @Mock
            public CloudConfiguration getVendedCloudConfiguration(String catalogName, IcebergTable table) {
                return freshConfig;
            }
        };
        node.setCloudConfiguration(vendedConfig(token, expiryMs));
        return node;
    }

    private ExecutionFragment scanFragmentWithInjectedIcebergNode(DefaultCoordinator coordinator,
                                                                  IcebergScanNode node) {
        ExecutionFragment scanFragment = coordinator.getExecutionDAG().getFragmentsInPostorder().stream()
                .filter(fragment -> !fragment.getScanNodes().isEmpty())
                .findFirst().orElseThrow();
        Map<PlanNodeId, ScanNode> scanNodes = Deencapsulation.getField(scanFragment, "scanNodes");
        scanNodes.put(node.getId(), node);
        return scanFragment;
    }

    // Deploys are mocked, so exec states stay CREATED; the tick only pushes to EXECUTING instances.
    private static void markExecuting(DefaultCoordinator coordinator) {
        for (FragmentInstanceExecState execState : coordinator.getExecutionDAG().getExecutions()) {
            Deencapsulation.setField(execState, "state", FragmentInstanceExecState.State.EXECUTING);
        }
    }

    private static Future<PExecPlanFragmentResult> okPushResult() {
        PExecPlanFragmentResult result = new PExecPlanFragmentResult();
        StatusPB status = new StatusPB();
        status.statusCode = TStatusCode.OK.getValue();
        result.status = status;
        return CompletableFuture.completedFuture(result);
    }

    @Test
    public void testRefreshTickPushesRefreshOnlyRequests() throws Exception {
        List<TExecPlanFragmentParams> pushed = new ArrayList<>();
        new MockUp<BackendServiceClient>() {
            @Mock
            public Future<PExecPlanFragmentResult> execPlanFragmentAsync(
                    TNetworkAddress address, TExecPlanFragmentParams tRequest, String protocol) {
                pushed.add(tRequest);
                return okPushResult();
            }
        };
        new MockUp<Deployer>() {
            @Mock
            public void deployFragments(DeployState deployState) {
                // no-op: deploys are not exercised by these tests
            }
        };
        DefaultCoordinator coordinator = startScheduling("select * from hive0.file_split_db.file_split_tbl");
        IcebergScanNode node = newIcebergNode(1000, "tok1", System.currentTimeMillis() + 60_000L);
        freshConfig = vendedConfig("tok2", System.currentTimeMillis() + 3600_000L);
        ExecutionFragment scanFragment = scanFragmentWithInjectedIcebergNode(coordinator, node);

        // Instances not yet EXECUTING are skipped and flagged for retry: the BE would drop the
        // push as OK before its fragment context exists.
        Deencapsulation.invoke(coordinator, "refreshVendedCredentialsTick");
        Assertions.assertTrue(pushed.isEmpty());
        markExecuting(coordinator);

        Deencapsulation.invoke(coordinator, "refreshVendedCredentialsTick");

        // One refresh-only request per instance: no scan ranges, credential keyed by scan node id.
        Assertions.assertEquals(scanFragment.getInstances().size(), pushed.size());
        TExecPlanFragmentParams request = pushed.get(0);
        Assertions.assertFalse(request.isSetFragment());
        Assertions.assertTrue(request.params.getPer_node_scan_ranges().isEmpty());
        TCloudConfiguration cc = request.params.getNode_to_cloud_configuration().get(node.getId().asInt());
        Assertions.assertEquals("tok2",
                cc.getCloud_properties().get(GCPCloudConfigurationProvider.ACCESS_TOKEN_KEY));

        // A second tick inside the re-vend cooldown pushes nothing.
        int pushedSoFar = pushed.size();
        Deencapsulation.invoke(coordinator, "refreshVendedCredentialsTick");
        Assertions.assertEquals(pushedSoFar, pushed.size());
    }

    @Test
    public void testFailedPushIsResentNextTick() throws Exception {
        AtomicBoolean failPush = new AtomicBoolean(true);
        List<TExecPlanFragmentParams> pushed = new ArrayList<>();
        new MockUp<BackendServiceClient>() {
            @Mock
            public Future<PExecPlanFragmentResult> execPlanFragmentAsync(
                    TNetworkAddress address, TExecPlanFragmentParams tRequest, String protocol) throws TException {
                if (failPush.get()) {
                    throw new TException("injected push failure");
                }
                pushed.add(tRequest);
                return okPushResult();
            }
        };
        new MockUp<Deployer>() {
            @Mock
            public void deployFragments(DeployState deployState) {
                // no-op: deploys are not exercised by these tests
            }
        };
        DefaultCoordinator coordinator = startScheduling("select * from hive0.file_split_db.file_split_tbl");
        IcebergScanNode node = newIcebergNode(1000, "tok1", System.currentTimeMillis() + 60_000L);
        freshConfig = vendedConfig("tok2", System.currentTimeMillis() + 3600_000L);
        scanFragmentWithInjectedIcebergNode(coordinator, node);
        markExecuting(coordinator);

        // Tick 1: the FE re-vends (credential becomes tok2) but every push fails.
        Deencapsulation.invoke(coordinator, "refreshVendedCredentialsTick");
        Assertions.assertTrue(pushed.isEmpty());

        // Tick 2: inside the re-vend cooldown and the catalog is unchanged, yet the current
        // credential must still be re-pushed because the previous delivery was lost.
        failPush.set(false);
        Deencapsulation.invoke(coordinator, "refreshVendedCredentialsTick");
        Assertions.assertFalse(pushed.isEmpty());
        TCloudConfiguration cc = pushed.get(0).params.getNode_to_cloud_configuration().get(node.getId().asInt());
        Assertions.assertEquals("tok2",
                cc.getCloud_properties().get(GCPCloudConfigurationProvider.ACCESS_TOKEN_KEY));

        // Tick 3: the resend succeeded, so nothing further is pushed.
        int pushedSoFar = pushed.size();
        Deencapsulation.invoke(coordinator, "refreshVendedCredentialsTick");
        Assertions.assertEquals(pushedSoFar, pushed.size());
    }

    @Test
    public void testDeliveryRoundAttachesRefreshedCredentials() throws Exception {
        new MockUp<Deployer>() {
            @Mock
            public void deployFragments(DeployState deployState) {
                // no-op: deploys are not exercised by these tests
            }
        };
        DefaultCoordinator coordinator = startScheduling("select * from hive0.file_split_db.file_split_tbl");
        IcebergScanNode node = newIcebergNode(1000, "tok1", System.currentTimeMillis() + 60_000L);
        freshConfig = vendedConfig("tok2", System.currentTimeMillis() + 3600_000L);
        ExecutionFragment scanFragment = scanFragmentWithInjectedIcebergNode(coordinator, node);

        Deencapsulation.invoke(coordinator, "maybeRefreshVendedCredentials", scanFragment);

        Map<Integer, TCloudConfiguration> refreshed = scanFragment.getRefreshedNodeCloudConfigs();
        Assertions.assertNotNull(refreshed);
        Assertions.assertEquals("tok2", refreshed.get(node.getId().asInt())
                .getCloud_properties().get(GCPCloudConfigurationProvider.ACCESS_TOKEN_KEY));
    }

    @Test
    public void testSlowTickIsSkippedNotStacked() throws Exception {
        new MockUp<Deployer>() {
            @Mock
            public void deployFragments(DeployState deployState) {
                // no-op: deploys are not exercised by these tests
            }
        };
        DefaultCoordinator coordinator = startScheduling("select * from hive0.file_split_db.file_split_tbl");
        AtomicBoolean running = Deencapsulation.getField(coordinator, "vendedRefreshTickRunning");

        // A tick arriving while the previous one is in flight is skipped, not stacked.
        running.set(true);
        Deencapsulation.invoke(coordinator, "runVendedRefreshTick");
        Assertions.assertTrue(running.get());

        // A normal run releases the flag on completion.
        running.set(false);
        Deencapsulation.invoke(coordinator, "runVendedRefreshTick");
        Assertions.assertFalse(running.get());
    }
}
