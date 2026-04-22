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

package com.starrocks.planner;

import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.delta.DeltaConnectorScanRangeSource;
import com.starrocks.connector.delta.DeltaLakeEngine;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.credential.CloudType;
import com.starrocks.credential.aws.AwsCloudConfiguration;
import com.starrocks.credential.aws.AwsCloudCredential;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.ScanOptimizeOption;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TPlanNode;
import io.delta.kernel.Snapshot;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;

public class DeltaLakeScanNodeTest {
    @Test
    public void testInit(@Mocked GlobalStateMgr globalStateMgr,
                         @Mocked CatalogConnector connector,
                         @Mocked DeltaLakeTable table) {
        String catalog = "XXX";
        CloudConfiguration cc = CloudConfigurationFactory.buildCloudConfigurationForStorage(new HashMap<>());
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getConnectorMgr().getConnector(catalog);
                result = connector;
                connector.getMetadata().getCloudConfiguration();
                result = cc;
                table.getCatalogName();
                result = catalog;
                // Explicit null so DeltaLakeScanNode does not prefer a cascading per-table mock.
                table.getCloudConfiguration();
                result = null;
            }
        };
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(table);
        DeltaLakeScanNode scanNode = new DeltaLakeScanNode(new PlanNodeId(0), desc, "XXX", null, null, null);
        scanNode.setReachLimit();
    }

    @Test
    public void testNodeExplain(@Mocked GlobalStateMgr globalStateMgr, @Mocked CatalogConnector connector,
                            @Mocked DeltaLakeTable table) {
        String catalogName = "delta0";
        CloudConfiguration cloudConfiguration = CloudConfigurationFactory.
                buildCloudConfigurationForStorage(new HashMap<>());
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getConnectorMgr().getConnector(catalogName);
                result = connector;
                minTimes = 0;

                connector.getMetadata().getCloudConfiguration();
                result = cloudConfiguration;
                minTimes = 0;

                table.getCatalogName();
                result = catalogName;
                minTimes = 0;

                table.getName();
                result = "table0";
                minTimes = 0;

                table.getCloudConfiguration();
                result = null;
                minTimes = 0;
            }
        };
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(table);
        DeltaLakeScanNode scanNode = new DeltaLakeScanNode(new PlanNodeId(0), desc, "Delta Scan Node", null, null, null);
        Assertions.assertFalse(scanNode.getNodeExplainString("", TExplainLevel.NORMAL).contains("partitions"));
        Assertions.assertTrue(scanNode.getNodeExplainString("", TExplainLevel.VERBOSE).contains("partitions"));
    }

    @Test
    public void testNodeExplainContainsVersion(@Mocked GlobalStateMgr globalStateMgr, @Mocked CatalogConnector connector,
                                               @Mocked DeltaLakeTable table, @Mocked Snapshot snapshot,
                                               @Mocked DeltaLakeEngine engine) {
        String catalogName = "delta0";
        CloudConfiguration cloudConfiguration = CloudConfigurationFactory.
                buildCloudConfigurationForStorage(new HashMap<>());

        new Expectations() {{
                GlobalStateMgr.getCurrentState().getConnectorMgr().getConnector(catalogName);
                result = connector;
                minTimes = 0;

                connector.getMetadata().getCloudConfiguration();
                result = cloudConfiguration;
                minTimes = 0;

                table.getCatalogName();
                result = catalogName;
                minTimes = 0;

                table.getName();
                result = "table0";
                minTimes = 0;

                table.getDeltaSnapshot();
                result = snapshot;
                minTimes = 0;

                table.getDeltaEngine();
                result = engine;
                minTimes = 0;

                snapshot.getVersion(engine);
                result = 123L;
                minTimes = 0;

                table.getCloudConfiguration();
                result = null;
                minTimes = 0;
            }};
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(table);
        DeltaLakeScanNode scanNode = new DeltaLakeScanNode(new PlanNodeId(0), desc, "Delta Scan Node", null, null, null);
        String explainString = scanNode.getNodeExplainString("", TExplainLevel.NORMAL);
        assertThat(explainString, containsString("TABLE VERSION: 123"));
    }

    @Test
    public void testPerTableCloudConfigurationPreferredOverCatalog(@Mocked GlobalStateMgr globalStateMgr,
                                                                   @Mocked CatalogConnector connector,
                                                                   @Mocked DeltaLakeTable table,
                                                                   @Mocked AwsCloudConfiguration perTableCC,
                                                                   @Mocked AwsCloudCredential perTableCred) {
        String catalog = "unity_cat";
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getConnectorMgr().getConnector(catalog);
                result = connector;
                table.getCatalogName();
                result = catalog;
                table.getCloudConfiguration();
                result = perTableCC;
                perTableCC.getCloudType();
                result = CloudType.AWS;
                // getMetadata() must NOT be invoked when the per-table CC is non-DEFAULT.
            }
        };
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(table);
        DeltaLakeScanNode scanNode = new DeltaLakeScanNode(new PlanNodeId(0), desc, "Delta Scan Node", null, null, null);

        Assertions.assertSame(perTableCC, Deencapsulation.getField(scanNode, "cloudConfiguration"),
                "When the table exposes a non-DEFAULT CloudConfiguration (e.g. UC vended creds) the " +
                        "scan node must use it instead of the catalog-level one, so the BE receives the " +
                        "short-lived credentials needed to read S3 data files.");
    }

    @Test
    public void testCatalogCloudConfigurationUsedWhenTableHasNone(@Mocked GlobalStateMgr globalStateMgr,
                                                                  @Mocked CatalogConnector connector,
                                                                  @Mocked DeltaLakeTable table) {
        String catalog = "plain_cat";
        CloudConfiguration catalogLevelCC = CloudConfigurationFactory.buildCloudConfigurationForStorage(new HashMap<>());
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getConnectorMgr().getConnector(catalog);
                result = connector;
                table.getCatalogName();
                result = catalog;
                table.getCloudConfiguration();
                result = null;
                connector.getMetadata().getCloudConfiguration();
                result = catalogLevelCC;
            }
        };
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(table);
        DeltaLakeScanNode scanNode = new DeltaLakeScanNode(new PlanNodeId(0), desc, "Delta Scan Node", null, null, null);

        Assertions.assertSame(catalogLevelCC, Deencapsulation.getField(scanNode, "cloudConfiguration"));
    }

    @Test
    public void testCatalogCloudConfigurationUsedWhenTableConfigIsDefault(@Mocked GlobalStateMgr globalStateMgr,
                                                                          @Mocked CatalogConnector connector,
                                                                          @Mocked DeltaLakeTable table,
                                                                          @Mocked CloudConfiguration tableCC) {
        String catalog = "fallback_cat";
        CloudConfiguration catalogLevelCC = CloudConfigurationFactory.buildCloudConfigurationForStorage(new HashMap<>());
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getConnectorMgr().getConnector(catalog);
                result = connector;
                table.getCatalogName();
                result = catalog;
                table.getCloudConfiguration();
                result = tableCC;
                tableCC.getCloudType();
                result = CloudType.DEFAULT;
                connector.getMetadata().getCloudConfiguration();
                result = catalogLevelCC;
            }
        };
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(table);
        DeltaLakeScanNode scanNode = new DeltaLakeScanNode(new PlanNodeId(0), desc, "Delta Scan Node", null, null, null);

        Assertions.assertSame(catalogLevelCC, Deencapsulation.getField(scanNode, "cloudConfiguration"),
                "A DEFAULT (empty) table-level CloudConfiguration is not useful and the catalog-level " +
                        "one should win.");
    }

    @Test
    public void testPrepareRetry(@Mocked GlobalStateMgr globalStateMgr,
                                 @Mocked CatalogConnector connector,
                                 @Mocked DeltaLakeTable table,
                                 @Mocked DeltaConnectorScanRangeSource mockSource) {
        String catalog = "delta_cat";
        CloudConfiguration cc = CloudConfigurationFactory.buildCloudConfigurationForStorage(new HashMap<>());
        new Expectations() {{
            GlobalStateMgr.getCurrentState().getConnectorMgr().getConnector(catalog);
            result = connector;
            connector.getMetadata().getCloudConfiguration();
            result = cc;
            table.getCatalogName();
            result = catalog;
            table.getCloudConfiguration();
            result = null;
        }};
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(table);
        DeltaLakeScanNode scanNode = new DeltaLakeScanNode(new PlanNodeId(0), desc, "Delta Scan Node", null, null, null);

        // Stub setupScanRangeSource so it does not invoke real Delta kernel objects
        new MockUp<DeltaLakeScanNode>() {
            @Mock
            public void setupScanRangeSource(boolean enableIncrementalScanRanges) throws StarRocksException {
                // no-op
            }
        };

        // Simulate partially consumed state
        Deencapsulation.setField(scanNode, "scanRangeSource", mockSource);
        Deencapsulation.setField(scanNode, "reachLimit", true);

        scanNode.prepareRetry();

        Assertions.assertFalse((boolean) Deencapsulation.getField(scanNode, "reachLimit"),
                "reachLimit should be reset to false");
        Assertions.assertNull(Deencapsulation.getField(scanNode, "scanRangeSource"),
                "scanRangeSource should be cleared by clear()");
    }

    public void testToThriftSetsConnectorCatalogType(@Mocked GlobalStateMgr globalStateMgr,
                                                      @Mocked CatalogConnector connector,
                                                      @Mocked DeltaLakeTable table) {
        String catalogName = "delta0";
        CloudConfiguration cloudConfiguration = CloudConfigurationFactory
                .buildCloudConfigurationForStorage(new HashMap<>());
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getConnectorMgr().getConnector(catalogName);
                result = connector;
                connector.getMetadata().getCloudConfiguration();
                result = cloudConfiguration;
                table.getCatalogName();
                result = catalogName;
                table.getName();
                result = "delta_tbl";
                table.getType();
                result = Table.TableType.DELTALAKE;
            }
        };
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(table);
        DeltaLakeScanNode scanNode = new DeltaLakeScanNode(new PlanNodeId(0), desc, "Delta Scan Node", null, null, null);
        scanNode.setScanOptimizeOption(new ScanOptimizeOption());
        TPlanNode node = new TPlanNode();
        scanNode.toThrift(node);
        Assertions.assertNotNull(node.getConnector_scan_node());
        Assertions.assertEquals("deltalake", node.getConnector_scan_node().getCatalog_type());
    }
}
