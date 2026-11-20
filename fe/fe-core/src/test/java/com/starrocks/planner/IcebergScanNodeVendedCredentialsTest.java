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

import com.starrocks.catalog.IcebergTable;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.iceberg.IcebergMORParams;
import com.starrocks.connector.iceberg.IcebergTableMORParams;
import com.starrocks.connector.iceberg.IcebergVendedCredentials;
import com.starrocks.connector.share.credential.CloudConfigurationConstants;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.ScanOptimizeOption;
import com.starrocks.thrift.TCloudConfiguration;
import com.starrocks.thrift.TPlanNode;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class IcebergScanNodeVendedCredentialsTest {
    private static final String CATALOG = "rest_catalog";

    private static CloudConfiguration awsCredentials(String token) {
        Map<String, String> properties = new HashMap<>();
        properties.put("s3.access-key-id", "ak");
        properties.put("s3.secret-access-key", "sk");
        properties.put("s3.session-token", token);
        return CloudConfigurationFactory.buildCloudConfigurationForAWSVendedCredentials(properties);
    }

    private static String tokenOf(TCloudConfiguration configuration) {
        return configuration.getCloud_properties().get(CloudConfigurationConstants.AWS_S3_SESSION_TOKEN);
    }

    private static IcebergScanNode newScanNode(IcebergTable table) {
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(table);
        IcebergScanNode scanNode = new IcebergScanNode(new PlanNodeId(3), desc, CATALOG,
                IcebergTableMORParams.EMPTY, IcebergMORParams.DATA_FILE_WITHOUT_EQ_DELETE, null);
        scanNode.setScanOptimizeOption(new ScanOptimizeOption());
        return scanNode;
    }

    // The catalog lookup is not under test here; the renewal sees whatever table it returns.
    private static IcebergScanNode withReload(IcebergScanNode scanNode, ConnectContext context) {
        IcebergScanNode spy = Mockito.spy(scanNode);
        Mockito.doReturn(Mockito.mock(org.apache.iceberg.Table.class)).when(spy).loadTableUncached(context);
        return spy;
    }

    private static TCloudConfiguration serialized(IcebergScanNode scanNode) {
        TPlanNode planNode = new TPlanNode();
        scanNode.toThrift(planNode);
        return planNode.getHdfs_scan_node().getCloud_configuration();
    }

    @Test
    public void testVendedCredentialsAreRefreshedWhenDue(@Mocked GlobalStateMgr globalStateMgr,
                                                        @Mocked CatalogConnector connector,
                                                        @Mocked IcebergTable table,
                                                        @Mocked ConnectContext context) {
        new Expectations() {{
            table.getCatalogName();
            result = CATALOG;
            minTimes = 0;
        }};
        try (MockedStatic<IcebergVendedCredentials> vended = Mockito.mockStatic(IcebergVendedCredentials.class)) {
            vended.when(() -> IcebergVendedCredentials.of(Mockito.any(), Mockito.anyLong(), Mockito.any()))
                    .thenReturn(new IcebergVendedCredentials(awsCredentials("t0"), Long.MAX_VALUE));
            vended.when(() -> IcebergVendedCredentials.renewedFrom(Mockito.any(), Mockito.any(), Mockito.anyLong(), Mockito.any()))
                    .thenAnswer(invocation -> new IcebergVendedCredentials(awsCredentials("t1"),
                            (long) invocation.getArgument(2) + 3600_000L));

            IcebergScanNode scanNode = newScanNode(table);
            TCloudConfiguration deployed = serialized(scanNode);
            Assertions.assertEquals("t0", tokenOf(deployed));
            Assertions.assertEquals(Long.MAX_VALUE, deployed.getRefresh_after_ms());
            Assertions.assertTrue(scanNode.acceptsCredentialRefreshToken(deployed.getRefresh_token()));
            Assertions.assertFalse(scanNode.acceptsCredentialRefreshToken("guess"));

            // Not due yet: the current configuration, without asking the catalog.
            Assertions.assertEquals("t0", tokenOf(scanNode.refreshScanCredentials(context)));
            vended.verify(() -> IcebergVendedCredentials.renewedFrom(Mockito.any(), Mockito.any(), Mockito.anyLong(), Mockito.any()),
                    Mockito.never());

            // Due: renewed once, and every later serialization (a retry) carries the renewal.
            vended.when(() -> IcebergVendedCredentials.of(Mockito.any(), Mockito.anyLong(), Mockito.any()))
                    .thenReturn(new IcebergVendedCredentials(awsCredentials("t0"), 0L));
            scanNode = withReload(newScanNode(table), context);
            TCloudConfiguration renewed = scanNode.refreshScanCredentials(context);
            Assertions.assertEquals("t1", tokenOf(renewed));
            Assertions.assertTrue(renewed.getRefresh_after_ms() > System.currentTimeMillis());
            Assertions.assertEquals("t1", tokenOf(scanNode.refreshScanCredentials(context)));
            Assertions.assertEquals("t1", tokenOf(serialized(scanNode)));
            vended.verify(() -> IcebergVendedCredentials.renewedFrom(Mockito.any(), Mockito.any(), Mockito.anyLong(), Mockito.any()),
                    Mockito.times(1));
        }
    }

    @Test
    public void testCatalogCredentialsAreNotRefreshable(@Mocked GlobalStateMgr globalStateMgr,
                                                       @Mocked CatalogConnector connector,
                                                       @Mocked IcebergTable table,
                                                       @Mocked ConnectContext context) {
        CloudConfiguration catalogCredentials = awsCredentials("static");
        new Expectations() {{
            table.getCatalogName();
            result = CATALOG;
            minTimes = 0;
            connector.getMetadata().getCatalogProperties();
            result = new HashMap<>();
            minTimes = 0;
            connector.getMetadata().getCloudConfiguration();
            result = catalogCredentials;
            minTimes = 0;
        }};
        try (MockedStatic<IcebergVendedCredentials> vended = Mockito.mockStatic(IcebergVendedCredentials.class)) {
            vended.when(() -> IcebergVendedCredentials.of(Mockito.any(), Mockito.anyLong(), Mockito.any())).thenReturn(null);

            IcebergScanNode scanNode = newScanNode(table);
            Assertions.assertNull(scanNode.refreshScanCredentials(context));
            TCloudConfiguration deployed = serialized(scanNode);
            Assertions.assertEquals("static", tokenOf(deployed));
            Assertions.assertFalse(deployed.isSetRefresh_after_ms());
            Assertions.assertFalse(deployed.isSetRefresh_token());
            Assertions.assertFalse(scanNode.acceptsCredentialRefreshToken(null));
        }
    }

    @Test
    public void testAFailedRenewalIsNotRetriedByEveryBackend(@Mocked GlobalStateMgr globalStateMgr,
                                                            @Mocked CatalogConnector connector,
                                                            @Mocked IcebergTable table,
                                                            @Mocked ConnectContext context) {
        new Expectations() {{
            table.getCatalogName();
            result = CATALOG;
            minTimes = 0;
        }};
        try (MockedStatic<IcebergVendedCredentials> vended = Mockito.mockStatic(IcebergVendedCredentials.class)) {
            vended.when(() -> IcebergVendedCredentials.of(Mockito.any(), Mockito.anyLong(), Mockito.any()))
                    .thenReturn(new IcebergVendedCredentials(awsCredentials("t0"), 0L));
            vended.when(() -> IcebergVendedCredentials.renewedFrom(Mockito.any(), Mockito.any(), Mockito.anyLong(), Mockito.any()))
                    .thenThrow(new RuntimeException("catalog down"));

            IcebergScanNode scanNode = withReload(newScanNode(table), context);
            Assertions.assertThrows(RuntimeException.class, () -> scanNode.refreshScanCredentials(context));
            RuntimeException again =
                    Assertions.assertThrows(RuntimeException.class, () -> scanNode.refreshScanCredentials(context));
            Assertions.assertTrue(again.getMessage().contains("catalog down"), again.getMessage());
            vended.verify(() -> IcebergVendedCredentials.renewedFrom(Mockito.any(), Mockito.any(), Mockito.anyLong(), Mockito.any()),
                    Mockito.times(1));
            // The current credentials still serialize, for a retry.
            Assertions.assertEquals("t0", tokenOf(serialized(scanNode)));
        }
    }

    @Test
    public void testACallerDoesNotWaitForARenewalInFlight(@Mocked GlobalStateMgr globalStateMgr,
                                                         @Mocked CatalogConnector connector,
                                                         @Mocked IcebergTable table,
                                                         @Mocked ConnectContext context) throws Exception {
        new Expectations() {{
            table.getCatalogName();
            result = CATALOG;
            minTimes = 0;
        }};
        ExecutorService other = Executors.newSingleThreadExecutor();
        try (MockedStatic<IcebergVendedCredentials> vended = Mockito.mockStatic(IcebergVendedCredentials.class)) {
            vended.when(() -> IcebergVendedCredentials.of(Mockito.any(), Mockito.anyLong(), Mockito.any()))
                    .thenReturn(new IcebergVendedCredentials(awsCredentials("t0"), 0L));
            vended.when(() -> IcebergVendedCredentials.renewedFrom(Mockito.any(), Mockito.any(), Mockito.anyLong(),
                    Mockito.any())).thenReturn(new IcebergVendedCredentials(awsCredentials("t1"), Long.MAX_VALUE));
            IcebergScanNode scanNode = Mockito.spy(newScanNode(table));
            // While the first caller loads the table, a second one answers at once with the current credentials.
            Mockito.doAnswer(invocation -> {
                TCloudConfiguration meanwhile =
                        other.submit(() -> scanNode.refreshScanCredentials(context)).get(10, TimeUnit.SECONDS);
                Assertions.assertEquals("t0", tokenOf(meanwhile));
                return Mockito.mock(org.apache.iceberg.Table.class);
            }).when(scanNode).loadTableUncached(context);

            Assertions.assertEquals("t1", tokenOf(scanNode.refreshScanCredentials(context)));
            vended.verify(() -> IcebergVendedCredentials.renewedFrom(Mockito.any(), Mockito.any(), Mockito.anyLong(),
                    Mockito.any()), Mockito.times(1));
        } finally {
            other.shutdownNow();
        }
    }
}
