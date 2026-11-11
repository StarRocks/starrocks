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

package com.starrocks.qe;

import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.ScanNode;
import com.starrocks.thrift.TCloudConfiguration;
import com.starrocks.thrift.TRefreshScanCredentialsRequest;
import com.starrocks.thrift.TRefreshScanCredentialsResponse;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TUniqueId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.function.Function;

public class ScanCredentialsRefresherTest {
    private static final TUniqueId QUERY_ID = new TUniqueId(1, 2);
    private static final String TOKEN = "0123456789abcdef";
    private static final int NODE_ID = 7;

    private DefaultCoordinator coordinator;
    private ScanNode scanNode;
    private ConnectContext context;
    private Function<TUniqueId, com.starrocks.qe.scheduler.Coordinator> coordinators;

    @BeforeEach
    public void setUp() {
        context = Mockito.mock(ConnectContext.class);
        scanNode = Mockito.mock(ScanNode.class);
        Mockito.when(scanNode.getId()).thenReturn(new PlanNodeId(NODE_ID));
        Mockito.when(scanNode.acceptsCredentialRefreshToken(TOKEN)).thenReturn(true);
        coordinator = Mockito.mock(DefaultCoordinator.class);
        Mockito.when(coordinator.getScanNodes()).thenReturn(List.of(scanNode));
        Mockito.when(coordinator.getConnectContext()).thenReturn(context);
        coordinators = id -> QUERY_ID.equals(id) ? coordinator : null;
    }

    private static TRefreshScanCredentialsRequest request(String token, int nodeId) {
        return new TRefreshScanCredentialsRequest().setQuery_id(QUERY_ID).setRefresh_token(token)
                .setPlan_node_id(nodeId);
    }

    private TStatusCode refresh(TRefreshScanCredentialsRequest request) {
        return ScanCredentialsRefresher.refresh(request, coordinators).getStatus().getStatus_code();
    }

    @Test
    public void testReturnsTheScanNodeConfiguration() {
        TCloudConfiguration configuration = new TCloudConfiguration().setRefresh_after_ms(123L);
        Mockito.when(scanNode.refreshScanCredentials(context)).thenReturn(configuration);
        TRefreshScanCredentialsResponse response =
                ScanCredentialsRefresher.refresh(request(TOKEN, NODE_ID), coordinators);
        Assertions.assertEquals(TStatusCode.OK, response.getStatus().getStatus_code());
        Assertions.assertSame(configuration, response.getCloud_configuration());
    }

    @Test
    public void testRejectsIncompleteRequest() {
        Assertions.assertEquals(TStatusCode.INVALID_ARGUMENT, refresh(new TRefreshScanCredentialsRequest()));
        Assertions.assertEquals(TStatusCode.INVALID_ARGUMENT,
                ScanCredentialsRefresher.refresh(null, coordinators).getStatus().getStatus_code());
    }

    @Test
    public void testUnknownQuery() {
        Assertions.assertEquals(TStatusCode.NOT_FOUND,
                refresh(request(TOKEN, NODE_ID).setQuery_id(new TUniqueId(9, 9))));
    }

    @Test
    public void testWrongTokenGetsNothing() {
        Assertions.assertEquals(TStatusCode.NOT_FOUND, refresh(request("fedcba9876543210", NODE_ID)));
        Assertions.assertEquals(TStatusCode.INVALID_ARGUMENT,
                refresh(new TRefreshScanCredentialsRequest().setQuery_id(QUERY_ID).setPlan_node_id(NODE_ID)));
        Mockito.verify(scanNode, Mockito.never()).refreshScanCredentials(Mockito.any());
    }

    @Test
    public void testUnknownScanNode() {
        Assertions.assertEquals(TStatusCode.NOT_FOUND, refresh(request(TOKEN, NODE_ID + 1)));
    }

    @Test
    public void testScanNodeWithoutRefreshableCredentials() {
        Assertions.assertEquals(TStatusCode.NOT_IMPLEMENTED_ERROR, refresh(request(TOKEN, NODE_ID)));
    }

    @Test
    public void testRenewalFailure() {
        Mockito.when(scanNode.refreshScanCredentials(context)).thenThrow(new RuntimeException("catalog down"));
        TRefreshScanCredentialsResponse response =
                ScanCredentialsRefresher.refresh(request(TOKEN, NODE_ID), coordinators);
        Assertions.assertEquals(TStatusCode.INTERNAL_ERROR, response.getStatus().getStatus_code());
        Assertions.assertFalse(response.isSetCloud_configuration());
    }
}
