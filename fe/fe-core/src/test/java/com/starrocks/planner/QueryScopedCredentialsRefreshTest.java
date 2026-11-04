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

import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.epack.connector.lakeformation.QueryScopedCredentials;
import com.starrocks.qe.ConnectContext;
import com.starrocks.thrift.TCloudConfiguration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.Map;

public class QueryScopedCredentialsRefreshTest {

    private static CloudConfiguration awsCredentials() {
        return CloudConfigurationFactory.buildCloudConfigurationForAWSVendedCredentials(Map.of(
                "s3.access-key-id", "ak", "s3.secret-access-key", "sk", "s3.session-token", "token"));
    }

    private static ScanNode owner() {
        ScanNode owner = Mockito.mock(ScanNode.class);
        Mockito.when(owner.credentialRefreshToken()).thenReturn("token");
        return owner;
    }

    @Test
    public void testSerializedWithRefreshTime() {
        ConnectContext context = Mockito.mock(ConnectContext.class);
        QueryScopedCredentials credentials = Mockito.mock(QueryScopedCredentials.class);
        Mockito.when(credentials.refreshAfter()).thenReturn(Instant.ofEpochMilli(1234L));
        Mockito.when(credentials.cloudConfiguration(context)).thenReturn(awsCredentials());

        TCloudConfiguration serialized = HdfsScanNode.queryScopedCloudConfigurationToThrift(owner(), credentials, context);
        Assertions.assertEquals(1234L, serialized.getRefresh_after_ms());
        Assertions.assertEquals("token", serialized.getRefresh_token());
        Assertions.assertTrue(serialized.isSetCloud_properties());

        // Read before the configuration, so a renewal in between can only make the backend ask early.
        InOrder order = Mockito.inOrder(credentials);
        order.verify(credentials).refreshAfter();
        order.verify(credentials).cloudConfiguration(context);
    }

    @Test
    public void testCredentialsThatCannotBeRenewed() {
        ConnectContext context = Mockito.mock(ConnectContext.class);
        QueryScopedCredentials credentials = Mockito.mock(QueryScopedCredentials.class);
        Mockito.when(credentials.cloudConfiguration(context)).thenReturn(awsCredentials());

        TCloudConfiguration serialized =
                HdfsScanNode.queryScopedCloudConfigurationToThrift(owner(), credentials, context);
        Assertions.assertFalse(serialized.isSetRefresh_after_ms());
        Assertions.assertFalse(serialized.isSetRefresh_token(), "no token for credentials that cannot be renewed");
        Assertions.assertNull(HdfsScanNode.refreshQueryScopedCredentials(owner(), credentials, context));
        Assertions.assertNull(HdfsScanNode.refreshQueryScopedCredentials(owner(), null, context));
        Mockito.verify(credentials, Mockito.never()).renewForRunningScan(Mockito.any());
    }

    @Test
    public void testRefreshRenewsThenAnswers() {
        ConnectContext context = Mockito.mock(ConnectContext.class);
        QueryScopedCredentials credentials = Mockito.mock(QueryScopedCredentials.class);
        Mockito.when(credentials.refreshAfter()).thenReturn(Instant.ofEpochMilli(1234L));
        Mockito.when(credentials.cloudConfiguration(context)).thenReturn(awsCredentials());

        Assertions.assertEquals(1234L,
                HdfsScanNode.refreshQueryScopedCredentials(owner(), credentials, context).getRefresh_after_ms());
        InOrder order = Mockito.inOrder(credentials);
        order.verify(credentials).renewForRunningScan(context);
        order.verify(credentials).cloudConfiguration(context);
    }
}
