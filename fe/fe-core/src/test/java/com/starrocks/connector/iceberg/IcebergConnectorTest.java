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

package com.starrocks.connector.iceberg;

import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.hive.ConnectorTableMetadataProcessor;
import com.starrocks.server.GlobalStateMgr;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.starrocks.connector.iceberg.IcebergCatalogProperties.ICEBERG_CATALOG_TYPE;
import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.RESOURCE_MAPPING_CATALOG_PREFIX;

public class IcebergConnectorTest {

    @Test
    public void testShutdownBackgroundJobPlanningExecutor() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put(ICEBERG_CATALOG_TYPE, "hive");
        String catalogName = RESOURCE_MAPPING_CATALOG_PREFIX + "iceberg_test";
        IcebergConnector connector = new IcebergConnector(
                new ConnectorContext(catalogName, "iceberg", properties));

        Method buildExecutor = IcebergConnector.class.getDeclaredMethod("buildBackgroundJobPlanningExecutor");
        buildExecutor.setAccessible(true);
        ExecutorService executor = (ExecutorService) buildExecutor.invoke(connector);
        Assertions.assertSame(executor, buildExecutor.invoke(connector));
        executor.submit(() -> null).get(5, TimeUnit.SECONDS);

        GlobalStateMgr globalStateMgr = Mockito.mock(GlobalStateMgr.class);
        ConnectorTableMetadataProcessor metadataProcessor = Mockito.mock(ConnectorTableMetadataProcessor.class);
        Mockito.when(globalStateMgr.getConnectorTableMetadataProcessor()).thenReturn(metadataProcessor);
        try (MockedStatic<GlobalStateMgr> mockedGlobalStateMgr = Mockito.mockStatic(GlobalStateMgr.class)) {
            mockedGlobalStateMgr.when(GlobalStateMgr::getCurrentState).thenReturn(globalStateMgr);
            connector.shutdown();
        }

        Assertions.assertTrue(executor.isShutdown());
        Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        Mockito.verify(metadataProcessor).unRegisterCachingIcebergCatalog(catalogName);
    }
}
