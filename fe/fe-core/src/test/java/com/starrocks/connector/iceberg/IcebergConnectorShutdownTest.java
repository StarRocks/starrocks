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

import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.iceberg.rest.IcebergRESTCatalog;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class IcebergConnectorShutdownTest {
    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createDefaultCtx();
    }

    @Test
    public void testShutdownStopsBackgroundJobPlanningExecutor() {
        new MockUp<IcebergRESTCatalog>() {
            @Mock
            public void $init(String name, Configuration conf, Map<String, String> properties) {
            }
        };
        // A non-resource-mapping catalog builds its native catalog (and the metadata cache) on construction.
        IcebergConnector connector = new IcebergConnector(new ConnectorContext("shutdown_test", "iceberg",
                Map.of("iceberg.catalog.type", "rest", "enable_iceberg_metadata_cache", "true")));
        Assertions.assertTrue(connector.getNativeCatalog() instanceof CachingIcebergCatalog);
        ExecutorService background = Deencapsulation.getField(connector, "backgroundJobPlanningExecutor");
        Assertions.assertNotNull(background);
        Assertions.assertFalse(background.isShutdown());

        connector.shutdown();

        Assertions.assertTrue(background.isShutdown());
    }

    @Test
    public void testShutdownClosesNativeCatalogThroughMetadataCache() {
        AtomicInteger closes = new AtomicInteger();
        new MockUp<IcebergRESTCatalog>() {
            @Mock
            public void $init(String name, Configuration conf, Map<String, String> properties) {
            }

            @Mock
            public void shutdown() {
                closes.incrementAndGet();
            }
        };
        for (String cacheEnabled : new String[] {"true", "false"}) {
            IcebergConnector connector = new IcebergConnector(new ConnectorContext("shutdown_test_" + cacheEnabled,
                    "iceberg", Map.of("iceberg.catalog.type", "rest", "enable_iceberg_metadata_cache", cacheEnabled)));
            connector.shutdown();
        }
        Assertions.assertEquals(2, closes.get());
    }
}
