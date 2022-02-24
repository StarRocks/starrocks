// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.external.iceberg.hive;

import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

public class HiveCatalogTest {

    @Test
    public void testInitialize() {
        try {
            HiveCatalog catalog = new HiveCatalog();
            catalog.initialize("hive", Maps.newHashMap());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }
}
