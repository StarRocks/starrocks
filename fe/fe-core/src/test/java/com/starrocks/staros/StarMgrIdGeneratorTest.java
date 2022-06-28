// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.staros;

import com.starrocks.catalog.CatalogIdGenerator;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

public class StarMgrIdGeneratorTest {
    @Mocked
    private CatalogIdGenerator generator;

    @Test
    public void testStarMgrIdGenerator() {
        new MockUp<CatalogIdGenerator>() {
            @Mock
            public long getNextId() {
                return 1234;
            }
        };

        StarMgrIdGenerator idGenerator = new StarMgrIdGenerator(generator);

        Assert.assertEquals(1234, idGenerator.getNextId());
    }
}
