// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.catalog;

import org.junit.Assert;
import org.junit.Test;

public class PrimitiveTypeTest {

    @Test
    public void testGetTypeSize() {
        for (PrimitiveType ptype : PrimitiveType.values()) {
            int size = ptype.getTypeSize();
            Assert.assertTrue(size >= 0);
        }
    }
}
