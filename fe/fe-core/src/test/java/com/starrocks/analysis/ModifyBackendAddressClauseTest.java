// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.analysis;

import com.starrocks.common.AnalysisException;

import com.starrocks.sql.ast.ModifyBackendAddressClause;
import org.junit.Assert;
import org.junit.Test;


public class ModifyBackendAddressClauseTest {
    
    @Test
    public void testCreateClause() {
        ModifyBackendAddressClause clause1 = new ModifyBackendAddressClause("originalHost-test", "sandbox");
        Assert.assertEquals("originalHost-test", clause1.getSrcHost());
        Assert.assertEquals("sandbox", clause1.getDestHost());
    }

    @Test
    public void testNormal() throws AnalysisException {
        ModifyBackendAddressClause clause = new ModifyBackendAddressClause("", "");
        Assert.assertTrue(clause.getHostPortPairs().size() == 0);
    }
}
