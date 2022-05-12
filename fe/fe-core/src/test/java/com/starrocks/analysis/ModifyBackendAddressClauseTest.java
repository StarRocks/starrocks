// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import com.starrocks.common.AnalysisException;

import org.junit.Assert;
import org.junit.Test;


public class ModifyBackendAddressClauseTest {
    
    @Test
    public void testCreateClause() {
        ModifyBackendAddressClause clause1 = new ModifyBackendAddressClause("originalHost-test", "sandbox");
        Assert.assertEquals("originalHost-test", clause1.getToBeModifyHost());
        Assert.assertEquals("sandbox", clause1.getFqdn());
    }

    @Test
    public void testNormal() throws AnalysisException {
        ModifyBackendAddressClause clause = new ModifyBackendAddressClause(null);
        Assert.assertTrue(clause.getHostPortPairs().size() == 0);
    }
}
