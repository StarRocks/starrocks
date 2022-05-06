// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;

import org.junit.Assert;
import org.junit.Test;


public class ModifyBackendAddressClauseTest {
    
    @Test
    public void testCreateClause() {
        ModifyBackendAddressClause clause1 = new ModifyBackendAddressClause(
            new Pair<String, Integer>("originalHost-test", 1000), "sandbox"
        );
        Assert.assertEquals(new Pair<String, Integer>("originalHost-test", 1000), clause1.getWantToModifyHostPortPair());
        Assert.assertEquals("sandbox", clause1.getFqdn());

        ModifyBackendAddressClause clause2 = new ModifyBackendAddressClause("originalHost-test:1000", "sandbox");
        Assert.assertEquals("originalHost-test:1000", clause2.getWantToModifyHostPort());
        clause2.setWantToModifyHostPortPair(new Pair<String, Integer>("originalHost-test", 1000));
        Assert.assertEquals(new Pair<String, Integer>("originalHost-test", 1000), clause2.getWantToModifyHostPortPair());
    }

    @Test
    public void testNormal() throws AnalysisException {
        ModifyBackendAddressClause clause = new ModifyBackendAddressClause(null);
        Assert.assertTrue(clause.getHostPortPairs().size() == 0);
    }

}
