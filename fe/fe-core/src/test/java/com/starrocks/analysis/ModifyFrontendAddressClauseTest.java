// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import com.starrocks.common.Pair;
import com.starrocks.ha.FrontendNodeType;

import org.junit.Assert;
import org.junit.Test;


public class ModifyFrontendAddressClauseTest {

    @Test
    public void testCreateClause() {    
        ModifyFrontendAddressClause clause = new ModifyFrontendAddressClause("originalHost-test:1000", "sandbox");
        Assert.assertEquals("sandbox", clause.getFqdn());
        Assert.assertEquals("originalHost-test:1000", clause.getWantToModifyHostPort());
        clause.setWantToModifyHostPortPair(new Pair<String, Integer>("originalHost-test", 1000));
        Assert.assertEquals(new Pair<String, Integer>("originalHost-test", 1000), clause.getWantToModifyHostPortPair());
    }

    @Test
    public void testNormal() {
        ModifyFrontendAddressClause clause = new ModifyFrontendAddressClause("test:1000", FrontendNodeType.FOLLOWER);
        Assert.assertTrue(clause.getHostPort().equals("test:1000"));
    }
}