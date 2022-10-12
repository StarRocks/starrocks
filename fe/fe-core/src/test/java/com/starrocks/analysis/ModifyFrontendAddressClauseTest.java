// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.analysis;

import com.starrocks.ha.FrontendNodeType;

import com.starrocks.sql.ast.ModifyFrontendAddressClause;
import org.junit.Assert;
import org.junit.Test;


public class ModifyFrontendAddressClauseTest {

    @Test
    public void testCreateClause() {    
        ModifyFrontendAddressClause clause = new ModifyFrontendAddressClause("originalHost-test", "sandbox");
        Assert.assertEquals("sandbox", clause.getDestHost());
        Assert.assertEquals("originalHost-test", clause.getSrcHost());
    }

    @Test
    public void testNormal() {
        ModifyFrontendAddressClause clause = new ModifyFrontendAddressClause("test:1000", FrontendNodeType.FOLLOWER);
        Assert.assertTrue(clause.getHostPort().equals("test:1000"));
    }
}