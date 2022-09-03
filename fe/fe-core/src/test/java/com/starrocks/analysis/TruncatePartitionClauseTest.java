// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.analysis;

import java.util.Arrays;

import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.ast.TruncatePartitionClause;
import org.junit.Assert;
import org.junit.Test;

import com.starrocks.alter.AlterOpType;

public class TruncatePartitionClauseTest {
    
    @Test
    public void testInitTruncatePartitionClause() {

        TruncatePartitionClause clause1 = new TruncatePartitionClause(AlterOpType.TRUNCATE_PARTITION);
        Assert.assertEquals(AlterOpType.TRUNCATE_PARTITION, clause1.getOpType()); 

        PartitionNames partitionNames = new PartitionNames(true, Arrays.asList("p1"));
        TruncatePartitionClause clause2 = new TruncatePartitionClause(partitionNames);
        Assert.assertEquals("p1", clause2.getPartitionNames().getPartitionNames().get(0));

        Assert.assertTrue(clause2.isSupportNewPlanner());
    }
}
