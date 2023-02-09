package com.starrocks.catalog;

import jersey.repackaged.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class UniqueConstraintTest {
    @Test
    public void testParse() {
        String constraintDescs = "col1, col2  , col3 ";
        List<UniqueConstraint> results = UniqueConstraint.parse(constraintDescs);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(Lists.newArrayList("col1", "col2", "col3"), results.get(0).getUniqueColumns());

        String constraintDescs2 = "col1, col2  , col3 ; col4, col5, col6,   col7  ; col8,;";
        List<UniqueConstraint> results2 = UniqueConstraint.parse(constraintDescs2);
        Assert.assertEquals(3, results2.size());
        Assert.assertEquals(Lists.newArrayList("col1", "col2", "col3"), results2.get(0).getUniqueColumns());
        Assert.assertEquals(Lists.newArrayList("col4", "col5", "col6", "col7"), results2.get(1).getUniqueColumns());
        Assert.assertEquals(Lists.newArrayList("col8"), results2.get(2).getUniqueColumns());
    }
}
