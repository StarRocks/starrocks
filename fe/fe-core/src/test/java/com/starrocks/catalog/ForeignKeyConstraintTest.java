package com.starrocks.catalog;

import jersey.repackaged.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class ForeignKeyConstraintTest {
    @Test
    public void testParse() {
        String constraintDescs = "(column1, column2) REFERENCES catalog.database.table(column1', column2')";
        List<UniqueConstraint> results = UniqueConstraint.parse(constraintDescs);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(Lists.newArrayList("col1", "col2", "col3"), results.get(0).getUniqueColumns());
    }
}
