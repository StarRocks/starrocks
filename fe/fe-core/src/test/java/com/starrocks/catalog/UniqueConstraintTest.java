// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.starrocks.common.AnalysisException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

public class UniqueConstraintTest {
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testParse() throws AnalysisException {
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

        String constraintDescs3 = "hive_catalog.db1.table1.col1, hive_catalog.db1.table1.col2, hive_catalog.db1.table1.col3;";
        List<UniqueConstraint> results3 = UniqueConstraint.parse(constraintDescs3);
        Assert.assertEquals(1, results3.size());
        Assert.assertEquals(Lists.newArrayList("col1", "col2", "col3"), results.get(0).getUniqueColumns());
        Assert.assertEquals("hive_catalog", results3.get(0).getCatalogName());
        Assert.assertEquals("db1", results3.get(0).getDbName());
        Assert.assertEquals("table1", results3.get(0).getTableName());

        String constraintDescs4 = "hive_catalog.db1.table1.col1, col2, col3;";
        List<UniqueConstraint> results4 = UniqueConstraint.parse(constraintDescs4);
        Assert.assertEquals(1, results4.size());
        Assert.assertEquals(Lists.newArrayList("col1", "col2", "col3"), results4.get(0).getUniqueColumns());
        Assert.assertEquals("hive_catalog", results4.get(0).getCatalogName());
        Assert.assertEquals("db1", results4.get(0).getDbName());
        Assert.assertEquals("table1", results4.get(0).getTableName());

        String constraintDescs5 = "hive_catalog.db1.table1.col1, col2, col3; hive_catalog.db1.table2.col1, col2, col3;";
        List<UniqueConstraint> results5 = UniqueConstraint.parse(constraintDescs5);
        Assert.assertEquals(2, results5.size());
        Assert.assertEquals(Lists.newArrayList("col1", "col2", "col3"), results5.get(0).getUniqueColumns());
        Assert.assertEquals("hive_catalog", results5.get(0).getCatalogName());
        Assert.assertEquals("db1", results5.get(0).getDbName());
        Assert.assertEquals("table1", results5.get(0).getTableName());
        Assert.assertEquals(Lists.newArrayList("col1", "col2", "col3"), results5.get(1).getUniqueColumns());
        Assert.assertEquals("hive_catalog", results5.get(1).getCatalogName());
        Assert.assertEquals("db1", results5.get(1).getDbName());
        Assert.assertEquals("table2", results5.get(1).getTableName());
    }

    @Test
    public void testParseException() throws AnalysisException {
        String constraintDescs = "hive_catalog.db1.table1.col1, col2, hive_catalog.db1.table2.col3";
        exception.expect(AnalysisException.class);
        exception.expectMessage("unique constraint column should be in same table");
        UniqueConstraint.parse(constraintDescs);
    }
}
