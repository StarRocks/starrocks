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
import com.starrocks.catalog.constraint.UniqueConstraint;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.analyzer.SemanticException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class UniqueConstraintTest {

    @Test
    public void testParse() throws AnalysisException {
        String constraintDescs = "col1, col2  , col3 ";
        List<UniqueConstraint> results = UniqueConstraint.parse(null, null, null, constraintDescs);
        Assertions.assertEquals(1, results.size());
        Assertions.assertEquals(Lists.newArrayList(ColumnId.create("col1"), ColumnId.create("col2"), ColumnId.create("col3")),
                results.get(0).getUniqueColumns());

        String constraintDescs2 = "col1, col2  , col3 ; col4, col5, col6,   col7  ; col8,;";
        List<UniqueConstraint> results2 = UniqueConstraint.parse(null, null, null, constraintDescs2);
        Assertions.assertEquals(3, results2.size());
        Assertions.assertEquals(Lists.newArrayList(ColumnId.create("col1"), ColumnId.create("col2"), ColumnId.create("col3")),
                results2.get(0).getUniqueColumns());
        Assertions.assertEquals(Lists.newArrayList(ColumnId.create("col4"), ColumnId.create("col5"),
                ColumnId.create("col6"), ColumnId.create("col7")), results2.get(1).getUniqueColumns());
        Assertions.assertEquals(Lists.newArrayList(ColumnId.create("col8")), results2.get(2).getUniqueColumns());

        String constraintDescs3 =
                "hive_catalog.db1.table1.col1, hive_catalog.db1.table1.col2, hive_catalog.db1.table1.col3;";
        List<UniqueConstraint> results3 = UniqueConstraint.parse(null, null, null, constraintDescs3);
        Assertions.assertEquals(1, results3.size());
        Assertions.assertEquals(Lists.newArrayList(ColumnId.create("col1"), ColumnId.create("col2"), ColumnId.create("col3")),
                results.get(0).getUniqueColumns());
        Assertions.assertEquals("hive_catalog", results3.get(0).getCatalogName());
        Assertions.assertEquals("db1", results3.get(0).getDbName());
        Assertions.assertEquals("table1", results3.get(0).getTableName());

        String constraintDescs4 = "hive_catalog.db1.table1.col1, col2, col3;";
        List<UniqueConstraint> results4 = UniqueConstraint.parse(null, null, null, constraintDescs4);
        Assertions.assertEquals(1, results4.size());
        Assertions.assertEquals(Lists.newArrayList(ColumnId.create("col1"), ColumnId.create("col2"), ColumnId.create("col3")),
                results4.get(0).getUniqueColumns());
        Assertions.assertEquals("hive_catalog", results4.get(0).getCatalogName());
        Assertions.assertEquals("db1", results4.get(0).getDbName());
        Assertions.assertEquals("table1", results4.get(0).getTableName());

        String constraintDescs5 = "hive_catalog.db1.table1.col1, col2, col3; hive_catalog.db1.table2.col1, col2, col3;";
        List<UniqueConstraint> results5 = UniqueConstraint.parse(null, null, null, constraintDescs5);
        Assertions.assertEquals(2, results5.size());
        Assertions.assertEquals(Lists.newArrayList(ColumnId.create("col1"), ColumnId.create("col2"), ColumnId.create("col3")),
                results5.get(0).getUniqueColumns());
        Assertions.assertEquals("hive_catalog", results5.get(0).getCatalogName());
        Assertions.assertEquals("db1", results5.get(0).getDbName());
        Assertions.assertEquals("table1", results5.get(0).getTableName());
        Assertions.assertEquals(Lists.newArrayList(ColumnId.create("col1"), ColumnId.create("col2"), ColumnId.create("col3")),
                results5.get(1).getUniqueColumns());
        Assertions.assertEquals("hive_catalog", results5.get(1).getCatalogName());
        Assertions.assertEquals("db1", results5.get(1).getDbName());
        Assertions.assertEquals("table2", results5.get(1).getTableName());
    }

    @Test
    public void testParseException() {
        String constraintDescs = "hive_catalog.db1.table1.col1, col2, hive_catalog.db1.table2.col3";
        Throwable exception =
                assertThrows(SemanticException.class, () -> UniqueConstraint.parse(null, null, null, constraintDescs));
        assertThat(exception.getMessage(), containsString("unique constraint column should be in same table"));
    }
}
