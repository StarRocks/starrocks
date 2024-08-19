// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.google.common.collect.Lists;
import com.starrocks.sql.analyzer.AlterTableClauseAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AddRollupClause;
import com.starrocks.sql.ast.RollupRenameClause;
import org.junit.Assert;
import org.junit.Test;

public class AddRollupClauseTest {
    @Test
    public void testNormal() {
        AddRollupClause clause = new AddRollupClause("testRollup", Lists.newArrayList("col1", "col2"),
                null, "baseRollup", null);
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(null);
        analyzer.analyze(null, clause);

        Assert.assertEquals("ADD ROLLUP `testRollup` (`col1`, `col2`) FROM `baseRollup`", clause.toString());
        Assert.assertEquals("baseRollup", clause.getBaseRollupName());
        Assert.assertEquals("testRollup", clause.getRollupName());
        Assert.assertEquals("[col1, col2]", clause.getColumnNames().toString());
        Assert.assertNull(clause.getProperties());

        clause = new AddRollupClause("testRollup", Lists.newArrayList("col1", "col2"), null, null, null);
        analyzer.analyze(null, clause);
        Assert.assertEquals("ADD ROLLUP `testRollup` (`col1`, `col2`)", clause.toString());

        clause = new AddRollupClause("testRollup", Lists.newArrayList("col1", "col2"), null, null, null);
        analyzer.analyze(null, clause);
        Assert.assertEquals("ADD ROLLUP `testRollup` (`col1`, `col2`)", clause.toString());
    }

    @Test(expected = SemanticException.class)
    public void testNoRollup() {
        AddRollupClause clause = new AddRollupClause("", Lists.newArrayList("col1", "col2"), null, null, null);
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(null);
        analyzer.analyze(null, clause);
        Assert.fail("No exception throws.");
    }

    @Test(expected = SemanticException.class)
    public void testNoCol() {
        AddRollupClause clause = new AddRollupClause("testRollup", null, null, null, null);
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(null);
        analyzer.analyze(null, clause);
        Assert.fail("No exception throws.");
    }

    @Test(expected = SemanticException.class)
    public void testDupCol() {
        AddRollupClause clause = new AddRollupClause("testRollup",
                Lists.newArrayList("col1", "col1"), null, null, null);
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(null);
        analyzer.analyze(null, clause);
        Assert.fail("No exception throws.");
    }

    @Test(expected = SemanticException.class)
    public void testInvalidCol() {
        AddRollupClause clause = new AddRollupClause("testRollup", Lists.newArrayList("", "col1"), null, null, null);
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(null);
        analyzer.analyze(null, clause);
        Assert.fail("No exception throws.");
    }

    @Test(expected = SemanticException.class)
    public void testRename1() {
        RollupRenameClause clause = new RollupRenameClause("testRollup", "");
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(null);
        analyzer.analyze(null, clause);
        Assert.fail("No exception throws.");
    }

    @Test(expected = SemanticException.class)
    public void testRename2() {
        RollupRenameClause clause = new RollupRenameClause("", "testRollup");
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(null);
        analyzer.analyze(null, clause);
        Assert.fail("No exception throws.");
    }

    @Test
    public void testRename3() {
        RollupRenameClause clause = new RollupRenameClause("testRollup", "testRollup2");
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(null);
        analyzer.analyze(null, clause);
    }
}
