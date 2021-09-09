// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/DropColumnClauseTest.java

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

import com.starrocks.common.AnalysisException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class DropColumnClauseTest {
    private static Analyzer analyzer;

    @BeforeClass
    public static void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(false);
    }

    @Test
    public void testNormal() throws AnalysisException {
        DropColumnClause clause = new DropColumnClause("col", "", null);
        clause.analyze(analyzer);
        Assert.assertEquals("col", clause.getColName());
        Assert.assertEquals("DROP COLUMN `col`", clause.toString());

        clause = new DropColumnClause("col", "rollup", null);
        clause.analyze(analyzer);
        Assert.assertEquals("rollup", clause.getRollupName());
        Assert.assertNull("rollup", clause.getProperties());
        Assert.assertEquals("DROP COLUMN `col` IN `rollup`", clause.toString());
    }

    @Test(expected = AnalysisException.class)
    public void testNoCol() throws AnalysisException {
        DropColumnClause clause = new DropColumnClause("", "", null);
        clause.analyze(analyzer);
        Assert.fail("No exception throws.");
    }
}