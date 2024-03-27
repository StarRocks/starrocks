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

package com.starrocks.sql.analyzer;

import com.starrocks.catalog.OlapTable;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.HashDistributionDesc;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
import com.starrocks.sql.ast.OptimizeClause;
import com.starrocks.sql.parser.NodePosition;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AlterTableClauseVisitorTest extends DDLTestBase {
    private static ConnectContext connectContext;
    private static AlterTableClauseVisitor clauseAnalyzerVisitor;

    @Before
    public void beforeClass() throws Exception {

        super.setUp();


    }

    @Test
    public void testVisitOptimizeClause() {

        NodePosition nodePosition = new NodePosition(1, 23, 1, 48);
        HashDistributionDesc hashDistributionDesc = new HashDistributionDesc();

        List<String> list = new ArrayList<>();
        list.add("id");

        OptimizeClause optimizeClause = new OptimizeClause(null, null, hashDistributionDesc, list, null, nodePosition);

        AlterTableClauseVisitor visitor = new AlterTableClauseVisitor();

        OlapTable table = new OlapTable();

        visitor.setTable(table);


        Assert.assertThrows("Getting analyzing error. Detail message: Unknown column 'id' does not exist.",
                SemanticException.class, () -> visitor.visitOptimizeClause(optimizeClause, null));


    }

    @Test
    public void testVisitModifyTablePropertiesClause() {
        connectContext = new ConnectContext();
        clauseAnalyzerVisitor = new AlterTableClauseVisitor();
        OlapTable table = new OlapTable();
        clauseAnalyzerVisitor.setTable(table);

        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE, "true");
        ModifyTablePropertiesClause alterClause = new ModifyTablePropertiesClause(properties);
        clauseAnalyzerVisitor.visitModifyTablePropertiesClause(alterClause, connectContext);
        properties.remove(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE);

        properties.put(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE, "false");
        clauseAnalyzerVisitor.visitModifyTablePropertiesClause(alterClause, connectContext);
        properties.remove(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE);

        properties.put(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE, "abcd");
        try {
            clauseAnalyzerVisitor.visitModifyTablePropertiesClause(alterClause, connectContext);
            Assert.assertTrue(false);
        } catch (SemanticException e) {
            Assert.assertEquals("Getting analyzing error. Detail message: Property datacache.enable must " +
                    "be bool type(false/true).", e.getMessage());
        }
        properties.remove(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE);
    }
}