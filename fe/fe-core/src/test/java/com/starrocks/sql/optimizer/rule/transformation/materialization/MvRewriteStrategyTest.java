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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Optimizer;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class MvRewriteStrategyTest extends MVTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        starRocksAssert.withTable(cluster, "test_base_part");
        starRocksAssert.withTable(cluster, "table_with_partition");
    }

    private OptExpression optimize(Optimizer optimizer, String sql) {
        try {
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            QueryStatement queryStatement = (QueryStatement) stmt;
            ColumnRefFactory columnRefFactory = new ColumnRefFactory();
            LogicalPlan logicalPlan = new RelationTransformer(columnRefFactory, connectContext)
                    .transformWithSelectLimit(queryStatement.getQueryRelation());
            return optimizer.optimize(connectContext, logicalPlan.getRoot(), new PhysicalPropertySet(),
                    new ColumnRefSet(logicalPlan.getOutputColumn()), columnRefFactory);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
            return null;
        }
    }

    @Test
    public void testSingleTableRewriteStrategy() throws Exception {
        createAndRefreshMv("create materialized view mv1 " +
                " partition by id_date" +
                " distributed by random " +
                " as" +
                " select t1a, id_date, t1b from table_with_partition");
        String sql =  "select t1a, id_date, t1b from table_with_partition";
        Optimizer optimizer = new Optimizer();
        OptExpression optExpression = optimize(optimizer, sql);
        Assert.assertTrue(optExpression != null);
        MvRewriteStrategy mvRewriteStrategy = optimizer.getMvRewriteStrategy();
        Assert.assertTrue(mvRewriteStrategy.enableMultiTableRewrite == false);
        Assert.assertTrue(mvRewriteStrategy.enableSingleTableRewrite == true);
        Assert.assertTrue(mvRewriteStrategy.enableMaterializedViewRewrite == true);
        Assert.assertTrue(mvRewriteStrategy.enableForceRBORewrite == true);
        Assert.assertTrue(mvRewriteStrategy.enableViewBasedRewrite == false);
    }
}