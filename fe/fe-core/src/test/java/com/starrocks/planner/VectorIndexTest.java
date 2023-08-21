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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/planner/QueryPlanTest.java

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

package com.starrocks.planner;

import com.google.common.collect.Lists;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Type;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.optimizer.Memo;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.transformation.RewriteToVectorPlanRule;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import mockit.Mocked;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class VectorIndexTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static String DB_NAME = "test";

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        connectContext.setQueryId(UUIDUtil.genUUID());
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase(DB_NAME).useDatabase(DB_NAME);

        starRocksAssert.withTable("create table test.test_vector(c0 INT, c1 array<float>) " +
            " duplicate key(c0) distributed by hash(c0) buckets 1 " +
            "properties('replication_num'='1');");
    }

    @Test
    public void testVectorIndexSyntax() throws Exception {
        String sql1 = "select c1 from test.test_vector " +
            "order by cosine_similarity(c1, [1.1,2.2,3.3]) desc limit 10";
        List<StatementBase> stmts1 = com.starrocks.sql.parser.SqlParser.parse(sql1,
                connectContext.getSessionVariable());
        StmtExecutor stmtExecutor1 = new StmtExecutor(connectContext, stmts1.get(0));
        stmtExecutor1.execute();
        Assert.assertTrue(!connectContext.getSessionVariable().isEnableUseANN());

        String sql2 = "select c1 from test.test_vector " +
                "order by approx_cosine_similarity([1.1,2.2,3.3], c1) desc limit 10";
        List<StatementBase> stmts2 = com.starrocks.sql.parser.SqlParser.parse(sql2,
                connectContext.getSessionVariable());
        StmtExecutor stmtExecutor2 = new StmtExecutor(connectContext, stmts2.get(0));
        stmtExecutor2.execute();
        Assert.assertTrue(connectContext.getSessionVariable().isEnableUseANN());

        String sql3 = "select c1 from test.test_vector " +
                "order by approx_cosine_similarity(\"1.1,2.2,3.3\", c1) desc limit 10";
        List<StatementBase> stmts3 = com.starrocks.sql.parser.SqlParser.parse(sql3,
                connectContext.getSessionVariable());
        StmtExecutor stmtExecutor3 = new StmtExecutor(connectContext, stmts3.get(0));
        stmtExecutor3.execute();
        Assert.assertTrue(connectContext.getSessionVariable().isEnableUseANN());

        String sql4 = "select c1 from test.test_vector " +
                "order by approx_l2_distance(\"1.1,2.2,3.3\", c1) limit 10";
        List<StatementBase> stmts4 = com.starrocks.sql.parser.SqlParser.parse(sql4,
                connectContext.getSessionVariable());
        StmtExecutor stmtExecutor4 = new StmtExecutor(connectContext, stmts4.get(0));
        stmtExecutor4.execute();
        Assert.assertTrue(connectContext.getSessionVariable().isEnableUseANN());

        // Sorting in desc order doesn't make sense in l2_distance
        // , which won't trigger the vector retrieval logic.
        String sql5 = "select c1 from test.test_vector " +
                "order by approx_l2_distance([1.1,2.2,3.3], c1) desc limit 10";
        List<StatementBase> stmts5 = com.starrocks.sql.parser.SqlParser.parse(sql5,
                connectContext.getSessionVariable());
        StmtExecutor stmtExecutor5 = new StmtExecutor(connectContext, stmts5.get(0));
        stmtExecutor5.execute();
        Assert.assertTrue(!connectContext.getSessionVariable().isEnableUseANN());
    }

    @Test
    public void testRewriteToVectorPlanRule(@Mocked OlapTable olapTable1) {
        // sql: select c1 from test.test_vector order by cosine_similarity([1.1,2.2,3.3], c1) limit 10;
        connectContext.getSessionVariable().setEnableUseANN(true);
        ColumnRefOperator c1ColumnRef = new ColumnRefOperator(2, Type.ARRAY_FLOAT, "c1", true);
        Map<ColumnRefOperator, ScalarOperator> topNProjectMap = new HashMap<ColumnRefOperator, ScalarOperator>() {
            {
                put(c1ColumnRef, c1ColumnRef);
            }
        };
        ColumnRefOperator cosineColumnRef = new ColumnRefOperator(3, Type.FLOAT, "approx_l2_distance", true);
        CallOperator cosineCallRef = new CallOperator("approx_l2_distance", Type.FLOAT, new ArrayList<>());
        OptExpression topN = new OptExpression(new LogicalTopNOperator(10, null, new Projection(topNProjectMap), null,
                0, Lists.newArrayList(new Ordering(cosineColumnRef, true, true)),
                0, null, null, false)
        );

        Map<ColumnRefOperator, ScalarOperator> scanProjectMap = new HashMap<ColumnRefOperator, ScalarOperator>() {
            {
                put(c1ColumnRef, c1ColumnRef);
                put(cosineColumnRef, cosineCallRef);
            }
        };
        OptExpression scan = new OptExpression(new LogicalOlapScanOperator(olapTable1, new Projection(scanProjectMap),
                10));

        topN.getInputs().add(scan);

        RewriteToVectorPlanRule rule = new RewriteToVectorPlanRule();
        List<OptExpression> list = rule.transform(topN, new OptimizerContext(new Memo(), new ColumnRefFactory(), connectContext));

        ScalarOperator scanProjectOperator =
                ((ScalarOperator) list.get(0).getInputs().get(0).getOp().getProjection().getColumnRefMap().values().toArray()[0]);

        // Substitution of sort column
        assertEquals(connectContext.getSessionVariable().getVectorDistanceColumnName(),
                ((LogicalTopNOperator) list.get(0).getOp()).getOrderByElements().get(0).getColumnRef().getName());
        assertTrue(scanProjectOperator instanceof ColumnRefOperator
                && ((ColumnRefOperator) scanProjectOperator).getName().equals(connectContext.getSessionVariable().getVectorDistanceColumnName()));
    }
}
