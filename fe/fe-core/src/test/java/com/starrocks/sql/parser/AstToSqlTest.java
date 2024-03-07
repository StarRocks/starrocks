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

package com.starrocks.sql.parser;

import com.google.common.collect.Lists;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.CaseExpr;
import com.starrocks.analysis.CaseWhenClause;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.common.FeConstants;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.Assert.fail;

public class AstToSqlTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
    }

    @ParameterizedTest
    @MethodSource("testSqls")
    void testAstToSQl(String sql) {

        TableName tableName = new TableName(null, "employee");
        Relation relation = new TableRelation(tableName);
        SelectList selectList = new SelectList();
        selectList.addItem(new SelectListItem(new SlotRef(tableName, "name"), "name"));


        Expr caseExpr = new SlotRef(tableName, "age"); // age列
        // 创建when表达式
        Expr whenExpr = new BinaryPredicate(BinaryType.GT, caseExpr, new IntLiteral(10));
        // 创建then表达式
        Expr thenExpr = new IntLiteral(1);
        // 创建else表达式
        Expr elseExpr = new IntLiteral(0);
        CaseExpr caseWhenExpr = new CaseExpr(caseExpr, Lists.newArrayList(new CaseWhenClause(whenExpr, thenExpr)), elseExpr);
        selectList.addItem(new SelectListItem(caseWhenExpr, "h"));

        List<Expr> args = new ArrayList<>();
        args.add(new SlotRef(tableName, "name"));
        args.add(new SlotRef(tableName, "age"));

        FunctionCallExpr funcExpr = new FunctionCallExpr("row", args);

        selectList.addItem(new SelectListItem(funcExpr, "f2"));

        SelectRelation selectRelation = new SelectRelation(selectList, relation, null, null, null);
        QueryStatement queryStatement = new QueryStatement(selectRelation);

        //System.out.println(AstToSQLBuilder.toSQL(queryStatement));


        TableName targetTbl = new TableName(null, "person");
        TableRelation targetRelation = new TableRelation(targetTbl);
        targetRelation.setAlias(new TableName(null, "a"));

        TableName sourceTbl = new TableName(null, "employee");
        TableRelation sourceRelation = new TableRelation(sourceTbl);
        sourceRelation.setAlias(new TableName(null, "b"));

        BinaryPredicate predicate = new BinaryPredicate(BinaryType.EQ,
                new SlotRef(sourceRelation.getAlias(), "name"),
                new SlotRef(targetRelation.getAlias(), "name"));

        JoinRelation joinRelation = new JoinRelation(JoinOperator.RIGHT_OUTER_JOIN,
                targetRelation, sourceRelation, predicate, false);
        String firstSQLInPlainText = AstToSQLBuilder.toSQL(
                new QueryStatement(new SelectRelation(selectList, joinRelation, null, null, null)));

        System.out.println(firstSQLInPlainText);

        String testSql = "select row_number() over(partition by name) from person where id='fasdf' and data='fdaf'";
        StatementBase testStmt = SqlParser.parse(testSql, connectContext.getSessionVariable()).get(0);

        String rnSql = "select row_number() over(partition by name) from person";
        StatementBase rnSqlStmt = SqlParser.parse(testSql, connectContext.getSessionVariable()).get(0);

        StatementBase stmt = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        Analyzer.analyze(stmt, connectContext);
        String afterSql = AstToSQLBuilder.toSQL(stmt);
        try {
            SqlParser.parse(afterSql, connectContext.getSessionVariable());
            System.out.println(afterSql);
            System.out.println("====");
        } catch (Exception e) {
            fail("failed to parse the sql: " + afterSql + ". errMsg: " + e.getMessage());
        }
    }


    private static Stream<Arguments> testSqls() {
        List<String> sqls = Lists.newArrayList();
//        sqls.add("with with_t_0 as (select t0.days_add(t0.v1, -1692245077) from t0) select * from t1, with_t_0;");
//        sqls.add("with with_t_0 as (select t0.days_add(v1, -1692245077) as c from t0) select * from t1, with_t_0;");
//        sqls.add("with with_t_0 (abc) as (select t0.days_add(v1, -1692245077) as c from t0) select * from t1, with_t_0;");
        sqls.add("with with_t_0 as (select t0.days_add(v1, -1692245077) as c from t0) " +
                "select * from t1, with_t_0 where t1.abs(v4) = 1 order by t1.abs(v6) = 2;");
        return sqls.stream().map(e -> Arguments.of(e));
    }
}
