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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/SetStmtTest.java

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

package com.starrocks.sql.ast.expression;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksException;
import com.starrocks.mysql.privilege.MockedAuth;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.planner.SlotId;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.analyzer.SetStmtAnalyzer;
import com.starrocks.sql.ast.SetListItem;
import com.starrocks.sql.ast.SetNamesVar;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.SetType;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.ast.UserVariable;
import com.starrocks.sql.common.QueryDebugOptions;
import com.starrocks.sql.parser.NodePosition;
import mockit.Mocked;
import org.apache.commons.lang3.EnumUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SetStmtTest {

    @Mocked
    private ConnectContext ctx;

    @BeforeEach
    public void setUp() {
        MockedAuth.mockedConnectContext(ctx, "root", "192.168.1.1");
    }

    @Test
    public void testNormal() throws StarRocksException {
        List<SetListItem> vars = Lists.newArrayList(new UserVariable("times", new IntLiteral(100L),
                        NodePosition.ZERO),
                new SetNamesVar("utf8"));
        SetStmt stmt = new SetStmt(vars);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);

        Assertions.assertEquals("times", ((UserVariable) stmt.getSetListItems().get(0)).getVariable());
        Assertions.assertEquals("100", ((UserVariable) stmt.getSetListItems().get(0)).getEvaluatedExpression().toSql());
        Assertions.assertTrue(stmt.getSetListItems().get(1) instanceof SetNamesVar);
        Assertions.assertEquals("utf8", ((SetNamesVar) stmt.getSetListItems().get(1)).getCharset());
    }

    @Test
    public void testNoVariable() {
        assertThrows(SemanticException.class, () -> {
            SystemVariable var = new SystemVariable(SetType.SESSION, "", new StringLiteral("utf-8"));
            SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(var)), ctx);
            Assertions.fail("No exception throws.");
        });
    }

    @Test
    public void testNonConstantExpr() {
        SlotDescriptor descriptor = new SlotDescriptor(new SlotId(1), "x",
                Type.INT, false);
        Expr lhsExpr = new SlotRef(descriptor);
        Expr rhsExpr = new IntLiteral(100L);
        ArithmeticExpr addExpr = new ArithmeticExpr(
                ArithmeticExpr.Operator.ADD, lhsExpr, rhsExpr);
        SystemVariable var = new SystemVariable(SetType.SESSION, SessionVariable.SQL_SELECT_LIMIT, addExpr);
        Throwable exception = assertThrows(SemanticException.class, () ->
                SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(var)), ctx));
        assertThat(exception.getMessage(), containsString("Set statement only support constant expr."));
    }

    @Test
    public void setResourceGroup() {
        SystemVariable setEmpty = new SystemVariable(SetType.SESSION, SessionVariable.RESOURCE_GROUP, new StringLiteral(""));
        SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setEmpty)), ctx);

        SystemVariable setVar =
                new SystemVariable(SetType.SESSION, SessionVariable.RESOURCE_GROUP, new StringLiteral("not_exists"));
        try {
            SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setVar)), ctx);
            Assertions.fail("should fail");
        } catch (SemanticException e) {
            Assertions.assertEquals("Getting analyzing error. Detail message: resource group not exists: not_exists.",
                    e.getMessage());
        }
    }

    @Test
    public void testSetNonNegativeLongVariable() throws StarRocksException {
        List<String> fields = Lists.newArrayList(
                SessionVariable.LOAD_MEM_LIMIT,
                SessionVariable.QUERY_MEM_LIMIT,
                SessionVariable.SQL_SELECT_LIMIT);

        for (String field : fields) {
            Assertions.assertThrows(SemanticException.class, () -> {
                SystemVariable setVar = new SystemVariable(SetType.SESSION, field, new StringLiteral("non_number"));
                SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setVar)), ctx);
            }, "is not a number");

            Assertions.assertThrows(SemanticException.class, () -> {
                SystemVariable setVar = new SystemVariable(SetType.SESSION, field, new StringLiteral("-1"));
                SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setVar)), ctx);
            }, "must be equal or greater than 0");

            SystemVariable var = new SystemVariable(SetType.SESSION, field, new StringLiteral("0"));
            SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(var)), ctx);
            Assertions.assertEquals(field, var.getVariable());
            Assertions.assertEquals("0", var.getResolvedExpression().getStringValue());

            var = new SystemVariable(SetType.SESSION, field, new StringLiteral("10"));
            SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(var)), ctx);
            Assertions.assertEquals(field, var.getVariable());
            Assertions.assertEquals("10", var.getResolvedExpression().getStringValue());

            var = new SystemVariable(SetType.SESSION, field, new IntLiteral(0));
            SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(var)), ctx);
            Assertions.assertEquals(field, var.getVariable());
            Assertions.assertEquals("0", var.getResolvedExpression().getStringValue());

            var = new SystemVariable(SetType.SESSION, field, new IntLiteral(10));
            SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(var)), ctx);
            Assertions.assertEquals(field, var.getVariable());
            Assertions.assertEquals("10", var.getResolvedExpression().getStringValue());
        }
    }

    @Test
    public void testMaterializedViewRewriteMode() throws AnalysisException {
        // normal
        {
            for (SessionVariable.MaterializedViewRewriteMode mode :
                    EnumUtils.getEnumList(SessionVariable.MaterializedViewRewriteMode.class)) {
                try {
                    SystemVariable setVar = new SystemVariable(SetType.SESSION, SessionVariable.MATERIALIZED_VIEW_REWRITE_MODE,
                            new StringLiteral(mode.toString()));
                    SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setVar)), ctx);
                } catch (Exception e) {
                    Assertions.fail();
                    ;
                }
            }

        }

        // empty
        {
            SystemVariable setVar = new SystemVariable(SetType.SESSION, SessionVariable.MATERIALIZED_VIEW_REWRITE_MODE,
                    new StringLiteral(""));
            try {
                SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setVar)), ctx);
                Assertions.fail();
            } catch (Exception e) {
                e.printStackTrace();
                Assertions.assertEquals("Getting analyzing error. Detail message: Unsupported materialized view " +
                                "rewrite mode: , supported list is DISABLE,DEFAULT,DEFAULT_OR_ERROR,FORCE,FORCE_OR_ERROR.",
                        e.getMessage());
            }
        }

        // bad case
        {
            SystemVariable setVar = new SystemVariable(SetType.SESSION, SessionVariable.MATERIALIZED_VIEW_REWRITE_MODE,
                    new StringLiteral("bad_case"));
            try {
                SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setVar)), ctx);
                Assertions.fail("should fail");
            } catch (SemanticException e) {
                Assertions.assertEquals("Getting analyzing error. Detail message: Unsupported " +
                        "materialized view rewrite mode: bad_case, " +
                        "supported list is DISABLE,DEFAULT,DEFAULT_OR_ERROR,FORCE,FORCE_OR_ERROR.", e.getMessage());
                ;
            }
        }
    }

    @Test
    public void testFollowerQueryForwardMode() throws AnalysisException {
        // normal
        {
            for (SessionVariable.FollowerQueryForwardMode mode :
                    EnumUtils.getEnumList(SessionVariable.FollowerQueryForwardMode.class)) {
                try {
                    SystemVariable setVar = new SystemVariable(SetType.SESSION, SessionVariable.FOLLOWER_QUERY_FORWARD_MODE,
                            new StringLiteral(mode.toString()));
                    SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setVar)), ctx);
                } catch (Exception e) {
                    Assertions.fail();
                    ;
                }
            }

        }

        // empty
        {
            SystemVariable setVar = new SystemVariable(SetType.SESSION, SessionVariable.FOLLOWER_QUERY_FORWARD_MODE,
                    new StringLiteral(""));
            try {
                SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setVar)), ctx);
                Assertions.fail();
            } catch (Exception e) {
                e.printStackTrace();
                Assertions.assertEquals("Getting analyzing error. Detail message: Unsupported follower " +
                                "query forward mode: , supported list is DEFAULT,FOLLOWER,LEADER.",
                        e.getMessage());
            }
        }

        // bad case
        {
            SystemVariable setVar = new SystemVariable(SetType.SESSION, SessionVariable.FOLLOWER_QUERY_FORWARD_MODE,
                    new StringLiteral("bad_case"));
            try {
                SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setVar)), ctx);
                Assertions.fail("should fail");
            } catch (SemanticException e) {
                Assertions.assertEquals("Getting analyzing error. Detail message: " +
                        "Unsupported follower query forward mode: bad_case, " +
                        "supported list is DEFAULT,FOLLOWER,LEADER.", e.getMessage());
                ;
            }
        }
    }

    @Test
    public void testQueryDebuOptions() throws AnalysisException {
        // normal
        {
            String[] jsons = {
                    "",
                    "{'enableNormalizePredicateAfterMVRewrite':'true'}",
                    "{'maxRefreshMaterializedViewRetryNum':2}",
                    "{'enableNormalizePredicateAfterMVRewrite':'true', 'maxRefreshMaterializedViewRetryNum':2}",
            };
            for (String json : jsons) {
                try {
                    SystemVariable setVar = new SystemVariable(SetType.SESSION, SessionVariable.QUERY_DEBUG_OPTIONS,
                            new StringLiteral(json));
                    SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setVar)), ctx);
                } catch (Exception e) {
                    Assertions.fail();
                    ;
                }
            }
        }
        // bad
        {
            String[] jsons = {
                    "abc",
                    "{abc",
            };
            for (String json : jsons) {
                try {
                    SystemVariable setVar = new SystemVariable(SetType.SESSION, SessionVariable.QUERY_DEBUG_OPTIONS,
                            new StringLiteral(json));
                    SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setVar)), ctx);
                    Assertions.fail();
                    ;
                } catch (Exception e) {
                    Assertions.assertTrue(e.getMessage().contains("Unsupported query_debug_option"));
                    e.printStackTrace();
                }
            }
        }
        // non normal
        {
            String[] jsons = {
                    "{'enableNormalizePredicateAfterMVRewrite2':'true'}",
                    "{'maxRefreshMaterializedViewRetryNum2':'2'}"
            };
            for (String json : jsons) {
                try {
                    SystemVariable setVar = new SystemVariable(SetType.SESSION, SessionVariable.QUERY_DEBUG_OPTIONS,
                            new StringLiteral(json));
                    SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setVar)), ctx);
                    QueryDebugOptions debugOptions = QueryDebugOptions.read(json);
                    Assertions.assertEquals(debugOptions.getMaxRefreshMaterializedViewRetryNum(), 1);
                    Assertions.assertEquals(debugOptions.isEnableNormalizePredicateAfterMVRewrite(), false);
                } catch (Exception e) {
                    Assertions.fail();
                    ;
                }
            }
        }
    }

    @Test
    public void testCBOMaterializedViewRewriteLimit() {
        // good
        {
            List<Pair<String, String>> goodCases = ImmutableList.of(
                    Pair.create(SessionVariable.CBO_MATERIALIZED_VIEW_REWRITE_CANDIDATE_LIMIT, "1"),
                    Pair.create(SessionVariable.CBO_MATERIALIZED_VIEW_REWRITE_RELATED_MVS_LIMIT, "1"),
                    Pair.create(SessionVariable.CBO_MATERIALIZED_VIEW_REWRITE_RULE_OUTPUT_LIMIT, "1")
            );
            for (Pair<String, String> goodCase : goodCases) {
                try {
                    SystemVariable setVar = new SystemVariable(SetType.SESSION, goodCase.first,
                            new StringLiteral(goodCase.second));
                    SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setVar)), ctx);
                } catch (Exception e) {
                    Assertions.fail();
                    ;
                }
            }
        }
    }

    // bad
    {
        List<Pair<String, String>> goodCases = ImmutableList.of(
                Pair.create(SessionVariable.CBO_MATERIALIZED_VIEW_REWRITE_CANDIDATE_LIMIT, "-1"),
                Pair.create(SessionVariable.CBO_MATERIALIZED_VIEW_REWRITE_CANDIDATE_LIMIT, "0"),
                Pair.create(SessionVariable.CBO_MATERIALIZED_VIEW_REWRITE_CANDIDATE_LIMIT, "abc"),
                Pair.create(SessionVariable.CBO_MATERIALIZED_VIEW_REWRITE_RELATED_MVS_LIMIT, "-1"),
                Pair.create(SessionVariable.CBO_MATERIALIZED_VIEW_REWRITE_RELATED_MVS_LIMIT, "0"),
                Pair.create(SessionVariable.CBO_MATERIALIZED_VIEW_REWRITE_RELATED_MVS_LIMIT, "abc"),
                Pair.create(SessionVariable.CBO_MATERIALIZED_VIEW_REWRITE_RULE_OUTPUT_LIMIT, "-1"),
                Pair.create(SessionVariable.CBO_MATERIALIZED_VIEW_REWRITE_RULE_OUTPUT_LIMIT, "0"),
                Pair.create(SessionVariable.CBO_MATERIALIZED_VIEW_REWRITE_RULE_OUTPUT_LIMIT, "abc")
        );
        for (Pair<String, String> goodCase : goodCases) {
            try {
                SystemVariable setVar = new SystemVariable(SetType.SESSION, goodCase.first,
                        new StringLiteral(goodCase.second));
                SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setVar)), ctx);
                Assertions.fail();
                ;
            } catch (Exception e) {
                Assertions.assertTrue(e instanceof SemanticException);
            }
        }
    }

    @Test
    public void testSetCatalog() {
        // good
        try {
            SystemVariable setVar = new SystemVariable(SetType.SESSION, "catalog",
                    new StringLiteral("default_catalog"));
            SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setVar)), ctx);
        } catch (Exception e) {
            Assertions.fail();
        }

        // bad
        try {
            SystemVariable setVar = new SystemVariable(SetType.SESSION, "catalog",
                    new StringLiteral("non_existent_catalog"));
            SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setVar)), ctx);
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertEquals("Getting analyzing error. Detail message: Unknown catalog non_existent_catalog.",
                    e.getMessage());
            ;
        }
    }

    @Test
    public void testCboDisabledRules() {
        {
            SystemVariable setVar = new SystemVariable(SetType.SESSION, SessionVariable.CBO_DISABLED_RULES,
                    new StringLiteral("TF_JOIN_COMMUTATIVITY"));
            SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setVar)), ctx);
            Assertions.assertEquals("TF_JOIN_COMMUTATIVITY", setVar.getResolvedExpression().getStringValue());
        }

        {
            SystemVariable setVar = new SystemVariable(SetType.SESSION, SessionVariable.CBO_DISABLED_RULES,
                    new StringLiteral("GP_PRUNE_COLUMNS"));
            SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setVar)), ctx);
            Assertions.assertEquals("GP_PRUNE_COLUMNS", setVar.getResolvedExpression().getStringValue());
        }

        {
            SystemVariable setVar = new SystemVariable(SetType.SESSION, SessionVariable.CBO_DISABLED_RULES,
                    new StringLiteral("TF_JOIN_COMMUTATIVITY,GP_PRUNE_COLUMNS,TF_PARTITION_PRUNE"));
            SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setVar)), ctx);
            Assertions.assertEquals("TF_JOIN_COMMUTATIVITY,GP_PRUNE_COLUMNS,TF_PARTITION_PRUNE",
                    setVar.getResolvedExpression().getStringValue());
        }

        {
            SystemVariable setVar = new SystemVariable(SetType.SESSION, SessionVariable.CBO_DISABLED_RULES,
                    new StringLiteral(""));
            SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setVar)), ctx);
            Assertions.assertEquals("", setVar.getResolvedExpression().getStringValue());
        }

        {
            SystemVariable setVar = new SystemVariable(SetType.SESSION, SessionVariable.CBO_DISABLED_RULES,
                    new StringLiteral(" TF_JOIN_COMMUTATIVITY , GP_PRUNE_COLUMNS "));
            SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setVar)), ctx);
            Assertions.assertEquals(" TF_JOIN_COMMUTATIVITY , GP_PRUNE_COLUMNS ",
                    setVar.getResolvedExpression().getStringValue());
        }

        {
            SystemVariable setVar = new SystemVariable(SetType.SESSION, SessionVariable.CBO_DISABLED_RULES,
                    new StringLiteral("INVALID_RULE_NAME"));
            try {
                SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setVar)), ctx);
                Assertions.fail("should fail for unknown rule name");
            } catch (SemanticException e) {
                assertThat(e.getMessage(), containsString("Unknown rule name(s): INVALID_RULE_NAME"));
            }
        }

        {
            SystemVariable setVar = new SystemVariable(SetType.SESSION, SessionVariable.CBO_DISABLED_RULES,
                    new StringLiteral("IMP_OLAP_LSCAN_TO_PSCAN"));
            try {
                SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setVar)), ctx);
                Assertions.fail("should fail for non-TF/GP rule");
            } catch (SemanticException e) {
                assertThat(e.getMessage(), containsString("Only TF_ (Transformation) and GP_ (Group combination)" +
                        " rules can be disabled"));
                assertThat(e.getMessage(), containsString("IMP_OLAP_LSCAN_TO_PSCAN"));
            }
        }

        {
            SystemVariable setVar = new SystemVariable(SetType.SESSION, SessionVariable.CBO_DISABLED_RULES,
                    new StringLiteral("TRANSFORMATION_RULES"));
            try {
                SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setVar)), ctx);
                Assertions.fail("should fail for category marker");
            } catch (SemanticException e) {
                assertThat(e.getMessage(), containsString("Only TF_ (Transformation) and GP_ (Group combination)" +
                        " rules can be disabled"));
            }
        }

        {
            SystemVariable setVar = new SystemVariable(SetType.SESSION, SessionVariable.CBO_DISABLED_RULES,
                    new StringLiteral("TF_JOIN_COMMUTATIVITY,INVALID_RULE,GP_PRUNE_COLUMNS"));
            try {
                SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setVar)), ctx);
                Assertions.fail("should fail when any rule is invalid");
            } catch (SemanticException e) {
                assertThat(e.getMessage(), containsString("Unknown rule name(s): INVALID_RULE"));
            }
        }

        {
            SystemVariable setVar = new SystemVariable(SetType.SESSION, SessionVariable.CBO_DISABLED_RULES,
                    new StringLiteral("INVALID1,INVALID2,INVALID3"));
            try {
                SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setVar)), ctx);
                Assertions.fail("should fail for multiple invalid rules");
            } catch (SemanticException e) {
                assertThat(e.getMessage(), containsString("Unknown rule name(s)"));
                assertThat(e.getMessage(), containsString("INVALID1"));
                assertThat(e.getMessage(), containsString("INVALID2"));
                assertThat(e.getMessage(), containsString("INVALID3"));
            }
        }
    }

    
}
