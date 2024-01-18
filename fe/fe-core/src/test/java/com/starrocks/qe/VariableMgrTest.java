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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/qe/VariableMgrTest.java

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

package com.starrocks.qe;

import com.google.common.collect.Lists;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.VariableExpr;
import com.starrocks.common.exception.AnalysisException;
import com.starrocks.common.exception.DdlException;
import com.starrocks.common.exception.UserException;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.persist.EditLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.analyzer.SetStmtAnalyzer;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.SetType;
import com.starrocks.sql.ast.SystemVariable;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class VariableMgrTest {
    private static final Logger LOG = LoggerFactory.getLogger(VariableMgrTest.class);
    @Mocked
    private GlobalStateMgr globalStateMgr;
    @Mocked
    private EditLog editLog;
    @Mocked
    private Auth auth;

    @Before
    public void setUp() {
        new Expectations() {
            {
                globalStateMgr.getEditLog();
                minTimes = 0;
                result = editLog;

                editLog.logGlobalVariable((SessionVariable) any);
                minTimes = 0;

                globalStateMgr.getAuth();
                minTimes = 0;
                result = auth;

                auth.checkGlobalPriv((ConnectContext) any, PrivPredicate.ADMIN);
                minTimes = 0;
                result = true;
            }
        };

        new Expectations(globalStateMgr) {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;
            }
        };
    }

    @Test
    public void testNormal() throws IllegalAccessException, NoSuchFieldException, UserException {
        GlobalStateMgr.getCurrentState().initAuth(false);
        SessionVariable var = VariableMgr.newSessionVariable();
        Assert.assertEquals(2147483648L, var.getMaxExecMemByte());
        Assert.assertEquals(300, var.getQueryTimeoutS());
        Assert.assertEquals(false, var.isEnableProfile());
        Assert.assertEquals(32L, var.getSqlMode());
        Assert.assertEquals(true, var.isInnodbReadOnly());

        List<List<String>> rows = VariableMgr.dump(SetType.SESSION, var, null);
        Assert.assertTrue(rows.size() > 5);
        for (List<String> row : rows) {
            if (row.get(0).equalsIgnoreCase("exec_mem_limit")) {
                Assert.assertEquals("2147483648", row.get(1));
            } else if (row.get(0).equalsIgnoreCase("enable_profile")) {
                Assert.assertEquals("false", row.get(1));
            } else if (row.get(0).equalsIgnoreCase("query_timeout")) {
                Assert.assertEquals("300", row.get(1));
            } else if (row.get(0).equalsIgnoreCase("sql_mode")) {
                Assert.assertEquals("ONLY_FULL_GROUP_BY", row.get(1));
            }
        }

        // Set global variable
        SystemVariable setVar = new SystemVariable(SetType.GLOBAL, "exec_mem_limit", new IntLiteral(12999934L));
        SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setVar)), null);
        VariableMgr.setSystemVariable(var, setVar, false);
        Assert.assertEquals(12999934L, var.getMaxExecMemByte());
        var = VariableMgr.newSessionVariable();
        Assert.assertEquals(12999934L, var.getMaxExecMemByte());

        SystemVariable setVar2 = new SystemVariable(SetType.GLOBAL, "parallel_fragment_exec_instance_num", new IntLiteral(5L));
        SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setVar2)), null);
        VariableMgr.setSystemVariable(var, setVar2, false);
        Assert.assertEquals(5L, var.getParallelExecInstanceNum());
        var = VariableMgr.newSessionVariable();
        Assert.assertEquals(5L, var.getParallelExecInstanceNum());

        SystemVariable setVar3 = new SystemVariable(SetType.GLOBAL, "time_zone", new StringLiteral("Asia/Shanghai"));
        SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setVar3)), null);
        VariableMgr.setSystemVariable(var, setVar3, false);
        Assert.assertEquals("Asia/Shanghai", var.getTimeZone());
        var = VariableMgr.newSessionVariable();
        Assert.assertEquals("Asia/Shanghai", var.getTimeZone());

        setVar3 = new SystemVariable(SetType.GLOBAL, "time_zone", new StringLiteral("CST"));
        SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setVar3)), null);
        VariableMgr.setSystemVariable(var, setVar3, false);
        Assert.assertEquals("CST", var.getTimeZone());
        var = VariableMgr.newSessionVariable();
        Assert.assertEquals("CST", var.getTimeZone());

        // Set session variable
        setVar = new SystemVariable(SetType.GLOBAL, "exec_mem_limit", new IntLiteral(12999934L));
        SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setVar)), null);
        VariableMgr.setSystemVariable(var, setVar, false);
        Assert.assertEquals(12999934L, var.getMaxExecMemByte());

        // onlySessionVar
        setVar = new SystemVariable(SetType.GLOBAL, "exec_mem_limit", new IntLiteral(12999935L));
        SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setVar)), null);
        VariableMgr.setSystemVariable(var, setVar, true);
        Assert.assertEquals(12999935L, var.getMaxExecMemByte());

        setVar3 = new SystemVariable(SetType.SESSION, "time_zone", new StringLiteral("Asia/Jakarta"));
        SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setVar3)), null);
        VariableMgr.setSystemVariable(var, setVar3, false);
        Assert.assertEquals("Asia/Jakarta", var.getTimeZone());

        // exec_mem_limit in expr style
        setVar = new SystemVariable(SetType.GLOBAL, "exec_mem_limit", new StringLiteral("20G"));
        SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setVar)), null);
        VariableMgr.setSystemVariable(var, setVar, true);
        Assert.assertEquals(21474836480L, var.getMaxExecMemByte());
        setVar = new SystemVariable(SetType.GLOBAL, "exec_mem_limit", new StringLiteral("20m"));
        SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setVar)), null);
        VariableMgr.setSystemVariable(var, setVar, true);
        Assert.assertEquals(20971520L, var.getMaxExecMemByte());

        // Get from name
        VariableExpr desc = new VariableExpr("exec_mem_limit");
        Assert.assertEquals(var.getMaxExecMemByte() + "", VariableMgr.getValue(var, desc));

        SystemVariable setVar4 = new SystemVariable(SetType.SESSION, "sql_mode", new StringLiteral(
                SqlModeHelper.encode("PIPES_AS_CONCAT").toString()));
        SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setVar4)), null);
        VariableMgr.setSystemVariable(var, setVar4, false);
        Assert.assertEquals(2L, var.getSqlMode());

        // Test checkTimeZoneValidAndStandardize
        SystemVariable setVar5 = new SystemVariable(SetType.GLOBAL, "time_zone", new StringLiteral("+8:00"));
        SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setVar5)), null);
        VariableMgr.setSystemVariable(var, setVar5, false);
        Assert.assertEquals("+08:00", VariableMgr.newSessionVariable().getTimeZone());

        SystemVariable setVar6 = new SystemVariable(SetType.GLOBAL, "time_zone", new StringLiteral("8:00"));
        SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setVar6)), null);
        VariableMgr.setSystemVariable(var, setVar6, false);
        Assert.assertEquals("+08:00", VariableMgr.newSessionVariable().getTimeZone());

        SystemVariable setVar7 = new SystemVariable(SetType.GLOBAL, "time_zone", new StringLiteral("-8:00"));
        SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setVar7)), null);
        VariableMgr.setSystemVariable(var, setVar7, false);
        Assert.assertEquals("-08:00", VariableMgr.newSessionVariable().getTimeZone());
    }

    @Test(expected = SemanticException.class)
    public void testInvalidType() {
        // Set global variable
        SystemVariable setVar = new SystemVariable(SetType.SESSION, "exec_mem_limit", new StringLiteral("abc"));
        try {
            SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setVar)), null);
        } catch (Exception e) {
            throw e;
        }
        Assert.fail("No exception throws.");
    }

    @Test(expected = SemanticException.class)
    public void testInvalidTimeZoneRegion() {
        // Set global variable
        // utc should be upper case (UTC)
        SystemVariable setVar = new SystemVariable(SetType.SESSION, "time_zone", new StringLiteral("utc"));
        try {
            SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setVar)), null);
        } catch (Exception e) {
            throw e;
        }
        Assert.fail("No exception throws.");
    }

    @Test(expected = SemanticException.class)
    public void testInvalidTimeZoneOffset() {
        // Set global variable
        SystemVariable setVar = new SystemVariable(SetType.SESSION, "time_zone", new StringLiteral("+15:00"));
        try {
            SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setVar)), null);
        } catch (Exception e) {
            throw e;
        }
        Assert.fail("No exception throws.");
    }

    @Test
    public void testInvalidExecMemLimit() {
        // Set global variable
        String[] values = {"2097151", "1k"};
        for (String value : values) {
            SystemVariable setVar = new SystemVariable(SetType.SESSION, "exec_mem_limit", new StringLiteral(value));
            try {
                SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setVar)), null);
                Assert.fail("No exception throws.");
            } catch (Exception e) {
                Assert.assertEquals("Getting analyzing error. Detail message: exec_mem_limit must be equal " +
                        "or greater than 2097152.", e.getMessage());
            }
        }
    }

    @Test(expected = DdlException.class)
    public void testReadOnly() throws AnalysisException, DdlException {
        VariableExpr desc = new VariableExpr("version_comment");
        LOG.info(VariableMgr.getValue(null, desc));

        // Set global variable
        SystemVariable setVar = new SystemVariable(SetType.SESSION, "version_comment", null);
        VariableMgr.setSystemVariable(null, setVar, false);
        Assert.fail("No exception throws.");
    }

    @Test
    public void testDumpInvisible() {
        SessionVariable sv = new SessionVariable();
        List<List<String>> vars = VariableMgr.dump(SetType.SESSION, sv, null);
        Assert.assertFalse(vars.toString().contains("enable_show_all_variables"));
        Assert.assertFalse(vars.toString().contains("cbo_use_correlated_join_estimate"));

        sv.setEnableShowAllVariables(true);
        vars = VariableMgr.dump(SetType.SESSION, sv, null);
        Assert.assertTrue(vars.toString().contains("cbo_use_correlated_join_estimate"));

        vars = VariableMgr.dump(SetType.SESSION, null, null);
        List<List<String>> vars1 = VariableMgr.dump(SetType.GLOBAL, null, null);
        Assert.assertTrue(vars.size() < vars1.size());

        List<List<String>> vars2 = VariableMgr.dump(SetType.SESSION, null, null);
        Assert.assertTrue(vars.size() == vars2.size());
    }
}

