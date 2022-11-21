// This file is made available under Elastic License 2.0.
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

import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.SetType;
import com.starrocks.analysis.SetVar;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.SysVariableDesc;
import com.starrocks.catalog.Catalog;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.UserException;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.persist.EditLog;
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
    private Catalog catalog;
    @Mocked
    private EditLog editLog;
    @Mocked
    private Auth auth;

    @Before
    public void setUp() {
        new Expectations() {
            {
                catalog.getEditLog();
                minTimes = 0;
                result = editLog;

                editLog.logGlobalVariable((SessionVariable) any);
                minTimes = 0;

                catalog.getAuth();
                minTimes = 0;
                result = auth;

                auth.checkGlobalPriv((ConnectContext) any, PrivPredicate.ADMIN);
                minTimes = 0;
                result = true;
            }
        };

        new Expectations(catalog) {
            {
                Catalog.getCurrentCatalog();
                minTimes = 0;
                result = catalog;
            }
        };
    }

    @Test
    public void testNormal() throws IllegalAccessException, NoSuchFieldException, UserException {
        SessionVariable var = VariableMgr.newSessionVariable();
        Assert.assertEquals(2147483648L, var.getMaxExecMemByte());
        Assert.assertEquals(300, var.getQueryTimeoutS());
        Assert.assertEquals(false, var.isReportSucc());
        Assert.assertEquals(0L, var.getSqlMode());

        List<List<String>> rows = VariableMgr.dump(SetType.SESSION, var, null);
        Assert.assertTrue(rows.size() > 5);
        for (List<String> row : rows) {
            if (row.get(0).equalsIgnoreCase("exec_mem_limit")) {
                Assert.assertEquals("2147483648", row.get(1));
            } else if (row.get(0).equalsIgnoreCase("is_report_success")) {
                Assert.assertEquals("false", row.get(1));
            } else if (row.get(0).equalsIgnoreCase("query_timeout")) {
                Assert.assertEquals("300", row.get(1));
            } else if (row.get(0).equalsIgnoreCase("sql_mode")) {
                Assert.assertEquals("", row.get(1));
            }
        }

        // Set global variable
        SetVar setVar = new SetVar(SetType.GLOBAL, "exec_mem_limit", new IntLiteral(12999934L));
        setVar.analyze(null);
        VariableMgr.setVar(var, setVar, false);
        Assert.assertEquals(12999934L, var.getMaxExecMemByte());
        var = VariableMgr.newSessionVariable();
        Assert.assertEquals(12999934L, var.getMaxExecMemByte());

        SetVar setVar2 = new SetVar(SetType.GLOBAL, "parallel_fragment_exec_instance_num", new IntLiteral(5L));
        setVar2.analyze(null);
        VariableMgr.setVar(var, setVar2, false);
        Assert.assertEquals(5L, var.getParallelExecInstanceNum());
        var = VariableMgr.newSessionVariable();
        Assert.assertEquals(5L, var.getParallelExecInstanceNum());

        SetVar setVar3 = new SetVar(SetType.GLOBAL, "time_zone", new StringLiteral("Asia/Shanghai"));
        setVar3.analyze(null);
        VariableMgr.setVar(var, setVar3, false);
        Assert.assertEquals("Asia/Shanghai", var.getTimeZone());
        var = VariableMgr.newSessionVariable();
        Assert.assertEquals("Asia/Shanghai", var.getTimeZone());

        setVar3 = new SetVar(SetType.GLOBAL, "time_zone", new StringLiteral("CST"));
        setVar3.analyze(null);
        VariableMgr.setVar(var, setVar3, false);
        Assert.assertEquals("CST", var.getTimeZone());
        var = VariableMgr.newSessionVariable();
        Assert.assertEquals("CST", var.getTimeZone());

        // Set session variable
        setVar = new SetVar(SetType.GLOBAL, "exec_mem_limit", new IntLiteral(12999934L));
        setVar.analyze(null);
        VariableMgr.setVar(var, setVar, false);
        Assert.assertEquals(12999934L, var.getMaxExecMemByte());

        // onlySessionVar
        setVar = new SetVar(SetType.GLOBAL, "exec_mem_limit", new IntLiteral(12999935L));
        setVar.analyze(null);
        VariableMgr.setVar(var, setVar, true);
        Assert.assertEquals(12999935L, var.getMaxExecMemByte());
        var = VariableMgr.newSessionVariable();
        Assert.assertEquals(12999934L, var.getMaxExecMemByte());

        setVar3 = new SetVar(SetType.SESSION, "time_zone", new StringLiteral("Asia/Jakarta"));
        setVar3.analyze(null);
        VariableMgr.setVar(var, setVar3, false);
        Assert.assertEquals("Asia/Jakarta", var.getTimeZone());

        // Get from name
        SysVariableDesc desc = new SysVariableDesc("exec_mem_limit");
        Assert.assertEquals(var.getMaxExecMemByte() + "", VariableMgr.getValue(var, desc));

        SetVar setVar4 = new SetVar(SetType.SESSION, "sql_mode", new StringLiteral(
                SqlModeHelper.encode("PIPES_AS_CONCAT").toString()));
        setVar4.analyze(null);
        VariableMgr.setVar(var, setVar4, false);
        Assert.assertEquals(2L, var.getSqlMode());

        // Test checkTimeZoneValidAndStandardize
        SetVar setVar5 = new SetVar(SetType.GLOBAL, "time_zone", new StringLiteral("+8:00"));
        setVar5.analyze(null);
        VariableMgr.setVar(var, setVar5, false);
        Assert.assertEquals("+08:00", VariableMgr.newSessionVariable().getTimeZone());

        SetVar setVar6 = new SetVar(SetType.GLOBAL, "time_zone", new StringLiteral("8:00"));
        setVar6.analyze(null);
        VariableMgr.setVar(var, setVar6, false);
        Assert.assertEquals("+08:00", VariableMgr.newSessionVariable().getTimeZone());

        SetVar setVar7 = new SetVar(SetType.GLOBAL, "time_zone", new StringLiteral("-8:00"));
        setVar7.analyze(null);
        VariableMgr.setVar(var, setVar7, false);
        Assert.assertEquals("-08:00", VariableMgr.newSessionVariable().getTimeZone());
    }

    @Test(expected = UserException.class)
    public void testInvalidType() throws UserException {
        // Set global variable
        SetVar setVar = new SetVar(SetType.SESSION, "exec_mem_limit", new StringLiteral("abc"));
        try {
            setVar.analyze(null);
        } catch (Exception e) {
            throw e;
        }
        Assert.fail("No exception throws.");
    }

    @Test(expected = UserException.class)
    public void testInvalidTimeZoneRegion() throws UserException {
        // Set global variable
        // utc should be upper case (UTC)
        SetVar setVar = new SetVar(SetType.SESSION, "time_zone", new StringLiteral("utc"));
        try {
            setVar.analyze(null);
        } catch (Exception e) {
            throw e;
        }
        Assert.fail("No exception throws.");
    }

    @Test(expected = UserException.class)
    public void testInvalidTimeZoneOffset() throws UserException {
        // Set global variable
        SetVar setVar = new SetVar(SetType.SESSION, "time_zone", new StringLiteral("+15:00"));
        try {
            setVar.analyze(null);
        } catch (Exception e) {
            throw e;
        }
        Assert.fail("No exception throws.");
    }

    @Test(expected = DdlException.class)
    public void testReadOnly() throws AnalysisException, DdlException {
        SysVariableDesc desc = new SysVariableDesc("version_comment");
        LOG.info(VariableMgr.getValue(null, desc));

        // Set global variable
        SetVar setVar = new SetVar(SetType.SESSION, "version_comment", null);
        VariableMgr.setVar(null, setVar, false);
        Assert.fail("No exception throws.");
    }
}

