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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/mysql/privilege/SetPasswordTest.java

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

package com.starrocks.mysql.privilege;

import com.starrocks.analysis.Analyzer;
import com.starrocks.common.AnalysisException;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.persist.EditLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.parseSql;

public class SetPasswordTest {

    @Mocked
    private Analyzer analyzer;
    @Mocked
    private EditLog editLog;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @Before
    public void setUp() throws NoSuchMethodException, SecurityException, AnalysisException {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public EditLog getEditLog() {
                return editLog;
            }
        };

        new Expectations() {
            {
                MysqlPassword.checkPassword(anyString);
                minTimes = 0;
                result = new byte[10];
            }
        };
    }

    @Test
    public void testAuditSetPasswordWithoutUser() {
        String sql = "SET PASSWORD = PASSWORD('testPass'), pipeline_dop = 2";
        SetStmt setStmt = (SetStmt) parseSql(sql);
        String setSql = AstToStringBuilder.toString(setStmt);
        Assert.assertFalse(setSql.contains("PASSWORD FOR"));
        Assert.assertTrue(setSql.contains("PASSWORD('***')"));
        Assert.assertTrue(setSql.contains("`pipeline_dop` = 2"));
        System.out.println(setSql);
    }

    @Test
    public void testAuditSetPassword() {
        String sql = "SET PASSWORD FOR admin = PASSWORD('testPass'), pipeline_dop = 2";
        SetStmt setStmt = (SetStmt) parseSql(sql);
        String setSql = AstToStringBuilder.toString(setStmt);
        Assert.assertTrue(setSql.contains("PASSWORD FOR"));
        Assert.assertTrue(setSql.contains("PASSWORD('***')"));
        Assert.assertTrue(setSql.contains("`pipeline_dop` = 2"));
        System.out.println(setSql);
    }

}
