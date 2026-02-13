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

package com.starrocks.qe;

import com.google.common.collect.Lists;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.VariableExpr;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.sql.analyzer.SetStmtAnalyzer;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.SetType;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class VariableMgrRunModeTest {

    @BeforeEach
    public void setUp() {
        // Initialize test environment
        UtFrameUtils.setUpForPersistTest();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testRunModeReadOnly() {
        VariableMgr variableMgr = new VariableMgr();
        SessionVariable var = variableMgr.newSessionVariable();
        VariableExpr desc = new VariableExpr("run_mode");

        // Check default value
        String runMode = variableMgr.getValue(var, desc);
        Assertions.assertEquals(Config.run_mode, runMode);

        // Try to set run_mode (should fail as it is read-only)
        SystemVariable setVar = new SystemVariable(SetType.GLOBAL, "run_mode", new StringLiteral("shared_data"));
        try {
            SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setVar)), null);
            variableMgr.setSystemVariable(var, setVar, false);
            Assertions.fail("No exception throws.");
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof DdlException);
            Assertions.assertEquals("Variable 'run_mode' is a read only variable", e.getMessage());
        }
    }
}

