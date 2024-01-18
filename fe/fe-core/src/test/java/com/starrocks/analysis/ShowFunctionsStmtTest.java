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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/ShowFunctionsStmtTest.java

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

import com.starrocks.common.exception.AnalysisException;
import com.starrocks.sql.analyzer.DDLTestBase;
import com.starrocks.sql.ast.ShowFunctionsStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ShowFunctionsStmtTest extends DDLTestBase {

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void testUnsupportFilter() throws Exception {
        ctx = UtFrameUtils.createDefaultCtx();
        expectedEx.expect(AnalysisException.class);
        expectedEx.expectMessage("Only support like 'function_pattern' syntax.");

        String showSQL = "SHOW FULL BUILTIN FUNCTIONS FROM `testDb` where a = 1";
        ShowFunctionsStmt stmt = (ShowFunctionsStmt) UtFrameUtils.parseStmtWithNewParser(showSQL, ctx);
    }
}
