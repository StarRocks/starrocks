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

package com.starrocks.analysis;

import com.starrocks.load.ExportMgr;
import com.starrocks.persist.EditLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CancelExportStmt;
import com.starrocks.sql.parser.NodePosition;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

public class ExportHandleTest {
    @Mocked
    private GlobalStateMgr globalStateMgr;

    @Test
    public void testCancelExportHandler(@Mocked ExportMgr exportMgr, @Mocked EditLog editLog) {

        new MockUp<ConnectContext>() {
            @Mock
            GlobalStateMgr getGlobalStateMgr() {
                return globalStateMgr;
            }
        };

        new Expectations() {
            {
                globalStateMgr.getExportMgr();
                result = exportMgr;
            }
        };

        try {
            DDLStmtExecutor.execute(new CancelExportStmt("repo", null, NodePosition.ZERO),
                    new ConnectContext());
        } catch (Exception ex) {
            Assert.fail();
        }
    }
}
