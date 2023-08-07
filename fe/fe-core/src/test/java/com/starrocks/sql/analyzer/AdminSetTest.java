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

package com.starrocks.sql.analyzer;

import com.starrocks.sql.ast.AdminSetReplicaStatusStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AdminSetTest {
    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @Test
    public void testAdminSetConfig() {
        analyzeSuccess("admin set frontend config(\"alter_table_timeout_second\" = \"60\");");
        analyzeFail("admin set frontend config;", "Getting syntax error at line 1, column 25. " +
                "Detail message: Unexpected input ';', the most similar input is {'('}.");
    }

    @Test
    public void testAdminSetReplicaStatus() {
        AdminSetReplicaStatusStmt stmt = (AdminSetReplicaStatusStmt) analyzeSuccess(
                "admin set replica status properties(\"tablet_id\" = \"10003\",\"backend_id\" = \"10001\",\"status\" = \"ok\");");
        Assert.assertEquals(10003, stmt.getTabletId());
        Assert.assertEquals(10001, stmt.getBackendId());
        Assert.assertEquals("OK", stmt.getStatus().name());

        analyzeFail("admin set replica status properties(\"backend_id\" = \"10001\",\"status\" = \"ok\");",
                "Getting analyzing error from line 1, column 59 to line 1, column 70. Detail message: " +
                        "Missing necessary properties 'TABLET_ID, BACKEND_ID and STATUS'.");
        analyzeFail("admin set replica status properties(\"tablet_id\" = \"10003\",\"status\" = \"ok\");",
                "Getting analyzing error from line 1, column 58 to line 1, column 69. Detail message: " +
                        "Missing necessary properties 'TABLET_ID, BACKEND_ID and STATUS'.");
        analyzeFail("admin set replica status properties(\"tablet_id\" = \"10003\",\"backend_id\" = \"10001\");",
                "Getting analyzing error from line 1, column 58 to line 1, column 73. Detail message: " +
                        "Missing necessary properties 'TABLET_ID, BACKEND_ID and STATUS'.");
        analyzeFail("admin set replica status " +
                        "properties(\"tablet_id\" = \"10003\",\"backend_id\" = \"10001\",\"status\" = \"MISSING\");",
                "Getting analyzing error from line 1, column 81 to line 1, column 92. Detail message: " +
                        "Invalid replica status value 'MISSING'.");
        analyzeFail("Getting analyzing error from line 1, column 36 to line 1, column 55. Detail message: " +
                "Unsupported properties 'unknown_config'.");
    }
}
