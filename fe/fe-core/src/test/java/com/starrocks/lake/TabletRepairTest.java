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

package com.starrocks.lake;

import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.FeConstants;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TabletRepairTest {
    private static final String DB_NAME = "test_repair";
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void beforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase(DB_NAME).useDatabase(DB_NAME);
    }

    @Test
    public void testRepairTableFail() throws Exception {
        starRocksAssert.withTable("CREATE TABLE t1(k1 DATE, k2 INT) PARTITION BY RANGE(k1) " +
                        "(PARTITION p202510 VALUES LESS THAN ('2025-11-01'), " +
                        " PARTITION p202511 VALUES LESS THAN ('2025-12-01')) " +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 2");
        try {
            String repairSql = "ADMIN REPAIR TABLE t1 " +
                    "PROPERTIES('enforce_consistent_version' = 'true', 'allow_empty_tablet_recovery' = 'false')";
            connectContext.setQueryId(UUIDUtil.genUUID());
            StatementBase stmt2 = SqlParser.parseSingleStatement(repairSql, connectContext.getSessionVariable().getSqlMode());
            // get_tablet_metadatas returns null response
            ExceptionChecker.expectThrowsWithMsg(StarRocksException.class, "Fail to repair tablet metadata for 2 partitions",
                    () -> DDLStmtExecutor.execute(stmt2, connectContext));
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }
}
