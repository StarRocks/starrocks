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

import com.starrocks.analysis.BrokerDesc;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.LoadException;
import com.starrocks.common.UserException;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.fs.HdfsUtil;
import com.starrocks.load.ExportJob;
import com.starrocks.load.ExportMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AuthorizerStmtVisitor;
import com.starrocks.sql.ast.ExportStmt;
import com.starrocks.thrift.THdfsProperties;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ExportHandleTest {
    private static ConnectContext connectContext;

    private static StarRocksAssert starRocksAssert;
    private ShowResultSet exportResultSet;

    @BeforeClass
    public static void beforeClass() throws Exception {
        Config.export_checker_interval_second = 1;
        Config.export_task_default_timeout_second = 2;
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.setUpForPersistTest();

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        connectContext.setQueryId(UUIDUtil.genUUID());
        starRocksAssert = new StarRocksAssert(connectContext);

        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test.export_tbl(k1 int, k2 int, k3 int) " +
                        "distributed by hash(k1) buckets 3 properties('replication_num' = '1');");
    }

    @Test
    public void testExportHandler() throws Exception {
        String exportStmtStr = "EXPORT TABLE export_tbl TO \"hdfs://hdfs_host:port/a/b/c/\" " +
                "WITH SYNC MODE " +
                "WITH BROKER (\"username\"=\"test\", \"password\"=\"test\");";
        ExportStmt exportStmt = (ExportStmt) UtFrameUtils.parseStmtWithNewParser(exportStmtStr, connectContext);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, exportStmt);
        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isLeader() {
                return true;
            }
        };
        new MockUp<AuthorizerStmtVisitor>() {
            @Mock
            public Void visitExportStatement(ExportStmt statement, ConnectContext context) {
                return null;
            }
        };
        new MockUp<StmtExecutor>() {
            @Mock
            public void sendShowResult(ShowResultSet resultSet) {
                exportResultSet = resultSet;
            }
        };
        new MockUp<HdfsUtil>() {
            @Mock
            public void getTProperties(String path, BrokerDesc brokerDesc, THdfsProperties tProperties) throws UserException {
            }
        };
        // let ExportChecker never run this job
        new MockUp<ExportMgr>() {
            @Mock
            public List<ExportJob> getExportJobs(ExportJob.JobState state) {
                return new ArrayList<>();
            }
        };
        try {
            stmtExecutor.execute();
        } catch (LoadException e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertNotNull(exportResultSet);
        Assert.assertEquals(1, exportResultSet.getResultRows().size());
        List<String> row = exportResultSet.getResultRows().get(0);
        Assert.assertEquals("PENDING", row.get(2));
    }
}
