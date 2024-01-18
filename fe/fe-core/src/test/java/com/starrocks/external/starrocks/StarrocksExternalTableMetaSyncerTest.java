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


package com.starrocks.external.starrocks;

import com.starrocks.catalog.ExternalOlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.MetaContext;
import com.starrocks.leader.LeaderImpl;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.thrift.TGetTableMetaRequest;
import com.starrocks.thrift.TGetTableMetaResponse;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class StarrocksExternalTableMetaSyncerTest {
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        starRocksAssert = new StarRocksAssert(AnalyzeTestUtil.getConnectContext());
        starRocksAssert.withDatabase("test_db").useDatabase("test_db");
    }

    @Test
    public void syncTableMeta() throws Exception {
        DDLStmtExecutor.execute(analyzeSuccess(
                "CREATE TABLE test_table" +
                "(" +
                    "event_time DATETIME," +
                    "channel VARCHAR(32) DEFAULT ''," +
                    "user VARCHAR(128) DEFAULT ''," +
                    "is_anonymous TINYINT DEFAULT '0'," +
                    "is_minor TINYINT DEFAULT '0'," +
                    "is_new TINYINT DEFAULT '0'," +
                    "is_robot TINYINT DEFAULT '0'," +
                    "is_unpatrolled TINYINT DEFAULT '0'," +
                    "delta INT DEFAULT '0'," +
                    "added INT DEFAULT '0'," +
                    "deleted INT DEFAULT '0'" +
                ")" +
                "DUPLICATE KEY" +
                "(" +
                    "event_time," +
                    "channel," +
                    "user," +
                    "is_anonymous," +
                    "is_minor," +
                    "is_new," +
                    "is_robot," +
                    "is_unpatrolled" +
                ")" +
                "PARTITION BY RANGE(event_time)" +
                "(" +
                    "PARTITION p06 VALUES LESS THAN ('2015-09-12 06:00:00')," +
                    "PARTITION p12 VALUES LESS THAN ('2015-09-12 12:00:00')," +
                    "PARTITION p18 VALUES LESS THAN ('2015-09-12 18:00:00')," +
                    "PARTITION p24 VALUES LESS THAN ('2015-09-13 00:00:00')" +
                ")" +
                "DISTRIBUTED BY HASH(user)" +
                "properties (" +
                    "\"replication_num\" = \"1\"" +
                ")"), starRocksAssert.getCtx());


        DDLStmtExecutor.execute(analyzeSuccess(
                "CREATE EXTERNAL TABLE test_ext_table" +
                "(" +
                    "event_time DATETIME," +
                    "channel VARCHAR(32) DEFAULT ''," +
                    "user VARCHAR(128) DEFAULT ''," +
                    "is_anonymous TINYINT DEFAULT '0'," +
                    "is_minor TINYINT DEFAULT '0'," +
                    "is_new TINYINT DEFAULT '0'," +
                    "is_robot TINYINT DEFAULT '0'," +
                    "is_unpatrolled TINYINT DEFAULT '0'," +
                    "delta INT DEFAULT '0'," +
                    "added INT DEFAULT '0'," +
                    "deleted INT DEFAULT '0'" +
                ")" +
                "DUPLICATE KEY" +
                "(" +
                    "event_time," +
                    "channel," +
                    "user," +
                    "is_anonymous," +
                    "is_minor," +
                    "is_new," +
                    "is_robot," +
                    "is_unpatrolled" +
                ")" +
                "DISTRIBUTED BY HASH(user)" +
                "properties (" +
                    "\"host\" = \"127.0.0.2\"," +
                    "\"port\" = \"9020\"," +
                    "\"user\" = \"root\"," +
                    "\"password\" = \"\"," +
                    "\"database\" = \"test_db\"," +
                    "\"table\" = \"test_ext_table\"" +
                ")"), starRocksAssert.getCtx());

        TGetTableMetaRequest request = new TGetTableMetaRequest();
        request.setDb_name("test_db");
        request.setTable_name("test_table");

        LeaderImpl leader = new LeaderImpl();
        TGetTableMetaResponse response = leader.getTableMeta(request);

        Table table = GlobalStateMgr.getCurrentState().getDb("test_db").getTable("test_ext_table");
        ExternalOlapTable extTable = (ExternalOlapTable) table;
        // remove the thread local meta context
        MetaContext.remove();
        extTable.updateMeta(request.getDb_name(), response.getTable_meta(), response.getBackends());
        Assert.assertNull(MetaContext.get());
        Assert.assertEquals(4, extTable.getPartitions().size());
    }
}
