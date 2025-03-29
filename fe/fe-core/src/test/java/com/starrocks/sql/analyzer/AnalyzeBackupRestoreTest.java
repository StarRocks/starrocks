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

import com.google.common.collect.Maps;
import com.starrocks.alter.AlterTest;
import com.starrocks.backup.BlobStorage;
import com.starrocks.backup.Repository;
import com.starrocks.backup.Status;
import com.starrocks.common.Pair;
import com.starrocks.server.GlobalStateMgr;
import mockit.Mock;
import mockit.MockUp;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzeBackupRestoreTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        AlterTest.beforeClass();
        AnalyzeTestUtil.init();
        new MockUp<Repository>() {
            @Mock
            public Status initRepository() {
                return Status.OK;
            }
        };
        Collection<Pair<String, Integer>> addresses = new ArrayList<>();
        Pair<String, Integer> pair = new Pair<String, Integer>("127.0.0.1", 8080);
        addresses.add(pair);
        String brokerName = "broker";
        String location = "bos://backup-cmy";
        GlobalStateMgr.getCurrentState().getBrokerMgr().addBrokers(brokerName, addresses);
        BlobStorage storage = new BlobStorage(brokerName, Maps.newHashMap());
        Repository repo = new Repository(10000, "repo", false, location, storage);
        repo.initRepository();
        GlobalStateMgr.getCurrentState().getBackupHandler().getRepoMgr().addAndInitRepoIfNotExist(repo, false);

        AnalyzeTestUtil.getStarRocksAssert().withFunction("CREATE FUNCTION Echostring(string) RETURNS string properties" +
                        "(\"symbol\" = \"Echostring\", \"type\" = \"StarrocksJar\", \"file\" = \"xxx\");");

        AnalyzeTestUtil.getStarRocksAssert().withView("CREATE VIEW v1 AS select 1;");
        AnalyzeTestUtil.getStarRocksAssert().withView("CREATE VIEW v2 AS select 2;");

        String sqlCatalog = "CREATE EXTERNAL CATALOG catalog1 PROPERTIES(\"type\"=\"hive\", \"hive.metastore.uris\"=\"thrift://127.0.0.1:9083\")";
        AnalyzeTestUtil.getStarRocksAssert().withCatalog(sqlCatalog);
    }

    @Test
    public void testBackup() {
        analyzeSuccess("BACKUP SNAPSHOT test.snapshot_label2 TO `repo` ON ( t0, t1 ) " +
                "PROPERTIES (\"type\" = \"full\",\"timeout\" = \"3600\");");
        analyzeSuccess("BACKUP SNAPSHOT snapshot_label2 TO `repo` ON ( t0, t1 ) " +
                "PROPERTIES (\"type\" = \"full\",\"timeout\" = \"3600\");");
        analyzeSuccess("BACKUP SNAPSHOT snapshot_label2 TO `repo` " +
                "PROPERTIES (\"type\" = \"full\",\"timeout\" = \"3600\");");
        analyzeSuccess("BACKUP SNAPSHOT snapshot_pk_label TO `repo` ON ( tprimary ) " +
                "PROPERTIES (\"type\" = \"full\",\"timeout\" = \"3600\");");
        analyzeSuccess("BACKUP SNAPSHOT test.snapshot_label2 TO `repo` ON ( t3, T3 ) " +
                "PROPERTIES (\"type\" = \"full\",\"timeout\" = \"3600\");");
        analyzeSuccess("BACKUP SNAPSHOT test.snapshot_label2 TO `repo` ON ( FUNCTION Echostring ) " +
                "PROPERTIES (\"type\" = \"full\",\"timeout\" = \"3600\");");
        analyzeSuccess("BACKUP SNAPSHOT test.snapshot_label2 TO `repo` ON ( FUNCTION Echostring, ALL VIEW ) " +
                "PROPERTIES (\"type\" = \"full\",\"timeout\" = \"3600\");");
        analyzeSuccess("BACKUP SNAPSHOT test.snapshot_label2 TO `repo` ON ( FUNCTION Echostring, ALL VIEW, VIEW v1 ) " +
                "PROPERTIES (\"type\" = \"full\",\"timeout\" = \"3600\");");
        analyzeSuccess("BACKUP SNAPSHOT test.snapshot_label2 TO `repo` ON ( FUNCTION Echostring, VIEW v1, ALL VIEW ) " +
                "PROPERTIES (\"type\" = \"full\",\"timeout\" = \"3600\");");
        analyzeSuccess("BACKUP SNAPSHOT test.snapshot_label2 TO `repo`" +
                "PROPERTIES (\"type\" = \"full\",\"timeout\" = \"3600\");");
        analyzeSuccess("BACKUP SNAPSHOT test.snapshot_label2 TO `repo` ON ( ALL FUNCTIONS, " +
                "ALL TABLES, ALL VIEWS, ALL MATERIALIZED VIEWS ) PROPERTIES (\"type\" = \"full\",\"timeout\" = \"3600\");");
        analyzeSuccess("BACKUP DATABASE test SNAPSHOT snapshot_label2 TO `repo` ON ( FUNCTION Echostring, ALL VIEW ) " +
                "PROPERTIES (\"type\" = \"full\",\"timeout\" = \"3600\");");
        analyzeSuccess("BACKUP DATABASE test SNAPSHOT snapshot_label2 TO `repo` ON ( FUNCTION Echostring, ALL VIEW, VIEW v1 ) " +
                "PROPERTIES (\"type\" = \"full\",\"timeout\" = \"3600\");");
        analyzeSuccess("BACKUP DATABASE test SNAPSHOT snapshot_label2 TO `repo` ON ( FUNCTION Echostring, VIEW v1, ALL VIEW ) " +
                "PROPERTIES (\"type\" = \"full\",\"timeout\" = \"3600\");");
        analyzeSuccess("BACKUP DATABASE test SNAPSHOT snapshot_label2 TO `repo`" +
                "PROPERTIES (\"type\" = \"full\",\"timeout\" = \"3600\");");
        analyzeSuccess("BACKUP DATABASE test SNAPSHOT snapshot_label2 TO `repo` ON " +
                "( ALL FUNCTIONS, ALL TABLES, ALL VIEWS, ALL MATERIALIZED VIEWS ) " +
                "PROPERTIES (\"type\" = \"full\",\"timeout\" = \"3600\");");
        analyzeSuccess("BACKUP ALL EXTERNAL CATALOGS SNAPSHOT snapshot_label2 TO `repo` " +
                       "PROPERTIES (\"type\" = \"full\",\"timeout\" = \"3600\");");
        analyzeSuccess("BACKUP EXTERNAL CATALOG (catalog1) SNAPSHOT snapshot_label2 TO `repo` " +
                       "PROPERTIES (\"type\" = \"full\",\"timeout\" = \"3600\");");
        analyzeFail("BACKUP SNAPSHOT test.snapshot_label2 TO `repo` ON ( t0, t0 ) " +
                "PROPERTIES (\"type\" = \"full\",\"timeout\" = \"3600\");");
        analyzeFail("BACKUP SNAPSHOT test.snapshot_label2 TO `repo` ON ( t0, t1 ) " +
                "PROPERTIES (\"type\" = \"full\",\"timeout\" = \"a\");");
        analyzeFail("BACKUP SNAPSHOT test.snapshot_label2 TO `repo` ON ( t0, t1 ) " +
                "PROPERTIES (\"type\" = \"full\",\"timeout\" = \"10\");");
        analyzeFail("BACKUP SNAPSHOT test.snapshot_label2 TO `repo` ON ( t0, t1 ) " +
                "PROPERTIES (\"type\" = \"a\",\"timeout\" = \"3600\");");
        analyzeFail("BACKUP SNAPSHOT test.snapshot_label2 TO `repo` ON ( t0, t1 ) " +
                "PROPERTIES (\"a\" = \"a\",\"timeout\" = \"3600\");");
        analyzeFail("BACKUP SNAPSHOT `` TO `repo` ON ( t0, t1 );");
        analyzeFail("BACKUP SNAPSHOT `2@3`.snapshot_label2 TO `repo` ON ( t0, t1 );");
        analyzeFail("BACKUP SNAPSHOT snapshot_label2 TO `` ON( t0, t1 );");
        analyzeFail("BACKUP SNAPSHOT test.snapshot_label2 TO `repo1` ON(t0);");
        analyzeFail("BACKUP SNAPSHOT test.snapshot_label2 TO `repo` ON(t999);");
        analyzeFail("BACKUP SNAPSHOT test.snapshot_label2 TO `repo` ON(t0 PARTITION (p1));");
        analyzeFail("BACKUP SNAPSHOT test1.snapshot_label2 TO `repo` ON(t0 PARTITION (p1), t1) " +
                "PROPERTIES(\"type\"=\"1\");");
        analyzeFail("BACKUP DATABASE test SNAPSHOT test.snapshot_label2 TO `repo`" +
                "PROPERTIES (\"type\" = \"full\",\"timeout\" = \"3600\");");
        analyzeFail("BACKUP DATABASE test SNAPSHOT snapshot_label2 TO `repo` ON (`Echostring`)" +
                "PROPERTIES (\"type\" = \"full\",\"timeout\" = \"3600\");");
        analyzeFail("BACKUP SNAPSHOT test.snapshot_label2 TO `repo` ON (`Echostring`)" +
                "PROPERTIES (\"type\" = \"full\",\"timeout\" = \"3600\");");
        analyzeFail("BACKUP SNAPSHOT test.snapshot_label2 TO `repo` ON (v1 AS newV1)" +
                "PROPERTIES (\"type\" = \"full\",\"timeout\" = \"3600\");");
        analyzeFail("BACKUP ALL EXTERNAL CATALOGS DATABASE test SNAPSHOT snapshot_label2 TO `repo` " +
                    "PROPERTIES (\"type\" = \"full\",\"timeout\" = \"3600\");");
        analyzeFail("BACKUP EXTERNAL CATALOG (catalog1) DATABASE test SNAPSHOT snapshot_label2 TO `repo` " +
                    "PROPERTIES (\"type\" = \"full\",\"timeout\" = \"3600\");");
        analyzeFail("BACKUP ALL EXTERNAL CATALOGS SNAPSHOT test.snapshot_label2 TO `repo` " +
                    "PROPERTIES (\"type\" = \"full\",\"timeout\" = \"3600\");");
        analyzeFail("BACKUP EXTERNAL CATALOG (catalog1) SNAPSHOT test.snapshot_label2 TO `repo` " +
                    "PROPERTIES (\"type\" = \"full\",\"timeout\" = \"3600\");");
        analyzeFail("BACKUP ALL EXTERNAL CATALOGS SNAPSHOT snapshot_label2 TO `repo` " +
                    " ON (`t0`) PROPERTIES (\"type\" = \"full\",\"timeout\" = \"3600\");");
        analyzeFail("BACKUP EXTERNAL CATALOG (catalog1) SNAPSHOT snapshot_label2 TO `repo` " +
                    "ON (`t0`) PROPERTIES (\"type\" = \"full\",\"timeout\" = \"3600\");");
        analyzeFail("BACKUP EXTERNAL CATALOG (catalog2) SNAPSHOT snapshot_label2 TO `repo` " +
                    "PROPERTIES (\"type\" = \"full\",\"timeout\" = \"3600\");");
    }

    @Test
    public void testShowBackup() {
        analyzeSuccess("SHOW BACKUP FROM test;");
        analyzeSuccess("SHOW BACKUP;");
        analyzeFail("SHOW BACKUP FROM test1;");
        analyzeFail("SHOW BACKUP FROM 1a;");
    }

    @Test
    public void testRestore() {
        analyzeSuccess("RESTORE SNAPSHOT test.`snapshot_2` FROM `repo` ON ( `t0` , `t1` AS `new_tbl` ) " +
                "PROPERTIES ( \"backup_timestamp\"=\"2018-05-04-17-11-01\",\"allow_load\"=\"true\"," +
                "\"replication_num\"=\"1\",\"meta_version\"=\"10\"," +
                "\"starrocks_meta_version\"=\"10\",\"timeout\"=\"3600\" );");
        analyzeSuccess("RESTORE SNAPSHOT test.`snapshot_2` FROM `repo` " +
                "PROPERTIES ( \"backup_timestamp\"=\"2018-05-04-17-11-01\",\"allow_load\"=\"true\"," +
                "\"replication_num\"=\"1\",\"meta_version\"=\"10\"," +
                "\"starrocks_meta_version\"=\"10\",\"timeout\"=\"3600\" );");
        analyzeSuccess("RESTORE SNAPSHOT test.`snapshot_pk_label` FROM `repo` ON ( FUNCTION Echostring )" +
                "PROPERTIES ( \"backup_timestamp\"=\"2018-05-04-17-11-01\",\"allow_load\"=\"true\"," +
                "\"replication_num\"=\"1\",\"meta_version\"=\"10\"," +
                "\"starrocks_meta_version\"=\"10\",\"timeout\"=\"3600\" );");
        analyzeSuccess("RESTORE SNAPSHOT test.`snapshot_pk_label` FROM `repo` ON ( `tprimary` )" +
                "PROPERTIES ( \"backup_timestamp\"=\"2018-05-04-17-11-01\",\"allow_load\"=\"true\"," +
                "\"replication_num\"=\"1\",\"meta_version\"=\"10\"," +
                "\"starrocks_meta_version\"=\"10\",\"timeout\"=\"3600\" );");
        analyzeSuccess("RESTORE SNAPSHOT testNew.`snapshot_2` FROM `repo` ON ( FUNCTION Echostring, ALL FUNCTIONS ) " +
                "PROPERTIES ( \"backup_timestamp\"=\"2018-05-04-17-11-01\");");
        analyzeSuccess("RESTORE SNAPSHOT testNew.`snapshot_2` FROM `repo` ON ( ALL FUNCTIONS, FUNCTION Echotinyint ) " +
                        "PROPERTIES ( \"backup_timestamp\"=\"2018-05-04-17-11-01\");");
        analyzeSuccess("RESTORE SNAPSHOT testNew.`snapshot_2` FROM `repo` ON ( FUNCTION Echotinyint ) " +
                        "PROPERTIES ( \"backup_timestamp\"=\"2018-05-04-17-11-01\");");
        analyzeSuccess("RESTORE SNAPSHOT testNew.`snapshot_2` FROM `repo` ON " +
                        "( ALL FUNCTIONS, FUNCTION Echotinyint, ALL TABLES, v1 ) " +
                        "PROPERTIES ( \"backup_timestamp\"=\"2018-05-04-17-11-01\");");
        analyzeSuccess("RESTORE SNAPSHOT testNew.`snapshot_2` FROM `repo` ON " +
                        "( ALL FUNCTIONS, FUNCTION Echotinyint, ALL TABLES, VIEW v1 ) " +
                        "PROPERTIES ( \"backup_timestamp\"=\"2018-05-04-17-11-01\");");
        analyzeSuccess("RESTORE SNAPSHOT testNew.`snapshot_2` FROM `repo` ON " +
                        "( ALL FUNCTIONS, FUNCTION Echotinyint AS newName, ALL TABLES, VIEW v1 AS V100 ) " +
                        "PROPERTIES ( \"backup_timestamp\"=\"2018-05-04-17-11-01\");");
        analyzeSuccess("RESTORE SNAPSHOT testNew.`snapshot_2` FROM `repo` ON " +
                        "( FUNCTION Echotinyint AS newName, ALL TABLES, VIEW v1 AS V100 ) " +
                        "PROPERTIES ( \"backup_timestamp\"=\"2018-05-04-17-11-01\");");
        analyzeSuccess("RESTORE SNAPSHOT `snapshot_2` FROM `repo` DATABASE test AS testNew ON " + 
                        "( FUNCTION Echostring, ALL FUNCTIONS ) " +
                        "PROPERTIES ( \"backup_timestamp\"=\"2018-05-04-17-11-01\");");
        analyzeSuccess("RESTORE SNAPSHOT `snapshot_2` FROM `repo` DATABASE test AS testNew ON " +
                        "( ALL FUNCTIONS, FUNCTION Echotinyint ) " +
                        "PROPERTIES ( \"backup_timestamp\"=\"2018-05-04-17-11-01\");");
        analyzeSuccess("RESTORE SNAPSHOT `snapshot_2` FROM `repo` DATABASE test AS testNew ON ( FUNCTION Echotinyint ) " +
                        "PROPERTIES ( \"backup_timestamp\"=\"2018-05-04-17-11-01\");");
        analyzeSuccess("RESTORE SNAPSHOT `snapshot_2` FROM `repo` DATABASE test AS testNew ON " +
                        "( ALL FUNCTIONS, FUNCTION Echotinyint, ALL TABLES, v1 ) " +
                        "PROPERTIES ( \"backup_timestamp\"=\"2018-05-04-17-11-01\");");
        analyzeSuccess("RESTORE SNAPSHOT `snapshot_2` FROM `repo` DATABASE test AS testNew ON " +
                        "( ALL FUNCTIONS, FUNCTION Echotinyint, ALL TABLES, VIEW v1 ) " +
                        "PROPERTIES ( \"backup_timestamp\"=\"2018-05-04-17-11-01\");");
        analyzeSuccess("RESTORE SNAPSHOT `snapshot_2` FROM `repo` DATABASE test AS testNew ON " +
                        "( ALL FUNCTIONS, FUNCTION Echotinyint AS newName, ALL TABLES, VIEW v1 AS V100 ) " +
                        "PROPERTIES ( \"backup_timestamp\"=\"2018-05-04-17-11-01\");");
        analyzeSuccess("RESTORE SNAPSHOT `snapshot_2` FROM `repo` DATABASE test AS testNew ON " +
                        "( FUNCTION Echotinyint AS newName, ALL TABLES, VIEW v1 AS V100 ) " +
                        "PROPERTIES ( \"backup_timestamp\"=\"2018-05-04-17-11-01\");");
        analyzeSuccess("RESTORE SNAPSHOT `snapshot_2` FROM `repo` " +
                        "PROPERTIES ( \"backup_timestamp\"=\"2018-05-04-17-11-01\");");
        analyzeSuccess("RESTORE SNAPSHOT `snapshot_2` FROM `repo` " + "ALL EXTERNAL CATALOGS " +
                       "PROPERTIES ( \"backup_timestamp\"=\"2018-05-04-17-11-01\");");
        analyzeSuccess("RESTORE SNAPSHOT `snapshot_2` FROM `repo` " + "EXTERNAL CATALOG (catalog1) " +
                        "PROPERTIES ( \"backup_timestamp\"=\"2018-05-04-17-11-01\");");
        analyzeSuccess("RESTORE SNAPSHOT `snapshot_2` FROM `repo` " + "EXTERNAL CATALOG (catalog1 AS catalog2) " +
                       "PROPERTIES ( \"backup_timestamp\"=\"2018-05-04-17-11-01\");");
        analyzeSuccess("RESTORE SNAPSHOT `snapshot_2` FROM `repo` PROPERTIES ( \"backup_timestamp\"=\"2018-05-04-17-11-01\");");
        analyzeFail("RESTORE SNAPSHOT test.`snapshot_2` FROM `repo` ON ( `t0` , `t1` AS `new_tbl` ) " +
                "PROPERTIES ( \"backup_timestamp\"=\"2018-05-04-17-11-01\",\"allow_load\"=\"a\" );");
        analyzeFail("RESTORE SNAPSHOT test.`snapshot_2` FROM `repo` ON ( `t0` , `t1` AS `new_tbl` ) " +
                "PROPERTIES ( \"backup_timestamp\"=\"2018-05-04-17-11-01\",\"replication_num\"=\"a\" );");
        analyzeFail("RESTORE SNAPSHOT test.`snapshot_2` FROM `repo` ON ( `t0` , `t1` AS `new_tbl` ) " +
                "PROPERTIES ( \"backup_timestamp\"=\"2018-05-04-17-11-01\",\"meta_version\"=\"a\" );");
        analyzeFail("RESTORE SNAPSHOT test.`snapshot_2` FROM `repo` ON ( `t0` , `t1` AS `new_tbl` ) " +
                "PROPERTIES ( \"backup_timestamp\"=\"2018-05-04-17-11-01\"," +
                "\"starrocks_meta_version\"=\"a\",\"timeout\"=\"3600\" );");
        analyzeFail("RESTORE SNAPSHOT test.`snapshot_2` FROM `repo` ON ( `t0` , `t1` AS `new_tbl` ) " +
                "PROPERTIES ( \"backup_timestamp\"=\"2018-05-04-17-11-01\",\"timeout\"=\"a\" );");
        analyzeFail("RESTORE SNAPSHOT test.`snapshot_2` FROM `repo1` ON ( `t0`)");
        analyzeFail("RESTORE SNAPSHOT test.`snapshot_2` FROM `repo` ON ( `test`.`t0`)");
        analyzeFail("RESTORE SNAPSHOT test1.`snapshot_2` FROM `repo1` ON ( `t0`)");
        analyzeFail("RESTORE SNAPSHOT `snapshot_2` FROM `repo` ON ( `t0` )");
        analyzeFail("RESTORE SNAPSHOT `test:2`.`snapshot_2` FROM `repo` ON ( `t0`)");
        analyzeFail("RESTORE SNAPSHOT `` FROM `repo` ON (`t0`)");
        analyzeFail("RESTORE SNAPSHOT `snapshot_2` FROM `repo` ON (t99)");
        analyzeFail("RESTORE SNAPSHOT `snapshot_2` FROM `repo` ON (`t0` AS `t1`)");
        analyzeFail("RESTORE SNAPSHOT `snapshot_2` FROM `repo` ON (`t0`PARTITION (p1,p1))");
        analyzeFail("RESTORE SNAPSHOT test.`snapshot_2` FROM `repo1` DATABASE test ON (`t0`)" +
                "PROPERTIES ( \"backup_timestamp\"=\"2018-05-04-17-11-01\",\"timeout\"=\"a\" );");
        analyzeFail("RESTORE SNAPSHOT newtest.`snapshot_2` FROM `repo1` DATABASE test AS newtest ON (`t0`)" +
                "PROPERTIES ( \"backup_timestamp\"=\"2018-05-04-17-11-01\",\"timeout\"=\"a\" );");
        analyzeFail("RESTORE SNAPSHOT `snapshot_2` FROM `repo1` DATABASE test ON (`Echostring`)" +
                "PROPERTIES ( \"backup_timestamp\"=\"2018-05-04-17-11-01\",\"timeout\"=\"a\" );");
        analyzeFail("RESTORE SNAPSHOT `snapshot_2` FROM `repo1` ON (`t0`)" +
                "PROPERTIES ( \"backup_timestamp\"=\"2018-05-04-17-11-01\",\"timeout\"=\"a\" );");
        analyzeFail("RESTORE SNAPSHOT .`snapshot_2` FROM `repo` " + "ALL EXTERNAL CATALOGS " +
                    "PROPERTIES ( \"backup_timestamp\"=\"2018-05-04-17-11-01\");");
        analyzeFail("RESTORE SNAPSHOT `test`.`snapshot_2` FROM `repo` " + "EXTERNAL CATALOG (catalog1) " +
                    "PROPERTIES ( \"backup_timestamp\"=\"2018-05-04-17-11-01\");");
        analyzeFail("RESTORE SNAPSHOT `snapshot_2` FROM `repo` " + "ALL EXTERNAL CATALOGS DATABASE test " +
                    "PROPERTIES ( \"backup_timestamp\"=\"2018-05-04-17-11-01\");");
        analyzeFail("RESTORE SNAPSHOT `snapshot_2` FROM `repo` " + "EXTERNAL CATALOG (catalog1) DATABASE test " +
                    "PROPERTIES ( \"backup_timestamp\"=\"2018-05-04-17-11-01\");");
        analyzeFail("RESTORE SNAPSHOT `snapshot_2` FROM `repo` " + "EXTERNAL CATALOG (catalog1) " +
                    "ON (`t0`) PROPERTIES ( \"backup_timestamp\"=\"2018-05-04-17-11-01\");");
        analyzeFail("RESTORE SNAPSHOT `snapshot_2` FROM `repo` " + "ALL EXTERNAL CATALOGS " +
                    "ON (`t0`) PROPERTIES ( \"backup_timestamp\"=\"2018-05-04-17-11-01\");");
    }

    @Test
    public void testShowRestore() {
        analyzeSuccess("SHOW RESTORE FROM `test`;");
        analyzeSuccess("SHOW RESTORE FROM test where true;");
        analyzeSuccess("SHOW RESTORE;");
        analyzeSuccess("SHOW RESTORE WHERE a=1;");
        analyzeFail("SHOW RESTORE FROM test1;");
        analyzeFail("SHOW RESTORE FROM `a:test1`;");
    }

    @Test
    public void testCancelRestore() {
        analyzeFail("CANCEL BACKUP;");
        analyzeFail("CANCEL RESTORE;");
        analyzeSuccess("CANCEL BACKUP FOR EXTERNAL CATALOG;");
        analyzeSuccess("CANCEL RESTORE FOR EXTERNAL CATALOG;");
    }

}
