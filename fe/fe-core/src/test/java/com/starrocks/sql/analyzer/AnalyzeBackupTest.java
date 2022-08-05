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

public class AnalyzeBackupTest {
    @BeforeClass
    public static void beforeClass() throws Exception {
        AlterTest.beforeClass();
        AnalyzeTestUtil.init();
        new MockUp<Repository>() {
            @Mock
            public Status initRepository(){
                return Status.OK;
            }
        };

        Collection<Pair<String, Integer>> addresses = new ArrayList<>();
        Pair<String, Integer> pair = new Pair<String, Integer>("127.0.0.1",8080);
        addresses.add(pair);
        String brokerName = "broker";
        String location = "bos://backup-cmy";
        GlobalStateMgr.getCurrentState().getBrokerMgr().addBrokers(brokerName,addresses);

        BlobStorage storage = new BlobStorage(brokerName, Maps.newHashMap());
        Repository repo = new Repository(10000, "repo", false, location, storage);
        repo.initRepository();
        GlobalStateMgr.getCurrentState().getBackupHandler().getRepoMgr().addAndInitRepoIfNotExist(repo, false);
    }

    @Test
    public void testBackup() {
        analyzeSuccess("BACKUP SNAPSHOT test.snapshot_label2 TO `repo` ON ( t0, t1 ) PROPERTIES (\"type\" = \"full\",\"timeout\" = \"3600\");");
        analyzeFail("BACKUP SNAPSHOT test.snapshot_label2 TO `repo` ON ( t0, t1 ) PROPERTIES (\"type\" = \"full\",\"timeout\" = \"a\");");
        analyzeFail("BACKUP SNAPSHOT test.snapshot_label2 TO `repo` ON ( t0, t1 ) PROPERTIES (\"type\" = \"full\",\"timeout\" = \"10\");");
        analyzeFail("BACKUP SNAPSHOT test.snapshot_label2 TO `repo` ON ( t0, t1 ) PROPERTIES (\"type\" = \"a\",\"timeout\" = \"3600\");");
        analyzeFail("BACKUP SNAPSHOT test.snapshot_label2 TO `repo` ON ( t0, t1 ) PROPERTIES (\"a\" = \"a\",\"timeout\" = \"3600\");");
        analyzeFail("BACKUP SNAPSHOT `` TO `repo` ON ( t0, t1 );");
        analyzeFail("BACKUP SNAPSHOT `2@3`.snapshot_label2 TO `repo` ON ( t0, t1 );");
        analyzeFail("BACKUP SNAPSHOT snapshot_label2 TO `` ON( t0, t1 );");
        analyzeFail("BACKUP SNAPSHOT test.snapshot_label2 TO `repo1` ON(t0);");
        analyzeFail("BACKUP SNAPSHOT test.snapshot_label2 TO `repo` ON(t999);");
        analyzeFail("BACKUP SNAPSHOT test.snapshot_label2 TO `repo` ON(t0 PARTITION (p1));");
        analyzeFail("BACKUP SNAPSHOT test1.snapshot_label2 TO `repo` ON(t0 PARTITION (p1), t1) PROPERTIES(\"type\"=\"1\");");
    }

}
