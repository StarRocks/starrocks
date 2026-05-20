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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.alter.AlterTest;
import com.starrocks.backup.BlobStorage;
import com.starrocks.backup.Repository;
import com.starrocks.backup.SnapshotInfo;
import com.starrocks.backup.Status;
import com.starrocks.catalog.BrokerMgr;
import com.starrocks.catalog.FsBroker;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.FrontendOptions;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzeDropSnapshotTest {

    private static SnapshotInfo info;
    private static String location = "bos://backup-cmy";
    private static String brokerName = "broker";

    @Mocked
    private BlobStorage storage;

    @BeforeClass
    public static void beforeClass() throws Exception {
        AlterTest.beforeClass();
        AnalyzeTestUtil.init();
        List<String> files = Lists.newArrayList();
        files.add("1.dat");
        files.add("1.hdr");
        files.add("1.idx");
        info = new SnapshotInfo(1, 2, 3, 4, 5, 6, 7, "/path/to/tablet/snapshot/", files);

        new MockUp<FrontendOptions>() {
            @Mock
            String getLocalHostAddress() {
                return "127.0.0.1";
            }
        };

        new MockUp<BrokerMgr>() {
            @Mock
            public FsBroker getBroker(String name, String host) throws AnalysisException {
                return new FsBroker("10.74.167.16", 8111);
            }
        };

        new MockUp<Repository>() {
            @Mock
            public Status initRepository() {
                return Status.OK;
            }
        };

        // Setup repository for testing
        Collection<Pair<String, Integer>> addresses = new ArrayList<>();
        Pair<String, Integer> pair = new Pair<String, Integer>("127.0.0.1", 8080);
        addresses.add(pair);
        GlobalStateMgr.getCurrentState().getBrokerMgr().addBrokers(brokerName, addresses);

        BlobStorage storage = new BlobStorage(brokerName, Maps.newHashMap());
        Repository repo = new Repository(10000, "repo", false, location, storage);
        repo.initRepository();
        GlobalStateMgr.getCurrentState().getBackupHandler().getRepoMgr().addAndInitRepoIfNotExist(repo, false);
    }

    @Test
    public void testDropSnapshotBasicSyntax() {
        // Test basic DROP SNAPSHOT syntax
        analyzeSuccess("DROP SNAPSHOT ON `repo` WHERE SNAPSHOT = 'backup1';");
        analyzeSuccess("DROP SNAPSHOT ON repo WHERE SNAPSHOT = 'backup1';");
        
        // Test with timestamp filters
        analyzeSuccess("DROP SNAPSHOT ON `repo` WHERE TIMESTAMP <= '2018-05-05-15-34-26';");
        analyzeSuccess("DROP SNAPSHOT ON `repo` WHERE TIMESTAMP >= '2018-05-05-15-34-26';");
        
        // Test combined conditions
        analyzeSuccess("DROP SNAPSHOT ON `repo` WHERE SNAPSHOT = 'backup1' AND TIMESTAMP <= '2018-05-05-15-34-26';");
        analyzeSuccess("DROP SNAPSHOT ON `repo` WHERE SNAPSHOT = 'backup1' AND TIMESTAMP >= '2018-05-05-15-34-26';");
        
        // Test IN clause for multiple snapshots
        analyzeSuccess("DROP SNAPSHOT ON `repo` WHERE SNAPSHOT IN ('backup1', 'backup2', 'backup3');");
    }

    @Test
    public void testDropSnapshotInvalidSyntax() {
        // Test missing WHERE clause - should fail as it's required for safety
        analyzeFail("DROP SNAPSHOT ON `repo`;");
        
        // Test invalid repository name
        analyzeFail("DROP SNAPSHOT ON `nonexistent_repo` WHERE SNAPSHOT = 'backup1';");
        
        // Test invalid operators
        analyzeFail("DROP SNAPSHOT ON `repo` WHERE SNAPSHOT > 'backup1';");
        analyzeFail("DROP SNAPSHOT ON `repo` WHERE TIMESTAMP = '2018-05-05-15-34-26';");
        analyzeFail("DROP SNAPSHOT ON `repo` WHERE TIMESTAMP < '2018-05-05-15-34-26';");
        analyzeFail("DROP SNAPSHOT ON `repo` WHERE TIMESTAMP > '2018-05-05-15-34-26';");
        
        // Test OR conditions - should only support AND
        analyzeFail("DROP SNAPSHOT ON `repo` WHERE SNAPSHOT = 'backup1' OR TIMESTAMP <= '2018-05-05-15-34-26';");
        
        // Test invalid column names
        analyzeFail("DROP SNAPSHOT ON `repo` WHERE invalid_column = 'backup1';");
        analyzeFail("DROP SNAPSHOT ON `repo` WHERE backup_name = 'backup1';");
        
        // Test empty values
        analyzeFail("DROP SNAPSHOT ON `repo` WHERE SNAPSHOT = '';");
        analyzeFail("DROP SNAPSHOT ON `repo` WHERE TIMESTAMP <= '';");
        
        // Test invalid expressions
        analyzeFail("DROP SNAPSHOT ON `repo` WHERE 1 = 1;");
        analyzeFail("DROP SNAPSHOT ON `repo` WHERE SNAPSHOT LIKE 'backup%';");
        analyzeFail("DROP SNAPSHOT ON `repo` WHERE SNAPSHOT > 'backup1';");
    }

    @Test
    public void testDropSnapshotTimestampFormats() {
        // Test valid timestamp formats
        analyzeSuccess("DROP SNAPSHOT ON `repo` WHERE TIMESTAMP <= '2018-05-05-15-34-26';");
        analyzeSuccess("DROP SNAPSHOT ON `repo` WHERE TIMESTAMP >= '2024-12-31-23-59-59';");
        analyzeSuccess("DROP SNAPSHOT ON `repo` WHERE TIMESTAMP <= '2020-01-01-00-00-00';");
        
        // Test combined with snapshot name
        analyzeSuccess("DROP SNAPSHOT ON `repo` WHERE SNAPSHOT = 'test_backup' AND TIMESTAMP <= '2018-05-05-15-34-26';");
        analyzeSuccess("DROP SNAPSHOT ON `repo` WHERE SNAPSHOT = 'test_backup' AND TIMESTAMP >= '2018-05-05-15-34-26';");
    }

    @Test
    public void testDropSnapshotComplexConditions() {
        // Test multiple snapshot names with IN clause
        analyzeSuccess("DROP SNAPSHOT ON `repo` WHERE SNAPSHOT IN ('backup1');");
        analyzeSuccess("DROP SNAPSHOT ON `repo` WHERE SNAPSHOT IN ('backup1', 'backup2');");
        analyzeSuccess("DROP SNAPSHOT ON `repo` WHERE SNAPSHOT IN ('backup1', 'backup2', 'backup3', 'backup4');");
        
        // Test invalid IN clause usage
        analyzeFail("DROP SNAPSHOT ON `repo` WHERE SNAPSHOT IN ();");
        analyzeFail("DROP SNAPSHOT ON `repo` WHERE TIMESTAMP IN ('2018-05-05-15-34-26');");
    }

    @Test
    public void testDropSnapshotRepositoryValidation() {
        // Test with valid repository
        analyzeSuccess("DROP SNAPSHOT ON `repo` WHERE SNAPSHOT = 'backup1';");
        
        // Test with invalid repository names
        analyzeFail("DROP SNAPSHOT ON `invalid_repo` WHERE SNAPSHOT = 'backup1';");
        analyzeFail("DROP SNAPSHOT ON `` WHERE SNAPSHOT = 'backup1';");
        
        // Test repository name without quotes
        analyzeSuccess("DROP SNAPSHOT ON repo WHERE SNAPSHOT = 'backup1';");
    }

    @Test
    public void testDropSnapshotEdgeCases() {
        // Test with special characters in snapshot names
        analyzeSuccess("DROP SNAPSHOT ON `repo` WHERE SNAPSHOT = 'backup_with_underscore';");
        analyzeSuccess("DROP SNAPSHOT ON `repo` WHERE SNAPSHOT = 'backup-with-dash';");
        analyzeSuccess("DROP SNAPSHOT ON `repo` WHERE SNAPSHOT = 'backup.with.dots';");
        
        // Test case sensitivity
        analyzeSuccess("DROP SNAPSHOT ON `repo` WHERE SNAPSHOT = 'BackupWithCaps';");
        analyzeSuccess("DROP SNAPSHOT ON `repo` WHERE snapshot = 'backup1';");
        analyzeSuccess("DROP SNAPSHOT ON `repo` WHERE SNAPSHOT = 'backup1';");
        
        // Test with numbers in snapshot names
        analyzeSuccess("DROP SNAPSHOT ON `repo` WHERE SNAPSHOT = 'backup123';");
        analyzeSuccess("DROP SNAPSHOT ON `repo` WHERE SNAPSHOT = '123backup';");
        analyzeSuccess("DROP SNAPSHOT ON `repo` WHERE SNAPSHOT = '123';");
    }

    @Test
    public void testDropSnapshotWhitespaceAndQuoting() {
        // Test various quoting styles
        analyzeSuccess("DROP SNAPSHOT ON `repo` WHERE SNAPSHOT = \"backup1\";");
        analyzeSuccess("DROP SNAPSHOT ON `repo` WHERE SNAPSHOT = 'backup1';");
        
        // Test with spaces in snapshot names
        analyzeSuccess("DROP SNAPSHOT ON `repo` WHERE SNAPSHOT = 'backup with spaces';");
        analyzeSuccess("DROP SNAPSHOT ON `repo` WHERE SNAPSHOT = \"backup with spaces\";");
        
        // Test whitespace handling
        analyzeSuccess("DROP SNAPSHOT ON `repo` WHERE SNAPSHOT='backup1';");
        analyzeSuccess("DROP SNAPSHOT ON `repo` WHERE SNAPSHOT = 'backup1' ;");
        analyzeSuccess("DROP SNAPSHOT ON `repo`  WHERE  SNAPSHOT  =  'backup1'  ;");
    }
}
