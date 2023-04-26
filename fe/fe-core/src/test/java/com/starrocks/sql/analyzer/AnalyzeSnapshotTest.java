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
import com.starrocks.common.DdlException;
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

public class AnalyzeSnapshotTest {

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

    }

    @Test
    public void testShowSnapshot() throws DdlException {
        Collection<Pair<String, Integer>> addresses = new ArrayList<>();
        Pair<String, Integer> pair = new Pair<String, Integer>("127.0.0.1", 8080);
        addresses.add(pair);
        GlobalStateMgr.getCurrentState().getBrokerMgr().addBrokers(brokerName, addresses);

        BlobStorage storage = new BlobStorage(brokerName, Maps.newHashMap());
        Repository repo = new Repository(10000, "repo", false, location, storage);
        repo.initRepository();
        GlobalStateMgr.getCurrentState().getBackupHandler().getRepoMgr().addAndInitRepoIfNotExist(repo, false);

        analyzeSuccess("SHOW SNAPSHOT ON `repo` WHERE SNAPSHOT = \"backup1\" AND TIMESTAMP = \"2018-05-05-15-34-26\";");
        analyzeSuccess("SHOW SNAPSHOT ON `repo` WHERE SNAPSHOT IN (\"backup1\")");
        analyzeFail("SHOW SNAPSHOT ON `repo` WHERE SNAPSHOT = \"backup1\" OR TIMESTAMP = \"2018-05-05-15-34-26\";");
        analyzeFail("SHOW SNAPSHOT ON `repo` WHERE SNAPSHOT = \"\" AND TIMESTAMP = \"\";");
        analyzeFail("SHOW SNAPSHOT ON `repo` WHERE 1 = 1 ;");
        analyzeFail("SHOW SNAPSHOT ON `repo1` WHERE SNAPSHOT = \"backup1\";");
        analyzeFail("SHOW SNAPSHOT ON `repo` WHERE a = 1;");
    }
}
