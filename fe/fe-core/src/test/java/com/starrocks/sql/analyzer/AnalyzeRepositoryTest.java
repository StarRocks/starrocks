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
import com.starrocks.common.exception.AnalysisException;
import com.starrocks.common.exception.DdlException;
import com.starrocks.common.structure.Pair;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.FrontendOptions;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzeRepositoryTest {

    private SnapshotInfo info;

    @Before
    public void setUp() {
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

    @BeforeClass
    public static void beforeClass() throws Exception {
        AlterTest.beforeClass();
        AnalyzeTestUtil.init();
    }

    @Test
    public void testCreateRepository() {
        analyzeSuccess("CREATE REPOSITORY `repo`\n" +
                "WITH BROKER `broker`\n" +
                "ON LOCATION \"bos://backup-cmy\"\n" +
                "PROPERTIES\n" +
                "(\n" +
                "    \"username\" = \"root\",\n" +
                "    \"password\" = \"root\"\n" +
                ");");
        analyzeSuccess("CREATE READ ONLY REPOSITORY `repo`\n" +
                "WITH BROKER `broker`\n" +
                "ON LOCATION \"bos://backup-cmy\"\n" +
                "PROPERTIES\n" +
                "(\n" +
                "    \"username\" = \"root\",\n" +
                "    \"password\" = \"root\"\n" +
                ");");
        analyzeSuccess("CREATE REPOSITORY `repo`\n" +
                "WITH BROKER \n" +
                "ON LOCATION \"bos://backup-cmy\"\n" +
                "PROPERTIES\n" +
                "(\n" +
                "    \"username\" = \"root\",\n" +
                "    \"password\" = \"root\"\n" +
                ");");
        analyzeFail("CREATE REPOSITORY ``\n" +
                "WITH BROKER `broker`\n" +
                "ON LOCATION \"bos://backup-cmy\"\n" +
                "PROPERTIES\n" +
                "(\n" +
                "    \"username\" = \"root\",\n" +
                "    \"password\" = \"root\"\n" +
                ");");
        analyzeFail("CREATE REPOSITORY `repo`\n" +
                "WITH BROKER ``\n" +
                "ON LOCATION \"bos://backup-cmy\"\n" +
                "PROPERTIES\n" +
                "(\n" +
                "    \"username\" = \"root\",\n" +
                "    \"password\" = \"root\"\n" +
                ");");
        analyzeFail("CREATE REPOSITORY `repo`\n" +
                "WITH BROKER `broker`\n" +
                "ON LOCATION \"\"\n" +
                "PROPERTIES\n" +
                "(\n" +
                "    \"username\" = \"root\",\n" +
                "    \"password\" = \"root\"\n" +
                ");");
        analyzeFail("CREATE REPOSITORY `repo1`\n" +
                "WITH BROKER `broker`\n" +
                "ON LOCATION \"bos://backup-cmy\"\n" +
                "PROPERTIES\n" +
                "(\n" +
                ");");
        analyzeFail("CREATE REPOSITORY `a:repo`\n" +
                "WITH BROKER `broker`\n" +
                "ON LOCATION \"bos://backup-cmy\"\n" +
                "PROPERTIES\n" +
                "(\n" +
                "    \"username\" = \"root\",\n" +
                "    \"password\" = \"root\"\n" +
                ");");
        new MockUp<BrokerMgr>() {
            @Mock
            public FsBroker getBroker(String name, String host) throws AnalysisException {
                return null;
            }
        };
        analyzeFail("CREATE REPOSITORY `repo`\n" +
                "WITH BROKER `broker`\n" +
                "ON LOCATION \"bos://backup-cmy\"\n" +
                "PROPERTIES\n" +
                "(\n" +
                "    \"username\" = \"root\",\n" +
                "    \"password\" = \"root\"\n" +
                ");");
    }

    @Test
    public void testDropRepository() throws DdlException {
        Collection<Pair<String, Integer>> addresses = new ArrayList<>();
        Pair<String, Integer> pair = new Pair<>("127.0.0.1", 8080);
        addresses.add(pair);
        GlobalStateMgr.getCurrentState().getBrokerMgr().addBrokers("broker", addresses);

        BlobStorage storage = new BlobStorage("broker", Maps.newHashMap());
        Repository repo = new Repository(10000, "repo", false, "bos://backup-cmy", storage);
        repo.initRepository();
        GlobalStateMgr.getCurrentState().getBackupHandler().getRepoMgr().addAndInitRepoIfNotExist(repo, false);

        analyzeSuccess("DROP REPOSITORY `repo`;");
        analyzeFail("DROP REPOSITORY ``;");
        analyzeFail("DROP REPOSITORY `repo1`;");
        analyzeFail("DROP REPOSITORY `1repo`;");
    }
}
