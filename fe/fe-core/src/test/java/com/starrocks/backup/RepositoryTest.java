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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/backup/RepositoryTest.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.backup;

import com.google.common.collect.Lists;
import com.starrocks.catalog.BrokerMgr;
import com.starrocks.catalog.FsBroker;
import com.starrocks.common.AnalysisException;
import com.starrocks.metric.MetricRepo;
import com.starrocks.service.FrontendOptions;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.ShowRepositoriesStmt;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.Timestamp;
import java.util.List;

public class RepositoryTest {

    private Repository repo;
    private long repoId = 10000;
    private String name = "repo";
    private String location = "bos://backup-cmy";
    private String brokerName = "broker";

    private SnapshotInfo info;

    @Mocked
    private BlobStorage storage;

    @BeforeEach
    public void setUp() {
        List<String> files = Lists.newArrayList();
        files.add("1.dat");
        files.add("1.hdr");
        files.add("1.idx");
        info = new SnapshotInfo(1, 2, 3, 4, 5, 6, 7, "/path/to/tablet/snapshot/", files);

        MetricRepo.init();

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

    }

    @Test
    public void testGet() {
        repo = new Repository(10000, "repo", false, location, storage);

        Assertions.assertEquals(repoId, repo.getId());
        Assertions.assertEquals(name, repo.getName());
        Assertions.assertEquals(false, repo.isReadOnly());
        Assertions.assertEquals(location, repo.getLocation());
        Assertions.assertEquals(null, repo.getErrorMsg());
        Assertions.assertTrue(System.currentTimeMillis() - repo.getCreateTime() < 1000);
    }

    @Test
    public void testInit() {
        new Expectations() {
            {
                storage.list(anyString, (List<RemoteFile>) any);
                minTimes = 0;
                result = new Delegate<Status>() {
                    public Status list(String remotePath, List<RemoteFile> result) {
                        result.clear();
                        return Status.OK;
                    }
                };

                storage.directUpload(anyString, anyString);
                minTimes = 0;
                result = Status.OK;
            }
        };

        repo = new Repository(10000, "repo", false, location, storage);

        Status st = repo.initRepository();
        System.out.println(st);
        Assertions.assertTrue(st.ok());
    }

    @Test
    public void testassemnblePath() {
        repo = new Repository(10000, "repo", false, location, storage);

        // job info
        String label = "label";
        String createTime = "2018-04-12 20:46:45";
        String createTime2 = "2018-04-12-20-46-45-000";
        Timestamp ts = Timestamp.valueOf(createTime);
        long creastTs = ts.getTime();

        // "location/__starrocks_repository_repo_name/__ss_my_sp1/__info_2018-01-01-08-00-00"
        String expected = location + "/" + repo.prefixRepo + name + "/" + Repository.PREFIX_SNAPSHOT_DIR
                + label + "/" + Repository.PREFIX_JOB_INFO + createTime2;
        Assertions.assertEquals(expected, repo.assembleJobInfoFilePath(label, creastTs));

        // meta info
        expected = location + "/" + repo.prefixRepo + name + "/" + Repository.PREFIX_SNAPSHOT_DIR
                + label + "/" + Repository.FILE_META_INFO;
        Assertions.assertEquals(expected, repo.assembleMetaInfoFilePath(label));

        // snapshot path
        // /location/__starrocks_repository_repo_name/__ss_my_ss1/__ss_content/__db_10001/__tbl_10020/__part_10031/__idx_10032/__10023/__3481721
        expected = location + "/" + repo.prefixRepo + name + "/" + Repository.PREFIX_SNAPSHOT_DIR
                + label + "/" + "__ss_content/__db_1/__tbl_2/__part_3/__idx_4/__5/__7";
        Assertions.assertEquals(expected, repo.assembleRemoteSnapshotPath(label, info));
    }

    @Test
    public void testPing() {
        new Expectations() {
            {
                storage.checkPathExist(anyString);
                minTimes = 0;
                result = Status.OK;
            }
        };

        repo = new Repository(10000, "repo", false, location, storage);
        Assertions.assertTrue(repo.ping());
        Assertions.assertTrue(repo.getErrorMsg() == null);
    }

    @Test
    public void testListSnapshots() {
        new Expectations() {
            {
                storage.list(anyString, (List<RemoteFile>) any);
                minTimes = 0;
                result = new Delegate() {
                    public Status list(String remotePath, List<RemoteFile> result) {
                        result.add(new RemoteFile(Repository.PREFIX_SNAPSHOT_DIR + "a", false, 100));
                        result.add(new RemoteFile("_ss_b", true, 100));
                        return Status.OK;
                    }
                };
            }
        };

        repo = new Repository(10000, "repo", false, location, storage);
        List<String> snapshotNames = Lists.newArrayList();
        Status st = repo.listSnapshots(snapshotNames);
        Assertions.assertTrue(st.ok());
        Assertions.assertEquals(1, snapshotNames.size());
        Assertions.assertEquals("a", snapshotNames.get(0));
    }

    @Test
    public void testUpload() {
        new Expectations() {
            {
                storage.upload(anyString, anyString);
                minTimes = 0;
                result = Status.OK;

                storage.rename(anyString, anyString);
                minTimes = 0;
                result = Status.OK;

                storage.delete(anyString);
                minTimes = 0;
                result = Status.OK;
            }
        };

        repo = new Repository(10000, "repo", false, location, storage);
        String localFilePath = "./tmp_" + System.currentTimeMillis();
        try (PrintWriter out = new PrintWriter(localFilePath)) {
            out.print("a");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            Assertions.fail();
        }
        try {
            String remoteFilePath = location + "/remote_file";
            Status st = repo.upload(localFilePath, remoteFilePath);
            Assertions.assertTrue(st.ok());
        } finally {
            File file = new File(localFilePath);
            file.delete();
        }
    }

    @Test
    public void testDownload() {
        String localFilePath = "./tmp_" + System.currentTimeMillis();
        File localFile = new File(localFilePath);
        try {
            try (PrintWriter out = new PrintWriter(localFile)) {
                out.print("a");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                Assertions.fail();
            }

            new Expectations() {
                {
                    storage.list(anyString, (List<RemoteFile>) any);
                    minTimes = 0;
                    result = new Delegate() {
                        public Status list(String remotePath, List<RemoteFile> result) {
                            result.add(new RemoteFile("remote_file.0cc175b9c0f1b6a831c399e269772661", true, 100));
                            return Status.OK;
                        }
                    };

                    storage.downloadWithFileSize(anyString, anyString, anyLong);
                    minTimes = 0;
                    result = Status.OK;
                }
            };

            repo = new Repository(10000, "repo", false, location, storage);
            String remoteFilePath = location + "/remote_file";
            Status st = repo.download(remoteFilePath, localFilePath);
            Assertions.assertTrue(st.ok());
        } finally {
            localFile.delete();
        }
    }

    @Test
    public void testGetInfo() {
        repo = new Repository(10000, "repo", false, location, storage);
        List<String> infos = repo.getInfo();
        Assertions.assertTrue(infos.size() == ShowRepositoriesStmt.TITLE_NAMES.size());
    }

    @Test
    public void testGetSnapshotInfo() {
        new Expectations() {
            {
                storage.list(anyString, (List<RemoteFile>) any);
                minTimes = 0;
                result = new Delegate() {
                    public Status list(String remotePath, List<RemoteFile> result) {
                        if (remotePath.contains(Repository.PREFIX_JOB_INFO)) {
                            result.add(new RemoteFile(" __info_2018-04-18-20-11-00.12345678123456781234567812345678",
                                    true,
                                    100));
                        } else {
                            result.add(new RemoteFile(Repository.PREFIX_SNAPSHOT_DIR + "s1", false, 100));
                            result.add(new RemoteFile(Repository.PREFIX_SNAPSHOT_DIR + "s2", false, 100));
                        }
                        return Status.OK;
                    }
                };
            }
        };

        repo = new Repository(10000, "repo", false, location, storage);
        String snapshotName = "";
        String timestamp = "";
        try {
            List<List<String>> infos = repo.getSnapshotInfos(snapshotName, timestamp, null);
            Assertions.assertEquals(2, infos.size());

        } catch (SemanticException e) {
            e.printStackTrace();
            Assertions.fail();
        }
    }
}
