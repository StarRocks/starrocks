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

package com.starrocks.lake.snapshot;

//import com.google.common.collect.Lists;
import com.starrocks.alter.AlterTest;
import com.starrocks.backup.BlobStorage;
import com.starrocks.backup.Status;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.DdlException;
import com.starrocks.common.MetaNotFoundException;
//import com.starrocks.common.Pair;
//import com.starrocks.journal.bdbje.BDBJEJournal;
import com.starrocks.lake.StarOSAgent;
//import com.starrocks.lake.snapshot.ClusterSnapshotJob.ClusterSnapshotJobState;
import com.starrocks.persist.ClusterSnapshotLog;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.Storage;
import com.starrocks.server.GlobalStateMgr;
//import com.starrocks.server.RunMode;
import com.starrocks.server.StorageVolumeMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.AdminSetAutomatedSnapshotOffStmt;
import com.starrocks.sql.ast.AdminSetAutomatedSnapshotOnStmt;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_ENDPOINT;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_REGION;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class ClusterSnapshotTest {
    @Mocked
    private EditLog editLog;

    private StarOSAgent starOSAgent = new StarOSAgent();

    private String storageVolumeName = StorageVolumeMgr.BUILTIN_STORAGE_VOLUME;
    private ClusterSnapshotMgr clusterSnapshotMgr = new ClusterSnapshotMgr();
    private boolean initSv = false;

    private File mockedFile = new File("/abc/abc");

    @BeforeClass
    public static void beforeClass() throws Exception {
        AlterTest.beforeClass();
        AnalyzeTestUtil.init();
    }

    @Before
    public void setUp() {
        try {
            initStorageVolume();
        } catch (Exception ignore) {
        }

        new Expectations() {
            {
                editLog.logClusterSnapshotLog((ClusterSnapshotLog) any);
                minTimes = 0;
                result = new Delegate() {
                    public void logClusterSnapshotLog(ClusterSnapshotLog log) {
                    }
                };
            }
        };

        new MockUp<GlobalStateMgr>() {
            @Mock
            public EditLog getEditLog() {
                return editLog;
            }

            @Mock
            public ClusterSnapshotMgr getClusterSnapshotMgr() {
                return clusterSnapshotMgr;
            }

            @Mock
            public long getNextId() {
                return 0L;
            }
        };

        new MockUp<BlobStorage>() {
            @Mock
            public Status delete(String remotePath) {
                return Status.OK;
            }
        };

        new MockUp<Storage>() {
            @Mock
            public File getCurrentImageFile() {
                return mockedFile;
            }

            @Mock
            public File getCurrentChecksumFile() {
                return mockedFile;
            }

            @Mock
            public File getRoleFile() {
                return mockedFile;
            }

            @Mock
            public File getVersionFile() {
                return mockedFile;
            }
        };

        new MockUp<GlobalStateMgr>() {
            @Mock
            public StarOSAgent getStarOSAgent() {
                return starOSAgent;
            }
        };

        new MockUp<StarOSAgent>() {
            @Mock
            public String getRawServiceId() {
                return "qwertty";
            }
        };

        setAutomatedSnapshotOff(false);
    }

    private void setAutomatedSnapshotOn(boolean testReplay) {
        if (!testReplay) {
            GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().setAutomatedSnapshotOn(
                new AdminSetAutomatedSnapshotOnStmt(storageVolumeName));
        } else {
            GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().setAutomatedSnapshotOn(storageVolumeName);
        }
    }

    private void setAutomatedSnapshotOff(boolean testReplay) {
        if (!testReplay) {
            GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().setAutomatedSnapshotOff(
                new AdminSetAutomatedSnapshotOffStmt());
        } else {
            GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().setAutomatedSnapshotOff();
        }
    }

    private void initStorageVolume() throws AlreadyExistsException, DdlException, MetaNotFoundException {
        if (!initSv) {
            List<String> locations = Arrays.asList("s3://abc");
            Map<String, String> storageParams = new HashMap<>();
            storageParams.put(AWS_S3_REGION, "region");
            storageParams.put(AWS_S3_ENDPOINT, "endpoint");
            storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
            String svKey = GlobalStateMgr.getCurrentState().getStorageVolumeMgr()
                           .createStorageVolume(storageVolumeName, "S3", locations, storageParams, Optional.empty(), "");
            Assert.assertEquals(true, GlobalStateMgr.getCurrentState().getStorageVolumeMgr().exists(storageVolumeName));
            Assert.assertEquals(storageVolumeName,
                                GlobalStateMgr.getCurrentState().getStorageVolumeMgr().getStorageVolumeName(svKey));
            initSv = true;
        }
    }

    @Test
    public void testOperationOfAutomatedSnapshot() throws DdlException {
        // 1. test analyer and execution
        String turnOnSql = "ADMIN SET AUTOMATED CLUSTER SNAPSHOT ON";
        // no sv
        analyzeFail(turnOnSql + " STORAGE VOLUME testSv");

        analyzeSuccess(turnOnSql);
        setAutomatedSnapshotOn(false);
        // duplicate creation
        analyzeFail(turnOnSql);

        setAutomatedSnapshotOff(false);

        String turnOFFSql = "ADMIN SET AUTOMATED CLUSTER SNAPSHOT OFF";
        analyzeFail(turnOFFSql);
        setAutomatedSnapshotOn(false);
        analyzeSuccess(turnOFFSql);
    }

    @Test
    public void testReplayClusterSnapshotLog() {
        // create atuomated snapshot request log
        ClusterSnapshotLog logCreate = new ClusterSnapshotLog();
        logCreate.setCreateSnapshotNamePrefix(ClusterSnapshotMgr.AUTOMATED_NAME_PREFIX, storageVolumeName);
        GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().replayLog(logCreate);

        // drop automated snapshot request log
        ClusterSnapshotLog logDrop = new ClusterSnapshotLog();
        logDrop.setDropSnapshot(ClusterSnapshotMgr.AUTOMATED_NAME_PREFIX);
        GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().replayLog(logDrop);
    }
}
