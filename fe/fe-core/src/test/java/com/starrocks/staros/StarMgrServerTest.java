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


package com.starrocks.staros;

import com.staros.exception.StarException;
import com.staros.manager.StarManagerServer;
import com.starrocks.common.Config;
import com.starrocks.journal.bdbje.BDBEnvironment;
import com.starrocks.leader.CheckpointController;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;

public class StarMgrServerTest {
    @Mocked
    private BDBEnvironment environment;

    @TempDir
    public File tempFolder;

    @BeforeEach
    public void setUp() {
        Config.aws_s3_path = "abc";
        Config.aws_s3_access_key = "abc";
        Config.aws_s3_secret_key = "abc";
    }

    @AfterEach
    public void tearDown() {
        Config.aws_s3_path = "";
        Config.aws_s3_access_key = "";
        Config.aws_s3_secret_key = "";
    }

    @Test
    public void testStarMgrServer() throws Exception {
        StarMgrServer server = new StarMgrServer();

        new MockUp<StarManagerServer>() {
            @Mock
            public void start(int port) throws IOException {
            }
        };

        server.initialize(environment, tempFolder.getPath());

        server.getJournalSystem().setReplayId(1L);
        Assertions.assertEquals(1L, server.getReplayId());

        new MockUp<StarOSBDBJEJournalSystem>() {
            @Mock
            public void replayTo(long journalId) throws StarException {
            }
            @Mock
            public long getReplayId() {
                return 0;
            }
        };
        server.replayAndGenerateImage(tempFolder.getPath(), 0L);

        new MockUp<StarOSBDBJEJournalSystem>() {
            @Mock
            public void replayTo(long journalId) throws StarException {
            }
            @Mock
            public long getReplayId() {
                return 1;
            }
        };
        server.replayAndGenerateImage(tempFolder.getPath(), 1L);
    }

    @Test
    public void testStopCheckpointControllerNullSafe() {
        // stopCheckpointController must be a no-op when startCheckpointController has not run.
        StarMgrServer server = new StarMgrServer();
        Assertions.assertNull(server.checkpointController);
        Assertions.assertDoesNotThrow(() -> server.stopCheckpointController(1000L));
    }

    @Test
    public void testStopCheckpointControllerStopsExistingController() {
        // stopCheckpointController must forward to LeaderDaemon.stopGracefully on the owned
        // controller. With a freshly constructed (never started) controller the join is a
        // no-op and onStopped() runs without side effects, so this exercises the line where
        // the not-null branch is taken.
        StarMgrServer server = new StarMgrServer();
        server.checkpointController = new CheckpointController(
                "star_os_checkpoint_controller_test", new com.starrocks.utframe.MockJournal(), "");

        Assertions.assertDoesNotThrow(() -> server.stopCheckpointController(1000L));
    }
}
