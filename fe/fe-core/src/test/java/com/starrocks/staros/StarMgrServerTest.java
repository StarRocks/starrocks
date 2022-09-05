// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.staros;

import com.staros.exception.StarException;
import com.staros.manager.StarManagerServer;
import com.starrocks.common.Config;
import com.starrocks.journal.bdbje.BDBEnvironment;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

public class StarMgrServerTest {
    @Mocked
    private BDBEnvironment environment;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setUp() {
        Config.starmgr_s3_bucket = "abc";
    }

    @After
    public void tearDown() {
        Config.starmgr_s3_bucket = "";
    }

    @Test
    public void testStarMgrServer() throws Exception {
        StarMgrServer server = new StarMgrServer();

        new MockUp<StarManagerServer>() {
            @Mock
            public void start(int port) throws IOException {
            }
        };

        server.initialize(environment, tempFolder.getRoot().getPath());

        server.getJournalSystem().setReplayId(1L);
        Assert.assertEquals(1L, server.getReplayId());

        new MockUp<BDBJEJournalSystem>() {
            @Mock
            public void replayTo(long journalId) throws StarException {
            }
            @Mock
            public long getReplayId() {
                return 0;
            }
        };
        Assert.assertTrue(server.replayAndGenerateImage(tempFolder.getRoot().getPath(), 0L));

        new MockUp<BDBJEJournalSystem>() {
            @Mock
            public void replayTo(long journalId) throws StarException {
            }
            @Mock
            public long getReplayId() {
                return 1;
            }
        };
        Assert.assertTrue(server.replayAndGenerateImage(tempFolder.getRoot().getPath(), 1L));
    }
}
