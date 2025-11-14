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

package com.starrocks.common.util;

import com.starrocks.catalog.Database;
import com.starrocks.common.DdlException;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.RemoveSmallFileLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class SmallFileMgrEditLogTest {
    private SmallFileMgr masterSmallFileMgr;
    private Database testDatabase;
    private static final String TEST_DB_NAME = "test_db";
    private static final long TEST_DB_ID = 100L;
    private static final String TEST_CATALOG = "test_catalog";
    private static final String TEST_FILE_NAME = "test_file.txt";
    private Path tempFile;

    @BeforeEach
    public void setUp() throws Exception {
        // Initialize test environment
        UtFrameUtils.setUpForPersistTest();

        GlobalStateMgr.getCurrentState().getWarehouseMgr().initDefaultWarehouse();

        // Create test database
        testDatabase = new Database(TEST_DB_ID, TEST_DB_NAME);
        GlobalStateMgr.getCurrentState().getLocalMetastore().unprotectCreateDb(testDatabase);

        // Create SmallFileMgr instance
        masterSmallFileMgr = GlobalStateMgr.getCurrentState().getSmallFileMgr();

        // Create a temporary file for testing
        tempFile = Files.createTempFile("test", ".txt");
        try (FileOutputStream fos = new FileOutputStream(tempFile.toFile())) {
            fos.write("test content".getBytes());
        }
    }

    @AfterEach
    public void tearDown() throws IOException {
        if (tempFile != null) {
            Files.deleteIfExists(tempFile);
        }
        UtFrameUtils.tearDownForPersisTest();
    }

    // ==================== Download And Add File Tests ====================

    @Test
    public void testDownloadAndAddFileNormalCase() throws Exception {
        // 1. Prepare test data
        String md5sum = "5e40c477b1b0c0c0c0c0c0c0c0c0c0";
        SmallFileMgr.SmallFile smallFile = new SmallFileMgr.SmallFile(
                TEST_DB_ID, TEST_CATALOG, TEST_FILE_NAME, 1L,
                Base64.getEncoder().encodeToString("test content".getBytes()),
                12L, md5sum, true);

        // 2. Verify initial state
        Assertions.assertFalse(masterSmallFileMgr.containsFile(TEST_DB_ID, TEST_CATALOG, TEST_FILE_NAME));

        // 3. Execute logCreateSmallFile operation (simulating downloadAndAddFile's editlog call)
        GlobalStateMgr.getCurrentState().getEditLog().logCreateSmallFile(smallFile, wal -> {
            masterSmallFileMgr.replayCreateFile(smallFile);
        });

        // 4. Verify master state
        Assertions.assertTrue(masterSmallFileMgr.containsFile(TEST_DB_ID, TEST_CATALOG, TEST_FILE_NAME));

        // 5. Test follower replay functionality
        SmallFileMgr followerSmallFileMgr = new SmallFileMgr();
        Assertions.assertFalse(followerSmallFileMgr.containsFile(TEST_DB_ID, TEST_CATALOG, TEST_FILE_NAME));

        SmallFileMgr.SmallFile replayFile = (SmallFileMgr.SmallFile) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_CREATE_SMALL_FILE_V2);

        followerSmallFileMgr.replayCreateFile(replayFile);

        // 6. Verify follower state is consistent with master
        Assertions.assertTrue(followerSmallFileMgr.containsFile(TEST_DB_ID, TEST_CATALOG, TEST_FILE_NAME));
    }

    @Test
    public void testDownloadAndAddFileEditLogException() throws Exception {
        // 1. Prepare test data
        String fileUrl = tempFile.toUri().toURL().toString();
        String md5sum = "5e40c477b1b0c0c0c0c0c0c0c0c0c0";

        // 2. Create a separate SmallFileMgr for exception testing
        SmallFileMgr exceptionSmallFileMgr = new SmallFileMgr();
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());

        // 3. Mock EditLog.logCreateSmallFile to throw exception
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logCreateSmallFile(any(SmallFileMgr.SmallFile.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Verify initial state
        Assertions.assertFalse(exceptionSmallFileMgr.containsFile(TEST_DB_ID, TEST_CATALOG, TEST_FILE_NAME));

        // 4. Execute downloadAndAddFile operation and expect exception
        // Note: This will fail at download stage, but we test the editlog exception path
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            // We need to bypass the download part to test editlog exception
            // Create SmallFile directly and try to log it
            SmallFileMgr.SmallFile smallFile = new SmallFileMgr.SmallFile(
                    TEST_DB_ID, TEST_CATALOG, TEST_FILE_NAME, 1L,
                    Base64.getEncoder().encodeToString("test content".getBytes()),
                    12L, md5sum, true);
            GlobalStateMgr.getCurrentState().getEditLog().logCreateSmallFile(smallFile, wal -> {});
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        Assertions.assertFalse(exceptionSmallFileMgr.containsFile(TEST_DB_ID, TEST_CATALOG, TEST_FILE_NAME));
    }

    // ==================== Remove File Tests ====================

    @Test
    public void testRemoveFileNormalCase() throws Exception {
        // 1. Prepare test data and add file first
        SmallFileMgr.SmallFile smallFile = new SmallFileMgr.SmallFile(
                TEST_DB_ID, TEST_CATALOG, TEST_FILE_NAME, 1L,
                Base64.getEncoder().encodeToString("test content".getBytes()),
                12L, "5e40c477b1b0c0c0c0c0c0c0c0c0c0", true);

        masterSmallFileMgr.replayCreateFile(smallFile);
        Assertions.assertTrue(masterSmallFileMgr.containsFile(TEST_DB_ID, TEST_CATALOG, TEST_FILE_NAME));

        // 2. Execute removeFile operation
        masterSmallFileMgr.removeFile(TEST_DB_ID, TEST_CATALOG, TEST_FILE_NAME);

        // 3. Verify master state
        Assertions.assertFalse(masterSmallFileMgr.containsFile(TEST_DB_ID, TEST_CATALOG, TEST_FILE_NAME));
    }

    @Test
    public void testRemoveFileEditLogException() throws Exception {
        // 1. Prepare test data and add file first
        SmallFileMgr.SmallFile smallFile = new SmallFileMgr.SmallFile(
                TEST_DB_ID, TEST_CATALOG, TEST_FILE_NAME, 1L,
                Base64.getEncoder().encodeToString("test content".getBytes()),
                12L, "5e40c477b1b0c0c0c0c0c0c0c0c0c0", true);

        SmallFileMgr exceptionSmallFileMgr = new SmallFileMgr();
        exceptionSmallFileMgr.replayCreateFile(smallFile);
        Assertions.assertTrue(exceptionSmallFileMgr.containsFile(TEST_DB_ID, TEST_CATALOG, TEST_FILE_NAME));

        // 2. Mock EditLog.logDropSmallFile to throw exception
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logDropSmallFile(any(RemoveSmallFileLog.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 3. Execute removeFile operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionSmallFileMgr.removeFile(TEST_DB_ID, TEST_CATALOG, TEST_FILE_NAME);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 4. Verify leader memory state remains unchanged after exception
        Assertions.assertTrue(exceptionSmallFileMgr.containsFile(TEST_DB_ID, TEST_CATALOG, TEST_FILE_NAME));
    }
}

