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
import com.starrocks.sql.ast.CreateFileStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

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
    }

    @AfterEach
    public void tearDown() throws IOException {
        UtFrameUtils.tearDownForPersisTest();
    }

    /**
     * Test subclass of SmallFileMgr that mocks downloadAndCheck method
     */
    private static class TestableSmallFileMgr extends SmallFileMgr {
        private SmallFile mockSmallFile;

        public void setMockSmallFile(SmallFile smallFile) {
            this.mockSmallFile = smallFile;
        }

        @Override
        protected SmallFile downloadAndCheck(long dbId, String catalog, String fileName,
                                             String downloadUrl, String md5sum, boolean saveContent) throws DdlException {
            if (mockSmallFile != null) {
                return mockSmallFile;
            }
            // Fallback to parent implementation if mock is not set
            return super.downloadAndCheck(dbId, catalog, fileName, downloadUrl, md5sum, saveContent);
        }
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

        // 2. Create testable SmallFileMgr with mocked downloadAndCheck
        TestableSmallFileMgr testableSmallFileMgr = new TestableSmallFileMgr();
        testableSmallFileMgr.setMockSmallFile(smallFile);

        // 3. Verify initial state
        Assertions.assertFalse(testableSmallFileMgr.containsFile(TEST_DB_ID, TEST_CATALOG, TEST_FILE_NAME));

        // 4. Create CreateFileStmt and execute createFile
        Map<String, String> properties = new HashMap<>();
        properties.put(CreateFileStmt.PROP_CATALOG, TEST_CATALOG);
        properties.put(CreateFileStmt.PROP_URL, "http://test.url/file.txt");
        properties.put(CreateFileStmt.PROP_MD5, md5sum);
        CreateFileStmt stmt = new CreateFileStmt(TEST_FILE_NAME, TEST_DB_NAME, properties);

        testableSmallFileMgr.createFile(stmt);

        // 5. Verify master state
        Assertions.assertTrue(testableSmallFileMgr.containsFile(TEST_DB_ID, TEST_CATALOG, TEST_FILE_NAME));

        // 6. Test follower replay functionality
        SmallFileMgr followerSmallFileMgr = new SmallFileMgr();
        Assertions.assertFalse(followerSmallFileMgr.containsFile(TEST_DB_ID, TEST_CATALOG, TEST_FILE_NAME));

        SmallFileMgr.SmallFile replayFile = (SmallFileMgr.SmallFile) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_CREATE_SMALL_FILE_V2);

        followerSmallFileMgr.replayCreateFile(replayFile);

        // 7. Verify follower state is consistent with master
        Assertions.assertTrue(followerSmallFileMgr.containsFile(TEST_DB_ID, TEST_CATALOG, TEST_FILE_NAME));
    }

    @Test
    public void testDownloadAndAddFileEditLogException() throws Exception {
        // 1. Prepare test data
        String md5sum = "5e40c477b1b0c0c0c0c0c0c0c0c0c0";
        SmallFileMgr.SmallFile smallFile = new SmallFileMgr.SmallFile(
                TEST_DB_ID, TEST_CATALOG, TEST_FILE_NAME, 1L,
                Base64.getEncoder().encodeToString("test content".getBytes()),
                12L, md5sum, true);

        // 2. Create testable SmallFileMgr with mocked downloadAndCheck
        TestableSmallFileMgr exceptionSmallFileMgr = new TestableSmallFileMgr();
        exceptionSmallFileMgr.setMockSmallFile(smallFile);

        // 3. Mock EditLog.logCreateSmallFile to throw exception
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logCreateSmallFile(any(SmallFileMgr.SmallFile.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 4. Verify initial state
        Assertions.assertFalse(exceptionSmallFileMgr.containsFile(TEST_DB_ID, TEST_CATALOG, TEST_FILE_NAME));

        // 5. Create CreateFileStmt and execute createFile, expect exception
        Map<String, String> properties = new HashMap<>();
        properties.put(CreateFileStmt.PROP_CATALOG, TEST_CATALOG);
        properties.put(CreateFileStmt.PROP_URL, "http://test.url/file.txt");
        properties.put(CreateFileStmt.PROP_MD5, md5sum);
        CreateFileStmt stmt = new CreateFileStmt(TEST_FILE_NAME, TEST_DB_NAME, properties);

        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionSmallFileMgr.createFile(stmt);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 6. Verify leader memory state remains unchanged after exception
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

