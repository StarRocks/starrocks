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

package com.starrocks.encryption;

import com.starrocks.common.Config;
import com.starrocks.common.io.Text;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.proto.EncryptionKeyPB;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class KeyMgrEditLogTest {
    private KeyMgr keyMgr;
    private String oldConfig;

    @BeforeEach
    public void setUp() throws Exception {
        // Initialize test environment
        UtFrameUtils.setUpForPersistTest();

        // Save old config
        oldConfig = Config.default_master_key;
        Config.default_master_key = "plain:aes_128:enwSdCUAiCLLx2Bs9E/neQ==";

        keyMgr = new KeyMgr();
        
        // Initialize with a master key using createFromSpec to ensure proper initialization
        EncryptionKey masterKey = EncryptionKey.createFromSpec(Config.default_master_key);
        masterKey.setId(KeyMgr.DEFAULT_MASTER_KYE_ID);
        EncryptionKeyPB masterKeyPB = new EncryptionKeyPB();
        masterKey.toPB(masterKeyPB, keyMgr);
        keyMgr.replayAddKey(masterKeyPB);
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
        Config.default_master_key = oldConfig;
    }

    @Test
    public void testAddKeyNormalCase() throws Exception {
        // 1. Verify initial state
        int initialKeyCount = keyMgr.numKeys();
        Assertions.assertEquals(1, initialKeyCount); // Master key

        // 2. Execute generateNewKEK which calls addKey internally
        EncryptionKey kek = keyMgr.generateNewKEK();

        // 3. Verify master state
        Assertions.assertNotNull(kek);
        Assertions.assertEquals(2, keyMgr.numKeys());
        EncryptionKey addedKey = keyMgr.getKeyById(kek.id);
        Assertions.assertNotNull(addedKey);
        Assertions.assertEquals(kek.id, addedKey.id);

        // 4. Test follower replay functionality
        KeyMgr followerKeyMgr = new KeyMgr();
        
        // Initialize follower with master key using createFromSpec to ensure proper initialization
        EncryptionKey followerMasterKey = EncryptionKey.createFromSpec(Config.default_master_key);
        followerMasterKey.setId(KeyMgr.DEFAULT_MASTER_KYE_ID);
        EncryptionKeyPB followerMasterKeyPB = new EncryptionKeyPB();
        followerMasterKey.toPB(followerMasterKeyPB, followerKeyMgr);
        followerKeyMgr.replayAddKey(followerMasterKeyPB);
        
        // Verify follower initial state
        Assertions.assertEquals(1, followerKeyMgr.numKeys());

        // Replay the operation
        // OP_ADD_KEY is serialized as Text (JSON string), need to deserialize it
        Text keyJson = (Text) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_ADD_KEY);
        EncryptionKeyPB replayKeyPB = GsonUtils.GSON.fromJson(keyJson.toString(), EncryptionKeyPB.class);
        followerKeyMgr.replayAddKey(replayKeyPB);

        // 5. Verify follower state is consistent with master
        EncryptionKey followerKey = followerKeyMgr.getKeyById(kek.id);
        Assertions.assertNotNull(followerKey);
        Assertions.assertEquals(kek.id, followerKey.id);
        Assertions.assertEquals(2, followerKeyMgr.numKeys());
    }

    @Test
    public void testAddKeyEditLogException() throws Exception {
        // 1. Verify initial state
        int initialKeyCount = keyMgr.numKeys();
        Assertions.assertEquals(1, initialKeyCount); // Master key

        // 2. Mock EditLog.logAddKey to throw exception
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logAddKey(any(EncryptionKeyPB.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Save initial state snapshot
        int initialCount = keyMgr.numKeys();

        // 3. Execute generateNewKEK which calls addKey internally and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            keyMgr.generateNewKEK();
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 4. Verify leader memory state remains unchanged after exception
        Assertions.assertEquals(initialCount, keyMgr.numKeys());
        // Verify no new key was added
        Assertions.assertEquals(1, keyMgr.numKeys());
    }
}

