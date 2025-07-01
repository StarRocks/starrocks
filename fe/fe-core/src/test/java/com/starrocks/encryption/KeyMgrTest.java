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
import com.starrocks.metric.MetricRepo;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.proto.EncryptionAlgorithmPB;
import com.starrocks.proto.EncryptionKeyPB;
import com.starrocks.proto.EncryptionKeyTypePB;
import com.starrocks.thrift.TGetKeysRequest;
import com.starrocks.thrift.TGetKeysResponse;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class KeyMgrTest {
    @BeforeAll
    public static void beforeClass() throws Exception {
        MetricRepo.init();
        UtFrameUtils.setUpForPersistTest();
    }

    @AfterAll
    public static void teardown() throws Exception {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testReplayAddKey() {
        KeyMgr keyMgr = new KeyMgr();
        EncryptionKeyPB pb = new EncryptionKeyPB();
        pb.id = 1L;
        pb.algorithm = EncryptionAlgorithmPB.AES_128;
        pb.encryptedKey = new byte[16];
        pb.type = EncryptionKeyTypePB.NORMAL_KEY;
        pb.createTime = 3L;
        keyMgr.replayAddKey(pb);
    }

    @Test
    public void testCheckKeyRotation() {
        String oldConfig = Config.default_master_key;
        try {
            Config.default_master_key = "plain:aes_128:enwSdCUAiCLLx2Bs9E/neQ==";
            KeyMgr keyMgr = new KeyMgr();
            EncryptionKeyPB pb = new EncryptionKeyPB();
            pb.id = 1L;
            pb.algorithm = EncryptionAlgorithmPB.AES_128;
            pb.encryptedKey = new byte[16];
            pb.type = EncryptionKeyTypePB.NORMAL_KEY;
            pb.createTime = 1L;
            keyMgr.replayAddKey(pb);
            Assertions.assertEquals(1, keyMgr.numKeys());
            EncryptionKey root = keyMgr.getKeyById(1);
            byte[] plainKey = new byte[16];
            plainKey[0] = 1;
            plainKey[8] = 1;
            ((NormalKey) root).setPlainKey(plainKey);
            EncryptionKey kek = root.generateKey();
            kek.id = 2;
            EncryptionKeyPB pb2 = new EncryptionKeyPB();
            kek.toPB(pb2, keyMgr);
            // set time to 1 so rotation do happen
            pb2.createTime = 1L;
            keyMgr.replayAddKey(pb2);
            Assertions.assertEquals(2, keyMgr.numKeys());
            keyMgr.checkKeyRotation();
            Assertions.assertEquals(3, keyMgr.numKeys());
        } finally {
            Config.default_master_key = oldConfig;
        }
    }

    @Test
    public void testLoadSaveImageJsonFormat() throws Exception {
        KeyMgr keyMgr = new KeyMgr();
        EncryptionKeyPB pb = new EncryptionKeyPB();
        pb.id = 1L;
        pb.algorithm = EncryptionAlgorithmPB.AES_128;
        pb.encryptedKey = new byte[16];
        pb.type = EncryptionKeyTypePB.NORMAL_KEY;
        pb.createTime = 3L;
        keyMgr.replayAddKey(pb);
        pb.id = 2L;
        keyMgr.replayAddKey(pb);

        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        keyMgr.save(image.getImageWriter());

        KeyMgr keyMgr2 = new KeyMgr();
        SRMetaBlockReader reader = image.getMetaBlockReader();
        keyMgr2.load(reader);
        reader.close();

        Assertions.assertEquals(2, keyMgr2.numKeys());
    }

    @Test
    public void testLoadSaveInitializedKeyMgrJsonFormat() throws Exception {
        String oldConfig = Config.default_master_key;
        try {
            Config.default_master_key = "plain:aes_128:enwSdCUAiCLLx2Bs9E/neQ==";
            KeyMgr keyMgr = new KeyMgr();
            keyMgr.initDefaultMasterKey();

            TGetKeysRequest tGetKeysRequest = new TGetKeysRequest();
            TGetKeysResponse tGetKeysResponse = new TGetKeysResponse();
            tGetKeysResponse = keyMgr.getKeys(tGetKeysRequest);
            Assertions.assertEquals(1, tGetKeysResponse.getKey_metasSize());

            UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
            keyMgr.save(image.getImageWriter());

            KeyMgr keyMgr2 = new KeyMgr();
            SRMetaBlockReader reader = image.getMetaBlockReader();
            keyMgr2.load(reader);
            reader.close();

            Assertions.assertEquals(2, keyMgr2.numKeys());
        } finally {
            Config.default_master_key = oldConfig;
        }
    }

    @Test
    public void testInitDefaultMasterKey() {
        new MockUp<System>() {
            @Mock
            public void exit(int value) {
                throw new RuntimeException(String.valueOf(value));
            }
        };
        String oldConfig = Config.default_master_key;
        try {
            Config.default_master_key = "plain:aes_128:enwSdCUAiCLLx2Bs9E/neQ==";
            KeyMgr keyMgr = new KeyMgr();
            keyMgr.initDefaultMasterKey();
            Assertions.assertEquals(2, keyMgr.numKeys());
            Config.default_master_key = "plain:aes_128:eCsM28LaDORFTZDUMz3y4g==";
            keyMgr.initDefaultMasterKey();
            Assertions.fail("should throw exception");
        } catch (RuntimeException e) {
            Assertions.assertEquals("-1", e.getMessage());
        } finally {
            Config.default_master_key = oldConfig;
        }
    }
}
