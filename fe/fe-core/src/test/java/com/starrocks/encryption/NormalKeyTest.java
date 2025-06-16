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

import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.proto.EncryptionAlgorithmPB;
import com.starrocks.proto.EncryptionKeyPB;
import com.starrocks.proto.EncryptionKeyTypePB;
import org.junit.Before;
import org.junit.Test;

import java.util.Base64;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

public class NormalKeyTest {
    private NormalKey normalKey;

    @Before
    public void setup() {
        normalKey = NormalKey.createRandom();
        normalKey.setId(111);
    }

    @Test
    public void testJsonSerde() {
        EncryptionKeyPB pb = new EncryptionKeyPB();
        pb.id = 2L;
        pb.parentId = 1L;
        pb.algorithm = EncryptionAlgorithmPB.AES_128;
        pb.encryptedKey = new byte[16];
        pb.type = EncryptionKeyTypePB.NORMAL_KEY;
        pb.createTime = 3L;
        pb.keyDesc = "desc";
        String js = GsonUtils.GSON.toJson(pb);
        EncryptionKeyPB pb2 = GsonUtils.GSON.fromJson(js, EncryptionKeyPB.class);
        assertEquals(pb.id, pb2.id);
        assertEquals(pb.parentId, pb2.parentId);
        assertEquals(pb.algorithm, pb2.algorithm);
        assertEquals(pb.createTime, pb2.createTime);
        assertEquals(pb.type, pb2.type);
        assertEquals(pb.keyDesc, pb2.keyDesc);
        assertArrayEquals(pb.encryptedKey, pb2.encryptedKey);
    }

    @Test
    public void testCreateRandom() {
        NormalKey key = NormalKey.createRandom();
        assertNotNull(key);
        assertNotNull(key.getPlainKey());
        assertEquals(EncryptionAlgorithmPB.AES_128, key.getAlgorithm());
    }

    @Test
    public void testCreateFromSpec() {
        String base64Key = Base64.getEncoder().encodeToString(normalKey.getPlainKey());
        String spec = "AES_128:" + base64Key;
        NormalKey key = NormalKey.createFromSpec(spec);
        assertNotNull(key);
        assertEquals(EncryptionAlgorithmPB.AES_128, key.getAlgorithm());
        assertArrayEquals(normalKey.getPlainKey(), key.getPlainKey());
    }

    @Test
    public void testToSpec() {
        String base64Key = Base64.getEncoder().encodeToString(normalKey.getPlainKey());
        String expectedSpec = "plain:aes_128:" + base64Key;
        assertEquals(expectedSpec, normalKey.toSpec());
    }

    @Test
    public void testWrapAndUnwrapKey() {
        byte[] plainKeyToWrap = normalKey.getPlainKey();
        byte[] wrappedKey = normalKey.wrapKey(EncryptionAlgorithmPB.AES_128, plainKeyToWrap);
        assertNotNull(wrappedKey);

        byte[] unwrappedKey = normalKey.unwrapKey(EncryptionAlgorithmPB.AES_128, wrappedKey);
        assertArrayEquals(plainKeyToWrap, unwrappedKey);
    }

    @Test
    public void testGenerateKey() {
        NormalKey newKey = (NormalKey) normalKey.generateKey();
        assertNotNull(newKey);
        assertNotNull(newKey.getEncryptedKey());
        assertEquals(normalKey, newKey.getParent());
    }

    @Test
    public void testDecryptKey() {
        NormalKey newKey = (NormalKey) normalKey.generateKey();
        byte[] plainKey = newKey.getPlainKey();
        newKey.setPlainKey(null);
        normalKey.decryptKey(newKey);
        assertArrayEquals(newKey.getPlainKey(), plainKey);
    }

    @Test
    public void testToPB() {
        EncryptionKeyPB pb = new EncryptionKeyPB();
        KeyMgr mgr = new KeyMgr();
        normalKey.toPB(pb, mgr);

        assertEquals(normalKey.getId(), pb.id.longValue());
        assertEquals(normalKey.getCreateTime(), pb.createTime.longValue());
        assertEquals(normalKey.getAlgorithm(), pb.algorithm);
        assertArrayEquals(normalKey.getEncryptedKey(), pb.encryptedKey);
    }

    @Test
    public void testFromPB() {
        EncryptionKeyPB pb = new EncryptionKeyPB();
        pb.id = 12345L;
        pb.createTime = System.currentTimeMillis();
        pb.algorithm = EncryptionAlgorithmPB.AES_128;
        pb.encryptedKey = ((NormalKey) normalKey.generateKey()).getEncryptedKey();

        NormalKey key = new NormalKey();
        KeyMgr mgr = new KeyMgr();
        key.fromPB(pb, mgr);

        assertEquals(pb.id.longValue(), key.getId());
        assertEquals(pb.createTime.longValue(), key.getCreateTime());
        assertEquals(pb.algorithm, key.getAlgorithm());
        assertArrayEquals(pb.encryptedKey, key.getEncryptedKey());
    }

    @Test
    public void testEquals() {
        NormalKey key1 = NormalKey.createRandom();
        NormalKey key2 = NormalKey.createRandom();
        assertNotEquals(key1, new String());
        assertNotEquals(key1, key2);
        NormalKey keyNoAlgo = new NormalKey(EncryptionAlgorithmPB.NO_ENCRYPTION, new byte[16], null);
        assertNotEquals(key1, keyNoAlgo);
        NormalKey key3 = (NormalKey) EncryptionKey.createFromSpec(key1.toSpec());
        assertEquals(key1, key3);
        assertEquals(key1.hashCode(), key3.hashCode());
        assertArrayEquals(key1.getPlainKey(), key3.getPlainKey());
        NormalKey keyEn1 = new NormalKey(EncryptionAlgorithmPB.AES_128, null, new byte[16]);
        NormalKey keyEn2 = new NormalKey(EncryptionAlgorithmPB.AES_128, null, new byte[16]);
        assertEquals(keyEn1, keyEn2);
        assertEquals(keyEn1.hashCode(), keyEn2.hashCode());
    }

    @Test
    public void testToString() {
        String expected = String.format("NormalKey(id:%d createTime:%d)", normalKey.getId(), normalKey.getCreateTime());
        assertEquals(expected, normalKey.toString());
    }

    @Test
    public void testCreateFromSpec_InvalidSpecFormat() {
        assertThrows(IllegalArgumentException.class, () -> {
            NormalKey.createFromSpec("invalid_spec_format");
        });
    }

    @Test
    public void testCreateFromSpec_InvalidBase64Key() {
        assertThrows(IllegalArgumentException.class, () -> {
            NormalKey.createFromSpec("AES_128:invalid_base64");
        });
    }

    @Test
    public void testCreateFromSpec_InvalidKeyLength() {
        assertThrows(IllegalArgumentException.class, () -> {
            NormalKey.createFromSpec("AES_128:" + Base64.getEncoder().encodeToString(new byte[10]));
        });
    }

    @Test
    public void testWrapKey_UnsupportedAlgorithm() {
        NormalKey key = NormalKey.createRandom(EncryptionAlgorithmPB.AES_128);
        assertThrows(IllegalArgumentException.class, () -> {
            key.wrapKey(EncryptionAlgorithmPB.NO_ENCRYPTION, new byte[16]);
        });
    }

    @Test
    public void testUnwrapKey_UnsupportedAlgorithm() {
        NormalKey key = NormalKey.createRandom(EncryptionAlgorithmPB.AES_128);
        assertThrows(IllegalArgumentException.class, () -> {
            key.unwrapKey(EncryptionAlgorithmPB.NO_ENCRYPTION, new byte[16]);
        });
    }

    @Test
    public void testDecryptKey_InvalidKeyType() {
        NormalKey key = NormalKey.createRandom(EncryptionAlgorithmPB.AES_128);
        EncryptionKey invalidKey = new EncryptionKey() {
            @Override
            public EncryptionKey generateKey() {
                return null;
            }

            @Override
            public void decryptKey(EncryptionKey key) {
            }

            @Override
            public String toSpec() {
                return null;
            }
        };
        assertThrows(IllegalArgumentException.class, () -> {
            key.decryptKey(invalidKey);
        });
    }

    @Test
    public void testFromPB_NoAlgorithm() {
        NormalKey key = new NormalKey();
        EncryptionKeyPB pb = new EncryptionKeyPB();
        pb.encryptedKey = new byte[16];
        KeyMgr mgr = new KeyMgr();
        assertThrows(IllegalArgumentException.class, () -> {
            key.fromPB(pb, mgr);
        });
    }

    @Test
    public void testFromPB_NoEncryptedKey() {
        NormalKey key = new NormalKey();
        EncryptionKeyPB pb = new EncryptionKeyPB();
        pb.algorithm = EncryptionAlgorithmPB.AES_128;
        KeyMgr mgr = new KeyMgr();
        assertThrows(IllegalArgumentException.class, () -> {
            key.fromPB(pb, mgr);
        });
    }

    @Test
    public void testFromPB_NoParent() {
        NormalKey key = new NormalKey();
        EncryptionKeyPB pb = new EncryptionKeyPB();
        pb.parentId = 1L;
        pb.algorithm = EncryptionAlgorithmPB.AES_128;
        pb.encryptedKey = new byte[16];
        KeyMgr mgr = new KeyMgr();
        assertThrows(IllegalArgumentException.class, () -> {
            key.fromPB(pb, mgr);
        });
    }

}
