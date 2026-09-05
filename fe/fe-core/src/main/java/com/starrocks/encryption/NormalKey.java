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

import com.starrocks.proto.EncryptionAlgorithmPB;
import com.starrocks.proto.EncryptionKeyPB;
import com.starrocks.proto.EncryptionKeyTypePB;

import java.util.Arrays;
import java.util.Base64;

public class NormalKey extends EncryptionKey {
    private EncryptionAlgorithmPB algorithm;
    private byte[] plainKey; // can be null means no plain key yet, need decryption
    private byte[] encryptedKey; // can be null

    public static NormalKey createRandom(EncryptionAlgorithmPB algorithm) {
        byte[] plainKey;
        if (algorithm == EncryptionAlgorithmPB.AES_128) {
            plainKey = EncryptionUtil.genRandomKey(16);
        } else {
            throw new IllegalArgumentException("Unsupported algorithm: " + algorithm);
        }
        return new NormalKey(algorithm, plainKey, null);
    }

    public static NormalKey createRandom() {
        return createRandom(EncryptionAlgorithmPB.AES_128);
    }

    /**
     * create a key by key spec string with predefined format
     *
     * @param spec [algorithm]:[base64 plain_key]
     * @return created key
     */
    public static NormalKey createFromSpec(String spec) {
        if (spec == null || !spec.contains(":")) {
            throw new IllegalArgumentException("Invalid spec format");
        }

        String[] parts = spec.split(":", 2);
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid spec format");
        }

        String algorithmName = parts[0];
        String base64Key = parts[1];

        EncryptionAlgorithmPB algorithm;
        if (algorithmName.equalsIgnoreCase("AES_128")) {
            algorithm = EncryptionAlgorithmPB.AES_128;
        } else {
            throw new IllegalArgumentException("Unsupported algorithm: " + algorithmName);
        }

        byte[] plainKey;
        try {
            plainKey = Base64.getDecoder().decode(base64Key);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid Base64 key", e);
        }

        if (plainKey.length != 16) {
            throw new IllegalArgumentException("Invalid key length " + plainKey.length * 8);
        }

        return new NormalKey(algorithm, plainKey, null);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof NormalKey)) {
            return false;
        }
        NormalKey rhs = (NormalKey) obj;
        if (algorithm != rhs.algorithm) {
            return false;
        }
        if (plainKey != null) {
            return Arrays.equals(plainKey, rhs.plainKey);
        } else {
            return Arrays.equals(encryptedKey, rhs.encryptedKey);
        }
    }

    @Override
    public int hashCode() {
        if (plainKey != null) {
            return Arrays.hashCode(plainKey) ^ algorithm.value();
        } else {
            return Arrays.hashCode(encryptedKey) ^ algorithm.value();
        }
    }

    @Override
    public String toSpec() {
        return String.format("plain:%s:%s", algorithm.toString().toLowerCase(), Base64.getEncoder().encodeToString(plainKey));
    }

    public NormalKey() {
    }

    public NormalKey(EncryptionAlgorithmPB algorithm, byte[] plainKey, byte[] encryptedKey) {
        this.algorithm = algorithm;
        this.plainKey = plainKey;
        this.encryptedKey = encryptedKey;
    }

    public EncryptionAlgorithmPB getAlgorithm() {
        return algorithm;
    }

    public byte[] getPlainKey() {
        return plainKey;
    }

    public void setPlainKey(byte[] plainKey) {
        this.plainKey = plainKey;
    }

    public byte[] getEncryptedKey() {
        return encryptedKey;
    }

    public void setEncryptedKey(byte[] encryptedKey) {
        this.encryptedKey = encryptedKey;
    }

    public byte[] wrapKey(EncryptionAlgorithmPB algorithm, byte[] plainKey) {
        return EncryptionUtil.wrapKey(this.plainKey, algorithm, plainKey);
    }

    public byte[] unwrapKey(EncryptionAlgorithmPB algorithm, byte[] encryptedKey) {
        return EncryptionUtil.unwrapKey(this.plainKey, algorithm, encryptedKey);
    }

    @Override
    public void toPB(EncryptionKeyPB pb, KeyMgr mgr) {
        super.toPB(pb, mgr);
        pb.type = EncryptionKeyTypePB.NORMAL_KEY;
        pb.algorithm = algorithm;
        if (encryptedKey == null && plainKey != null) {
            // it's a plain master key
            pb.plainKey = plainKey;
        } else {
            pb.encryptedKey = encryptedKey;
        }
    }

    @Override
    public void fromPB(EncryptionKeyPB pb, KeyMgr mgr) {
        super.fromPB(pb, mgr);
        if (pb.algorithm == null) {
            throw new IllegalArgumentException("no algorithm in EncryptionKeyPB for NormalKey id:" + id);
        }
        algorithm = pb.algorithm;
        if (pb.plainKey != null) {
            plainKey = pb.plainKey;
        } else if (pb.encryptedKey != null) {
            encryptedKey = pb.encryptedKey;
        } else {
            throw new IllegalArgumentException("no encryptedKey in EncryptionKeyPB for NormalKey id:" + id);
        }
    }

    @Override
    public EncryptionKey generateKey() {
        NormalKey ret = createRandom(this.algorithm);
        ret.setEncryptedKey(wrapKey(this.algorithm, ret.getPlainKey()));
        ret.setParent(this);
        return ret;
    }

    @Override
    public void decryptKey(EncryptionKey key) {
        assert plainKey != null;
        if (!(key instanceof NormalKey)) {
            throw new IllegalArgumentException("NormalKey cannot not decrypt " + key.getClass().getName());
        }
        NormalKey normalKey = (NormalKey) key;
        normalKey.setPlainKey(unwrapKey(algorithm, normalKey.getEncryptedKey()));
    }

    @Override
    public String toString() {
        return String.format("NormalKey(id:%d createTime:%d)", id, createTime);
    }
}
