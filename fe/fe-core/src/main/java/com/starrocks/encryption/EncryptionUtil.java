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
import org.apache.parquet.crypto.AesGcmDecryptor;
import org.apache.parquet.crypto.AesGcmEncryptor;
import org.apache.parquet.crypto.AesMode;
import org.apache.parquet.crypto.ModuleCipherFactory;

import java.security.SecureRandom;

public class EncryptionUtil {

    static SecureRandom rand = new SecureRandom();

    static byte[] genRandomKey(int len) {
        byte[] ret = new byte[len];
        rand.nextBytes(ret);
        return ret;
    }

    static byte[] wrapKey(byte[] parentPlainKey, EncryptionAlgorithmPB algorithm, byte[] plainKey) {
        switch (algorithm) {
            case AES_128:
                AesGcmEncryptor keyEncryptor =
                        (AesGcmEncryptor) ModuleCipherFactory.getEncryptor(AesMode.GCM, parentPlainKey);
                return keyEncryptor.encrypt(false, plainKey, null);
            default:
                throw new IllegalArgumentException("Unsupported encryption algorithm:" + algorithm);
        }
    }

    static byte[] unwrapKey(byte[] parentPlainKey, EncryptionAlgorithmPB algorithm, byte[] encryptedKey) {
        switch (algorithm) {
            case AES_128:
                AesGcmDecryptor keyDecryptor =
                        (AesGcmDecryptor) ModuleCipherFactory.getDecryptor(AesMode.GCM, parentPlainKey);
                return keyDecryptor.decrypt(encryptedKey, 0, encryptedKey.length, null);
            default:
                throw new IllegalArgumentException("Unsupported encryption algorithm:" + algorithm);
        }
    }
}
