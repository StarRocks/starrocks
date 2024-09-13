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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.SecureRandom;
import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

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

    public static String aesEncrypt(String message, String encodingAesKey) throws Exception {
        // 1. Check the length of encodingAesKey before decoding
        if (encodingAesKey.length() != 43) {
            throw new IllegalArgumentException("Invalid encodingAesKey length. encodingAesKey must be 43 characters");
        }

        // 2. Base64 decode the AES key
        byte[] aesKey = Base64.getDecoder().decode(encodingAesKey + "=");

        // 3. Randomly generate a 16-byte string
        byte[] randomBytes = new byte[16];
        rand.nextBytes(randomBytes);

        // 4. Calculate the message length
        byte[] messageBytes = message.getBytes();
        int msgLen = messageBytes.length;

        // 5. Create a 4-byte message length (network byte order - Big Endian)
        ByteBuffer msgLenBuffer = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
        msgLenBuffer.putInt(msgLen);
        byte[] msgLenBytes = msgLenBuffer.array();

        // 6. Concatenate random + msg_len + msg
        ByteBuffer fullBuffer = ByteBuffer.allocate(randomBytes.length + msgLenBytes.length + messageBytes.length);
        fullBuffer.put(randomBytes);        // Put in the 16-byte random string
        fullBuffer.put(msgLenBytes);        // Put in the 4-byte message length
        fullBuffer.put(messageBytes);       // Put in the message content
        byte[] fullStr = fullBuffer.array();

        // 7. Apply PKCS#7 padding
        byte[] paddedData = pkcs7Padding(fullStr, aesKey.length);

        // 8. Initialize AES CBC encryption
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        SecretKey secretKey = new SecretKeySpec(aesKey, "AES");
        IvParameterSpec iv = new IvParameterSpec(aesKey, 0, 16);  // IV is typically the first 16 bytes of AESKey
        cipher.init(Cipher.ENCRYPT_MODE, secretKey, iv);

        // 9. Perform encryption
        byte[] encrypted = cipher.doFinal(paddedData);

        // 10. Base64 encode the encrypted data
        return Base64.getEncoder().encodeToString(encrypted);
    }

    // PKCS#7 padding
    private static byte[] pkcs7Padding(byte[] data, int blockSize) {
        int paddingLength = blockSize - (data.length % blockSize);
        byte[] padding = new byte[paddingLength];
        for (int i = 0; i < paddingLength; i++) {
            padding[i] = (byte) paddingLength;
        }

        // Add the padding to the data
        ByteBuffer paddedBuffer = ByteBuffer.allocate(data.length + paddingLength);
        paddedBuffer.put(data);
        paddedBuffer.put(padding);
        return paddedBuffer.array();
    }
}
