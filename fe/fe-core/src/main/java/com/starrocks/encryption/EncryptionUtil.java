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
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Arrays;
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
        if (encodingAesKey.length() != 43) {
            throw new IllegalArgumentException("Invalid encodingAesKey length. encodingAesKey must be 43 characters");
        }

        byte[] aesKey = Base64.getDecoder().decode(encodingAesKey + "=");
        byte[] randomBytes = new byte[16];
        rand.nextBytes(randomBytes);

        byte[] messageBytes = message.getBytes(StandardCharsets.UTF_8);
        int msgLen = messageBytes.length;

        ByteBuffer msgLenBuffer = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
        msgLenBuffer.putInt(msgLen);
        byte[] msgLenBytes = msgLenBuffer.array();

        ByteBuffer fullBuffer = ByteBuffer.allocate(randomBytes.length + msgLenBytes.length + messageBytes.length);
        fullBuffer.put(randomBytes);
        fullBuffer.put(msgLenBytes);
        fullBuffer.put(messageBytes);
        byte[] fullStr = fullBuffer.array();

        byte[] paddedData = pkcs7Padding(fullStr, 32);

        Cipher cipher = Cipher.getInstance("AES/CBC/NoPadding");
        SecretKey secretKey = new SecretKeySpec(aesKey, "AES");
        IvParameterSpec iv = new IvParameterSpec(aesKey, 0, 16);
        cipher.init(Cipher.ENCRYPT_MODE, secretKey, iv);

        byte[] encrypted = cipher.doFinal(paddedData);

        return Base64.getEncoder().encodeToString(encrypted);
    }

    private static byte[] pkcs7Padding(byte[] data, int blockSize) {
        int padding = blockSize - (data.length % blockSize);
        if (padding == 0) {
            padding = blockSize;
        }

        byte[] paddedData = Arrays.copyOf(data, data.length + padding);
        Arrays.fill(paddedData, data.length, paddedData.length, (byte) padding);
        return paddedData;
    }

    public static String aesDecrypt(String encryptedData, String encodingAesKey) throws Exception {
        if (encodingAesKey.length() != 43) {
            throw new IllegalArgumentException("Invalid encodingAesKey length. encodingAesKey must be 43 characters");
        }

        byte[] aesKey = Base64.getDecoder().decode(encodingAesKey + "=");
        byte[] encryptedBytes = Base64.getDecoder().decode(encryptedData);
        if (encryptedBytes.length < 16) {
            throw new IllegalArgumentException("Invalid encrypted data");
        }

        Cipher cipher = Cipher.getInstance("AES/CBC/NoPadding");
        SecretKey secretKey = new SecretKeySpec(aesKey, "AES");
        IvParameterSpec ivSpec = new IvParameterSpec(aesKey, 0, 16);
        cipher.init(Cipher.DECRYPT_MODE, secretKey, ivSpec);

        byte[] decryptedPaddedData = cipher.doFinal(encryptedBytes);
        byte[] decryptedData = pkcs7UnPadding(decryptedPaddedData);

        ByteBuffer buffer = ByteBuffer.wrap(decryptedData).order(ByteOrder.BIG_ENDIAN);

        byte[] randomBytes = new byte[16];
        buffer.get(randomBytes);

        int msgLen = buffer.getInt();
        if (msgLen < 0 || msgLen > buffer.remaining()) {
            throw new IllegalArgumentException("Invalid message length");
        }

        byte[] messageBytes = new byte[msgLen];
        buffer.get(messageBytes);

        return new String(messageBytes, StandardCharsets.UTF_8);
    }

    private static byte[] pkcs7UnPadding(byte[] data) {
        int paddingLength = data[data.length - 1];
        if (paddingLength < 1 || paddingLength > 32) {
            throw new IllegalArgumentException("Invalid PKCS7 padding");
        }
        for (int i = 0; i < paddingLength; i++) {
            if (data[data.length - 1 - i] != paddingLength) {
                throw new IllegalArgumentException("Invalid PKCS7 padding");
            }
        }
        return Arrays.copyOfRange(data, 0, data.length - paddingLength);
    }
}
