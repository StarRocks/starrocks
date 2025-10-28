// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <cstdint>
#include <string>

namespace starrocks {

// Extended AES encryption modes, supporting more modes including GCM authenticated encryption
enum AesMode {
    // ECB mode
    AES_128_ECB = 0,
    AES_192_ECB = 1,
    AES_256_ECB = 2,
    // CBC mode
    AES_128_CBC = 3,
    AES_192_CBC = 4,
    AES_256_CBC = 5,
    // CFB mode
    AES_128_CFB = 6,
    AES_192_CFB = 7,
    AES_256_CFB = 8,
    AES_128_CFB1 = 9,
    AES_192_CFB1 = 10,
    AES_256_CFB1 = 11,
    AES_128_CFB8 = 12,
    AES_192_CFB8 = 13,
    AES_256_CFB8 = 14,
    AES_128_CFB128 = 15,
    AES_192_CFB128 = 16,
    AES_256_CFB128 = 17,
    // OFB mode
    AES_128_OFB = 18,
    AES_192_OFB = 19,
    AES_256_OFB = 20,
    // CTR mode
    AES_128_CTR = 21,
    AES_192_CTR = 22,
    AES_256_CTR = 23,
    // GCM mode (authenticated encryption)
    AES_128_GCM = 24,
    AES_192_GCM = 25,
    AES_256_GCM = 26,
};

enum AesState { AES_SUCCESS = 0, AES_BAD_DATA = -1 };

class AesUtil {
public:
    // GCM mode TAG size (16 bytes)
    //https://tools.ietf.org/html/rfc5116#section-5.1
    static constexpr int GCM_TAG_SIZE = 16;
    // Default IV
    static constexpr const char* DEFAULT_IV = "STARROCKS_16BYTE";

    // Extended encryption function, supports IV string and AAD (for GCM mode)
    static int encrypt_ex(AesMode mode, const unsigned char* source, uint32_t source_length, const unsigned char* key,
                          uint32_t key_length, const char* iv_str, uint32_t iv_input_length, bool padding,
                          unsigned char* encrypt, const unsigned char* aad = nullptr, uint32_t aad_length = 0);

    // Extended decryption function, supports IV string and AAD (for GCM mode)
    static int decrypt_ex(AesMode mode, const unsigned char* encrypt, uint32_t encrypt_length, const unsigned char* key,
                          uint32_t key_length, const char* iv_str, uint32_t iv_input_length, bool padding,
                          unsigned char* decrypt_content, const unsigned char* aad = nullptr, uint32_t aad_length = 0);

    // Get AES mode from string
    static AesMode get_mode_from_string(const std::string& mode_str);

    // Check if it is GCM mode
    static bool is_gcm_mode(AesMode mode);

    // Check if it is stream mode (CFB/OFB/CTR - no padding needed)
    static bool is_stream_mode(AesMode mode);

    // Check if it is ECB mode (ECB mode does not require IV)
    static bool is_ecb_mode(AesMode mode);
};

} // namespace starrocks
