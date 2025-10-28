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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/aes_util.cpp

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

#include "aes_util.h"

#include <openssl/err.h>
#include <openssl/evp.h>

#include <algorithm>

#include "util/string_util.h"

namespace starrocks {

static const int AES_MAX_KEY_LENGTH = 256;

const EVP_CIPHER* get_evp_type(const AesMode mode) {
    switch (mode) {
    // ECB mode
    case AES_128_ECB:
        return EVP_aes_128_ecb();
    case AES_192_ECB:
        return EVP_aes_192_ecb();
    case AES_256_ECB:
        return EVP_aes_256_ecb();
    // CBC mode
    case AES_128_CBC:
        return EVP_aes_128_cbc();
    case AES_192_CBC:
        return EVP_aes_192_cbc();
    case AES_256_CBC:
        return EVP_aes_256_cbc();
    // CFB mode
    case AES_128_CFB:
        return EVP_aes_128_cfb();
    case AES_192_CFB:
        return EVP_aes_192_cfb();
    case AES_256_CFB:
        return EVP_aes_256_cfb();
    case AES_128_CFB1:
        return EVP_aes_128_cfb1();
    case AES_192_CFB1:
        return EVP_aes_192_cfb1();
    case AES_256_CFB1:
        return EVP_aes_256_cfb1();
    case AES_128_CFB8:
        return EVP_aes_128_cfb8();
    case AES_192_CFB8:
        return EVP_aes_192_cfb8();
    case AES_256_CFB8:
        return EVP_aes_256_cfb8();
    case AES_128_CFB128:
        return EVP_aes_128_cfb128();
    case AES_192_CFB128:
        return EVP_aes_192_cfb128();
    case AES_256_CFB128:
        return EVP_aes_256_cfb128();
    // OFB mode
    case AES_128_OFB:
        return EVP_aes_128_ofb();
    case AES_192_OFB:
        return EVP_aes_192_ofb();
    case AES_256_OFB:
        return EVP_aes_256_ofb();
    // CTR mode
    case AES_128_CTR:
        return EVP_aes_128_ctr();
    case AES_192_CTR:
        return EVP_aes_192_ctr();
    case AES_256_CTR:
        return EVP_aes_256_ctr();
    // GCM mode
    case AES_128_GCM:
        return EVP_aes_128_gcm();
    case AES_192_GCM:
        return EVP_aes_192_gcm();
    case AES_256_GCM:
        return EVP_aes_256_gcm();
    default:
        return nullptr;
    }
}

// Extended key length array, supporting all modes
static uint aes_mode_key_sizes[] = {
        128, 192, 256, // ECB
        128, 192, 256, // CBC
        128, 192, 256, // CFB
        128, 192, 256, // CFB1
        128, 192, 256, // CFB8
        128, 192, 256, // CFB128
        128, 192, 256, // OFB
        128, 192, 256, // CTR
        128, 192, 256, // GCM
};

static void aes_create_key(const unsigned char* origin_key, uint32_t key_length, uint8_t* encrypt_key, AesMode mode) {
    const uint key_size = aes_mode_key_sizes[mode] / 8;
    uint8_t* origin_key_end = ((uint8_t*)origin_key) + key_length; /* origin key boundary*/

    uint8_t* encrypt_key_end; /* encrypt key boundary */
    encrypt_key_end = encrypt_key + key_size;

    std::memset(encrypt_key, 0, key_size); /* initialize key  */

    uint8_t* ptr;        /* Start of the encrypt key*/
    uint8_t* origin_ptr; /* Start of the origin key */
    for (ptr = encrypt_key, origin_ptr = (uint8_t*)origin_key; origin_ptr < origin_key_end; ptr++, origin_ptr++) {
        if (ptr == encrypt_key_end) {
            /* loop over origin key until we used all key */
            ptr = encrypt_key;
        }
        *ptr ^= *origin_ptr;
    }
}

static int do_encrypt(EVP_CIPHER_CTX* aes_ctx, const EVP_CIPHER* cipher, const unsigned char* source,
                      uint32_t source_length, const unsigned char* encrypt_key, const unsigned char* iv, bool padding,
                      unsigned char* encrypt, int* length_ptr) {
    int ret = EVP_EncryptInit(aes_ctx, cipher, encrypt_key, iv);
    if (ret == 0) {
        return ret;
    }
    ret = EVP_CIPHER_CTX_set_padding(aes_ctx, padding);
    if (ret == 0) {
        return ret;
    }
    int u_len = 0;
    ret = EVP_EncryptUpdate(aes_ctx, encrypt, &u_len, source, source_length);
    if (ret == 0) {
        return ret;
    }
    int f_len = 0;
    ret = EVP_EncryptFinal(aes_ctx, encrypt + u_len, &f_len);
    *length_ptr = u_len + f_len;
    return ret;
}

// GCM mode specific encryption function
static int do_gcm_encrypt(EVP_CIPHER_CTX* cipher_ctx, const EVP_CIPHER* cipher, const unsigned char* source,
                          uint32_t source_length, const unsigned char* encrypt_key, const unsigned char* iv,
                          int iv_length, unsigned char* encrypt, int* length_ptr, const unsigned char* aad,
                          uint32_t aad_length) {
    // 1. Initialize and set IV length
    int ret = EVP_EncryptInit_ex(cipher_ctx, cipher, nullptr, nullptr, nullptr);
    if (ret == 0) {
        return ret;
    }
    ret = EVP_CIPHER_CTX_ctrl(cipher_ctx, EVP_CTRL_GCM_SET_IVLEN, iv_length, nullptr);
    if (ret == 0) {
        return ret;
    }

    // 2. Set key and IV
    ret = EVP_EncryptInit_ex(cipher_ctx, nullptr, nullptr, encrypt_key, iv);
    if (ret == 0) {
        return ret;
    }

    // 3. Process AAD (Additional Authenticated Data)
    int tmp_len = 0;
    if (aad != nullptr && aad_length > 0) {
        ret = EVP_EncryptUpdate(cipher_ctx, nullptr, &tmp_len, aad, aad_length);
        if (ret == 0) {
            return ret;
        }
    }

    // 4. Encrypt data
    int u_len = 0;
    ret = EVP_EncryptUpdate(cipher_ctx, encrypt + iv_length, &u_len, source, source_length);
    if (ret == 0) {
        return ret;
    }

    // 5. Finalize encryption
    int f_len = 0;
    ret = EVP_EncryptFinal_ex(cipher_ctx, encrypt + iv_length + u_len, &f_len);
    if (ret == 0) {
        return ret;
    }

    // 6. Get authentication TAG
    ret = EVP_CIPHER_CTX_ctrl(cipher_ctx, EVP_CTRL_GCM_GET_TAG, AesUtil::GCM_TAG_SIZE,
                              encrypt + iv_length + u_len + f_len);
    if (ret == 0) {
        return ret;
    }

    // 7. Output format: [IV][Ciphertext][TAG]
    std::memcpy(encrypt, iv, iv_length);
    *length_ptr = iv_length + u_len + f_len + AesUtil::GCM_TAG_SIZE;
    return ret;
}

// Extended encryption function, supports IV string and AAD
int AesUtil::encrypt_ex(AesMode mode, const unsigned char* source, uint32_t source_length, const unsigned char* key,
                        uint32_t key_length, const char* iv_str, uint32_t iv_input_length, bool padding,
                        unsigned char* encrypt, const unsigned char* aad, uint32_t aad_length) {
    const EVP_CIPHER* cipher = get_evp_type(mode);
    if (cipher == nullptr) {
        return AES_BAD_DATA;
    }

    // Create encryption key
    unsigned char encrypt_key[AES_MAX_KEY_LENGTH / 8];
    aes_create_key(key, key_length, encrypt_key, mode);

    // Process IV
    int iv_length = EVP_CIPHER_iv_length(cipher);
    std::string iv_default(DEFAULT_IV);
    unsigned char init_vec[EVP_MAX_IV_LENGTH];
    std::memset(init_vec, 0, EVP_MAX_IV_LENGTH);

    if (iv_length > 0) {
        if (iv_str != nullptr && iv_input_length > 0) {
            int copy_len = std::min((int)iv_input_length, iv_length);
            std::memcpy(init_vec, iv_str, copy_len);

        } else {
            // Use default IV
            int copy_len = std::min((int)iv_default.size(), iv_length);
            std::memcpy(init_vec, iv_default.c_str(), copy_len);
        }
    }

    EVP_CIPHER_CTX* aes_ctx = EVP_CIPHER_CTX_new();
    int length = 0;
    int ret = 0;

    // GCM mode uses dedicated function
    if (is_gcm_mode(mode)) {
        ret = do_gcm_encrypt(aes_ctx, cipher, source, source_length, encrypt_key, init_vec, iv_length, encrypt, &length,
                             aad, aad_length);
    } else {
        // Stream modes (CFB/OFB/CTR) don't need padding, only block modes (ECB/CBC) do
        bool use_padding = is_stream_mode(mode) ? false : padding;
        ret = do_encrypt(aes_ctx, cipher, source, source_length, encrypt_key, iv_length > 0 ? init_vec : nullptr,
                         use_padding, encrypt, &length);
    }

    EVP_CIPHER_CTX_free(aes_ctx);
    if (ret == 0) {
        ERR_clear_error();
        return AES_BAD_DATA;
    }
    return length;
}

static int do_decrypt(EVP_CIPHER_CTX* aes_ctx, const EVP_CIPHER* cipher, const unsigned char* encrypt,
                      uint32_t encrypt_length, const unsigned char* encrypt_key, const unsigned char* iv, bool padding,
                      unsigned char* decrypt_content, int* length_ptr) {
    int ret = EVP_DecryptInit(aes_ctx, cipher, encrypt_key, iv);
    if (ret == 0) {
        return ret;
    }
    ret = EVP_CIPHER_CTX_set_padding(aes_ctx, padding);
    if (ret == 0) {
        return ret;
    }
    int u_len = 0;
    ret = EVP_DecryptUpdate(aes_ctx, decrypt_content, &u_len, encrypt, encrypt_length);
    if (ret == 0) {
        return ret;
    }
    int f_len = 0;
    ret = EVP_DecryptFinal_ex(aes_ctx, decrypt_content + u_len, &f_len);
    *length_ptr = u_len + f_len;
    return ret;
}

// GCM mode specific decryption function
static int do_gcm_decrypt(EVP_CIPHER_CTX* cipher_ctx, const EVP_CIPHER* cipher, const unsigned char* encrypt,
                          uint32_t encrypt_length, const unsigned char* encrypt_key, int iv_length,
                          unsigned char* decrypt_content, int* length_ptr, const unsigned char* aad,
                          uint32_t aad_length) {
    // GCM format: [IV][Ciphertext][TAG]
    if (encrypt_length < (uint32_t)(iv_length + AesUtil::GCM_TAG_SIZE)) {
        return 0; // Insufficient data length
    }

    const unsigned char* iv = encrypt;
    const unsigned char* ciphertext = encrypt + iv_length;
    int ciphertext_len = encrypt_length - iv_length - AesUtil::GCM_TAG_SIZE;
    const unsigned char* tag = encrypt + encrypt_length - AesUtil::GCM_TAG_SIZE;

    // 1. Initialize and set IV length
    int ret = EVP_DecryptInit_ex(cipher_ctx, cipher, nullptr, nullptr, nullptr);
    if (ret == 0) {
        return ret;
    }
    ret = EVP_CIPHER_CTX_ctrl(cipher_ctx, EVP_CTRL_GCM_SET_IVLEN, iv_length, nullptr);
    if (ret == 0) {
        return ret;
    }

    // 2. Set key and IV
    ret = EVP_DecryptInit_ex(cipher_ctx, nullptr, nullptr, encrypt_key, iv);
    if (ret == 0) {
        return ret;
    }

    // 3. Process AAD
    int tmp_len = 0;
    if (aad != nullptr && aad_length > 0) {
        ret = EVP_DecryptUpdate(cipher_ctx, nullptr, &tmp_len, aad, aad_length);
        if (ret == 0) {
            return ret;
        }
    }

    // 4. Decrypt data
    int u_len = 0;
    ret = EVP_DecryptUpdate(cipher_ctx, decrypt_content, &u_len, ciphertext, ciphertext_len);
    if (ret == 0) {
        return ret;
    }

    // 5. Set TAG for verification
    ret = EVP_CIPHER_CTX_ctrl(cipher_ctx, EVP_CTRL_GCM_SET_TAG, AesUtil::GCM_TAG_SIZE, (void*)tag);
    if (ret == 0) {
        return ret;
    }

    // 6. Finalize decryption and verify TAG
    int f_len = 0;
    ret = EVP_DecryptFinal_ex(cipher_ctx, decrypt_content + u_len, &f_len);
    if (ret > 0) {
        *length_ptr = u_len + f_len;
    }
    return ret;
}

// Extended decryption function, supports IV string and AAD
int AesUtil::decrypt_ex(AesMode mode, const unsigned char* encrypt, uint32_t encrypt_length, const unsigned char* key,
                        uint32_t key_length, const char* iv_str, uint32_t iv_input_length, bool padding,
                        unsigned char* decrypt_content, const unsigned char* aad, uint32_t aad_length) {
    const EVP_CIPHER* cipher = get_evp_type(mode);
    if (cipher == nullptr) {
        return AES_BAD_DATA;
    }

    // Create encryption key
    unsigned char encrypt_key[AES_MAX_KEY_LENGTH / 8];
    aes_create_key(key, key_length, encrypt_key, mode);

    // Process IV
    int iv_length = EVP_CIPHER_iv_length(cipher);
    std::string iv_default(DEFAULT_IV);
    unsigned char init_vec[EVP_MAX_IV_LENGTH];
    std::memset(init_vec, 0, EVP_MAX_IV_LENGTH);

    if (iv_length > 0) {
        if (iv_str != nullptr && iv_input_length > 0) {
            int copy_len = std::min((int)iv_input_length, iv_length);
            std::memcpy(init_vec, iv_str, copy_len);

        } else {
            // Use default IV
            int copy_len = std::min((int)iv_default.size(), iv_length);
            std::memcpy(init_vec, iv_default.c_str(), copy_len);
        }
    }

    EVP_CIPHER_CTX* aes_ctx = EVP_CIPHER_CTX_new();
    int length = 0;
    int ret = 0;

    // GCM mode uses dedicated function
    if (is_gcm_mode(mode)) {
        ret = do_gcm_decrypt(aes_ctx, cipher, encrypt, encrypt_length, encrypt_key, iv_length, decrypt_content, &length,
                             aad, aad_length);
    } else {
        // Stream modes (CFB/OFB/CTR) don't need padding, only block modes (ECB/CBC) do
        bool use_padding = is_stream_mode(mode) ? false : padding;
        ret = do_decrypt(aes_ctx, cipher, encrypt, encrypt_length, encrypt_key, iv_length > 0 ? init_vec : nullptr,
                         use_padding, decrypt_content, &length);
    }

    EVP_CIPHER_CTX_free(aes_ctx);
    if (ret > 0) {
        return length;
    } else {
        ERR_clear_error();
        return AES_BAD_DATA;
    }
}

// AES mode mapping table with case-insensitive lookup
inline const StringCaseUnorderedMap<AesMode> AES_MODE_MAP = {
        // ECB modes
        {"AES_128_ECB", AES_128_ECB},
        {"AES_192_ECB", AES_192_ECB},
        {"AES_256_ECB", AES_256_ECB},
        // CBC modes
        {"AES_128_CBC", AES_128_CBC},
        {"AES_192_CBC", AES_192_CBC},
        {"AES_256_CBC", AES_256_CBC},
        // CFB modes
        {"AES_128_CFB", AES_128_CFB},
        {"AES_192_CFB", AES_192_CFB},
        {"AES_256_CFB", AES_256_CFB},
        {"AES_128_CFB1", AES_128_CFB1},
        {"AES_192_CFB1", AES_192_CFB1},
        {"AES_256_CFB1", AES_256_CFB1},
        {"AES_128_CFB8", AES_128_CFB8},
        {"AES_192_CFB8", AES_192_CFB8},
        {"AES_256_CFB8", AES_256_CFB8},
        {"AES_128_CFB128", AES_128_CFB128},
        {"AES_192_CFB128", AES_192_CFB128},
        {"AES_256_CFB128", AES_256_CFB128},
        // OFB modes
        {"AES_128_OFB", AES_128_OFB},
        {"AES_192_OFB", AES_192_OFB},
        {"AES_256_OFB", AES_256_OFB},
        // CTR modes
        {"AES_128_CTR", AES_128_CTR},
        {"AES_192_CTR", AES_192_CTR},
        {"AES_256_CTR", AES_256_CTR},
        // GCM modes
        {"AES_128_GCM", AES_128_GCM},
        {"AES_192_GCM", AES_192_GCM},
        {"AES_256_GCM", AES_256_GCM},
};

// Get AES mode from string (case-insensitive)
AesMode AesUtil::get_mode_from_string(const std::string& mode_str) {
    auto it = AES_MODE_MAP.find(mode_str);
    if (it != AES_MODE_MAP.end()) {
        return it->second;
    }
    // Default return AES_128_ECB
    return AES_128_ECB;
}

// Check if it is GCM mode
bool AesUtil::is_gcm_mode(AesMode mode) {
    return mode >= AES_128_GCM && mode <= AES_256_GCM;
}

// Check if it is stream mode (CFB/OFB/CTR - no padding needed)
bool AesUtil::is_stream_mode(AesMode mode) {
    // Stream modes: CFB (all variants), OFB, CTR
    return (mode >= AES_128_CFB && mode <= AES_256_CFB128) || (mode >= AES_128_OFB && mode <= AES_256_OFB) ||
           (mode >= AES_128_CTR && mode <= AES_256_CTR);
}

// Check if it is ECB mode (ECB mode does not require IV)
bool AesUtil::is_ecb_mode(AesMode mode) {
    return mode >= AES_128_ECB && mode <= AES_256_ECB;
}

} // namespace starrocks