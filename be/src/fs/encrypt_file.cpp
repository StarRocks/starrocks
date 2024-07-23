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

#include <openssl/crypto.h>
#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/rand.h>
#include <openssl/ssl.h>
#if OPENSSL_VERSION_NUMBER >= 0x30000000L
#include <openssl/types.h>
#endif
#include <bvar/bvar.h>

#include "common/config.h"
#include "fmt/format.h"
#include "fs/encrypt_file.h"
#include "gutil/endian.h"
#include "io/input_stream.h"
#include "util/defer_op.h"

#ifdef __x86_64__
extern "C" unsigned int OPENSSL_ia32cap_P[];
#endif

namespace starrocks {

bool openssl_supports_aesni() {
#ifdef OPENSSL_INIT_ENGINE_ALL_BUILTIN
    // Initialize ciphers and random engines
    if (!OPENSSL_init_crypto(OPENSSL_INIT_ENGINE_ALL_BUILTIN | OPENSSL_INIT_ADD_ALL_CIPHERS, nullptr)) {
        LOG(FATAL) << "OpenSSL initialization failed";
    }
#endif
#ifdef __x86_64__
    return OPENSSL_ia32cap_P[1] & (1 << (57 - 32));
#else
    return false;
#endif
}

bvar::Adder<int64_t> g_encryption_bytes("encryption_bytes");
bvar::Adder<int64_t> g_decryption_bytes("decryption_bytes");

bvar::PerSecond<bvar::Adder<int64_t>> g_encryption_bytes_second("encryption_bytes_second", &g_encryption_bytes);
bvar::PerSecond<bvar::Adder<int64_t>> g_decryption_bytes_second("decryption_bytes_second", &g_decryption_bytes);

const EVP_CIPHER* get_evp_cipher(const FileEncryptionInfo& info) {
    switch (info.algorithm) {
    case EncryptionAlgorithmPB::AES_128:
        if (info.key.size() != 16) {
            LOG(WARNING) << "key size for AES_128 is not 128: " << info.key.size() * 8;
            return nullptr;
        }
        return EVP_aes_128_ctr();
    default:
        return nullptr;
    }
}

// copy from kudu/src/kudu/util/env_posix.cc
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

std::string get_openssl_errors() {
    std::ostringstream serr;
    unsigned long l;
    int line, flags;
    const char *file, *data;
    char buf[256];
    while ((l = ERR_get_error_line_data(&file, &line, &data, &flags)) != 0) {
        ERR_error_string_n(l, buf, sizeof(buf));
        serr << " " << buf << ":" << file << ":" << line << ((flags & ERR_TXT_STRING) ? data : "");
    }
    return serr.str();
}

#define OPENSSL_RET_NOT_OK(call, msg)                                                  \
    if ((call) <= 0) {                                                                 \
        return Status::InternalError(fmt::format("{} {}", msg, get_openssl_errors())); \
    }

const uint8_t kEncryptionBlockSize = 16;

// Encrypts the data in 'cleartext' and writes it to 'ciphertext'. It requires
// 'offset' to be set in the file as it's used to set the initialization vector.
Status DoEncryptV(const FileEncryptionInfo& eh, uint64_t offset, const Slice* cleartext, Slice* ciphertext, size_t n) {
    // Set the initialization vector based on the offset.
    uint8_t iv[16];
    *reinterpret_cast<uint64*>(&iv[0]) = BigEndian::FromHost64(0);
    *reinterpret_cast<uint64*>(&iv[8]) = BigEndian::FromHost64(offset / kEncryptionBlockSize);

    const auto* cipher = get_evp_cipher(eh);
    if (PREDICT_FALSE(!cipher)) {
        return Status::InternalError(
                fmt::format("get cipher for algorithm {} key_size: {} failed", eh.algorithm, eh.key.size()));
    }
    auto ctx = EVP_CIPHER_CTX_new();
    if (!ctx) {
        return Status::InternalError("failed to create cipher context");
    }
    DeferOp cleanup_ctx([&ctx]() { EVP_CIPHER_CTX_free(ctx); });
    OPENSSL_RET_NOT_OK(EVP_EncryptInit_ex(ctx, cipher, nullptr, eh.key_bytes(), iv), "Failed to initialize encryption");
    OPENSSL_RET_NOT_OK(EVP_CIPHER_CTX_set_padding(ctx, 0), "failed to disable padding");
    const size_t offset_mod = offset % kEncryptionBlockSize;
    if (offset_mod) {
        unsigned char scratch_clear[kEncryptionBlockSize];
        unsigned char scratch_cipher[kEncryptionBlockSize];
        int out_length;
        OPENSSL_RET_NOT_OK(EVP_EncryptUpdate(ctx, scratch_cipher, &out_length, scratch_clear, offset_mod),
                           "Failed to encrypt scratch data");
        DCHECK_LE(out_length, kEncryptionBlockSize);
    }
    for (auto i = 0; i < n; ++i) {
        int out_length;
        OPENSSL_RET_NOT_OK(EVP_EncryptUpdate(ctx, (uint8_t*)ciphertext[i].mutable_data(), &out_length,
                                             (const uint8_t*)cleartext[i].data, cleartext[i].size),
                           "Failed to encrypt data");
        DCHECK_EQ(out_length, cleartext[i].size);
        DCHECK_LE(out_length, ciphertext[i].size);
        g_encryption_bytes << out_length;
    }
    return Status::OK();
}

// Decrypts 'data'. Uses 'offset' in the file to set the initialization vector.
Status DoDecryptV(const FileEncryptionInfo& eh, uint64_t offset, Slice* data, size_t n) {
    // Set the initialization vector based on the offset.
    uint8_t iv[16];
    *reinterpret_cast<uint64*>(&iv[0]) = BigEndian::FromHost64(0);
    *reinterpret_cast<uint64*>(&iv[8]) = BigEndian::FromHost64(offset / kEncryptionBlockSize);

    const auto* cipher = get_evp_cipher(eh);
    if (PREDICT_FALSE(!cipher)) {
        return Status::InternalError(
                fmt::format("get cipher for algorithm {} key_size: {} failed", eh.algorithm, eh.key.size()));
    }
    auto ctx = EVP_CIPHER_CTX_new();
    if (!ctx) {
        return Status::InternalError("failed to create cipher context");
    }
    DeferOp cleanup_ctx([&ctx]() { EVP_CIPHER_CTX_free(ctx); });
    OPENSSL_RET_NOT_OK(EVP_DecryptInit_ex(ctx, cipher, nullptr, eh.key_bytes(), iv), "Failed to initialize decryption");
    OPENSSL_RET_NOT_OK(EVP_CIPHER_CTX_set_padding(ctx, 0), "failed to disable padding");
    const size_t offset_mod = offset % kEncryptionBlockSize;
    if (offset_mod) {
        unsigned char scratch_clear[kEncryptionBlockSize];
        unsigned char scratch_cipher[kEncryptionBlockSize];
        int out_length;
        OPENSSL_RET_NOT_OK(EVP_DecryptUpdate(ctx, scratch_clear, &out_length, scratch_cipher, offset_mod),
                           "Failed to decrypt scratch data");
    }

    for (auto i = 0; i < n; ++i) {
        const Slice& ciphertext_slice = data[i];
        int in_length = ciphertext_slice.size;
        if (!in_length) continue;
        int out_length;
        OPENSSL_RET_NOT_OK(EVP_DecryptUpdate(ctx, (uint8_t*)data[i].mutable_data(), &out_length,
                                             (const uint8_t*)ciphertext_slice.data, in_length),
                           "Failed to decrypt data");
        g_decryption_bytes << in_length;
    }
    return Status::OK();
}

EncryptWritableFile::EncryptWritableFile(WritableFile* file, Ownership ownership, FileEncryptionInfo encryption_info)
        : WritableFileWrapper(file, ownership), _encryption_info(std::move(encryption_info)) {}

EncryptWritableFile::~EncryptWritableFile() = default;

Status EncryptWritableFile::append(const Slice& data) {
    std::vector<uint8_t> ciphertext(data.size);
    Slice ciphertext_slice(ciphertext.data(), ciphertext.size());
    RETURN_IF_ERROR(DoEncryptV(_encryption_info, _file->size(), &data, &ciphertext_slice, 1));
    return _file->append(ciphertext_slice);
}

Status EncryptWritableFile::appendv(const Slice* data, size_t cnt) {
    size_t total_size = 0;
    for (size_t i = 0; i < cnt; ++i) {
        total_size += data[i].size;
    }
    std::vector<uint8_t> ciphertext(total_size);
    std::vector<Slice> ciphertext_slices(cnt);
    size_t offset = 0;
    for (size_t i = 0; i < cnt; ++i) {
        ciphertext_slices[i] = Slice(ciphertext.data() + offset, data[i].size);
        offset += data[i].size;
    }
    RETURN_IF_ERROR(DoEncryptV(_encryption_info, _file->size(), data, ciphertext_slices.data(), cnt));
    return _file->appendv(ciphertext_slices.data(), cnt);
}

EncryptSeekableInputStream::EncryptSeekableInputStream(std::unique_ptr<SeekableInputStream> stream,
                                                       FileEncryptionInfo encryption_info)
        : SeekableInputStreamWrapper(stream.get(), kDontTakeOwnership),
          _stream(std::move(stream)),
          _encryption_info(std::move(encryption_info)) {}

StatusOr<int64_t> EncryptSeekableInputStream::read(void* data, int64_t count) {
    auto pos = _stream->position();
    if (!pos.ok()) return pos.status();
    auto st = _stream->read(data, count);
    if (!st.ok()) return st;
    Slice slice(static_cast<const char*>(data), st.value());
    RETURN_IF_ERROR(DoDecryptV(_encryption_info, pos.value(), &slice, 1));
    return st;
}

Status EncryptSeekableInputStream::read_fully(void* data, int64_t count) {
    auto pos = _stream->position();
    if (!pos.ok()) return pos.status();
    RETURN_IF_ERROR(_stream->read_fully(data, count));
    Slice slice(static_cast<const char*>(data), count);
    return DoDecryptV(_encryption_info, pos.value(), &slice, 1);
}

StatusOr<int64_t> EncryptSeekableInputStream::read_at(int64_t offset, void* out, int64_t count) {
    auto st = _stream->read_at(offset, out, count);
    if (!st.ok()) return st;
    Slice slice(static_cast<const char*>(out), st.value());
    RETURN_IF_ERROR(DoDecryptV(_encryption_info, offset, &slice, 1));
    return st;
}

Status EncryptSeekableInputStream::read_at_fully(int64_t offset, void* out, int64_t count) {
    RETURN_IF_ERROR(_stream->read_at_fully(offset, out, count));
    Slice slice(static_cast<const char*>(out), count);
    return DoDecryptV(_encryption_info, offset, &slice, 1);
}

StatusOr<std::string> EncryptSeekableInputStream::read_all() {
    auto st = _stream->read_all();
    if (!st.ok()) return st.status();
    Slice slice(st.value().data(), st.value().size());
    RETURN_IF_ERROR(DoDecryptV(_encryption_info, 0, &slice, 1));
    return st;
}

std::unique_ptr<WritableFile> wrap_encrypted(std::unique_ptr<WritableFile> file,
                                             const FileEncryptionInfo& encryption_info) {
    return encryption_info.is_encrypted()
                   ? std::make_unique<EncryptWritableFile>(file.release(), kTakesOwnership, encryption_info)
                   : std::move(file);
}

constexpr int kGcmTagLength = 16;
constexpr int kNonceLength = 12;

void ssl_random_bytes(void* buf, int size) {
    RAND_bytes((uint8_t*)buf, size);
}

StatusOr<std::string> wrap_key(EncryptionAlgorithmPB algorithm, const std::string& plain_kek,
                               const std::string& plain_key) {
    if (algorithm != EncryptionAlgorithmPB::AES_128) {
        return Status::NotSupported(fmt::format("algorithm not support:{}", algorithm));
    }
    if (plain_key.size() == 0) {
        return Status::InternalError("wrap empty plain_key");
    }
    uint8_t tag[kGcmTagLength];
    memset(tag, 0, kGcmTagLength);
    uint8_t nonce[kNonceLength];
    ssl_random_bytes(nonce, kNonceLength);
    auto ctx = EVP_CIPHER_CTX_new();
    if (!ctx) {
        return Status::InternalError("failed to create cipher context");
    }
    DeferOp cleanup_ctx([&ctx]() { EVP_CIPHER_CTX_free(ctx); });
    OPENSSL_RET_NOT_OK(EVP_EncryptInit_ex(ctx, EVP_aes_128_gcm(), nullptr, (const uint8_t*)plain_kek.c_str(), nonce),
                       "Failed to initialize decryption");
    std::string encrypted_key(plain_key.size() + kGcmTagLength + kNonceLength, '\0');
    int outlen = 0;
    int ciphertext_len = 0;
    OPENSSL_RET_NOT_OK(EVP_EncryptUpdate(ctx, (uint8_t*)encrypted_key.data() + kNonceLength, &outlen,
                                         (uint8_t*)plain_key.data(), plain_key.size()),
                       "failed encrypt key");
    ciphertext_len = outlen;
    OPENSSL_RET_NOT_OK(EVP_EncryptFinal_ex(ctx, (uint8_t*)encrypted_key.data() + kNonceLength + outlen, &outlen),
                       "failed encryption finalization");
    ciphertext_len += outlen;
    DCHECK(ciphertext_len == plain_key.length());
    if (ciphertext_len != plain_key.length()) {
        // should not happen
        return Status::InternalError(
                fmt::format("unexpected encryption length plain:{} ciphertext:{}", plain_key.length(), ciphertext_len));
    }
    OPENSSL_RET_NOT_OK(EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_GET_TAG, kGcmTagLength, tag), "failed get AES-GCM tag");
    std::copy(nonce, nonce + kNonceLength, encrypted_key.data());
    std::copy(tag, tag + kGcmTagLength, encrypted_key.data() + kNonceLength + ciphertext_len);
    return encrypted_key;
}

StatusOr<std::string> unwrap_key(EncryptionAlgorithmPB algorithm, const std::string& plain_kek,
                                 const std::string& encrypted_key) {
    if (algorithm != EncryptionAlgorithmPB::AES_128) {
        return Status::NotSupported(fmt::format("algorithm not support:{}", algorithm));
    }
    // decrypt AES128 GCM encrypted encrypted_key to plaintext and return
    if (encrypted_key.size() <= kGcmTagLength + kNonceLength) {
        return Status::InternalError(fmt::format("bad encrypted_key length:{}", encrypted_key.length()));
    }
    uint8_t tag[kGcmTagLength];
    memset(tag, 0, kGcmTagLength);
    uint8_t nonce[kNonceLength];
    memset(nonce, 0, kNonceLength);
    auto ciphertext = encrypted_key.c_str();
    std::copy(ciphertext, ciphertext + kNonceLength, nonce);
    std::copy(ciphertext + encrypted_key.size() - kGcmTagLength, ciphertext + encrypted_key.size(), tag);
    auto ctx = EVP_CIPHER_CTX_new();
    if (!ctx) {
        return Status::InternalError("failed to create cipher context");
    }
    DeferOp cleanup_ctx([&ctx]() { EVP_CIPHER_CTX_free(ctx); });
    OPENSSL_RET_NOT_OK(EVP_DecryptInit_ex(ctx, EVP_aes_128_gcm(), nullptr, (const uint8_t*)plain_kek.c_str(), nonce),
                       "Failed to initialize decryption");
    std::string decrypted_key(encrypted_key.size() - kGcmTagLength - kNonceLength, '\0');
    int outlen = 0;
    OPENSSL_RET_NOT_OK(EVP_DecryptUpdate(ctx, (uint8_t*)decrypted_key.data(), &outlen,
                                         (const uint8_t*)ciphertext + kNonceLength, decrypted_key.size()),
                       "Failed to decrypt key");
    OPENSSL_RET_NOT_OK(EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_TAG, kGcmTagLength, tag), "failed to set verify tag");
    OPENSSL_RET_NOT_OK(EVP_DecryptFinal_ex(ctx, (uint8_t*)decrypted_key.data() + outlen, &outlen),
                       "Failed to verify tag");
    return decrypted_key;
}

} // namespace starrocks
