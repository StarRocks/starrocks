// Copyright 2010-present vivo, Inc. All rights reserved.
//
//Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "InMemoryKeystore.hh"
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <algorithm>
#include <iostream>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include "orc_proto.pb.h"
namespace orc {
  InMemoryKeystore::InMemoryKeystore() : random(rd()) {}

  InMemoryKeystore::~InMemoryKeystore() {
    keys.clear();
    currentVersion.clear();
  }

  InMemoryKeystore* InMemoryKeystore::addKey(std::string name, EncryptionAlgorithm* algorithm,
                                             std::string material) {
    return addKey(name, 0, algorithm, material);
  }
  InMemoryKeystore* InMemoryKeystore::addKey(std::string keyName, int version,
                                             EncryptionAlgorithm* algorithm,
                                             std::string password) {
    std::vector<unsigned char> material = std::vector<unsigned char>(password.begin(), password.end());
    return  addKey(keyName,version,algorithm,material);
  }
  InMemoryKeystore* InMemoryKeystore::addKey(std::string keyName, int version,
                                             EncryptionAlgorithm* algorithm,
                                             std::vector<unsigned char> material) {
    // Test whether platform supports the algorithm
    if (!testSupportsAes256() && (algorithm != EncryptionAlgorithm::AES_CTR_128)) {
      algorithm = EncryptionAlgorithm::AES_CTR_128;
    }
    int masterKeyLength = material.size();
    int algorithmLength = algorithm->getKeyLength();
    std::vector<unsigned char> buffer(algorithmLength, 0);
    // std::vector<unsigned char>* buffer = new std::vector<unsigned char>(algorithmLength);
    int dataLen = algorithmLength > masterKeyLength ? masterKeyLength : algorithmLength;
    std::copy(material.begin(), material.begin() + dataLen, buffer.begin());

    std::unique_ptr<SecurityKey> key = std::make_unique<SecurityKey>(keyName, version, algorithm, buffer);

    // Check whether the key is already present and has a smaller version
    if (currentVersion.find(keyName) != currentVersion.end()) {
      int oldV = currentVersion[keyName];
      if (oldV >= version) {
        throw std::runtime_error("Key " + key->toString() + " with equal or higher version " +
                                 std::to_string(version) + " already exists");
      }
    }
    //std::pair<std::string, std::unique_ptr<SecurityKey>> keyPair(buildVersionName(keyName, version), std::move(key));
    //keys[](keyPair);
    keys.emplace(buildVersionName(keyName, version),std::move(key));
    currentVersion[keyName] = version;

    return this;
  }
  bool InMemoryKeystore::testSupportsAes256() {
    const EVP_CIPHER * cipher = EVP_aes_256_cbc();
    int maxKeyLength = EVP_CIPHER_key_length(cipher);
    //EVP_CIPHER_free(const_cast<evp_cipher_st*>(cipher));
    return maxKeyLength >= 256 / 8;
  }

  std::string InMemoryKeystore::buildVersionName(const std::string& name, int version) {
    return name + "@" + std::to_string(version);
  }

  std::vector<std::string> InMemoryKeystore::getKeyNames() {
    std::vector<std::string> names;
    for (const auto& kv : currentVersion) {
      names.push_back(kv.first);
    }
    return names;
  }

  std::unique_ptr<SecurityKey>& InMemoryKeystore::getCurrentKeyVersion(std::string keyName) {
    int version = currentVersion[keyName];
    const std::string keyVersionName = buildVersionName(keyName, version);
    return keys[keyVersionName];
  }

  LocalKey* InMemoryKeystore::createLocalKey(KeyMetadata* key) {
    const std::string keyVersion = buildVersionName(key->getKeyName(), key->getVersion());
    if (keys.count(keyVersion) == 0) {
      throw std::invalid_argument("Unknown key " + keyVersion);
    }
    SecurityKey* secret = keys[keyVersion].get();
    EncryptionAlgorithm* algorithm = secret->getAlgorithm();
    std::vector<unsigned char> encryptedKey(algorithm->getKeyLength());
        //new std::vector<unsigned char>(algorithm->getKeyLength());
    RAND_bytes(encryptedKey.data(), encryptedKey.size());

    std::vector<unsigned char> iv(algorithm->getIvLength());
    std::copy(encryptedKey.begin(), encryptedKey.begin() + algorithm->getIvLength(), iv.begin());

    EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
    if (ctx == nullptr) {
      throw std::runtime_error("EVP_CIPHER_CTX_new() failed");
    }
    const EVP_CIPHER* cipher = algorithm->createCipher();
    if (EVP_DecryptInit_ex(ctx, cipher, nullptr, secret->getMaterial().data(),
                           iv.data()) != 1) {
      EVP_CIPHER_CTX_free(ctx);
      //EVP_CIPHER_free(const_cast<evp_cipher_st*>(cipher));
      throw std::runtime_error("EVP_DecryptInit_ex() failed");
    }
    int blockSize = EVP_CIPHER_block_size(cipher);
    std::vector<unsigned char> decryptedKey(encryptedKey.size() + blockSize);
       // new std::vector<unsigned char>(encryptedKey->size() + blockSize);
    int outlen = 0;
    if (EVP_DecryptUpdate(ctx, decryptedKey.data(), &outlen, encryptedKey.data(),
                          encryptedKey.size()) != 1) {
      EVP_CIPHER_CTX_free(ctx);
      //EVP_CIPHER_free(const_cast<evp_cipher_st*>(cipher));
      throw std::runtime_error("EVP_DecryptUpdate() failed");
    }
    int plaintext_len = outlen;
    if (EVP_DecryptFinal_ex(ctx, decryptedKey.data() + outlen, &outlen) != 1) {
      EVP_CIPHER_CTX_free(ctx);
      //EVP_CIPHER_free(const_cast<evp_cipher_st*>(cipher));
      throw std::runtime_error("EVP_DecryptFinal_ex() failed");
    }
    plaintext_len += outlen;
    decryptedKey.resize(plaintext_len);
    EVP_CIPHER_CTX_free(ctx);
    //EVP_CIPHER_free(const_cast<evp_cipher_st*>(cipher));
    LocalKey* localKey = new LocalKey(algorithm->getAlgorithm(), decryptedKey, encryptedKey);
    return localKey;
  }

  std::shared_ptr<SecretKeySpec> InMemoryKeystore::decryptLocalKey(
      KeyMetadata* key, std::vector<unsigned char> encryptedKey) {
    const std::string keyVersion = buildVersionName(key->getKeyName(), key->getVersion());
    if (keys.count(keyVersion) == 0) {
      throw std::invalid_argument("Unknown key " + keyVersion);
    }
    SecurityKey* secret = keys[keyVersion].get();
    EncryptionAlgorithm* algorithm = secret->getAlgorithm();

    std::vector<unsigned char> iv(algorithm->getIvLength());
    std::copy(encryptedKey.begin(), encryptedKey.begin() + algorithm->getIvLength(), iv.begin());

    EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
    if (ctx == nullptr) {
      throw std::runtime_error("EVP_CIPHER_CTX_new() failed");
    }
    const evp_cipher_st* cipher = algorithm->createCipher();
    if (EVP_DecryptInit_ex(ctx, cipher, nullptr, secret->getMaterial().data(),
                           iv.data()) != 1) {
      EVP_CIPHER_CTX_free(ctx);
      //EVP_CIPHER_free(const_cast<evp_cipher_st*>(cipher));
      throw std::runtime_error("EVP_DecryptInit_ex() failed");
    }
    int blockSize = EVP_CIPHER_block_size(cipher);
    std::vector<unsigned char> decryptedKey((encryptedKey.size() + blockSize));
        //new std::vector<unsigned char>(encryptedKey->size() + blockSize);
    int outlen = 0;
    if (EVP_DecryptUpdate(ctx, decryptedKey.data(), &outlen, encryptedKey.data(),
                          encryptedKey.size()) != 1) {
      EVP_CIPHER_CTX_free(ctx);
      //EVP_CIPHER_free(const_cast<evp_cipher_st*>(cipher));
      throw std::runtime_error("EVP_DecryptUpdate() failed");
    }
    int plaintext_len = outlen;
    if (EVP_DecryptFinal_ex(ctx, decryptedKey.data() + outlen, &outlen) != 1) {
      EVP_CIPHER_CTX_free(ctx);
      //EVP_CIPHER_free(const_cast<evp_cipher_st*>(cipher));
      throw std::runtime_error("EVP_DecryptFinal_ex() failed");
    }
    plaintext_len += outlen;
    decryptedKey.resize(plaintext_len);
    EVP_CIPHER_CTX_free(ctx);
    //EVP_CIPHER_free(const_cast<evp_cipher_st*>(cipher));
    return std::make_unique<SecretKeySpec>(decryptedKey, algorithm->getAlgorithm());
  }
  LocalKey::LocalKey(std::string algorithm, std::vector<unsigned char> decryptedKey,
                     std::vector<unsigned char> encryptedKey):encryptedKey(encryptedKey) {
    if (!decryptedKey.empty()) {
      setDecryptedKey(algorithm, decryptedKey);
    }
  }

  void LocalKey::setDecryptedKey(std::string algorithm, std::vector<unsigned char> decrypt) {
    this->decryptedKey = std::make_unique<SecretKeySpec>(decrypt, algorithm);
  }
  void LocalKey::setDecryptedKey(std::shared_ptr<SecretKeySpec> secretKeySpec) {
    this->decryptedKey = secretKeySpec;
  }

  std::shared_ptr<SecretKeySpec> LocalKey::getDecryptedKey() {
    return decryptedKey;
  }

  std::vector<unsigned char> LocalKey::getEncryptedKey() {
    return encryptedKey;
  }
  EncryptionAlgorithm::EncryptionAlgorithm(const std::string& algorithm, const std::string& mode,
                                           int keyLength, int serialization)
      : algorithm(algorithm), mode(mode), keyLength(keyLength) {}
  EncryptionAlgorithm* EncryptionAlgorithm::AES_CTR_128 =
      new EncryptionAlgorithm("AES", "CTR/NoPadding", 16, 1);
  EncryptionAlgorithm* EncryptionAlgorithm::AES_CTR_256 =
      new EncryptionAlgorithm("AES", "CTR/NoPadding", 32, 2);
  int EncryptionAlgorithm::getIvLength() {
    return 16;
  }

  int EncryptionAlgorithm::getKeyLength() {
    return this->keyLength;
  }
  const std::string EncryptionAlgorithm::getAlgorithm() {
    return this->algorithm;
  }

  const std::string EncryptionAlgorithm::toString() {
    return algorithm + std::to_string(keyLength * 8);
  }
  const EVP_CIPHER* EncryptionAlgorithm::createCipher() {
    if (algorithm == "AES" && mode == "CTR/NoPadding" && keyLength == 16){
      return EVP_aes_128_ctr();
    }else if (algorithm == "AES" && mode == "CTR/NoPadding" && keyLength == 32)
      return EVP_aes_256_ctr();
    else {
      throw std::invalid_argument("Bad algorithm or padding");
    }
    return NULL;
  }
}  // namespace orc
