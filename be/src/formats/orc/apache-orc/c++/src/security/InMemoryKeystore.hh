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

#ifndef CPPDEMO_INMEMORYKEYSTORE_H
#define CPPDEMO_INMEMORYKEYSTORE_H

#include <openssl/evp.h>
#include <openssl/rand.h>
#include <iostream>
#include <map>
#include <random>
#include <stdexcept>
#include <string>
#include <vector>
#include "gen_cpp/orc_proto.pb.h"
namespace orc {
  class EncryptionAlgorithm {
   public:
    EncryptionAlgorithm(const std::string& algorithm, const std::string& mode, int keyLength,
                        int serialization);
    //Get the EVP_CIPHER and use it for every decryption
    const EVP_CIPHER* createCipher();
    int getIvLength();
    //algorithm name like AES
    const std::string getAlgorithm();
    const std::string toString();
    static EncryptionAlgorithm* AES_CTR_128;
    static EncryptionAlgorithm* AES_CTR_256;
    //The length of the algorithm key
    int getKeyLength();

   private:
    std::string algorithm;
    std::string mode;
    int keyLength;
  };
  //describing the KeyProvider key
  class KeyMetadata {
   private:
    std::string keyName;
    int version;
    EncryptionAlgorithm* algorithm;

   public:
    KeyMetadata(std::string key, int version, EncryptionAlgorithm* algorithm)
        : keyName(key), version(version), algorithm(algorithm) {}

    std::string getKeyName() const {
      return keyName;
    }

    EncryptionAlgorithm* getAlgorithm() {
      return algorithm;
    }

    int getVersion() const {
      return version;
    }

    std::string toString() {
      return keyName + "@" + std::to_string(version) + " " + algorithm->toString();
    }
  };
  //The key used for decrypting data
  class SecurityKey : public KeyMetadata {
   private:
    std::vector<unsigned char> material;
   public:
    SecurityKey(std::string keyName, int version, EncryptionAlgorithm* algorithm,
               std::vector<unsigned char>& material)
        : KeyMetadata(keyName, version, algorithm), material(material) {}
    std::vector<unsigned char> getMaterial() {
      return material;
    }
  };

  //Use the SecurityKey to encrypt the encryptedKey and obtain the reuslt
  class SecretKeySpec {
   private:
    std::vector<unsigned char> key;
    std::string algorithm;

   public:
    SecretKeySpec(std::vector<unsigned char> var1, const std::string& var2) {
      if (!var1.empty() && !var2.empty()) {
        key = std::vector<unsigned char>(var1);
        algorithm = var2;
      } else {
        throw std::invalid_argument("Missing argument");
      }
    }
    std::string getAlgorithm() const {
      return algorithm;
    }

    std::vector<unsigned char> getEncoded() const {
      return key;
    }
  };

  //Use the SecurityKey to encrypt the encryptedKey with the algorithm and obtain the decryptedKey
  class LocalKey {
   public:
    LocalKey(std::string algorithm, std::vector<unsigned char> decryptedKey ,
             std::vector<unsigned char> encryptedKey);

    void setDecryptedKey(std::string algorithm, std::vector<unsigned char> key);
    void setDecryptedKey(std::shared_ptr<SecretKeySpec> secretKeySpec);


    std::shared_ptr<SecretKeySpec> getDecryptedKey();

    std::vector<unsigned char> getEncryptedKey();

   private:
    std::vector<unsigned char> encryptedKey;
    std::shared_ptr<SecretKeySpec> decryptedKey = nullptr;
  };

  class KeyProvider {
   public:
    //virtual ~KeyProvider() = 0;
    virtual std::vector<std::string> getKeyNames() = 0;
    //obtain the master key based on the keyName for decrypting the localKey
    virtual std::unique_ptr<SecurityKey>& getCurrentKeyVersion(std::string keyName) = 0;
    //decrypt the random number using the master key to generate the local key
    virtual  LocalKey* createLocalKey(KeyMetadata* key) = 0;
    //Use the key to find the master key in the KeyProvider, then decrypt the encryptedKey and return the plaintext
    virtual  std::shared_ptr<SecretKeySpec> decryptLocalKey(KeyMetadata* key, std::vector<unsigned char> encryptedKey) = 0;
  };
  //Used for storing the master key and generating multiple local keys when writing data
  class InMemoryKeystore : public KeyProvider{
   private:
    //Used for generating  local keys
    std::random_device rd;
    std::mt19937 random;
    //the master key stored in this keys
    std::map<std::string, std::unique_ptr<SecurityKey>> keys;
    // The latest version of the master key used for storage
    std::map<std::string, int> currentVersion;

   public:
    InMemoryKeystore();
    ~InMemoryKeystore();
    //get the names of all master keys
    std::vector<std::string> getKeyNames() override;
    //add master key
    InMemoryKeystore* addKey(std::string name, EncryptionAlgorithm* algorithm,
                             std::string password);
    // add master key with version
    InMemoryKeystore* addKey(std::string keyName, int version, EncryptionAlgorithm* algorithm,
                             std::string password);
    InMemoryKeystore* addKey(std::string keyName, int version,
                             EncryptionAlgorithm* algorithm,
                             std::vector<unsigned char> material);

    std::unique_ptr<SecurityKey>& getCurrentKeyVersion(std::string keyName)  override;

    LocalKey* createLocalKey(KeyMetadata* key)  override;

    std::shared_ptr<SecretKeySpec> decryptLocalKey(KeyMetadata* key, std::vector<unsigned char> encryptedKey)  override;
    //Check if Aes256 encryption and decryption is supported
    bool testSupportsAes256();
    std::string buildVersionName(const std::string& name, int version);
  };

}
#endif //CPPDEMO_INMEMORYKEYSTORE_H
