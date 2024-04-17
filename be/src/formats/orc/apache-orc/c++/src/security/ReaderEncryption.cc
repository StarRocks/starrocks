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

#include "ReaderEncryption.hh"
#include "../Reader.hh"
namespace orc {
  EncryptionAlgorithm* fromSerialization(orc::proto::EncryptionAlgorithm& serialization) {
    switch (serialization) {
      case orc::proto::EncryptionAlgorithm::AES_CTR_128:
        return EncryptionAlgorithm::AES_CTR_128;
      case orc::proto::EncryptionAlgorithm::AES_CTR_256:
        return EncryptionAlgorithm::AES_CTR_256;
      default:
        throw std::invalid_argument("Unknown code in encryption algorithm");
    }
  }
  ReaderEncryption::ReaderEncryption(){};

  ReaderEncryption::ReaderEncryption(std::shared_ptr<FileContents> contents,
                                     long stripeStatisticsOffset,
                                     std::shared_ptr<KeyProvider> provider) {
    if (nullptr != contents->footer || contents->footer->has_encryption()) {
      keyProvider = provider;
      const proto::Encryption& encrypt = contents->footer->encryption();
      //Initialize keys.
      int keysSize = encrypt.key_size();
      for (int k = 0; k < keysSize; ++k) {
        keys.push_back(std::make_unique<ReaderEncryptionKey>(encrypt.key(k)));
      }
      //Initialize variants
      int variantsSize = encrypt.variants_size();
      //The offset of the stripe statistics information for the column that is encrypted.
      uint64_t offset = stripeStatisticsOffset;
      for (int v = 0; v < variantsSize; ++v) {
        const proto::EncryptionVariant& variant = encrypt.variants(v);
        std::unique_ptr<ReaderEncryptionKey>& k = keys[static_cast<int>(variant.key())];
        ReaderEncryptionVariant* readerEncryptionVariant =
            new ReaderEncryptionVariant(variant,k.get(), v, contents, offset, provider);
        variants.push_back(std::unique_ptr<ReaderEncryptionVariant>(readerEncryptionVariant));
        offset += variants[v]->getStripeStatisticsLength();
      }
      //ColumnVariants's size is columnSize +1, and for unencrypted columns, its value is null.
      int columnVariantsSize = contents->schema->getMaximumColumnId() + 1;
      columnVariants.resize(columnVariantsSize);
      for (auto it = variants.begin(); it != variants.end(); ++it) {
        std::shared_ptr<ReaderEncryptionVariant> variant = *it;
        Type* root = variant->getRoot();
        //If nested columns are encrypted, they share the same variant object with their parent and sibling columns.
        for (int c = root->getColumnId(); c <= static_cast<int>(root->getMaximumColumnId()); ++c) {
          // set the variant if it is the first one that we've found
          if (columnVariants[c] == nullptr) {
            columnVariants[c] = std::shared_ptr<ReaderEncryptionVariant>(variant);
          }
        }
      }
    }
  }
  ReaderEncryptionVariant* ReaderEncryption::getVariant(int column) {
    if (columnVariants.size() == 0) {
      return nullptr;
    } else {
      //If a given variant cannot obtain the decrypted footer key, then look for a variant object that can be decrypted for that column.
      //Look for a variant object that can be decrypted within its subcolumns.
      while (columnVariants[column] != nullptr &&
             !columnVariants[column]->getKeyDescription()->isAvailable()) {
        if (keyProvider != nullptr) {
          columnVariants[column].reset(findNextVariant(column, columnVariants[column]->getVariantId()));
        }
      }
      return columnVariants[column].get();
    }
  }

  ReaderEncryptionVariant* ReaderEncryption::findNextVariant(int column, int lastVariant) {
    for (int v = lastVariant + 1; v < static_cast<int>(this->variants.size()); ++v) {
      Type* root = variants[v]->getRoot();
      if (static_cast<int>(root->getColumnId()) <= column &&
          column <= static_cast<int>(root->getMaximumColumnId())) {
        return variants[v].get();
      }
    }
    return nullptr;
  }
  std::vector<std::unique_ptr<ReaderEncryptionKey>>& ReaderEncryption::getKeys() {
    return keys;
  }
  std::vector<std::shared_ptr<ReaderEncryptionVariant>>& ReaderEncryption::getVariants() {
    return variants;
  }

  ReaderEncryptionKey::ReaderEncryptionKey(const orc::proto::EncryptionKey& key)
      : name(key.keyname()), version(key.keyversion()), state(State::UNTRIED) {
    orc::proto::EncryptionAlgorithm al = key.algorithm();
    algorithm = orc::fromSerialization(al);
  }
  std::string ReaderEncryptionKey::getKeyName() const {
    return name;
  }

  int ReaderEncryptionKey::getKeyVersion() {
    return version;
  }

  orc::EncryptionAlgorithm* ReaderEncryptionKey::getAlgorithm() {
    return algorithm;
  }

  std::vector<ReaderEncryptionVariant*>& ReaderEncryptionKey::getEncryptionRoots() {
    return roots;
  }

  std::unique_ptr<orc::KeyMetadata> ReaderEncryptionKey::getMetadata() {
    return std::make_unique<KeyMetadata>(name, version, algorithm);
  }

  ReaderEncryptionKey::State& ReaderEncryptionKey::getState() {
    return this->state;
  }

  void ReaderEncryptionKey::setFailure() {
    this->state = State::SUCCESS;
  }

  void ReaderEncryptionKey::setSuccess() {
    this->state = State::SUCCESS;
  }

  void ReaderEncryptionKey::addVariant(ReaderEncryptionVariant* newVariant) {
    roots.push_back(newVariant);
  }
  bool ReaderEncryptionKey::isAvailable() {
    if (getState() == ReaderEncryptionKey::State::SUCCESS) {
      return true;
    } else if (getState() == ReaderEncryptionKey::State::UNTRIED && !getEncryptionRoots().empty()) {
      // Check to see if we can decrypt the footer key of the first variant.
      try {
        return getEncryptionRoots()[0]->getFileFooterKey() != nullptr;
      } catch (const std::exception& e) {
        setFailure();
      }
    }
    return false;
  }
  ReaderEncryptionVariant::ReaderEncryptionVariant(
      const proto::EncryptionVariant& proto,
      ReaderEncryptionKey* key,
      int variantId,
      std::shared_ptr<FileContents> contents,
      uint64_t stripeStatsOffset,
      std::shared_ptr<KeyProvider> provider)
      : proto(proto),
        key(key),
        variantId(variantId),
        stripeStatsOffset(stripeStatsOffset),
        provider(provider){
    if (proto.has_root()) {
      column = contents->schema->getTypeByColumnId(proto.root());
    } else {
      column = contents->schema.get();
    }
    // Each strip in this column has its own key.
    size_t stripeCount = contents->stripeList.size();
    if (proto.has_encryptedkey()) {
      std::string algorithm = key->getAlgorithm()->getAlgorithm();
      for (size_t s = 0; s < stripeCount; ++s) {
        StripeInformation* stripe = contents->stripeList.at(s).get();
        std::vector<unsigned char> colKey = stripe->getEncryptedLocalKeyByVariantId(variantId);
        std::vector<unsigned char> deKey;
        LocalKey* localKey = new LocalKey(algorithm,deKey, colKey);
        localKeys.push_back(std::shared_ptr<LocalKey>(localKey));
      }
      // The FileStat and StripStat information for decrypting this column.
      std::string footerKeyStr = proto.encryptedkey();
      std::vector<unsigned char> enKey(footerKeyStr.begin(), footerKeyStr.end());
      std::vector<unsigned char> deKey;
      footerKey =std::shared_ptr<LocalKey>(new LocalKey(algorithm, deKey,enKey));
      key->addVariant(this);
    } else {
      footerKey = nullptr;
    }
  }
  ReaderEncryptionKey* ReaderEncryptionVariant::getKeyDescription() {
    return this->key;
  }
  Type* ReaderEncryptionVariant::getRoot() {
    return const_cast<Type*>(this->column);
  }

  int ReaderEncryptionVariant::getVariantId() {
    return this->variantId;
  }

  std::shared_ptr<SecretKeySpec> ReaderEncryptionVariant::getFileFooterKey() {
    if (this->key != nullptr && this->provider != nullptr) {
      return  getDecryptedKey(footerKey.get());;
    } else {
      return nullptr;
    }
  }
  long ReaderEncryptionVariant::getStripeStatisticsLength() {
    long result = 0;
    // proto.stripestatistics()
    for (int i = 0; i < proto.stripestatistics().size(); i++) {
      result +=  proto.stripestatistics().Get(i).length();
    }
    return result;
  }
  uint64_t ReaderEncryptionVariant::getStripeStatsOffset(){
      return stripeStatsOffset;
  }
  orc::proto::ColumnarStripeStatistics* ReaderEncryptionVariant::getAllEncryptStripeStatistics(){
    return allEncryptionStripeStatistics.get();
  }
  void ReaderEncryptionVariant::setAllEncryptStripeStatistics(orc::proto::ColumnarStripeStatistics* tmp){
    allEncryptionStripeStatistics = std::unique_ptr<orc::proto::ColumnarStripeStatistics>(tmp);
  }
  void ReaderEncryptionVariant::setFileStatistics(orc::proto::FileStatistics* tmp){
    fileStatistics = std::unique_ptr<orc::proto::FileStatistics>(tmp);
  }
  orc::proto::FileStatistics* ReaderEncryptionVariant::getFileStatistics(){
    return fileStatistics.get();
  }
  const google::protobuf::RepeatedPtrField<::orc::proto::Stream>& ReaderEncryptionVariant::getStripeStats(){
      return this->proto.stripestatistics();
  }
  std::shared_ptr<SecretKeySpec> ReaderEncryptionVariant::getStripeKey(long stripe) {
    if (key == nullptr || provider == nullptr) return nullptr;
    return getDecryptedKey(localKeys[stripe].get());
  }

  std::shared_ptr<SecretKeySpec> ReaderEncryptionVariant::getDecryptedKey(LocalKey* localKey) const {
    std::shared_ptr<SecretKeySpec> result = localKey->getDecryptedKey();
    //Check if localkey has been decrypted.
    if (result == nullptr) {
      switch (key->getState()) {
        case ReaderEncryptionKey::State::UNTRIED:
          try {
            result = provider->decryptLocalKey(key->getMetadata().get(), localKey->getEncryptedKey());
          } catch (const std::exception& e) {
            std::cout << "Can't decrypt using key " << key->getKeyName() << std::endl;
          }
          if (result != nullptr) {
            localKey->setDecryptedKey(result);
            key->setSuccess();
          } else {
            key->setFailure();
          }
          break;
        case ReaderEncryptionKey::State::SUCCESS:
          result = provider->decryptLocalKey(key->getMetadata().get(), localKey->getEncryptedKey());
          if (result == nullptr) {
            throw std::runtime_error("Can't decrypt local key " + key->getKeyName());
          }
          localKey->setDecryptedKey(result);
          break;
        case ReaderEncryptionKey::State::FAILURE:
          return nullptr;
      }
    }
    return result;
  };

}  // namespace orc