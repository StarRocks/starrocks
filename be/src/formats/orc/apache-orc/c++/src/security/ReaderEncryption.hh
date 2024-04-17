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

#ifndef ORC_READERENCRYPTION_H
#define ORC_READERENCRYPTION_H

#include <memory>
#include "InMemoryKeystore.hh"
#include "orc/Common.hh"
#include "orc/Statistics.hh"
#include "orc/Type.hh"
#include "orc_proto.pb.h"
namespace orc {
  class ReaderEncryptionKey;
  struct FileContents;
  class EncryptionVariant {
   public:
    std::shared_ptr<ReaderEncryptionKey> getKeyDescription();

    Type* getRoot();

    int getVariantId();

    std::shared_ptr<SecretKeySpec> getFileFooterKey();

    std::shared_ptr<SecretKeySpec> getStripeKey(long stripe);
  };

  // 加密变体，每个列一个
  // 每个strip 都有个StripeKey,用于解密数据的
  // 每个列都有一个FileFooterKey，用于解密FileStat，StripStat
  class ReaderEncryptionVariant : public EncryptionVariant {
   public:
    ReaderEncryptionVariant(const proto::EncryptionVariant& proto,
                            ReaderEncryptionKey* key,
                            int variantId,
                            std::shared_ptr<FileContents> contents,
                            uint64_t stripeStatsOffset,
                            std::shared_ptr<KeyProvider> provider);
    //Type* getColumn();
    long getStripeStatisticsLength();
    ReaderEncryptionKey* getKeyDescription();
    Type* getRoot();
    int getVariantId();
    /**
     * Get the footer key of this column and decrypt it.
     * @return
     */
    std::shared_ptr<SecretKeySpec> getFileFooterKey();
    /**
     * Get the local key of a specific stripe in this column and decrypt it.
     * @param stripe
     * @return
     */
    std::shared_ptr<SecretKeySpec> getStripeKey(long stripe);
    /**
     * Get the length of the statistical information of the column
     * @return
     */
    uint64_t getStripeStatsOffset();
    /**
     * Getting the column statistics information through a stream does not give you the actual statistics information.
     * You need to read the actual statistics information through the stream.
     * @return
     */
    const google::protobuf::RepeatedPtrField<::orc::proto::Stream>& getStripeStats();
    /**
     * All the statistical information of this column after stripping is stored here for caching,
     * avoiding multiple readings and affecting performance.
     * @return
     */
    orc::proto::ColumnarStripeStatistics* getAllEncryptStripeStatistics();
    /**
     * All the statistical information of this column after stripping is stored here for caching,
     * avoiding multiple readings and affecting performance.
     * @return
     */
    void setAllEncryptStripeStatistics(orc::proto::ColumnarStripeStatistics* tmp);
    void setFileStatistics(orc::proto::FileStatistics* tmp);
    orc::proto::FileStatistics* getFileStatistics();
   private:
    const proto::EncryptionVariant& proto;
    ReaderEncryptionKey* key;
    const Type* column;
    //The indexes of all encrypted columns.
    int variantId;
    //Each stripe in this column has a local key.
    std::vector<std::shared_ptr<LocalKey>> localKeys;
    //All stripes in this column have a footer key.
    std::shared_ptr<LocalKey> footerKey;
    //
    uint64_t stripeStatsOffset;
    std::shared_ptr<KeyProvider> provider;
    // All the statistical information of this column after stripping is stored here for caching,
    // avoiding multiple readings and affecting performance.
    std::unique_ptr<orc::proto::ColumnarStripeStatistics> allEncryptionStripeStatistics = nullptr;
    std::unique_ptr<orc::proto::FileStatistics> fileStatistics = nullptr;
    /**
     * Decrypt local key,
     * @param pKey
     * @return
     */
    std::shared_ptr<SecretKeySpec> getDecryptedKey(LocalKey* pKey) const;
  };
  // 定义 加密key的信息，不存储密钥
  class ReaderEncryptionKey {
   public:
    enum class State { UNTRIED, FAILURE, SUCCESS };
    ReaderEncryptionKey();
    ReaderEncryptionKey(const proto::EncryptionKey& key);
    std::string getKeyName() const;
    int getKeyVersion();
    //Check if the footerkey can be decrypted.
    bool isAvailable();
    EncryptionAlgorithm* getAlgorithm();
    std::vector<ReaderEncryptionVariant*>& getEncryptionRoots();
    std::unique_ptr<KeyMetadata> getMetadata();
    ReaderEncryptionKey::State& getState();
    void setFailure();
    void setSuccess();
    void addVariant(ReaderEncryptionVariant* newVariant);
    bool operator==(const ReaderEncryptionKey& other) const {
      return (name == other.name) && (version == other.version) && (algorithm == other.algorithm);
    }

    bool operator!=(const ReaderEncryptionKey& other) const {
      return !(*this == other);
    }

   private:
    std::string name;
    int version;
    EncryptionAlgorithm* algorithm;
    std::vector<ReaderEncryptionVariant*> roots;
    State state;
  };
  class ReaderEncryption {
   public:
    ReaderEncryption(std::shared_ptr<FileContents> contents,
                     long stripeStatisticsOffset,
                     std::shared_ptr<KeyProvider> provider);
    ReaderEncryption();
    std::vector<std::unique_ptr<ReaderEncryptionKey>>& getKeys();
    ReaderEncryptionVariant* getVariant(int column);
    std::vector<std::shared_ptr<ReaderEncryptionVariant>>& getVariants();

   private:
    std::shared_ptr<KeyProvider> keyProvider;
    std::vector<std::unique_ptr<ReaderEncryptionKey>> keys;
    std::vector<std::shared_ptr<ReaderEncryptionVariant>> variants;
    //ColumnVariants's size is columnSize +1, and for unencrypted columns, its value is null.
    std::vector<std::shared_ptr<ReaderEncryptionVariant>> columnVariants;
    ReaderEncryptionVariant* findNextVariant(int column, int lastVariant);
  };
  class CryptoUtil {
   private:
    static const int COLUMN_ID_LENGTH = 3;
    static const int KIND_LENGTH = 2;
    static const int STRIPE_ID_LENGTH = 3;
    static const int MIN_COUNT_BYTES = 8;

    static const int MAX_COLUMN = 0xffffff;
    static const int MAX_KIND = 0xffff;
    static const int MAX_STRIPE = 0xffffff;

   public:
    static unsigned char* modifyIvForStream(int columnId, proto::Stream_Kind kind, long stripeId,
                                            unsigned char* iv, int ivLength) {
      if (columnId < 0 || columnId > MAX_COLUMN) {
        throw std::invalid_argument("ORC encryption is limited to " + std::to_string(MAX_COLUMN) +
                                    " columns. Value = " + std::to_string(columnId));
      }
      int k = kind;
      if (k < 0 || k > MAX_KIND) {
        throw std::invalid_argument("ORC encryption is limited to " + std::to_string(MAX_KIND) +
                                    " stream kinds. Value = " + std::to_string(k));
      }
      if (ivLength - (COLUMN_ID_LENGTH + KIND_LENGTH + STRIPE_ID_LENGTH) < MIN_COUNT_BYTES) {
        throw std::invalid_argument("Not enough space in the iv for the count");
      }
      iv[0] = static_cast<char>(columnId >> 16);
      iv[1] = static_cast<char>(columnId >> 8);
      iv[2] = static_cast<char>(columnId);
      iv[COLUMN_ID_LENGTH] = static_cast<char>(k >> 8);
      iv[COLUMN_ID_LENGTH + 1] = static_cast<char>(k);
      modifyIvForStripe(stripeId, iv, ivLength);
      return iv;
    }

    static unsigned char* modifyIvForStripe(long stripeId, unsigned char* iv, int ivLength) {
      if (stripeId < 1 || stripeId > MAX_STRIPE) {
        throw std::invalid_argument("ORC encryption is limited to " + std::to_string(MAX_STRIPE) +
                                    " stripes. Value = " + std::to_string(stripeId));
      }
      iv[COLUMN_ID_LENGTH + KIND_LENGTH] = static_cast<char>(stripeId >> 16);
      iv[COLUMN_ID_LENGTH + KIND_LENGTH + 1] = static_cast<char>(stripeId >> 8);
      iv[COLUMN_ID_LENGTH + KIND_LENGTH + 2] = static_cast<char>(stripeId);
      for (int i = COLUMN_ID_LENGTH + KIND_LENGTH + STRIPE_ID_LENGTH; i < ivLength; ++i) {
        iv[i] = 0;
      }
      return iv;
    }
  };
}  // namespace orc
#endif  // ORC_READERENCRYPTION_H
