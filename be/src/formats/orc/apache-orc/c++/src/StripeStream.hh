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
//   https://github.com/apache/orc/tree/main/c++/src/StripeStream.hh

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include "Timezone.hh"
#include "TypeImpl.hh"
#include "orc/Int128.hh"
#include "orc/OrcFile.hh"
#include "orc/Reader.hh"

namespace orc {

class RowReaderImpl;
/**
* StreamInformation Implementation
*/

class StreamInformationImpl : public StreamInformation {
private:
StreamKind kind;
uint64_t column;
uint64_t offset;
uint64_t length;
proto::Stream_Kind originalKind;

public:
StreamInformationImpl(uint64_t _offset, const proto::Stream& stream)
    : kind(static_cast<StreamKind>(stream.kind())),
      column(stream.column()),
      offset(_offset),
      length(stream.length()) {
  // PASS
}
StreamInformationImpl(proto::Stream_Kind originalKind, uint64_t column, uint64_t _offset,
                      uint64_t length)
    : kind(static_cast<StreamKind>(originalKind)),
      column(column),
      offset(_offset),
      length(length),
      originalKind(originalKind) {
  // PASS
}
~StreamInformationImpl() override;

StreamKind getKind() const override {
  return kind;
}

uint64_t getColumnId() const override {
  return column;
}

uint64_t getOffset() const override {
  return offset;
}

uint64_t getLength() const override {
  return length;
}
};

enum class Area { DATA, INDEX, FOOTER };
/**
  * StripeStream Implementation
  */

class StripeStreamsImpl : public StripeStreams {
private:
    const RowReaderImpl& reader;
    const proto::StripeInformation& stripeInfo;
    const proto::StripeFooter& footer;
    const uint64_t stripeIndex;
    long originalStripeId = 0;
    const uint64_t stripeStart;
    InputStream& input;
    const Timezone& writerTimezone;
    const Timezone& readerTimezone;
    mutable std::map<std::string, std::shared_ptr<StreamInformation>> streamMap;
    std::vector<std::shared_ptr<StreamInformation>> streams;
    static const long handleStream(long offset, const proto::Stream& stream, Area area,
                                   ReaderEncryptionVariant* variant, ReaderEncryption* encryption,
                                   std::vector<std::shared_ptr<StreamInformation>>& streams) {
        int column = stream.column();
        if (stream.has_kind()) {
            proto::Stream_Kind kind = stream.kind();
            // If there are no encrypted columns
/*            if (encryption->getKeys().empty()) {
                StreamInformationImpl* info = new StreamInformationImpl(kind, column, offset, stream.length());
                streams.push_back(std::shared_ptr<StreamInformation>(info));
                return stream.length();
            }*/
            if (getArea(kind) != area || kind == proto::Stream_Kind::Stream_Kind_ENCRYPTED_INDEX ||
                kind == proto::Stream_Kind::Stream_Kind_ENCRYPTED_DATA) {
                //Ignore the placeholder that should not be included in the offset calculation.
                return 0;
            }
            if (encryption->getVariant(column) == variant) {
                StreamInformationImpl* info = new StreamInformationImpl(kind, column, offset, stream.length());
                streams.push_back(std::shared_ptr<StreamInformation>(info));
            }
        }
        return stream.length();
    }

public:
    StripeStreamsImpl(const RowReaderImpl& reader, uint64_t index, long originalStripeId,
                      const proto::StripeInformation& stripeInfo, const proto::StripeFooter& footer,
                      uint64_t stripeStart, InputStream& input, const Timezone& writerTimezone,
                      const Timezone& readerTimezone);

    ~StripeStreamsImpl() override;

    const std::vector<bool>& getSelectedColumns() const override;
    const std::vector<bool>& getLazyLoadColumns() const override;

    proto::ColumnEncoding getEncoding(uint64_t columnId) const override;

    std::unique_ptr<SeekableInputStream> getStream(uint64_t columnId, proto::Stream_Kind kind,
                                                   bool shouldStream) const override;

    MemoryPool& getMemoryPool() const override;

    ReaderMetrics* getReaderMetrics() const override;

    const Timezone& getWriterTimezone() const override;

    const Timezone& getReaderTimezone() const override;

    std::ostream* getErrorStream() const override;

    bool getThrowOnHive11DecimalOverflow() const override;

    bool isDecimalAsLong() const override;

    int32_t getForcedScaleOnHive11Decimal() const override;
    //
    static Area getArea(proto::Stream_Kind kind) {
        switch (kind) {
        case proto::Stream_Kind::Stream_Kind_FILE_STATISTICS:
        case proto::Stream_Kind::Stream_Kind_STRIPE_STATISTICS:
            return Area::FOOTER;
        case proto::Stream_Kind::Stream_Kind_ROW_INDEX:
        case proto::Stream_Kind::Stream_Kind_DICTIONARY_COUNT:
        case proto::Stream_Kind::Stream_Kind_BLOOM_FILTER:
        case proto::Stream_Kind::Stream_Kind_BLOOM_FILTER_UTF8:
        case proto::Stream_Kind::Stream_Kind_ENCRYPTED_INDEX:
            return Area::INDEX;
        default:
            return Area::DATA;
        }
    }
    static long findStreamsByArea(proto::StripeFooter& footer, long currentOffset, Area area,
                                  ReaderEncryption* encryption,
                                  std::vector<std::shared_ptr<StreamInformation>>& streams) {
        // Look for the unencrypted stream.
        for (const proto::Stream& stream : footer.streams()) {
            currentOffset += handleStream(currentOffset, stream, area, nullptr, encryption, streams);
        }
        //If there are encrypted columns
        if (!encryption->getKeys().empty()) {
            std::vector<std::shared_ptr<ReaderEncryptionVariant>>& vList = encryption->getVariants();
            for (std::vector<orc::ReaderEncryptionVariant*>::size_type i = 0; i < vList.size(); i++) {
                ReaderEncryptionVariant* variant = vList.at(i).get();
                int variantId = variant->getVariantId();
                const proto::StripeEncryptionVariant& stripeVariant = footer.encryption(variantId);
                for (const proto::Stream& stream : stripeVariant.streams()) {
                    currentOffset += handleStream(currentOffset, stream, area, variant, encryption, streams);
                }
            }
        }
        return currentOffset;
    }

    bool getUseWriterTimezone() const override;

    DataBuffer<char>* getSharedBuffer() const override;
};


/**
 * StripeInformation Implementation
 */

class StripeInformationImpl : public StripeInformation {
    uint64_t offset;
    uint64_t indexLength;
    uint64_t dataLength;
    uint64_t footerLength;
    uint64_t numRows;
    InputStream* stream;
    MemoryPool& memory;
    CompressionKind compression;
    uint64_t blockSize;
    mutable std::unique_ptr<proto::StripeFooter> stripeFooter;
    ReaderMetrics* metrics;
    std::shared_ptr<std::vector<std::vector<unsigned char>>> encryptedKeys;
    long originalStripeId = 0;
    ReaderEncryption* encryption;
    void ensureStripeFooterLoaded() const;

public:
    StripeInformationImpl(uint64_t _offset, uint64_t _indexLength, uint64_t _dataLength, uint64_t _footerLength,
                          uint64_t _numRows, InputStream* _stream, MemoryPool& _memory, CompressionKind _compression,
                          uint64_t _blockSize, ReaderMetrics* _metrics)
            : offset(_offset),
              indexLength(_indexLength),
              dataLength(_dataLength),
              footerLength(_footerLength),
              numRows(_numRows),
              stream(_stream),
              memory(_memory),
              compression(_compression),
              blockSize(_blockSize),
              metrics(_metrics) {
        // PASS
    }
    StripeInformationImpl(proto::StripeInformation* stripeInfo, ReaderEncryption* encryption,
                          long previousOriginalStripeId,
                          std::shared_ptr<std::vector<std::vector<unsigned char>>> previousKeys, InputStream* _stream,
                          MemoryPool& _memory, CompressionKind _compression, uint64_t _blockSize,
                          ReaderMetrics* _metrics)
            : offset(stripeInfo->offset()),
              indexLength(stripeInfo->indexlength()),
              dataLength(stripeInfo->datalength()),
              footerLength(stripeInfo->footerlength()),
              numRows(stripeInfo->numberofrows()),
              stream(_stream),
              memory(_memory),
              compression(_compression),
              blockSize(_blockSize),
              metrics(_metrics),
              encryption(encryption) {
        // It is usually the first strip that has this value.
        if (stripeInfo->has_encryptstripeid()) {
            originalStripeId = stripeInfo->encryptstripeid();
        } else {
            originalStripeId = previousOriginalStripeId + 1;
        }
        // The value is generally present in the first strip.
        // Each encrypted column corresponds to a key.
        if (stripeInfo->encryptedlocalkeys_size() != 0) {
            encryptedKeys = std::shared_ptr<std::vector<std::vector<unsigned char>>>(
                    new std::vector<std::vector<unsigned char>>());
            for (int i = 0; i < static_cast<int>(stripeInfo->encryptedlocalkeys_size()); i++) {
                std::string str = stripeInfo->encryptedlocalkeys(i);
                std::vector<unsigned char> chars(str.begin(), str.end());
                encryptedKeys->push_back(chars);
            }
        } else {
            encryptedKeys = std::shared_ptr<std::vector<std::vector<unsigned char>>>(previousKeys);
        }
    }

    ~StripeInformationImpl() override {
        // PASS
    }

    uint64_t getOffset() const override { return offset; }

    uint64_t getLength() const override { return indexLength + dataLength + footerLength; }
    uint64_t getIndexLength() const override { return indexLength; }

    uint64_t getDataLength() const override { return dataLength; }

    uint64_t getFooterLength() const override { return footerLength; }

    uint64_t getNumberOfRows() const override { return numRows; }

    uint64_t getNumberOfStreams() const override {
        ensureStripeFooterLoaded();
        return static_cast<uint64_t>(stripeFooter->streams_size());
    }

    std::unique_ptr<StreamInformation> getStreamInformation(uint64_t streamId) const override;

    ColumnEncodingKind getColumnEncoding(uint64_t colId) const override {
        ensureStripeFooterLoaded();
        ReaderEncryptionVariant* variant = this->encryption->getVariant(colId);
        if (variant != nullptr) {
            int subColumn = colId - variant->getRoot()->getColumnId();
            return static_cast<ColumnEncodingKind>(
                    stripeFooter->encryption().Get(variant->getVariantId()).encoding(subColumn).kind());
        } else {
            return static_cast<ColumnEncodingKind>(stripeFooter->columns(static_cast<int>(colId)).kind());
        }
    }

    uint64_t getDictionarySize(uint64_t colId) const override {
        ensureStripeFooterLoaded();
        ReaderEncryptionVariant* variant = this->encryption->getVariant(colId);
        if (variant != nullptr) {
            int subColumn = colId - variant->getRoot()->getColumnId();
            return static_cast<ColumnEncodingKind>(
                    stripeFooter->encryption().Get(variant->getVariantId()).encoding(subColumn).dictionarysize());
        } else {
            return static_cast<ColumnEncodingKind>(stripeFooter->columns(static_cast<int>(colId)).dictionarysize());
        }
    }

    const std::string& getWriterTimezone() const override {
        ensureStripeFooterLoaded();
        return stripeFooter->writertimezone();
    }
    std::shared_ptr<std::vector<std::vector<unsigned char>>> getEncryptedLocalKeys() const override {
        return this->encryptedKeys;
    }
    std::vector<unsigned char>& getEncryptedLocalKeyByVariantId(int col) const override {
        return getEncryptedLocalKeys()->at(col);
    }
    long getOriginalStripeId() const override { return originalStripeId; }
};

} // namespace orc
