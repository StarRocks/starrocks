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
//   https://github.com/apache/orc/tree/main/c++/src/StripeStream.cc

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

// note(yan): include order matters
// clang-format off
#include "orc/Exceptions.hh"
#include "RLE.hh"
#include "Reader.hh"
#include "StripeStream.hh"

#include "wrap/coded-stream-wrapper.h"
// clang-format on

namespace orc {

StripeStreamsImpl::StripeStreamsImpl(const RowReaderImpl& _reader, uint64_t _index,long originalStripeId,
                                     const proto::StripeInformation& _stripeInfo,
                                     const proto::StripeFooter& _footer, uint64_t _stripeStart,
                                     InputStream& _input, const Timezone& _writerTimezone,
                                     const Timezone& _readerTimezone)
        : reader(_reader),
          stripeInfo(_stripeInfo),
          footer(_footer),
          stripeIndex(_index),
          originalStripeId(originalStripeId),
          stripeStart(_stripeStart),
          input(_input),
          writerTimezone(_writerTimezone),
          readerTimezone(_readerTimezone) {
    // +-----------------+---------------+-----------------+---------------+
    // |                 |               |                 |               |
    // |   unencrypted   |   encrypted   |   unencrypted   |   encrypted   |
    // |      index      |     index     |      data       |     data      |
    // |                 |               |                 |               |
    // +-----------------+---------------+-----------------+---------------+
    // The above refers to the storage layout of indexes and data, hence we need to follow this order to seek the stream.
    // Look for the index stream, first encrypted stream and then unencrypted stream.
    long currentOffset = stripeStart;
    currentOffset = StripeStreamsImpl::findStreamsByArea(const_cast<proto::StripeFooter&>(footer),currentOffset, Area::INDEX,reader.getReaderEncryption(),streams);
    //Look for the data stream, first the encrypted stream and then the unencrypted stream.
    findStreamsByArea(const_cast<proto::StripeFooter&>(footer),currentOffset, Area::DATA,reader.getReaderEncryption(),streams);

    for (size_t i = 0; i < streams.size(); i++) {
        std::shared_ptr<StreamInformation> stream = streams.at(i);
        std::string key =
                std::to_string(stream->getColumnId()) + ":" + std::to_string(stream->getKind());
        streamMap.emplace(key, std::shared_ptr<StreamInformation>(stream));
    }
}

StripeStreamsImpl::~StripeStreamsImpl() {
    // PASS
}

StreamInformation::~StreamInformation() {
    // PASS
}

StripeInformation::~StripeInformation() {
    // PASS
}

StreamInformationImpl::~StreamInformationImpl() {
    // PASS
}

const std::vector<bool>& StripeStreamsImpl::getSelectedColumns() const {
    return reader.getSelectedColumns();
}

const std::vector<bool>& StripeStreamsImpl::getLazyLoadColumns() const {
    return reader.getLazyLoadColumns();
}

proto::ColumnEncoding StripeStreamsImpl::getEncoding(uint64_t columnId) const {
    // The encoding of encrypted columns needs to be obtained in a special way.
    ReaderEncryptionVariant* variant = this->reader.getFileContents().encryption->getVariant(columnId);
    if(variant != nullptr){
      int subColumn = columnId - variant->getRoot()->getColumnId();
      return footer.encryption().Get(variant->getVariantId()).encoding(subColumn);
    }else{
      return footer.columns(static_cast<int>(columnId));
    }
  }

const Timezone& StripeStreamsImpl::getWriterTimezone() const {
    return writerTimezone;
}

const Timezone& StripeStreamsImpl::getReaderTimezone() const {
    return readerTimezone;
}

DataBuffer<char>* StripeStreamsImpl::getSharedBuffer() const {
    return reader.getSharedBuffer();
}

std::ostream* StripeStreamsImpl::getErrorStream() const {
    return reader.getFileContents().errorStream;
}

std::unique_ptr<SeekableInputStream> StripeStreamsImpl::getStream(uint64_t columnId, proto::Stream_Kind kind,
                                                                  bool shouldStream) const {
    MemoryPool* pool = reader.getFileContents().pool;
    const std::string skey = std::to_string(columnId) + ":" + std::to_string(kind);
    StreamInformation* streamInformation = streamMap[skey].get();
    if(streamInformation == nullptr){
      return nullptr;
    }
    uint64_t myBlock = shouldStream ? input.getNaturalReadSize() : streamInformation->getLength();
    auto inputStream = std::make_unique<SeekableFileInputStream>(
        &input, streamInformation->getOffset(), streamInformation->getLength(), *pool, myBlock);
    ReaderEncryptionVariant* variant = reader.getReaderEncryption()->getVariant(columnId);
    if (variant != nullptr) {
      ReaderEncryptionKey* encryptionKey = variant->getKeyDescription();
      const int ivLength = encryptionKey->getAlgorithm()->getIvLength();
      std::vector<unsigned char> iv(ivLength);
      orc::CryptoUtil::modifyIvForStream(columnId, kind, originalStripeId, iv.data(), ivLength);
      const EVP_CIPHER* cipher = encryptionKey->getAlgorithm()->createCipher();
      std::vector<unsigned char> key = variant->getStripeKey(stripeIndex)->getEncoded();
      std::unique_ptr<SeekableInputStream> decompressStream = createDecompressorAndDecryption(
          reader.getCompression(), std::move(inputStream), reader.getCompressionSize(), *pool,
          reader.getFileContents().readerMetrics, key,
          iv, const_cast<EVP_CIPHER*>(cipher));
      return decompressStream;
    } else {
      return createDecompressor(reader.getCompression(),
                                std::move(inputStream),
                                reader.getCompressionSize(), *pool,reader.getFileContents().readerMetrics);
    }
}

MemoryPool& StripeStreamsImpl::getMemoryPool() const {
    return *reader.getFileContents().pool;
}

ReaderMetrics* StripeStreamsImpl::getReaderMetrics() const {
    return reader.getFileContents().readerMetrics;
}

bool StripeStreamsImpl::getThrowOnHive11DecimalOverflow() const {
    return reader.getThrowOnHive11DecimalOverflow();
}

bool StripeStreamsImpl::isDecimalAsLong() const {
    return reader.getIsDecimalAsLong();
}

int32_t StripeStreamsImpl::getForcedScaleOnHive11Decimal() const {
    return reader.getForcedScaleOnHive11Decimal();
}

bool StripeStreamsImpl::getUseWriterTimezone() const {
    return reader.getUseWriterTimezone();
}

void StripeInformationImpl::ensureStripeFooterLoaded() const {
    if (stripeFooter == nullptr) {
        std::unique_ptr<SeekableInputStream> pbStream =
                createDecompressor(compression,
                                   std::unique_ptr<SeekableInputStream>(new SeekableFileInputStream(
                                           stream, offset + indexLength + dataLength, footerLength, memory)),
                                   blockSize, memory, metrics);
        stripeFooter.reset(new proto::StripeFooter());
        if (!stripeFooter->ParseFromZeroCopyStream(pbStream.get())) {
            throw ParseError("Failed to parse the stripe footer");
        }
    }
}

std::unique_ptr<StreamInformation> StripeInformationImpl::getStreamInformation(uint64_t streamId) const {
    ensureStripeFooterLoaded();
    uint64_t streamOffset = offset;
    for (uint64_t s = 0; s < streamId; ++s) {
        streamOffset += stripeFooter->streams(static_cast<int>(s)).length();
    }
    return ORC_UNIQUE_PTR<StreamInformation>(
            new StreamInformationImpl(streamOffset, stripeFooter->streams(static_cast<int>(streamId))));
}

} // namespace orc
