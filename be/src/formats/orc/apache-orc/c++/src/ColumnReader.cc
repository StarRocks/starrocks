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
//   https://github.com/apache/orc/tree/main/c++/src/ColumnReader.cc

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

#include "ColumnReader.hh"

#include <bits/endian.h>

#include <cmath>
#include <iostream>

#include "Adaptor.hh"
#include "ByteRLE.hh"
#include "RLE.hh"
#include "orc/Exceptions.hh"
#include "orc/Int128.hh"

namespace orc {

StripeStreams::~StripeStreams() {
    // PASS
}

inline RleVersion convertRleVersion(proto::ColumnEncoding_Kind kind) {
    switch (static_cast<int64_t>(kind)) {
    case proto::ColumnEncoding_Kind_DIRECT:
    case proto::ColumnEncoding_Kind_DICTIONARY:
        return RleVersion_1;
    case proto::ColumnEncoding_Kind_DIRECT_V2:
    case proto::ColumnEncoding_Kind_DICTIONARY_V2:
        return RleVersion_2;
    default:
        throw ParseError("Unknown encoding in convertRleVersion");
    }
}

ColumnReader::ColumnReader(const Type& type, StripeStreams& stripe)
        : columnId(type.getColumnId()), memoryPool(stripe.getMemoryPool()), metrics(stripe.getReaderMetrics()) {
    std::unique_ptr<SeekableInputStream> stream = stripe.getStream(columnId, proto::Stream_Kind_PRESENT, true);
    if (stream) {
        notNullDecoder = createBooleanRleDecoder(std::move(stream), metrics);
    }
}

ColumnReader::~ColumnReader() {
    // PASS
}

uint64_t ColumnReader::skip(uint64_t numValues) {
    ByteRleDecoder* decoder = notNullDecoder.get();
    if (decoder) {
        // page through the values that we want to skip
        // and count how many are non-null
        const size_t MAX_BUFFER_SIZE = 32768;
        size_t bufferSize = std::min(MAX_BUFFER_SIZE, static_cast<size_t>(numValues));
        char buffer[MAX_BUFFER_SIZE];
        uint64_t remaining = numValues;
        while (remaining > 0) {
            uint64_t chunkSize = std::min(remaining, static_cast<uint64_t>(bufferSize));
            decoder->next(buffer, chunkSize, nullptr);
            remaining -= chunkSize;
            for (uint64_t i = 0; i < chunkSize; ++i) {
                if (!buffer[i]) {
                    numValues -= 1;
                }
            }
        }
    }
    return numValues;
}

void ColumnReader::next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* incomingMask) {
    if (numValues > rowBatch.capacity) {
        rowBatch.resize(numValues);
    }
    rowBatch.numElements = numValues;
    ByteRleDecoder* decoder = notNullDecoder.get();
    if (decoder) {
        char* notNullArray = rowBatch.notNull.data();
        decoder->next(notNullArray, numValues, incomingMask);
        // check to see if there are nulls in this batch
        for (uint64_t i = 0; i < numValues; ++i) {
            if (!notNullArray[i]) {
                rowBatch.hasNulls = true;
                return;
            }
        }
    } else if (incomingMask) {
        // If we don't have a notNull stream, copy the incomingMask
        rowBatch.hasNulls = true;
        memcpy(rowBatch.notNull.data(), incomingMask, numValues);
        return;
    }
    rowBatch.hasNulls = false;
}

void ColumnReader::seekToRowGroup(PositionProviderMap* positions) {
    if (notNullDecoder) {
        notNullDecoder->seek(positions->at(columnId));
    }
}

/**
   * Expand an array of bytes in place to the corresponding array of longs.
   * Has to work backwards so that they data isn't clobbered during the
   * expansion.
   * @param buffer the array of chars and array of longs that need to be
   *        expanded
   * @param numValues the number of bytes to convert to longs
   */
void expandBytesToLongs(int64_t* buffer, uint64_t numValues) {
    for (size_t i = numValues - 1; i < numValues; --i) {
        buffer[i] = reinterpret_cast<char*>(buffer)[i];
    }
}

class BooleanColumnReader : public ColumnReader {
private:
    std::unique_ptr<orc::ByteRleDecoder> rle;

public:
    BooleanColumnReader(const Type& type, StripeStreams& stipe);
    ~BooleanColumnReader() override;

    uint64_t skip(uint64_t numValues) override;

    void next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) override;

    void seekToRowGroup(PositionProviderMap* positions) override;
};

BooleanColumnReader::BooleanColumnReader(const Type& type, StripeStreams& stripe) : ColumnReader(type, stripe) {
    std::unique_ptr<SeekableInputStream> stream = stripe.getStream(columnId, proto::Stream_Kind_DATA, true);
    if (stream == nullptr) throw ParseError("DATA stream not found in Boolean column");
    rle = createBooleanRleDecoder(std::move(stream), metrics);
}

BooleanColumnReader::~BooleanColumnReader() {
    // PASS
}

uint64_t BooleanColumnReader::skip(uint64_t numValues) {
    numValues = ColumnReader::skip(numValues);
    rle->skip(numValues);
    return numValues;
}

void BooleanColumnReader::next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) {
    ColumnReader::next(rowBatch, numValues, notNull);
    // Since the byte rle places the output in a char* instead of long*,
    // we cheat here and use the long* and then expand it in a second pass.
    int64_t* ptr = dynamic_cast<LongVectorBatch&>(rowBatch).data.data();
    rle->next(reinterpret_cast<char*>(ptr), numValues, rowBatch.hasNulls ? rowBatch.notNull.data() : nullptr);
    expandBytesToLongs(ptr, numValues);
}

void BooleanColumnReader::seekToRowGroup(PositionProviderMap* positions) {
    ColumnReader::seekToRowGroup(positions);
    rle->seek(positions->at(columnId));
}

class ByteColumnReader : public ColumnReader {
private:
    std::unique_ptr<orc::ByteRleDecoder> rle;

public:
    ByteColumnReader(const Type& type, StripeStreams& stipe);
    ~ByteColumnReader() override;

    uint64_t skip(uint64_t numValues) override;

    void next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) override;

    void seekToRowGroup(PositionProviderMap* positions) override;
};

ByteColumnReader::ByteColumnReader(const Type& type, StripeStreams& stripe) : ColumnReader(type, stripe) {
    std::unique_ptr<SeekableInputStream> stream = stripe.getStream(columnId, proto::Stream_Kind_DATA, true);
    if (stream == nullptr) throw ParseError("DATA stream not found in Byte column");
    rle = createByteRleDecoder(std::move(stream), nullptr);
}

ByteColumnReader::~ByteColumnReader() {
    // PASS
}

uint64_t ByteColumnReader::skip(uint64_t numValues) {
    numValues = ColumnReader::skip(numValues);
    rle->skip(numValues);
    return numValues;
}

void ByteColumnReader::next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) {
    ColumnReader::next(rowBatch, numValues, notNull);
    // Since the byte rle places the output in a char* instead of long*,
    // we cheat here and use the long* and then expand it in a second pass.
    int64_t* ptr = dynamic_cast<LongVectorBatch&>(rowBatch).data.data();
    rle->next(reinterpret_cast<char*>(ptr), numValues, rowBatch.hasNulls ? rowBatch.notNull.data() : nullptr);
    expandBytesToLongs(ptr, numValues);
}

void ByteColumnReader::seekToRowGroup(PositionProviderMap* positions) {
    ColumnReader::seekToRowGroup(positions);
    rle->seek(positions->at(columnId));
}

class IntegerColumnReader : public ColumnReader {
protected:
    std::unique_ptr<orc::RleDecoder> rle;

public:
    IntegerColumnReader(const Type& type, StripeStreams& stripe);
    ~IntegerColumnReader() override;

    uint64_t skip(uint64_t numValues) override;

    void next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) override;

    void seekToRowGroup(PositionProviderMap* positions) override;
};

IntegerColumnReader::IntegerColumnReader(const Type& type, StripeStreams& stripe) : ColumnReader(type, stripe) {
    RleVersion vers = convertRleVersion(stripe.getEncoding(columnId).kind());
    std::unique_ptr<SeekableInputStream> stream = stripe.getStream(columnId, proto::Stream_Kind_DATA, true);
    if (stream == nullptr) throw ParseError("DATA stream not found in Integer column");
    rle = createRleDecoder(std::move(stream), true, vers, memoryPool, metrics, stripe.getSharedBuffer());
}

IntegerColumnReader::~IntegerColumnReader() {
    // PASS
}

uint64_t IntegerColumnReader::skip(uint64_t numValues) {
    numValues = ColumnReader::skip(numValues);
    rle->skip(numValues);
    return numValues;
}

void IntegerColumnReader::next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) {
    ColumnReader::next(rowBatch, numValues, notNull);
    rle->next(dynamic_cast<LongVectorBatch&>(rowBatch).data.data(), numValues,
              rowBatch.hasNulls ? rowBatch.notNull.data() : nullptr);
}

void IntegerColumnReader::seekToRowGroup(PositionProviderMap* positions) {
    ColumnReader::seekToRowGroup(positions);
    rle->seek(positions->at(columnId));
}

class TimestampColumnReader : public ColumnReader {
private:
    std::unique_ptr<orc::RleDecoder> secondsRle;
    std::unique_ptr<orc::RleDecoder> nanoRle;
    const Timezone& writerTimezone;
    const Timezone& readerTimezone;
    const int64_t epochOffset;
    const bool sameTimezone;

public:
    TimestampColumnReader(const Type& type, StripeStreams& stripe, bool isInstantType);
    ~TimestampColumnReader() override;

    uint64_t skip(uint64_t numValues) override;

    void next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) override;

    void seekToRowGroup(PositionProviderMap* positions) override;
};

TimestampColumnReader::TimestampColumnReader(const Type& type, StripeStreams& stripe, bool isInstantType)
        : ColumnReader(type, stripe),
          writerTimezone(isInstantType ? getTimezoneByName("GMT") : stripe.getWriterTimezone()),
          readerTimezone(isInstantType ? getTimezoneByName("GMT") : stripe.getReaderTimezone()),
          epochOffset(writerTimezone.getEpoch()),
          sameTimezone(stripe.getUseWriterTimezone() || (&writerTimezone == &readerTimezone)) {
    RleVersion vers = convertRleVersion(stripe.getEncoding(columnId).kind());
    std::unique_ptr<SeekableInputStream> stream = stripe.getStream(columnId, proto::Stream_Kind_DATA, true);
    if (stream == nullptr) throw ParseError("DATA stream not found in Timestamp column");
    secondsRle = createRleDecoder(std::move(stream), true, vers, memoryPool, metrics, stripe.getSharedBuffer());
    stream = stripe.getStream(columnId, proto::Stream_Kind_SECONDARY, true);
    if (stream == nullptr) throw ParseError("SECONDARY stream not found in Timestamp column");
    nanoRle = createRleDecoder(std::move(stream), false, vers, memoryPool, metrics, stripe.getSharedBuffer());
}

TimestampColumnReader::~TimestampColumnReader() {
    // PASS
}

uint64_t TimestampColumnReader::skip(uint64_t numValues) {
    numValues = ColumnReader::skip(numValues);
    secondsRle->skip(numValues);
    nanoRle->skip(numValues);
    return numValues;
}

void TimestampColumnReader::next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) {
    ColumnReader::next(rowBatch, numValues, notNull);
    notNull = rowBatch.hasNulls ? rowBatch.notNull.data() : nullptr;
    auto& timestampBatch = dynamic_cast<TimestampVectorBatch&>(rowBatch);
    int64_t* secsBuffer = timestampBatch.data.data();
    secondsRle->next(secsBuffer, numValues, notNull);
    int64_t* nanoBuffer = timestampBatch.nanoseconds.data();
    nanoRle->next(nanoBuffer, numValues, notNull);

    // Construct the values
    for (uint64_t i = 0; i < numValues; i++) {
        if (notNull == nullptr || notNull[i]) {
            uint64_t zeros = nanoBuffer[i] & 0x7;
            nanoBuffer[i] >>= 3;
            if (zeros != 0) {
                for (uint64_t j = 0; j <= zeros; ++j) {
                    nanoBuffer[i] *= 10;
                }
            }
            int64_t writerTime = secsBuffer[i] + epochOffset;
            if (!sameTimezone) {
                // adjust timestamp value to same wall clock time if writer and reader
                // time zones have different rules, which is required for Apache Orc.
                const auto& wv = writerTimezone.getVariant(writerTime);
                const auto& rv = readerTimezone.getVariant(writerTime);
                if (!wv.hasSameTzRule(rv)) {
                    // If the timezone adjustment moves the millis across a DST boundary,
                    // we need to reevaluate the offsets.
                    int64_t adjustedTime = writerTime + wv.gmtOffset - rv.gmtOffset;
                    const auto& adjustedReader = readerTimezone.getVariant(adjustedTime);
                    writerTime = writerTime + wv.gmtOffset - adjustedReader.gmtOffset;
                }
            }
            secsBuffer[i] = writerTime;
            if (secsBuffer[i] < 0 && nanoBuffer[i] > 999999) {
                secsBuffer[i] -= 1;
            }
        }
    }
}

void TimestampColumnReader::seekToRowGroup(PositionProviderMap* positions) {
    ColumnReader::seekToRowGroup(positions);
    secondsRle->seek(positions->at(columnId));
    nanoRle->seek(positions->at(columnId));
}

class DoubleColumnReader : public ColumnReader {
public:
    DoubleColumnReader(const Type& type, StripeStreams& stripe);
    ~DoubleColumnReader() override;

    uint64_t skip(uint64_t numValues) override;

    void next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) override;

    void seekToRowGroup(PositionProviderMap* positions) override;

private:
    std::unique_ptr<SeekableInputStream> inputStream;
    TypeKind columnKind;
    const uint64_t bytesPerValue;
    const char* bufferPointer{nullptr};
    const char* bufferEnd{nullptr};
    DataBuffer<double> localDoubleBuffer;
    // this shared buffer is just for testing
    // in prod environment, sharedBufferPtr is a pointer to sharedBuffer in Reader
    // and that sharedBuffer will be reused across multiple column readers.
    DataBuffer<char> sharedBuffer;
    DataBuffer<char>* sharedBufferPtr = &sharedBuffer;

    unsigned char readByte() {
        if (bufferPointer == bufferEnd) {
            int length;
            if (!inputStream->Next(reinterpret_cast<const void**>(&bufferPointer), &length)) {
                throw ParseError("bad read in DoubleColumnReader::next()");
            }
            bufferEnd = bufferPointer + length;
        }
        return static_cast<unsigned char>(*(bufferPointer++));
    }

    size_t bufferSize() const { return bufferEnd - bufferPointer; }
    void bufferForward(size_t n) { bufferPointer += n; }

    const char* readFullyToBuffer(size_t expSize) {
        const char* buf = nullptr;
        if (bufferSize() >= expSize) {
            buf = bufferPointer;
            bufferForward(expSize);
        } else {
            sharedBufferPtr->reserve(expSize);
            char* sbdata = sharedBufferPtr->data();
            size_t needSize = expSize;
            for (;;) {
                size_t bufSize = bufferSize();
                size_t readSize = std::min(bufSize, needSize);
                memcpy(sbdata, bufferPointer, readSize);
                bufferForward(readSize);
                sbdata += readSize;
                needSize -= readSize;
                if (needSize == 0) break;
                // load and backward one char.
                readByte();
                bufferForward(-1);
            }
            buf = sharedBufferPtr->data();
        }
        return buf;
    }

    // It's same effect to following PR, so I don't merge it.
    // ORC-1137: [C++] Unroll loops and copy data directly in DoubleColumnReader::next() by stiga-huang ·
    // Pull Request #1071 · apache/orc https://github.com/apache/orc/pull/1071
    void readDoubleToLocalBuffer(int n) {
        const auto* data = reinterpret_cast<const uint8_t*>(readFullyToBuffer(n * 8));
        localDoubleBuffer.reserve(n);

#if __BYTE_ORDER == __LITTLE_ENDIAN
        memcpy(localDoubleBuffer.data(), data, n * 8);
#else
        for (int i = 0; i < n; i++) {
            int64_t bits = 0;
            for (int j = 0; j < 8; j++) {
                bits |= static_cast<int64_t>(uint8_t(data[j])) << (j * 8);
            }
            data += 8;
            memcpy(localDoubleBuffer.data() + i, &bits, 8);
        }
#endif
    }

    void readFloatToLocalBuffer(int n) {
        const auto* data = reinterpret_cast<const uint8_t*>(readFullyToBuffer(n * 4));
        localDoubleBuffer.reserve(n);
        for (int i = 0; i < n; i++) {
#if __BYTE_ORDER == __LITTLE_ENDIAN
            int32_t bits = *reinterpret_cast<const int32_t*>(data);
#else
            int32_t bits = 0;
            for (int j = 0; j < 4; j++) {
                bits |= static_cast<int32_t>(uint8_t(data[j])) << (j * 8);
            }
#endif
            data += 4;
            float t = 0;
            memcpy(&t, &bits, sizeof(t));
            localDoubleBuffer[i] = t;
        }
    }

    double readDouble() {
        int64_t bits = 0;
        for (uint64_t i = 0; i < 8; i++) {
            bits |= static_cast<int64_t>(readByte()) << (i * 8);
        }
        auto* result = reinterpret_cast<double*>(&bits);
        return *result;
    }

    double readFloat() {
        int32_t bits = 0;
        for (uint64_t i = 0; i < 4; i++) {
            bits |= static_cast<int32_t>(readByte()) << (i * 8);
        }
        auto* result = reinterpret_cast<float*>(&bits);
        return static_cast<double>(*result);
    }

    double readDoubleFast() {
        int64_t bits = 0;
        const char* data = bufferPointer;
        for (int i = 0; i < 8; i++) {
            bits |= static_cast<int64_t>(uint8_t(data[i])) << (i * 8);
        }
        bufferPointer += 8;
        double result;
        memcpy(&result, &bits, sizeof(result));
        return result;
    }

    double readFloatFast() {
        int32_t bits = 0;
        const char* data = bufferPointer;
        for (int i = 0; i < 4; i++) {
            bits |= static_cast<int32_t>(uint8_t(data[i])) << (i * 8);
        }
        bufferPointer += 4;
        float result;
        memcpy(&result, &bits, sizeof(result));
        return result;
    }
};

DoubleColumnReader::DoubleColumnReader(const Type& type, StripeStreams& stripe)
        : ColumnReader(type, stripe),
          columnKind(type.getKind()),
          bytesPerValue((type.getKind() == FLOAT) ? 4 : 8),

          localDoubleBuffer(stripe.getMemoryPool(), 0),
          sharedBuffer(stripe.getMemoryPool(), 0) {
    inputStream = stripe.getStream(columnId, proto::Stream_Kind_DATA, true);
    if (inputStream == nullptr) throw ParseError("DATA stream not found in Double column");
    if (stripe.getSharedBuffer() != nullptr) {
        sharedBufferPtr = stripe.getSharedBuffer();
    }
}

DoubleColumnReader::~DoubleColumnReader() {
    // PASS
}

uint64_t DoubleColumnReader::skip(uint64_t numValues) {
    numValues = ColumnReader::skip(numValues);

    if (static_cast<size_t>(bufferEnd - bufferPointer) >= bytesPerValue * numValues) {
        bufferPointer += bytesPerValue * numValues;
    } else {
        size_t sizeToSkip = bytesPerValue * numValues - static_cast<size_t>(bufferEnd - bufferPointer);
        const auto cap = static_cast<size_t>(std::numeric_limits<int>::max());
        while (sizeToSkip != 0) {
            size_t step = sizeToSkip > cap ? cap : sizeToSkip;
            inputStream->Skip(static_cast<int>(step));
            sizeToSkip -= step;
        }
        bufferEnd = nullptr;
        bufferPointer = nullptr;
    }

    return numValues;
}

#define OPT_DOUBLE_READER
#ifndef OPT_DOUBLE_READER

void DoubleColumnReader::next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) {
    ColumnReader::next(rowBatch, numValues, notNull);
    // update the notNull from the parent class
    notNull = rowBatch.hasNulls ? rowBatch.notNull.data() : nullptr;
    double* outArray = dynamic_cast<DoubleVectorBatch&>(rowBatch).data.data();

    size_t bufSize = bufferSize();

    if (columnKind == FLOAT) {
        if (notNull) {
            for (size_t i = 0; i < numValues; ++i) {
                if (notNull[i]) {
                    if (bufSize >= 4) {
                        outArray[i] = readFloatFast();
                        bufSize -= 4;
                    } else {
                        outArray[i] = readFloat();
                        bufSize = bufferSize();
                    }
                }
            }
        } else {
            for (size_t i = 0; i < numValues; ++i) {
                if (bufSize >= 4) {
                    outArray[i] = readFloatFast();
                    bufSize -= 4;
                } else {
                    outArray[i] = readFloat();
                    bufSize = bufferSize();
                }
            }
        }
    } else {
        if (notNull) {
            for (size_t i = 0; i < numValues; ++i) {
                if (notNull[i]) {
                    if (bufSize >= 8) {
                        outArray[i] = readDoubleFast();
                        bufSize -= 8;
                    } else {
                        outArray[i] = readDouble();
                        bufSize = bufferSize();
                    }
                }
            }
        } else {
            for (size_t i = 0; i < numValues; ++i) {
                if (bufSize >= 8) {
                    outArray[i] = readDoubleFast();
                    bufSize -= 8;
                } else {
                    outArray[i] = readDouble();
                    bufSize = bufferSize();
                }
            }
        }
    }
}

#else

void DoubleColumnReader::next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) {
    ColumnReader::next(rowBatch, numValues, notNull);
    // update the notNull from the parent class
    notNull = rowBatch.hasNulls ? rowBatch.notNull.data() : nullptr;
    double* outArray = dynamic_cast<DoubleVectorBatch&>(rowBatch).data.data();

    int number = int(numValues);
    if (notNull) {
        number = 0;
        for (size_t i = 0; i < numValues; i++) {
            number += (notNull[i] & 0x1);
        }
    }

    if (columnKind == FLOAT) {
        readFloatToLocalBuffer(number);
    } else {
        readDoubleToLocalBuffer(number);
    }

    if (notNull) {
        int offset = 0;
        for (size_t i = 0; i < numValues; ++i) {
            if (notNull[i]) {
                outArray[i] = localDoubleBuffer[offset];
                offset += 1;
            }
        }
    } else {
        memcpy(outArray, localDoubleBuffer.data(), sizeof(outArray[0]) * numValues);
    }
}

#endif

void readFully(char* buffer, int64_t bufferSize, SeekableInputStream* stream) {
    int64_t posn = 0;
    while (posn < bufferSize) {
        const void* chunk;
        int length;
        if (!stream->Next(&chunk, &length)) {
            throw ParseError("bad read in readFully");
        }
        if (posn + length > bufferSize) {
            throw ParseError("Corrupt dictionary blob in StringDictionaryColumn");
        }
        memcpy(buffer + posn, chunk, static_cast<size_t>(length));
        posn += length;
    }
}

void DoubleColumnReader::seekToRowGroup(PositionProviderMap* positions) {
    ColumnReader::seekToRowGroup(positions);
    inputStream->seek(positions->at(columnId));
    // clear buffer state after seek
    bufferEnd = nullptr;
    bufferPointer = nullptr;
}

class StringDictionaryColumnReader : public ColumnReader {
private:
    std::shared_ptr<StringDictionary> dictionary;
    std::unique_ptr<RleDecoder> rle;
    std::unique_ptr<RleDecoder> lengthDecoder;
    std::unique_ptr<SeekableInputStream> blobStream;
    uint32_t dictSize = 0;
    bool dictionaryLoaded = false;

public:
    StringDictionaryColumnReader(const Type& type, StripeStreams& stipe);
    ~StringDictionaryColumnReader() override;

    uint64_t skip(uint64_t numValues) override;

    void next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) override;

    void nextEncoded(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) override;

    void seekToRowGroup(PositionProviderMap* positions) override;

    StringDictionary* getDictionary() {
        loadDictionary();
        return dictionary.get();
    }

    void loadDictionary();
};

StringDictionaryColumnReader::StringDictionaryColumnReader(const Type& type, StripeStreams& stripe)
        : ColumnReader(type, stripe), dictionary(new StringDictionary(stripe.getMemoryPool())) {
    RleVersion rleVersion = convertRleVersion(stripe.getEncoding(columnId).kind());
    dictSize = stripe.getEncoding(columnId).dictionarysize();

    std::unique_ptr<SeekableInputStream> stream = stripe.getStream(columnId, proto::Stream_Kind_DATA, true);
    if (stream == nullptr) {
        throw ParseError("DATA stream not found in StringDictionaryColumn");
    }
    rle = createRleDecoder(std::move(stream), false, rleVersion, memoryPool, metrics, stripe.getSharedBuffer());

    stream = stripe.getStream(columnId, proto::Stream_Kind_LENGTH, false);
    if (dictSize > 0 && stream == nullptr) {
        throw ParseError("LENGTH stream not found in StringDictionaryColumn");
    }
    lengthDecoder =
            createRleDecoder(std::move(stream), false, rleVersion, memoryPool, metrics, stripe.getSharedBuffer());
    blobStream = stripe.getStream(columnId, proto::Stream_Kind_DICTIONARY_DATA, false);
    dictionaryLoaded = false;
}

void StringDictionaryColumnReader::loadDictionary() {
    if (dictionaryLoaded) return;
    dictionaryLoaded = true;

    dictionary->dictionaryOffset.resize(dictSize + 1);
    int64_t* lengthArray = dictionary->dictionaryOffset.data();
    lengthDecoder->next(lengthArray + 1, dictSize, nullptr);
    lengthArray[0] = 0;
    for (uint32_t i = 1; i < dictSize + 1; ++i) {
        if (lengthArray[i] < 0) {
            throw ParseError("Negative dictionary entry length");
        }
        lengthArray[i] += lengthArray[i - 1];
    }
    int64_t blobSize = lengthArray[dictSize];
    dictionary->dictionaryBlob.resize(static_cast<uint64_t>(blobSize));

    if (blobSize > 0 && blobStream == nullptr) {
        throw ParseError("DICTIONARY_DATA stream not found in StringDictionaryColumn");
    }
    readFully(dictionary->dictionaryBlob.data(), blobSize, blobStream.get());
}

StringDictionaryColumnReader::~StringDictionaryColumnReader() {
    // PASS
}

uint64_t StringDictionaryColumnReader::skip(uint64_t numValues) {
    numValues = ColumnReader::skip(numValues);
    rle->skip(numValues);
    return numValues;
}

void StringDictionaryColumnReader::next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) {
    loadDictionary();
    ColumnReader::next(rowBatch, numValues, notNull);
    // update the notNull from the parent class
    notNull = rowBatch.hasNulls ? rowBatch.notNull.data() : nullptr;
    auto& byteBatch = dynamic_cast<StringVectorBatch&>(rowBatch);
    char* blob = dictionary->dictionaryBlob.data();
    int64_t* dictionaryOffsets = dictionary->dictionaryOffset.data();
    char** outputStarts = byteBatch.data.data();
    int64_t* outputLengths = byteBatch.length.data();
    rle->next(outputLengths, numValues, notNull);
    uint64_t dictionaryCount = dictionary->dictionaryOffset.size() - 1;
    byteBatch.use_codes = true;
    if (byteBatch.codes.capacity() < numValues) {
        byteBatch.codes.reserve(numValues);
    }
    int64_t* codes = byteBatch.codes.data();
    if (notNull) {
        for (uint64_t i = 0; i < numValues; ++i) {
            if (notNull[i]) {
                int64_t entry = outputLengths[i];
                if (entry < 0 || static_cast<uint64_t>(entry) >= dictionaryCount) {
                    throw ParseError("Entry index out of range in StringDictionaryColumn");
                }
                outputStarts[i] = blob + dictionaryOffsets[entry];
                outputLengths[i] = dictionaryOffsets[entry + 1] - dictionaryOffsets[entry];
                codes[i] = entry;
            } else {
                // use largest index.
                codes[i] = static_cast<int64_t>(dictionaryCount);
                outputStarts[i] = nullptr;
                outputLengths[i] = 0;
            }
        }
    } else {
        for (uint64_t i = 0; i < numValues; ++i) {
            int64_t entry = outputLengths[i];
            if (entry < 0 || static_cast<uint64_t>(entry) >= dictionaryCount) {
                throw ParseError("Entry index out of range in StringDictionaryColumn");
            }
            outputStarts[i] = blob + dictionaryOffsets[entry];
            outputLengths[i] = dictionaryOffsets[entry + 1] - dictionaryOffsets[entry];
            codes[i] = entry;
        }
    }
}

void StringDictionaryColumnReader::nextEncoded(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) {
    loadDictionary();
    ColumnReader::next(rowBatch, numValues, notNull);
    notNull = rowBatch.hasNulls ? rowBatch.notNull.data() : nullptr;
    rowBatch.isEncoded = true;

    auto& batch = dynamic_cast<EncodedStringVectorBatch&>(rowBatch);
    batch.dictionary = this->dictionary;

    // Length buffer is reused to save dictionary entry ids
    rle->next(batch.index.data(), numValues, notNull);
}

void StringDictionaryColumnReader::seekToRowGroup(PositionProviderMap* positions) {
    ColumnReader::seekToRowGroup(positions);
    rle->seek(positions->at(columnId));
}

class StringDirectColumnReader : public ColumnReader {
private:
    std::unique_ptr<RleDecoder> lengthRle;
    std::unique_ptr<SeekableInputStream> blobStream;
    const char* lastBuffer;
    size_t lastBufferLength;

    /**
     * Compute the total length of the values.
     * @param lengths the array of lengths
     * @param notNull the array of notNull flags
     * @param numValues the lengths of the arrays
     * @return the total number of bytes for the non-null values
     */
    size_t computeSize(const int64_t* lengths, const char* notNull, uint64_t numValues);

public:
    StringDirectColumnReader(const Type& type, StripeStreams& stipe);
    ~StringDirectColumnReader() override;

    uint64_t skip(uint64_t numValues) override;

    void next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) override;

    void seekToRowGroup(PositionProviderMap* positions) override;
};

StringDirectColumnReader::StringDirectColumnReader(const Type& type, StripeStreams& stripe)
        : ColumnReader(type, stripe) {
    RleVersion rleVersion = convertRleVersion(stripe.getEncoding(columnId).kind());
    std::unique_ptr<SeekableInputStream> stream = stripe.getStream(columnId, proto::Stream_Kind_LENGTH, true);
    if (stream == nullptr) throw ParseError("LENGTH stream not found in StringDirectColumn");
    lengthRle = createRleDecoder(std::move(stream), false, rleVersion, memoryPool, metrics, stripe.getSharedBuffer());
    blobStream = stripe.getStream(columnId, proto::Stream_Kind_DATA, true);
    if (blobStream == nullptr) throw ParseError("DATA stream not found in StringDirectColumn");
    lastBuffer = nullptr;
    lastBufferLength = 0;
}

StringDirectColumnReader::~StringDirectColumnReader() {
    // PASS
}

uint64_t StringDirectColumnReader::skip(uint64_t numValues) {
    const size_t BUFFER_SIZE = 1024;
    numValues = ColumnReader::skip(numValues);
    int64_t buffer[BUFFER_SIZE];
    uint64_t done = 0;
    size_t totalBytes = 0;
    // read the lengths, so we know haw many bytes to skip
    while (done < numValues) {
        uint64_t step = std::min(BUFFER_SIZE, static_cast<size_t>(numValues - done));
        lengthRle->next(buffer, step, nullptr);
        totalBytes += computeSize(buffer, nullptr, step);
        done += step;
    }
    if (totalBytes <= lastBufferLength) {
        // subtract the needed bytes from the ones left over
        lastBufferLength -= totalBytes;
        lastBuffer += totalBytes;
    } else {
        // move the stream forward after accounting for the buffered bytes
        totalBytes -= lastBufferLength;
        const auto cap = static_cast<size_t>(std::numeric_limits<int>::max());
        while (totalBytes != 0) {
            size_t step = totalBytes > cap ? cap : totalBytes;
            blobStream->Skip(static_cast<int>(step));
            totalBytes -= step;
        }
        lastBufferLength = 0;
        lastBuffer = nullptr;
    }
    return numValues;
}

size_t StringDirectColumnReader::computeSize(const int64_t* lengths, const char* notNull, uint64_t numValues) {
    size_t totalLength = 0;
    if (notNull) {
        for (size_t i = 0; i < numValues; ++i) {
            if (notNull[i]) {
                totalLength += static_cast<size_t>(lengths[i]);
            }
        }
    } else {
        for (size_t i = 0; i < numValues; ++i) {
            totalLength += static_cast<size_t>(lengths[i]);
        }
    }
    return totalLength;
}

void StringDirectColumnReader::next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) {
    ColumnReader::next(rowBatch, numValues, notNull);
    // update the notNull from the parent class
    notNull = rowBatch.hasNulls ? rowBatch.notNull.data() : nullptr;
    auto& byteBatch = dynamic_cast<StringVectorBatch&>(rowBatch);
    char** startPtr = byteBatch.data.data();
    int64_t* lengthPtr = byteBatch.length.data();

    // read the length vector
    lengthRle->next(lengthPtr, numValues, notNull);

    // figure out the total length of data we need from the blob stream
    const size_t totalLength = computeSize(lengthPtr, notNull, numValues);

    // Load data from the blob stream into our buffer until we have enough
    // to get the rest directly out of the stream's buffer.
    size_t bytesBuffered = 0;
    byteBatch.blob.resize(totalLength);
    char* ptr = byteBatch.blob.data();
    byteBatch.use_codes = false;
    while (bytesBuffered + lastBufferLength < totalLength) {
        memcpy(ptr + bytesBuffered, lastBuffer, lastBufferLength);
        bytesBuffered += lastBufferLength;
        const void* readBuffer;
        int readLength;
        if (!blobStream->Next(&readBuffer, &readLength)) {
            throw ParseError("failed to read in StringDirectColumnReader.next");
        }
        lastBuffer = static_cast<const char*>(readBuffer);
        lastBufferLength = static_cast<size_t>(readLength);
    }

    if (bytesBuffered < totalLength) {
        size_t moreBytes = totalLength - bytesBuffered;
        memcpy(ptr + bytesBuffered, lastBuffer, moreBytes);
        lastBuffer += moreBytes;
        lastBufferLength -= moreBytes;
    }

    size_t filledSlots = 0;
    ptr = byteBatch.blob.data();
    if (notNull) {
        while (filledSlots < numValues) {
            startPtr[filledSlots] = const_cast<char*>(ptr);
            if (notNull[filledSlots]) {
                ptr += lengthPtr[filledSlots];
            } else {
                lengthPtr[filledSlots] = 0;
            }
            filledSlots += 1;
        }
    } else {
        while (filledSlots < numValues) {
            startPtr[filledSlots] = const_cast<char*>(ptr);
            ptr += lengthPtr[filledSlots];
            filledSlots += 1;
        }
    }
}

void StringDirectColumnReader::seekToRowGroup(PositionProviderMap* positions) {
    ColumnReader::seekToRowGroup(positions);
    blobStream->seek(positions->at(columnId));
    lengthRle->seek(positions->at(columnId));
    // clear buffer state after seek
    lastBuffer = nullptr;
    lastBufferLength = 0;
}

class StructColumnReader : public ColumnReader {
private:
    std::vector<std::unique_ptr<ColumnReader>> children;
    std::vector<uint64_t> fieldIndex;
    std::vector<std::unique_ptr<ColumnReader>> lazyLoadChildren;
    std::vector<uint64_t> lazyLoadFieldIndex;

public:
    StructColumnReader(const Type& type, StripeStreams& stipe);

    uint64_t skip(uint64_t numValues) override;

    void next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) override;

    void nextEncoded(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) override;

    void seekToRowGroup(PositionProviderMap* positions) override;

    const size_t size() { return children.size(); }
    ColumnReader* childReaderAt(size_t idx) { return children[idx].get(); }

    // The pace of lazy load fields and active load fields are different.
    // We read `active load fields` first, if predicates on them are true
    // then we read `lazy load fields`.
    // And that's why we need standalone methods just for lazy load fields.
    void lazyLoadSeekToRowGroup(PositionProviderMap* providers) override;
    void lazyLoadSkip(uint64_t numValues) override;
    void lazyLoadNext(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) override;
    void lazyLoadNextEncoded(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) override;

private:
    template <bool encoded, bool lazyLoad>
    void nextInternal(const std::vector<std::unique_ptr<ColumnReader>>& children,
                      const std::vector<uint64_t>& fieldIndex, ColumnVectorBatch& rowBatch, uint64_t numValues,
                      char* notNull);

    bool isAllFieldLazy();
};

StructColumnReader::StructColumnReader(const Type& type, StripeStreams& stripe) : ColumnReader(type, stripe) {
    // count the number of selected sub-columns
    const std::vector<bool> selectedColumns = stripe.getSelectedColumns();
    const std::vector<bool> lazyLoadColumns = stripe.getLazyLoadColumns();
    switch (static_cast<int64_t>(stripe.getEncoding(columnId).kind())) {
    case proto::ColumnEncoding_Kind_DIRECT: {
        uint64_t fi = 0;
        for (unsigned int i = 0; i < type.getSubtypeCount(); ++i) {
            const Type& child = *type.getSubtype(i);
            auto columnId = static_cast<uint64_t>(child.getColumnId());
            if (selectedColumns[columnId]) {
                if (lazyLoadColumns[columnId]) {
                    lazyLoadChildren.push_back(buildReader(child, stripe));
                    lazyLoadFieldIndex.push_back(fi);
                } else {
                    children.push_back(buildReader(child, stripe));
                    fieldIndex.push_back(fi);
                    if (child.getKind() == TypeKind::STRUCT) {
                        // If current child is a struct type, considering it's subfields may need
                        // lazy load, we should add it to lazyLoadChildren too.
                        lazyLoadChildren.push_back(buildReader(child, stripe));
                        lazyLoadFieldIndex.push_back(fi);
                    }
                }
                fi++;
            }
        }
    } break;
    case proto::ColumnEncoding_Kind_DIRECT_V2:
    case proto::ColumnEncoding_Kind_DICTIONARY:
    case proto::ColumnEncoding_Kind_DICTIONARY_V2:
    default:
        throw ParseError("Unknown encoding for StructColumnReader");
    }
}

uint64_t StructColumnReader::skip(uint64_t numValues) {
    numValues = ColumnReader::skip(numValues);
    for (auto& ptr : children) {
        ptr->skip(numValues);
    }
    return numValues;
}

void StructColumnReader::next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) {
    nextInternal<false, false>(children, fieldIndex, rowBatch, numValues, notNull);
}

void StructColumnReader::nextEncoded(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) {
    nextInternal<true, false>(children, fieldIndex, rowBatch, numValues, notNull);
}

template <bool encoded, bool lazyLoad>
void StructColumnReader::nextInternal(const std::vector<std::unique_ptr<ColumnReader>>& children,
                                      const std::vector<uint64_t>& fieldIndex, ColumnVectorBatch& rowBatch,
                                      uint64_t numValues, char* notNull) {
    if constexpr (!lazyLoad) {
        ColumnReader::next(rowBatch, numValues, notNull);
    } else {
        if (isAllFieldLazy()) {
            ColumnReader::next(rowBatch, numValues, notNull);
        }
    }
    uint64_t i = 0;
    notNull = rowBatch.hasNulls ? rowBatch.notNull.data() : nullptr;
    for (auto iter = children.begin(); iter != children.end(); ++iter, ++i) {
        uint64_t fi = fieldIndex[i];
        if constexpr (lazyLoad) {
            if constexpr (encoded) {
                (*iter)->lazyLoadNextEncoded(*(dynamic_cast<StructVectorBatch&>(rowBatch).fields[fi]), numValues,
                                             notNull);
            } else {
                (*iter)->lazyLoadNext(*(dynamic_cast<StructVectorBatch&>(rowBatch).fields[fi]), numValues, notNull);
            }
        } else {
            if constexpr (encoded) {
                (*iter)->nextEncoded(*(dynamic_cast<StructVectorBatch&>(rowBatch).fields[fi]), numValues, notNull);
            } else {
                (*iter)->next(*(dynamic_cast<StructVectorBatch&>(rowBatch).fields[fi]), numValues, notNull);
            }
        }
    }
}

bool StructColumnReader::isAllFieldLazy() {
    return children.empty();
}

void StructColumnReader::seekToRowGroup(PositionProviderMap* positions) {
    ColumnReader::seekToRowGroup(positions);
    for (auto& ptr : children) {
        ptr->seekToRowGroup(positions);
    }
}

void StructColumnReader::lazyLoadSeekToRowGroup(PositionProviderMap* positions) {
    if (isAllFieldLazy()) {
        ColumnReader::seekToRowGroup(positions);
    }
    for (auto& ptr : lazyLoadChildren) {
        ptr->lazyLoadSeekToRowGroup(positions);
    }
}

void StructColumnReader::lazyLoadSkip(uint64_t numValues) {
    if (isAllFieldLazy()) {
        ColumnReader::skip(numValues);
    }
    for (auto& ptr : lazyLoadChildren) {
        ptr->lazyLoadSkip(numValues);
    }
}

void StructColumnReader::lazyLoadNext(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) {
    nextInternal<false, true>(lazyLoadChildren, lazyLoadFieldIndex, rowBatch, numValues, notNull);
}

void StructColumnReader::lazyLoadNextEncoded(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) {
    nextInternal<true, true>(lazyLoadChildren, lazyLoadFieldIndex, rowBatch, numValues, notNull);
}

class ListColumnReader : public ColumnReader {
private:
    std::unique_ptr<ColumnReader> child;
    std::unique_ptr<RleDecoder> rle;

public:
    ListColumnReader(const Type& type, StripeStreams& stipe);
    ~ListColumnReader() override;

    uint64_t skip(uint64_t numValues) override;

    void next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) override;

    void nextEncoded(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) override;

    void seekToRowGroup(PositionProviderMap* positions) override;

    void lazyLoadSeekToRowGroup(PositionProviderMap* positions) override;
    void lazyLoadSkip(uint64_t numValues) override;
    void lazyLoadNext(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) override;
    void lazyLoadNextEncoded(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) override;

private:
    template <bool encoded, bool lazyLoad>
    void nextInternal(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull);
};

ListColumnReader::ListColumnReader(const Type& type, StripeStreams& stripe) : ColumnReader(type, stripe) {
    // count the number of selected sub-columns
    const std::vector<bool> selectedColumns = stripe.getSelectedColumns();
    RleVersion vers = convertRleVersion(stripe.getEncoding(columnId).kind());
    std::unique_ptr<SeekableInputStream> stream = stripe.getStream(columnId, proto::Stream_Kind_LENGTH, true);
    if (stream == nullptr) throw ParseError("LENGTH stream not found in List column");
    rle = createRleDecoder(std::move(stream), false, vers, memoryPool, metrics, stripe.getSharedBuffer());
    const Type& childType = *type.getSubtype(0);
    if (selectedColumns[static_cast<uint64_t>(childType.getColumnId())]) {
        child = buildReader(childType, stripe);
    }
}

ListColumnReader::~ListColumnReader() {
    // PASS
}

uint64_t ListColumnReader::skip(uint64_t numValues) {
    numValues = ColumnReader::skip(numValues);
    ColumnReader* childReader = child.get();
    if (childReader) {
        const uint64_t BUFFER_SIZE = 1024;
        int64_t buffer[BUFFER_SIZE];
        uint64_t childrenElements = 0;
        uint64_t lengthsRead = 0;
        while (lengthsRead < numValues) {
            uint64_t chunk = std::min(numValues - lengthsRead, BUFFER_SIZE);
            rle->next(buffer, chunk, nullptr);
            for (size_t i = 0; i < chunk; ++i) {
                childrenElements += static_cast<size_t>(buffer[i]);
            }
            lengthsRead += chunk;
        }
        childReader->skip(childrenElements);
    } else {
        rle->skip(numValues);
    }
    return numValues;
}

void ListColumnReader::lazyLoadSkip(uint64_t numValues) {
    // duplicate code
    numValues = ColumnReader::skip(numValues);
    ColumnReader* childReader = child.get();
    if (childReader) {
        const uint64_t BUFFER_SIZE = 1024;
        int64_t buffer[BUFFER_SIZE];
        uint64_t childrenElements = 0;
        uint64_t lengthsRead = 0;
        while (lengthsRead < numValues) {
            uint64_t chunk = std::min(numValues - lengthsRead, BUFFER_SIZE);
            rle->next(buffer, chunk, nullptr);
            for (size_t i = 0; i < chunk; ++i) {
                childrenElements += static_cast<size_t>(buffer[i]);
            }
            lengthsRead += chunk;
        }
        childReader->lazyLoadSkip(childrenElements);
    } else {
        rle->skip(numValues);
    }
}

void ListColumnReader::next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) {
    nextInternal<false, false>(rowBatch, numValues, notNull);
}

void ListColumnReader::lazyLoadNext(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) {
    nextInternal<false, true>(rowBatch, numValues, notNull);
}

void ListColumnReader::nextEncoded(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) {
    nextInternal<true, false>(rowBatch, numValues, notNull);
}

void ListColumnReader::lazyLoadNextEncoded(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) {
    nextInternal<true, true>(rowBatch, numValues, notNull);
}

template <bool encoded, bool lazyLoad>
void ListColumnReader::nextInternal(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) {
    ColumnReader::next(rowBatch, numValues, notNull);
    auto& listBatch = dynamic_cast<ListVectorBatch&>(rowBatch);
    int64_t* offsets = listBatch.offsets.data();
    notNull = listBatch.hasNulls ? listBatch.notNull.data() : nullptr;
    rle->next(offsets, numValues, notNull);
    uint64_t totalChildren = 0;
    if (notNull) {
        for (size_t i = 0; i < numValues; ++i) {
            if (notNull[i]) {
                auto tmp = static_cast<uint64_t>(offsets[i]);
                offsets[i] = static_cast<int64_t>(totalChildren);
                totalChildren += tmp;
            } else {
                offsets[i] = static_cast<int64_t>(totalChildren);
            }
        }
    } else {
        for (size_t i = 0; i < numValues; ++i) {
            auto tmp = static_cast<uint64_t>(offsets[i]);
            offsets[i] = static_cast<int64_t>(totalChildren);
            totalChildren += tmp;
        }
    }
    offsets[numValues] = static_cast<int64_t>(totalChildren);
    ColumnReader* childReader = child.get();
    if (childReader) {
        if constexpr (lazyLoad) {
            if (encoded) {
                childReader->lazyLoadNextEncoded(*(listBatch.elements.get()), totalChildren, nullptr);
            } else {
                childReader->lazyLoadNext(*(listBatch.elements.get()), totalChildren, nullptr);
            }
        } else {
            if (encoded) {
                childReader->nextEncoded(*(listBatch.elements.get()), totalChildren, nullptr);
            } else {
                childReader->next(*(listBatch.elements.get()), totalChildren, nullptr);
            }
        }

    }
}

void ListColumnReader::seekToRowGroup(PositionProviderMap* positions) {
    ColumnReader::seekToRowGroup(positions);
    rle->seek(positions->at(columnId));
    if (child) {
        child->seekToRowGroup(positions);
    }
}

void ListColumnReader::lazyLoadSeekToRowGroup(PositionProviderMap* positions) {
    // If List support lazy load, here need to check
    if (child) {
        child->lazyLoadSeekToRowGroup(positions);
    }
}

class MapColumnReader : public ColumnReader {
private:
    std::unique_ptr<ColumnReader> keyReader;
    std::unique_ptr<ColumnReader> elementReader;
    std::unique_ptr<RleDecoder> rle;

public:
    MapColumnReader(const Type& type, StripeStreams& stipe);
    ~MapColumnReader() override;

    uint64_t skip(uint64_t numValues) override;

    void next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) override;

    void nextEncoded(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) override;

    void seekToRowGroup(PositionProviderMap* positions) override;

    void lazyLoadSeekToRowGroup(PositionProviderMap* positions) override;
    void lazyLoadSkip(uint64_t numValues) override;
    void lazyLoadNext(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) override;
    void lazyLoadNextEncoded(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) override;

private:
    template <bool encoded, bool lazyLoad>
    void nextInternal(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull);
};

MapColumnReader::MapColumnReader(const Type& type, StripeStreams& stripe) : ColumnReader(type, stripe) {
    // Determine if the key and/or value columns are selected
    const std::vector<bool> selectedColumns = stripe.getSelectedColumns();
    RleVersion vers = convertRleVersion(stripe.getEncoding(columnId).kind());
    std::unique_ptr<SeekableInputStream> stream = stripe.getStream(columnId, proto::Stream_Kind_LENGTH, true);
    if (stream == nullptr) throw ParseError("LENGTH stream not found in Map column");
    rle = createRleDecoder(std::move(stream), false, vers, memoryPool, metrics, stripe.getSharedBuffer());
    const Type& keyType = *type.getSubtype(0);
    if (selectedColumns[static_cast<uint64_t>(keyType.getColumnId())]) {
        keyReader = buildReader(keyType, stripe);
    }
    const Type& elementType = *type.getSubtype(1);
    if (selectedColumns[static_cast<uint64_t>(elementType.getColumnId())]) {
        elementReader = buildReader(elementType, stripe);
    }
}

MapColumnReader::~MapColumnReader() {
    // PASS
}

uint64_t MapColumnReader::skip(uint64_t numValues) {
    numValues = ColumnReader::skip(numValues);
    ColumnReader* rawKeyReader = keyReader.get();
    ColumnReader* rawElementReader = elementReader.get();
    if (rawKeyReader || rawElementReader) {
        const uint64_t BUFFER_SIZE = 1024;
        int64_t buffer[BUFFER_SIZE];
        uint64_t childrenElements = 0;
        uint64_t lengthsRead = 0;
        while (lengthsRead < numValues) {
            uint64_t chunk = std::min(numValues - lengthsRead, BUFFER_SIZE);
            rle->next(buffer, chunk, nullptr);
            for (size_t i = 0; i < chunk; ++i) {
                childrenElements += static_cast<size_t>(buffer[i]);
            }
            lengthsRead += chunk;
        }
        if (rawKeyReader) {
            rawKeyReader->skip(childrenElements);
        }
        if (rawElementReader) {
            rawElementReader->skip(childrenElements);
        }
    } else {
        rle->skip(numValues);
    }
    return numValues;
}

void MapColumnReader::lazyLoadSkip(uint64_t numValues) {
    // duplicate code
    numValues = ColumnReader::skip(numValues);
    ColumnReader* rawKeyReader = keyReader.get();
    ColumnReader* rawElementReader = elementReader.get();
    if (rawKeyReader || rawElementReader) {
        const uint64_t BUFFER_SIZE = 1024;
        int64_t buffer[BUFFER_SIZE];
        uint64_t childrenElements = 0;
        uint64_t lengthsRead = 0;
        while (lengthsRead < numValues) {
            uint64_t chunk = std::min(numValues - lengthsRead, BUFFER_SIZE);
            rle->next(buffer, chunk, nullptr);
            for (size_t i = 0; i < chunk; ++i) {
                childrenElements += static_cast<size_t>(buffer[i]);
            }
            lengthsRead += chunk;
        }
        if (rawKeyReader) {
            rawKeyReader->lazyLoadSkip(childrenElements);
        }
        if (rawElementReader) {
            rawElementReader->lazyLoadSkip(childrenElements);
        }
    } else {
        rle->skip(numValues);
    }
}


void MapColumnReader::next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) {
    nextInternal<false, false>(rowBatch, numValues, notNull);
}

void MapColumnReader::lazyLoadNext(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) {
    nextInternal<false, true>(rowBatch, numValues, notNull);
}

void MapColumnReader::nextEncoded(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) {
    nextInternal<true, false>(rowBatch, numValues, notNull);
}

void MapColumnReader::lazyLoadNextEncoded(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) {
    nextInternal<true, true>(rowBatch, numValues, notNull);
}

template <bool encoded, bool lazyLoad>
void MapColumnReader::nextInternal(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) {
    ColumnReader::next(rowBatch, numValues, notNull);
    auto& mapBatch = dynamic_cast<MapVectorBatch&>(rowBatch);
    int64_t* offsets = mapBatch.offsets.data();
    notNull = mapBatch.hasNulls ? mapBatch.notNull.data() : nullptr;
    rle->next(offsets, numValues, notNull);
    uint64_t totalChildren = 0;
    if (notNull) {
        for (size_t i = 0; i < numValues; ++i) {
            if (notNull[i]) {
                auto tmp = static_cast<uint64_t>(offsets[i]);
                offsets[i] = static_cast<int64_t>(totalChildren);
                totalChildren += tmp;
            } else {
                offsets[i] = static_cast<int64_t>(totalChildren);
            }
        }
    } else {
        for (size_t i = 0; i < numValues; ++i) {
            auto tmp = static_cast<uint64_t>(offsets[i]);
            offsets[i] = static_cast<int64_t>(totalChildren);
            totalChildren += tmp;
        }
    }
    offsets[numValues] = static_cast<int64_t>(totalChildren);
    ColumnReader* rawKeyReader = keyReader.get();
    if (rawKeyReader) {
        if constexpr (lazyLoad) {
            if (encoded) {
                rawKeyReader->lazyLoadNextEncoded(*(mapBatch.keys.get()), totalChildren, nullptr);
            } else {
                rawKeyReader->lazyLoadNext(*(mapBatch.keys.get()), totalChildren, nullptr);
            }
        } else {
            if (encoded) {
                rawKeyReader->nextEncoded(*(mapBatch.keys.get()), totalChildren, nullptr);
            } else {
                rawKeyReader->next(*(mapBatch.keys.get()), totalChildren, nullptr);
            }
        }
    }
    ColumnReader* rawElementReader = elementReader.get();
    if (rawElementReader) {
        if constexpr (lazyLoad) {
            if (encoded) {
                rawElementReader->lazyLoadNextEncoded(*(mapBatch.elements.get()), totalChildren, nullptr);
            } else {
                rawElementReader->lazyLoadNext(*(mapBatch.elements.get()), totalChildren, nullptr);
            }
        } else {
            if (encoded) {
                rawElementReader->nextEncoded(*(mapBatch.elements.get()), totalChildren, nullptr);
            } else {
                rawElementReader->next(*(mapBatch.elements.get()), totalChildren, nullptr);
            }
        }
    }
}

void MapColumnReader::seekToRowGroup(PositionProviderMap* positions) {
    ColumnReader::seekToRowGroup(positions);
    rle->seek(positions->at(columnId));
    if (keyReader) {
        keyReader->seekToRowGroup(positions);
    }
    if (elementReader) {
        elementReader->seekToRowGroup(positions);
    }
}

void MapColumnReader::lazyLoadSeekToRowGroup(PositionProviderMap* positions) {
    // TODO If map want to support lazy load, here need to check
    if (keyReader) {
        keyReader->lazyLoadSeekToRowGroup(positions);
    }
    if (elementReader) {
        elementReader->lazyLoadSeekToRowGroup(positions);
    }
}

class UnionColumnReader : public ColumnReader {
private:
    std::unique_ptr<ByteRleDecoder> rle;
    std::vector<std::unique_ptr<ColumnReader>> childrenReader;
    std::vector<int64_t> childrenCounts;
    uint64_t numChildren;

public:
    UnionColumnReader(const Type& type, StripeStreams& stipe);

    uint64_t skip(uint64_t numValues) override;

    void next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) override;

    void nextEncoded(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) override;

    void seekToRowGroup(PositionProviderMap* positions) override;

private:
    template <bool encoded>
    void nextInternal(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull);
};

UnionColumnReader::UnionColumnReader(const Type& type, StripeStreams& stripe) : ColumnReader(type, stripe) {
    numChildren = type.getSubtypeCount();
    childrenReader.resize(numChildren);
    childrenCounts.resize(numChildren);

    std::unique_ptr<SeekableInputStream> stream = stripe.getStream(columnId, proto::Stream_Kind_DATA, true);
    if (stream == nullptr) throw ParseError("LENGTH stream not found in Union column");
    rle = createByteRleDecoder(std::move(stream), metrics);
    // figure out which types are selected
    const std::vector<bool> selectedColumns = stripe.getSelectedColumns();
    for (unsigned int i = 0; i < numChildren; ++i) {
        const Type& child = *type.getSubtype(i);
        if (selectedColumns[static_cast<size_t>(child.getColumnId())]) {
            childrenReader[i] = buildReader(child, stripe);
        }
    }
}

uint64_t UnionColumnReader::skip(uint64_t numValues) {
    numValues = ColumnReader::skip(numValues);
    const uint64_t BUFFER_SIZE = 1024;
    char buffer[BUFFER_SIZE];
    uint64_t lengthsRead = 0;
    int64_t* counts = childrenCounts.data();
    memset(counts, 0, sizeof(int64_t) * numChildren);
    while (lengthsRead < numValues) {
        uint64_t chunk = std::min(numValues - lengthsRead, BUFFER_SIZE);
        rle->next(buffer, chunk, nullptr);
        for (size_t i = 0; i < chunk; ++i) {
            counts[static_cast<size_t>(buffer[i])] += 1;
        }
        lengthsRead += chunk;
    }
    for (size_t i = 0; i < numChildren; ++i) {
        if (counts[i] != 0 && childrenReader[i] != nullptr) {
            childrenReader[i]->skip(static_cast<uint64_t>(counts[i]));
        }
    }
    return numValues;
}

void UnionColumnReader::next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) {
    nextInternal<false>(rowBatch, numValues, notNull);
}

void UnionColumnReader::nextEncoded(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) {
    nextInternal<true>(rowBatch, numValues, notNull);
}

template <bool encoded>
void UnionColumnReader::nextInternal(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) {
    ColumnReader::next(rowBatch, numValues, notNull);
    auto& unionBatch = dynamic_cast<UnionVectorBatch&>(rowBatch);
    uint64_t* offsets = unionBatch.offsets.data();
    int64_t* counts = childrenCounts.data();
    memset(counts, 0, sizeof(int64_t) * numChildren);
    unsigned char* tags = unionBatch.tags.data();
    notNull = unionBatch.hasNulls ? unionBatch.notNull.data() : nullptr;
    rle->next(reinterpret_cast<char*>(tags), numValues, notNull);
    // set the offsets for each row
    if (notNull) {
        for (size_t i = 0; i < numValues; ++i) {
            if (notNull[i]) {
                offsets[i] = static_cast<uint64_t>(counts[static_cast<size_t>(tags[i])]++);
            }
        }
    } else {
        for (size_t i = 0; i < numValues; ++i) {
            offsets[i] = static_cast<uint64_t>(counts[static_cast<size_t>(tags[i])]++);
        }
    }
    // read the right number of each child column
    for (size_t i = 0; i < numChildren; ++i) {
        if (childrenReader[i] != nullptr) {
            if (encoded) {
                childrenReader[i]->nextEncoded(*(unionBatch.children[i]), static_cast<uint64_t>(counts[i]), nullptr);
            } else {
                childrenReader[i]->next(*(unionBatch.children[i]), static_cast<uint64_t>(counts[i]), nullptr);
            }
        }
    }
}

void UnionColumnReader::seekToRowGroup(PositionProviderMap* positions) {
    ColumnReader::seekToRowGroup(positions);
    rle->seek(positions->at(columnId));
    for (size_t i = 0; i < numChildren; ++i) {
        if (childrenReader[i] != nullptr) {
            childrenReader[i]->seekToRowGroup(positions);
        }
    }
}

/**
   * Destructively convert the number from zigzag encoding to the
   * natural signed representation.
   */
void unZigZagInt128(Int128& value) {
    bool needsNegate = value.getLowBits() & 1;
    value >>= 1;
    if (needsNegate) {
        value.negate();
        value -= 1;
    }
}

class Decimal64ColumnReader : public ColumnReader {
public:
    static const uint32_t MAX_PRECISION_64 = 18;
    static const uint32_t MAX_PRECISION_128 = 38;
    static const int64_t POWERS_OF_TEN[MAX_PRECISION_64 + 1];

protected:
    std::unique_ptr<SeekableInputStream> valueStream;
    int32_t precision;
    int32_t scale;
    const char* buffer;
    const char* bufferEnd;

    std::unique_ptr<RleDecoder> scaleDecoder;

    /**
     * Read the valueStream for more bytes.
     */
    void readBuffer() {
        while (buffer == bufferEnd) {
            int length;
            if (!valueStream->Next(reinterpret_cast<const void**>(&buffer), &length)) {
                throw ParseError("Read past end of stream in Decimal64ColumnReader " + valueStream->getName());
            }
            bufferEnd = buffer + length;
        }
    }

    void readInt64(int64_t& value, int32_t currentScale) {
        value = 0;
        size_t offset = 0;
        while (true) {
            readBuffer();
            auto ch = static_cast<unsigned char>(*(buffer++));
            value |= static_cast<uint64_t>(ch & 0x7f) << offset;
            offset += 7;
            if (!(ch & 0x80)) {
                break;
            }
        }
        value = unZigZag(static_cast<uint64_t>(value));
        if (scale > currentScale && static_cast<uint64_t>(scale - currentScale) <= MAX_PRECISION_64) {
            value *= POWERS_OF_TEN[scale - currentScale];
        } else if (scale < currentScale && static_cast<uint64_t>(currentScale - scale) <= MAX_PRECISION_64) {
            value /= POWERS_OF_TEN[currentScale - scale];
        } else if (scale != currentScale) {
            throw ParseError("Decimal scale out of range");
        }
    }

public:
    Decimal64ColumnReader(const Type& type, StripeStreams& stipe);
    ~Decimal64ColumnReader() override;

    uint64_t skip(uint64_t numValues) override;

    void next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) override;

    void seekToRowGroup(PositionProviderMap* positions) override;
};
const uint32_t Decimal64ColumnReader::MAX_PRECISION_64;
const uint32_t Decimal64ColumnReader::MAX_PRECISION_128;
const int64_t Decimal64ColumnReader::POWERS_OF_TEN[MAX_PRECISION_64 + 1] = {1,
                                                                            10,
                                                                            100,
                                                                            1000,
                                                                            10000,
                                                                            100000,
                                                                            1000000,
                                                                            10000000,
                                                                            100000000,
                                                                            1000000000,
                                                                            10000000000,
                                                                            100000000000,
                                                                            1000000000000,
                                                                            10000000000000,
                                                                            100000000000000,
                                                                            1000000000000000,
                                                                            10000000000000000,
                                                                            100000000000000000,
                                                                            1000000000000000000};

Decimal64ColumnReader::Decimal64ColumnReader(const Type& type, StripeStreams& stripe) : ColumnReader(type, stripe) {
    scale = static_cast<int32_t>(type.getScale());
    precision = static_cast<int32_t>(type.getPrecision());
    valueStream = stripe.getStream(columnId, proto::Stream_Kind_DATA, true);
    if (valueStream == nullptr) throw ParseError("DATA stream not found in Decimal64Column");
    buffer = nullptr;
    bufferEnd = nullptr;
    RleVersion vers = convertRleVersion(stripe.getEncoding(columnId).kind());
    std::unique_ptr<SeekableInputStream> stream = stripe.getStream(columnId, proto::Stream_Kind_SECONDARY, true);
    if (stream == nullptr) throw ParseError("SECONDARY stream not found in Decimal64Column");
    scaleDecoder = createRleDecoder(std::move(stream), true, vers, memoryPool, metrics, stripe.getSharedBuffer());
}

Decimal64ColumnReader::~Decimal64ColumnReader() {
    // PASS
}

uint64_t Decimal64ColumnReader::skip(uint64_t numValues) {
    numValues = ColumnReader::skip(numValues);
    uint64_t skipped = 0;
    while (skipped < numValues) {
        readBuffer();
        if (!(0x80 & *(buffer++))) {
            skipped += 1;
        }
    }
    scaleDecoder->skip(numValues);
    return numValues;
}

void Decimal64ColumnReader::next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) {
    ColumnReader::next(rowBatch, numValues, notNull);
    notNull = rowBatch.hasNulls ? rowBatch.notNull.data() : nullptr;
    auto& batch = dynamic_cast<Decimal64VectorBatch&>(rowBatch);
    int64_t* values = batch.values.data();
    // read the next group of scales
    int64_t* scaleBuffer = batch.readScales.data();
    scaleDecoder->next(scaleBuffer, numValues, notNull);
    batch.precision = precision;
    batch.scale = scale;
    if (notNull) {
        for (size_t i = 0; i < numValues; ++i) {
            if (notNull[i]) {
                readInt64(values[i], static_cast<int32_t>(scaleBuffer[i]));
            }
        }
    } else {
        for (size_t i = 0; i < numValues; ++i) {
            readInt64(values[i], static_cast<int32_t>(scaleBuffer[i]));
        }
    }
}

void scaleInt128(Int128& value, uint32_t scale, uint32_t currentScale) {
    if (scale > currentScale) {
        while (scale > currentScale) {
            uint32_t scaleAdjust = std::min(Decimal64ColumnReader::MAX_PRECISION_64, scale - currentScale);
            value *= Decimal64ColumnReader::POWERS_OF_TEN[scaleAdjust];
            currentScale += scaleAdjust;
        }
    } else if (scale < currentScale) {
        Int128 remainder;
        while (currentScale > scale) {
            uint32_t scaleAdjust = std::min(Decimal64ColumnReader::MAX_PRECISION_64, currentScale - scale);
            value = value.divide(Decimal64ColumnReader::POWERS_OF_TEN[scaleAdjust], remainder);
            currentScale -= scaleAdjust;
        }
    }
}

void Decimal64ColumnReader::seekToRowGroup(PositionProviderMap* positions) {
    ColumnReader::seekToRowGroup(positions);
    valueStream->seek(positions->at(columnId));
    scaleDecoder->seek(positions->at(columnId));
    // clear buffer state after seek
    buffer = nullptr;
    bufferEnd = nullptr;
}

class Decimal128ColumnReader : public Decimal64ColumnReader {
public:
    Decimal128ColumnReader(const Type& type, StripeStreams& stipe);
    ~Decimal128ColumnReader() override;

    void next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) override;

private:
    void readInt128(Int128& value, int32_t currentScale) {
        value = 0;
        Int128 work;
        uint32_t offset = 0;
        while (true) {
            readBuffer();
            auto ch = static_cast<unsigned char>(*(buffer++));
            work = ch & 0x7f;
            work <<= offset;
            value |= work;
            offset += 7;
            if (!(ch & 0x80)) {
                break;
            }
        }
        unZigZagInt128(value);
        scaleInt128(value, static_cast<uint32_t>(scale), static_cast<uint32_t>(currentScale));
    }
};

Decimal128ColumnReader::Decimal128ColumnReader(const Type& type, StripeStreams& stripe)
        : Decimal64ColumnReader(type, stripe) {
    // PASS
}

Decimal128ColumnReader::~Decimal128ColumnReader() {
    // PASS
}

void Decimal128ColumnReader::next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) {
    ColumnReader::next(rowBatch, numValues, notNull);
    notNull = rowBatch.hasNulls ? rowBatch.notNull.data() : nullptr;
    auto& batch = dynamic_cast<Decimal128VectorBatch&>(rowBatch);
    Int128* values = batch.values.data();
    // read the next group of scales
    int64_t* scaleBuffer = batch.readScales.data();
    scaleDecoder->next(scaleBuffer, numValues, notNull);
    batch.precision = precision;
    batch.scale = scale;
    if (notNull) {
        for (size_t i = 0; i < numValues; ++i) {
            if (notNull[i]) {
                readInt128(values[i], static_cast<int32_t>(scaleBuffer[i]));
            }
        }
    } else {
        for (size_t i = 0; i < numValues; ++i) {
            readInt128(values[i], static_cast<int32_t>(scaleBuffer[i]));
        }
    }
}

class Decimal64ColumnReaderV2 : public ColumnReader {
protected:
    std::unique_ptr<RleDecoder> valueDecoder;
    int32_t precision;
    int32_t scale;

public:
    Decimal64ColumnReaderV2(const Type& type, StripeStreams& stripe);
    ~Decimal64ColumnReaderV2() override;

    uint64_t skip(uint64_t numValues) override;

    void next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) override;
};

Decimal64ColumnReaderV2::Decimal64ColumnReaderV2(const Type& type, StripeStreams& stripe) : ColumnReader(type, stripe) {
    scale = static_cast<int32_t>(type.getScale());
    precision = static_cast<int32_t>(type.getPrecision());
    std::unique_ptr<SeekableInputStream> stream = stripe.getStream(columnId, proto::Stream_Kind_DATA, true);
    if (stream == nullptr) {
        std::stringstream ss;
        ss << "DATA stream not found in Decimal64V2 column. ColumnId=" << columnId;
        throw ParseError(ss.str());
    }
    valueDecoder =
            createRleDecoder(std::move(stream), true, RleVersion_2, memoryPool, metrics, stripe.getSharedBuffer());
}

Decimal64ColumnReaderV2::~Decimal64ColumnReaderV2() {
    // PASS
}

uint64_t Decimal64ColumnReaderV2::skip(uint64_t numValues) {
    numValues = ColumnReader::skip(numValues);
    valueDecoder->skip(numValues);
    return numValues;
}

void Decimal64ColumnReaderV2::next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) {
    ColumnReader::next(rowBatch, numValues, notNull);
    notNull = rowBatch.hasNulls ? rowBatch.notNull.data() : nullptr;
    auto& batch = dynamic_cast<Decimal64VectorBatch&>(rowBatch);
    valueDecoder->next(batch.values.data(), numValues, notNull);
    batch.precision = precision;
    batch.scale = scale;
}

class DecimalHive11ColumnReader : public Decimal64ColumnReader {
private:
    bool throwOnOverflow;
    std::ostream* errorStream;

    /**
     * Read an Int128 from the stream and correct it to the desired scale.
     */
    bool readInt128(Int128& value, int32_t currentScale) {
        // -/+ 99999999999999999999999999999999999999
        static const Int128 MIN_VALUE(-0x4b3b4ca85a86c47b, 0xf675ddc000000001);
        static const Int128 MAX_VALUE(0x4b3b4ca85a86c47a, 0x098a223fffffffff);

        value = 0;
        Int128 work;
        uint32_t offset = 0;
        bool result = true;
        while (true) {
            readBuffer();
            auto ch = static_cast<unsigned char>(*(buffer++));
            work = ch & 0x7f;
            // If we have read more than 128 bits, we flag the error, but keep
            // reading bytes so the stream isn't thrown off.
            if (offset > 128 || (offset == 126 && work > 3)) {
                result = false;
            }
            work <<= offset;
            value |= work;
            offset += 7;
            if (!(ch & 0x80)) {
                break;
            }
        }

        if (!result) {
            return result;
        }
        unZigZagInt128(value);
        scaleInt128(value, static_cast<uint32_t>(scale), static_cast<uint32_t>(currentScale));
        return value >= MIN_VALUE && value <= MAX_VALUE;
    }

public:
    DecimalHive11ColumnReader(const Type& type, StripeStreams& stipe);
    ~DecimalHive11ColumnReader() override;

    void next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) override;
};

DecimalHive11ColumnReader::DecimalHive11ColumnReader(const Type& type, StripeStreams& stripe)
        : Decimal64ColumnReader(type, stripe) {
    scale = stripe.getForcedScaleOnHive11Decimal();
    throwOnOverflow = stripe.getThrowOnHive11DecimalOverflow();
    errorStream = stripe.getErrorStream();
}

DecimalHive11ColumnReader::~DecimalHive11ColumnReader() {
    // PASS
}

void DecimalHive11ColumnReader::next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) {
    ColumnReader::next(rowBatch, numValues, notNull);
    notNull = rowBatch.hasNulls ? rowBatch.notNull.data() : nullptr;
    auto& batch = dynamic_cast<Decimal128VectorBatch&>(rowBatch);
    Int128* values = batch.values.data();
    // read the next group of scales
    int64_t* scaleBuffer = batch.readScales.data();

    scaleDecoder->next(scaleBuffer, numValues, notNull);

    batch.precision = precision;
    batch.scale = scale;
    if (notNull) {
        for (size_t i = 0; i < numValues; ++i) {
            if (notNull[i]) {
                if (!readInt128(values[i], static_cast<int32_t>(scaleBuffer[i]))) {
                    if (throwOnOverflow) {
                        throw ParseError("Hive 0.11 decimal was more than 38 digits.");
                    } else {
                        *errorStream << "Warning: "
                                     << "Hive 0.11 decimal with more than 38 digits "
                                     << "replaced by NULL.\n";
                        notNull[i] = false;
                    }
                }
            }
        }
    } else {
        for (size_t i = 0; i < numValues; ++i) {
            if (!readInt128(values[i], static_cast<int32_t>(scaleBuffer[i]))) {
                if (throwOnOverflow) {
                    throw ParseError("Hive 0.11 decimal was more than 38 digits.");
                } else {
                    *errorStream << "Warning: "
                                 << "Hive 0.11 decimal with more than 38 digits "
                                 << "replaced by NULL.\n";
                    batch.hasNulls = true;
                    batch.notNull[i] = false;
                }
            }
        }
    }
}

/**
   * Create a reader for the given stripe.
   */
std::unique_ptr<ColumnReader> buildReader(const Type& type, StripeStreams& stripe) {
    switch (static_cast<int64_t>(type.getKind())) {
    case DATE:
    case INT:
    case LONG:
    case SHORT:
        return std::unique_ptr<ColumnReader>(new IntegerColumnReader(type, stripe));
    case BINARY:
    case CHAR:
    case STRING:
    case VARCHAR:
        switch (static_cast<int64_t>(stripe.getEncoding(type.getColumnId()).kind())) {
        case proto::ColumnEncoding_Kind_DICTIONARY:
        case proto::ColumnEncoding_Kind_DICTIONARY_V2:
            return std::unique_ptr<ColumnReader>(new StringDictionaryColumnReader(type, stripe));
        case proto::ColumnEncoding_Kind_DIRECT:
        case proto::ColumnEncoding_Kind_DIRECT_V2:
            return std::unique_ptr<ColumnReader>(new StringDirectColumnReader(type, stripe));
        default:
            throw NotImplementedYet("buildReader unhandled string encoding");
        }

    case BOOLEAN:
        return std::unique_ptr<ColumnReader>(new BooleanColumnReader(type, stripe));

    case BYTE:
        return std::unique_ptr<ColumnReader>(new ByteColumnReader(type, stripe));

    case LIST:
        return std::unique_ptr<ColumnReader>(new ListColumnReader(type, stripe));

    case MAP:
        return std::unique_ptr<ColumnReader>(new MapColumnReader(type, stripe));

    case UNION:
        return std::unique_ptr<ColumnReader>(new UnionColumnReader(type, stripe));

    case STRUCT:
        return std::unique_ptr<ColumnReader>(new StructColumnReader(type, stripe));

    case FLOAT:
    case DOUBLE:
        return std::unique_ptr<ColumnReader>(new DoubleColumnReader(type, stripe));

    case TIMESTAMP:
        return std::unique_ptr<ColumnReader>(new TimestampColumnReader(type, stripe, false));

    case TIMESTAMP_INSTANT:
        return std::unique_ptr<ColumnReader>(new TimestampColumnReader(type, stripe, true));

    case DECIMAL:
        // is this a Hive 0.11 or 0.12 file?
        if (type.getPrecision() == 0) {
            return std::unique_ptr<ColumnReader>(new DecimalHive11ColumnReader(type, stripe));
        }
        // can we represent the values using int64_t?
        if (type.getPrecision() <= Decimal64ColumnReader::MAX_PRECISION_64) {
            if (stripe.isDecimalAsLong()) {
                return std::unique_ptr<ColumnReader>(new Decimal64ColumnReaderV2(type, stripe));
            }
            return std::unique_ptr<ColumnReader>(new Decimal64ColumnReader(type, stripe));
        }
        // otherwise we use the Int128 implementation
        return std::unique_ptr<ColumnReader>(new Decimal128ColumnReader(type, stripe));

    default:
        throw NotImplementedYet("buildReader unhandled type");
    }
}

void collectStringDictionary(ColumnReader* reader, std::unordered_map<uint64_t, StringDictionary*>& coll) {
    auto* sreader = static_cast<StructColumnReader*>(reader);
    for (size_t i = 0; i < sreader->size(); i++) {
        ColumnReader* cr = sreader->childReaderAt(i);
        auto* sr = dynamic_cast<StringDictionaryColumnReader*>(cr);
        if (sr != nullptr) {
            coll[cr->getColumnId()] = sr->getDictionary();
        }
    }
}

} // namespace orc
