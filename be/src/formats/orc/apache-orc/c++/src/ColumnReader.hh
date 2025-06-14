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
//   https://github.com/apache/orc/tree/main/c++/src/ColumnReader.hh

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

#include <unordered_map>

#include "ByteRLE.hh"
#include "Compression.hh"
#include "Timezone.hh"
#include "io/InputStream.hh"
#include "orc/Vector.hh"
#include "wrap/orc-proto-wrapper.hh"

namespace orc {

class StripeStreams {
public:
    virtual ~StripeStreams();

    /**
     * Get the array of booleans for which columns are selected.
     * @return the address of an array which contains true at the index of
     *    each columnId is selected.
     */
    virtual const std::vector<bool>& getSelectedColumns() const = 0;
    virtual const std::vector<bool>& getLazyLoadColumns() const = 0;

    /**
     * Get the encoding for the given column for this stripe.
     */
    virtual proto::ColumnEncoding getEncoding(uint64_t columnId) const = 0;

    /**
     * Get the stream for the given column/kind in this stripe.
     * @param columnId the id of the column
     * @param kind the kind of the stream
     * @param shouldStream should the reading page the stream in
     * @return the new stream
     */
    virtual std::unique_ptr<SeekableInputStream> getStream(uint64_t columnId, proto::Stream_Kind kind,
                                                           bool shouldStream) const = 0;

    /**
     * Get the memory pool for this reader.
     */
    virtual MemoryPool& getMemoryPool() const = 0;

    /**
     * Get the reader metrics for this reader.
     */
    virtual ReaderMetrics* getReaderMetrics() const = 0;

    /**
     * Get the writer's timezone, so that we can convert their dates correctly.
     */
    virtual const Timezone& getWriterTimezone() const = 0;

    /**
     * Get the reader's timezone, so that we can convert their dates correctly.
     */
    virtual const Timezone& getReaderTimezone() const = 0;

    /**
     * Get the error stream.
     * @return a pointer to the stream that should get error messages
     */
    virtual std::ostream* getErrorStream() const = 0;

    /**
     * Should the reader throw when the scale overflows when reading Hive 0.11
     * decimals.
     * @return true if it should throw
     */
    virtual bool getThrowOnHive11DecimalOverflow() const = 0;

    /**
     * What is the scale forced on the Hive 0.11 decimals?
     * @return the number of scale digits
     */
    virtual int32_t getForcedScaleOnHive11Decimal() const = 0;

    /**
     * Whether decimals that have precision <=18 are encoded as fixed scale and values
     * encoded in RLE.
     */
    virtual bool isDecimalAsLong() const = 0;

    virtual bool getUseWriterTimezone() const { return false; }

    virtual DataBuffer<char>* getSharedBuffer() const { return nullptr; }
};

/**
   * The interface for reading ORC data types.
   */
class ColumnReader {
protected:
    std::unique_ptr<ByteRleDecoder> notNullDecoder;
    uint64_t columnId;
    MemoryPool& memoryPool;
    ReaderMetrics* metrics;

public:
    // Default construtor, only used for LazyColumnReader
    ColumnReader() : columnId(0), memoryPool(*getDefaultPool()), metrics(nullptr) {}
    ColumnReader(const Type& type, StripeStreams& stipe);

    virtual ~ColumnReader();

    /**
     * Skip number of specified rows.
     * @param numValues the number of values to skip
     * @return the number of non-null values skipped
     */
    virtual uint64_t skip(uint64_t numValues);

    /**
     * Read the next group of values into this rowBatch.
     * @param rowBatch the memory to read into.
     * @param numValues the number of values to read
     * @param notNull if null, all values are not null. Otherwise, it is
     *           a mask (with at least numValues bytes) for which values to
     *           set.
     */
    virtual void next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull);

    /**
     * Read the next group of values without decoding
     * @param rowBatch the memory to read into.
     * @param numValues the number of values to read
     * @param notNull if null, all values are not null. Otherwise, it is
     *           a mask (with at least numValues bytes) for which values to
     *           set.
     */
    virtual void nextEncoded(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) {
        rowBatch.isEncoded = false;
        next(rowBatch, numValues, notNull);
    }

    /**
     * Seek to beginning of a row group in the current stripe
     * @param positions a list of PositionProviders storing the positions
     */
    virtual void seekToRowGroup(PositionProviderMap* providers);

    uint64_t getColumnId() { return columnId; }

    // Functions for lazy load fields.
    virtual void lazyLoadSkip(uint64_t numValues) { skip(numValues); }

    virtual void lazyLoadNext(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) {
        next(rowBatch, numValues, notNull);
    }

    virtual void lazyLoadNextEncoded(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) {
        rowBatch.isEncoded = false;
        lazyLoadNext(rowBatch, numValues, notNull);
    }

    virtual void lazyLoadSeekToRowGroup(PositionProviderMap* providers) { seekToRowGroup(providers); }
};

/**
   * Create a reader for the given stripe.
   */
std::unique_ptr<ColumnReader> buildReader(const Type& type, std::shared_ptr<StripeStreams>& stripe);

// collect string dictionary from column reader
void collectStringDictionary(ColumnReader* reader, std::unordered_map<uint64_t, StringDictionary*>& coll);
} // namespace orc
