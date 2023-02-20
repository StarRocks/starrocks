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
//   https://github.com/apache/orc/tree/main/c++/src/Reader.hh

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

#include "ColumnReader.hh"
#include "RLE.hh"
#include "TypeImpl.hh"
#include "orc/Exceptions.hh"
#include "orc/Int128.hh"
#include "orc/OrcFile.hh"
#include "orc/Reader.hh"
#include "sargs/SargsApplier.hh"

namespace orc {

static const uint64_t DIRECTORY_SIZE_GUESS = 16 * 1024;

/**
  * WriterVersion Implementation
  */
class WriterVersionImpl {
private:
    WriterVersion version;

public:
    // Known Versions with issues resolved
    // The static method below is to fix global constructors Clang warning
    static const WriterVersionImpl& VERSION_HIVE_8732();

    WriterVersionImpl(WriterVersion ver) : version(ver) {}

    bool compareGT(const WriterVersion other) const { return version > other; }
};

/**
  * State shared between Reader and Row Reader
  */
struct FileContents {
    std::unique_ptr<InputStream> stream;
    std::unique_ptr<proto::PostScript> postscript;
    std::unique_ptr<proto::Footer> footer;
    std::unique_ptr<Type> schema;
    uint64_t blockSize;
    CompressionKind compression;
    MemoryPool* pool;
    std::ostream* errorStream;
    /// Decimal64 in ORCv2 uses RLE to store values. This flag indicates whether
    /// this new encoding is used.
    bool isDecimalAsLong;
    std::unique_ptr<proto::Metadata> metadata;
    ReaderMetrics* readerMetrics;
};

proto::StripeFooter getStripeFooter(const proto::StripeInformation& info, const FileContents& contents);

class ReaderImpl;
class Timezone;

class ColumnSelector {
private:
    std::map<std::string, uint64_t> nameIdMap;
    std::map<uint64_t, const Type*> idTypeMap;
    const FileContents* contents;
    std::vector<std::string> columns;

    // build map from type name and id, id to Type
    void buildTypeNameIdMap(const Type* type);
    std::string toDotColumnPath();

public:
    // Select a field by name
    void updateSelectedByName(std::vector<bool>& selectedColumns, const std::string& name);
    // Select a field by id
    void updateSelectedByFieldId(std::vector<bool>& selectedColumns, uint64_t fieldId);
    // Select a type by id
    void updateSelectedByTypeId(std::vector<bool>& selectedColumns, uint64_t typeId);

    // Select all of the recursive children of the given type.
    void selectChildren(std::vector<bool>& selectedColumns, const Type& type);

    // For each child of type, select it if one of its children
    // is selected.
    bool selectParents(std::vector<bool>& selectedColumns, const Type& type);
    /**
    * Constructor that selects columns.
    * @param contents of the file
    */
    ColumnSelector(const FileContents* contents);

    // Select the columns from the RowReaderoptions object
    void updateSelected(std::vector<bool>& selectedColumns, std::vector<bool>& lazyLoadColumns,
                        const RowReaderOptions& options);
};

class RowReaderImpl : public RowReader {
private:
    const Timezone& localTimezone;

    // contents
    std::shared_ptr<FileContents> contents;
    const bool throwOnHive11DecimalOverflow;
    const int32_t forcedScaleOnHive11Decimal;

    // inputs
    std::vector<bool> selectedColumns;
    std::vector<bool> lazyLoadColumns;

    // footer
    proto::Footer* footer;
    DataBuffer<uint64_t> firstRowOfStripe;
    mutable std::unique_ptr<Type> selectedSchema;
    bool skipBloomFilters;

    // reading state
    uint64_t previousRow;
    uint64_t firstStripe;
    uint64_t currentStripe;
    uint64_t lastStripe; // the stripe AFTER the last one
    uint64_t currentRowInStripe;
    uint64_t lazyLoadLastUsedRowInStripe; // which row in stripe loazy load files are used in last time.

    uint64_t rowsInCurrentStripe;
    // number of row groups between first stripe and last stripe
    uint64_t numRowGroupsInStripeRange;
    proto::StripeInformation currentStripeInfo;
    proto::StripeFooter currentStripeFooter;
    std::unique_ptr<ColumnReader> reader;

    bool enableEncodedBlock;
    // internal methods
    void startNextStripe();
    inline void markEndOfFile();

    // row index of current stripe with column id as the key
    std::unordered_map<uint64_t, proto::RowIndex> rowIndexes;
    std::map<uint32_t, BloomFilterIndex> bloomFilterIndex;
    std::shared_ptr<SearchArgument> sargs;
    std::unique_ptr<SargsApplier> sargsApplier;

    // desired timezone to return data of timestamp types.
    const Timezone& readerTimezone;
    bool useWriterTimezone;

    // this shared buffer will be used in reading lifetime.
    // it's introduced because we want to gather input bytes into a consecutive memory area
    // and apply RLEv2 or double decoding algorithm on this consecutive memory area efficiently
    // otherwise we have to read byte by byte which is impossible to apply efficient algorithm.
    // it's can be used only in a scope of function, and can be used by many column readers.

    mutable DataBuffer<char> sharedBuffer;

    // load stripe index if not done so
    void loadStripeIndex();

    // In case of PPD, batch size should be aware of row group boundaries.
    // If only a subset of row groups are selected then the next read should
    // stop at the end of selected range.
    static uint64_t computeBatchSize(uint64_t requestedSize, uint64_t currentRowInStripe, uint64_t rowsInCurrentStripe,
                                     uint64_t rowIndexStride, const std::vector<uint64_t>& nextSkippedRows);

    // Skip non-selected rows
    static uint64_t advanceToNextRowGroup(uint64_t currentRowInStripe, uint64_t rowsInCurrentStripe,
                                          uint64_t rowIndexStride, const std::vector<uint64_t>& nextSkippedRows);

    friend class TestRowReader_advanceToNextRowGroup_Test;
    friend class TestRowReader_computeBatchSize_Test;

    /**
     * Seek to the start of a row group in the current stripe
     * @param rowGroupEntryId the row group id to seek to
     */
    void seekToRowGroup(uint32_t rowGroupEntryId);
    void getRowGroupPosition(uint32_t rowGroupEntryId, PositionProviderMap* map);

    /**
     * Check if the file has bad bloom filters. We will skip using them in the
     * following reads.
     * @return true if it has.
     */
    bool hasBadBloomFilters();

    void buildIORanges(std::vector<InputStream::IORange>* io_ranges);

public:
    /**
    * Constructor that lets the user specify additional options.
    * @param contents of the file
    * @param options options for reading
    */
    RowReaderImpl(const std::shared_ptr<FileContents>& contents, const RowReaderOptions& options);

    // Select the columns from the options object
    const std::vector<bool>& getSelectedColumns() const override;
    const std::vector<bool>& getLazyLoadColumns() const override;

    const Type& getSelectedType() const override;

    std::unique_ptr<ColumnVectorBatch> createRowBatch(uint64_t size) const override;

    bool next(ColumnVectorBatch& data, ReadPosition* pos) override;
    void lazyLoadSeekTo(uint64_t rowInStripe) override;
    void lazyLoadNext(ColumnVectorBatch& data, uint64_t numValues) override;

    CompressionKind getCompression() const;

    uint64_t getCompressionSize() const;

    uint64_t getRowNumber() const override;

    void seekToRow(uint64_t rowNumber) override;

    const FileContents& getFileContents() const;
    bool getThrowOnHive11DecimalOverflow() const;
    bool getIsDecimalAsLong() const;
    int32_t getForcedScaleOnHive11Decimal() const;
    bool getUseWriterTimezone() const;
    DataBuffer<char>* getSharedBuffer() const;
};

class ReaderImpl : public Reader {
private:
    // FileContents
    std::shared_ptr<FileContents> contents;

    // inputs
    const ReaderOptions options;
    const uint64_t fileLength;
    const uint64_t postscriptLength;

    // footer
    proto::Footer* footer;
    uint64_t numberOfStripes;
    uint64_t getMemoryUse(int stripeIx, std::vector<bool>& selectedColumns);

    // internal methods
    void readMetadata() const;
    void checkOrcVersion();
    void getRowIndexStatistics(const proto::StripeInformation& stripeInfo, uint64_t stripeIndex,
                               const proto::StripeFooter& currentStripeFooter,
                               std::vector<std::vector<proto::ColumnStatistics> >* indexStats) const;

    // metadata
    mutable bool isMetadataLoaded;

public:
    /**
     * Constructor that lets the user specify additional options.
     * @param contents of the file
     * @param options options for reading
     * @param fileLength the length of the file in bytes
     * @param postscriptLength the length of the postscript in bytes
     */
    ReaderImpl(std::shared_ptr<FileContents> contents, const ReaderOptions& options, uint64_t fileLength,
               uint64_t postscriptLength);

    const ReaderOptions& getReaderOptions() const;

    CompressionKind getCompression() const override;

    FileVersion getFormatVersion() const override;

    WriterId getWriterId() const override;

    uint32_t getWriterIdValue() const override;

    std::string getSoftwareVersion() const override;

    WriterVersion getWriterVersion() const override;

    uint64_t getNumberOfRows() const override;

    uint64_t getRowIndexStride() const override;

    std::list<std::string> getMetadataKeys() const override;

    std::string getMetadataValue(const std::string& key) const override;

    bool hasMetadataValue(const std::string& key) const override;

    uint64_t getCompressionSize() const override;

    uint64_t getNumberOfStripes() const override;

    std::unique_ptr<StripeInformation> getStripe(uint64_t) const override;

    uint64_t getNumberOfStripeStatistics() const override;

    const std::string& getStreamName() const override;

    std::unique_ptr<StripeStatistics> getStripeStatistics(uint64_t stripeIndex) const override;

    std::unique_ptr<RowReader> createRowReader() const override;

    std::unique_ptr<RowReader> createRowReader(const RowReaderOptions& options) const override;

    uint64_t getContentLength() const override;
    uint64_t getStripeStatisticsLength() const override;
    uint64_t getFileFooterLength() const override;
    uint64_t getFilePostscriptLength() const override;
    uint64_t getFileLength() const override;

    std::unique_ptr<Statistics> getStatistics() const override;

    std::unique_ptr<ColumnStatistics> getColumnStatistics(uint32_t columnId) const override;

    std::string getSerializedFileTail() const override;

    const Type& getType() const override;

    bool hasCorrectStatistics() const override;

    const ReaderMetrics* getReaderMetrics() const override { return contents->readerMetrics; }

    const proto::PostScript* getPostscript() const { return contents->postscript.get(); }

    uint64_t getBlockSize() const { return contents->blockSize; }

    const proto::Footer* getFooter() const { return contents->footer.get(); }

    const Type* getSchema() const { return contents->schema.get(); }

    InputStream* getStream() const { return contents->stream.get(); }

    uint64_t getMemoryUse(int stripeIx = -1) override;

    uint64_t getMemoryUseByFieldId(const std::list<uint64_t>& include, int stripeIx = -1) override;

    uint64_t getMemoryUseByName(const std::list<std::string>& names, int stripeIx = -1) override;

    uint64_t getMemoryUseByTypeId(const std::list<uint64_t>& include, int stripeIx = -1) override;

    std::map<uint32_t, BloomFilterIndex> getBloomFilters(uint32_t stripeIndex,
                                                         const std::set<uint32_t>& included) const override;
};

} // namespace orc
