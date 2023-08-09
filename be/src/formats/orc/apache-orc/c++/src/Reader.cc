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
//   https://github.com/apache/orc/tree/main/c++/src/Reader.cc

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

#include "Reader.hh"

#include <algorithm>
#include <iostream>
#include <iterator>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "Adaptor.hh"
#include "BloomFilter.hh"
#include "Options.hh"
#include "Statistics.hh"
#include "StripeStream.hh"
#include "Utils.hh"
#include "wrap/coded-stream-wrapper.h"

namespace orc {
// ORC files writen by these versions of cpp writers have inconsistent bloom filter
// hashing. Bloom filters of them should not be used.
static const char* BAD_CPP_BLOOM_FILTER_VERSIONS[] = {"1.6.0", "1.6.1", "1.6.2", "1.6.3",  "1.6.4",  "1.6.5", "1.6.6",
                                                      "1.6.7", "1.6.8", "1.6.9", "1.6.10", "1.6.11", "1.7.0"};

ReaderMetrics* getDefaultReaderMetrics() {
    static ReaderMetrics internal;
    return &internal;
}

const WriterVersionImpl& WriterVersionImpl::VERSION_HIVE_8732() {
    static const WriterVersionImpl version(WriterVersion_HIVE_8732);
    return version;
}

uint64_t getCompressionBlockSize(const proto::PostScript& ps) {
    if (ps.has_compressionblocksize()) {
        return ps.compressionblocksize();
    } else {
        return 256 * 1024;
    }
}

CompressionKind convertCompressionKind(const proto::PostScript& ps) {
    if (ps.has_compression()) {
        return static_cast<CompressionKind>(ps.compression());
    } else {
        throw ParseError("Unknown compression type");
    }
}

std::string ColumnSelector::toDotColumnPath() {
    if (columns.empty()) {
        return {};
    }
    std::ostringstream columnStream;
    std::copy(columns.begin(), columns.end(), std::ostream_iterator<std::string>(columnStream, "."));
    std::string columnPath = columnStream.str();
    return columnPath.substr(0, columnPath.length() - 1);
}

WriterVersion getWriterVersionImpl(const FileContents* contents) {
    if (!contents->postscript->has_writerversion()) {
        return WriterVersion_ORIGINAL;
    }
    return static_cast<WriterVersion>(contents->postscript->writerversion());
}

void ColumnSelector::selectChildren(std::vector<bool>& selectedColumns, const Type& type) {
    auto id = static_cast<size_t>(type.getColumnId());
    if (!selectedColumns[id]) {
        selectedColumns[id] = true;
        for (size_t c = id; c <= type.getMaximumColumnId(); ++c) {
            selectedColumns[c] = true;
        }
    }
}

/**
   * Recurses over a type tree and selects the parents of every selected type.
   * @return true if any child was selected.
   */
bool ColumnSelector::selectParents(std::vector<bool>& selectedColumns, const Type& type) {
    auto id = static_cast<size_t>(type.getColumnId());
    bool result = selectedColumns[id];
    uint64_t numSubtypeSelected = 0;
    for (uint64_t c = 0; c < type.getSubtypeCount(); ++c) {
        if (selectParents(selectedColumns, *type.getSubtype(c))) {
            result = true;
            numSubtypeSelected++;
        }
    }
    selectedColumns[id] = result;

    if (type.getKind() == TypeKind::UNION && selectedColumns[id]) {
        if (0 < numSubtypeSelected && numSubtypeSelected < type.getSubtypeCount()) {
            // Subtypes of UNION should be fully selected or not selected at all.
            // Override partial subtype selections with full selections.
            for (uint64_t c = 0; c < type.getSubtypeCount(); ++c) {
                selectChildren(selectedColumns, *type.getSubtype(c));
            }
        }
    }
    return result;
}

/**
   * Recurse over a type tree and build two maps
   * map<TypeName, TypeId>, map<TypeId, Type>
   */
void ColumnSelector::buildTypeNameIdMap(const Type* type) {
    // map<type_id, Type*>
    idTypeMap[type->getColumnId()] = type;

    if (STRUCT == type->getKind()) {
        for (size_t i = 0; i < type->getSubtypeCount(); ++i) {
            const std::string& fieldName = type->getFieldName(i);
            columns.push_back(fieldName);
            nameIdMap[toDotColumnPath()] = type->getSubtype(i)->getColumnId();
            buildTypeNameIdMap(type->getSubtype(i));
            columns.pop_back();
        }
    } else {
        // other non-logical type
        for (size_t j = 0; j < type->getSubtypeCount(); ++j) {
            buildTypeNameIdMap(type->getSubtype(j));
        }
    }
}

void ColumnSelector::updateSelected(std::vector<bool>& selectedColumns, std::vector<bool>& lazyLoadColumns,
                                    const RowReaderOptions& options) {
    selectedColumns.assign(static_cast<size_t>(contents->footer->types_size()), false);
    lazyLoadColumns.assign(static_cast<size_t>(contents->footer->types_size()), false);

    if (contents->schema->getKind() == STRUCT && options.getIndexesSet()) {
        for (unsigned long field : options.getInclude()) {
            updateSelectedByFieldId(selectedColumns, field);
        }
    } else if (contents->schema->getKind() == STRUCT && options.getNamesSet()) {
        for (const auto& field : options.getIncludeNames()) {
            updateSelectedByName(selectedColumns, field);
        }
        for (const auto& field : options.getLazyLoadColumnNames()) {
            updateSelectedByName(lazyLoadColumns, field);
        }
    } else if (options.getTypeIdsSet()) {
        for (unsigned long typeId : options.getInclude()) {
            updateSelectedByTypeId(selectedColumns, typeId);
        }
        for (unsigned long typeId : options.getLazyLoadColumnIndexes()) {
            updateSelectedByTypeId(lazyLoadColumns, typeId);
        }
    } else {
        // default is to select all columns
        std::fill(selectedColumns.begin(), selectedColumns.end(), true);
    }
    selectParents(selectedColumns, *contents->schema);
    selectedColumns[0] = true; // column 0 is selected by default
}

void ColumnSelector::updateSelectedByFieldId(std::vector<bool>& selectedColumns, uint64_t fieldId) {
    if (fieldId < contents->schema->getSubtypeCount()) {
        selectChildren(selectedColumns, *contents->schema->getSubtype(fieldId));
    } else {
        std::stringstream buffer;
        buffer << "Invalid column selected " << fieldId << " out of " << contents->schema->getSubtypeCount();
        throw ParseError(buffer.str());
    }
}

void ColumnSelector::updateSelectedByTypeId(std::vector<bool>& selectedColumns, uint64_t typeId) {
    if (typeId < selectedColumns.size()) {
        const Type& type = *idTypeMap[typeId];
        selectChildren(selectedColumns, type);
    } else {
        std::stringstream buffer;
        buffer << "Invalid type id selected " << typeId << " out of " << selectedColumns.size();
        throw ParseError(buffer.str());
    }
}

void ColumnSelector::updateSelectedByName(std::vector<bool>& selectedColumns, const std::string& fieldName) {
    std::map<std::string, uint64_t>::const_iterator ite = nameIdMap.find(fieldName);
    if (ite != nameIdMap.end()) {
        updateSelectedByTypeId(selectedColumns, ite->second);
    } else {
        bool first = true;
        std::ostringstream ss;
        ss << "Invalid column selected " << fieldName << ". Valid names are ";
        for (auto& it : nameIdMap) {
            if (!first) ss << ", ";
            ss << it.first;
            first = false;
        }
        throw ParseError(ss.str());
    }
}

ColumnSelector::ColumnSelector(const FileContents* _contents) : contents(_contents) {
    buildTypeNameIdMap(contents->schema.get());
}

RowReaderImpl::RowReaderImpl(const std::shared_ptr<FileContents>& _contents, const RowReaderOptions& opts)
        : localTimezone(getLocalTimezone()),
          contents(_contents),
          throwOnHive11DecimalOverflow(opts.getThrowOnHive11DecimalOverflow()),
          forcedScaleOnHive11Decimal(opts.getForcedScaleOnHive11Decimal()),
          footer(contents->footer.get()),
          firstRowOfStripe(*contents->pool, 0),
          enableEncodedBlock(opts.getEnableLazyDecoding()),
          readerTimezone(getTimezoneByName(opts.getTimezoneName())),
          useWriterTimezone(opts.getUseWriterTimezone()),
          sharedBuffer(*contents->pool, 0) {
    uint64_t numberOfStripes;
    numberOfStripes = static_cast<uint64_t>(footer->stripes_size());
    currentStripe = numberOfStripes;
    lastStripe = 0;
    currentRowInStripe = 0;
    lazyLoadLastUsedRowInStripe = 0;
    rowsInCurrentStripe = 0;
    numRowGroupsInStripeRange = 0;
    uint64_t rowTotal = 0;

    firstRowOfStripe.resize(numberOfStripes);
    for (size_t i = 0; i < numberOfStripes; ++i) {
        firstRowOfStripe[i] = rowTotal;
        proto::StripeInformation stripeInfo = footer->stripes(static_cast<int>(i));
        rowTotal += stripeInfo.numberofrows();
        bool isStripeInRange =
                stripeInfo.offset() >= opts.getOffset() && stripeInfo.offset() < opts.getOffset() + opts.getLength();
        if (isStripeInRange) {
            if (i < currentStripe) {
                currentStripe = i;
            }
            if (i >= lastStripe) {
                lastStripe = i + 1;
            }

            if (footer->rowindexstride() > 0) {
                numRowGroupsInStripeRange +=
                        (stripeInfo.numberofrows() + footer->rowindexstride() - 1) / footer->rowindexstride();
            }
        }
    }
    firstStripe = currentStripe;

    if (currentStripe == 0) {
        previousRow = (std::numeric_limits<uint64_t>::max)();
    } else if (currentStripe == numberOfStripes) {
        previousRow = footer->numberofrows();
    } else {
        previousRow = firstRowOfStripe[firstStripe] - 1;
    }

    ColumnSelector column_selector(contents.get());
    column_selector.updateSelected(selectedColumns, lazyLoadColumns, opts);

    // prepare SargsApplier if SearchArgument is available
    if (opts.getSearchArgument() && footer->rowindexstride() > 0) {
        sargs = opts.getSearchArgument();
        sargsApplier.reset(new SargsApplier(*contents->schema, sargs.get(), footer->rowindexstride(),
                                            getWriterVersionImpl(_contents.get()), contents->readerMetrics,
                                            opts.getRowReaderFilter().get()));
    }

    skipBloomFilters = hasBadBloomFilters();
}

// Check if the file has inconsistent bloom filters.
bool RowReaderImpl::hasBadBloomFilters() {
    // Only C++ writer in old releases could have bad bloom filters.
    if (footer->writer() != ORC_CPP_WRITER) return false;
    // 'softwareVersion' is added in 1.5.13, 1.6.11, and 1.7.0.
    // 1.6.x releases before 1.6.11 won't have it. On the other side, the C++ writer
    // supports writing bloom filters since 1.6.0. So files written by the C++ writer
    // and with 'softwareVersion' unset would have bad bloom filters.
    if (!footer->has_softwareversion()) return true;

    const std::string& fullVersion = footer->softwareversion();
    std::string version;
    // Deal with snapshot versions, e.g. 1.6.12-SNAPSHOT.
    if (fullVersion.find('-') != std::string::npos) {
        version = fullVersion.substr(0, fullVersion.find('-'));
    } else {
        version = fullVersion;
    }
    for (const char* v : BAD_CPP_BLOOM_FILTER_VERSIONS) {
        if (version == v) {
            return true;
        }
    }
    return false;
}

CompressionKind RowReaderImpl::getCompression() const {
    return contents->compression;
}

uint64_t RowReaderImpl::getCompressionSize() const {
    return contents->blockSize;
}

const std::vector<bool>& RowReaderImpl::getSelectedColumns() const {
    return selectedColumns;
}

const std::vector<bool>& RowReaderImpl::getLazyLoadColumns() const {
    return lazyLoadColumns;
}

const Type& RowReaderImpl::getSelectedType() const {
    if (selectedSchema == nullptr) {
        selectedSchema = buildSelectedType(contents->schema.get(), selectedColumns);
    }
    return *(selectedSchema.get());
}

uint64_t RowReaderImpl::getRowNumber() const {
    return previousRow;
}

void RowReaderImpl::seekToRow(uint64_t rowNumber) {
    // Empty file
    if (lastStripe == 0) {
        return;
    }

    // If we are reading only a portion of the file
    // (bounded by firstStripe and lastStripe),
    // seeking before or after the portion of interest should return no data.
    // Implement this by setting previousRow to the number of rows in the file.

    // seeking past lastStripe
    auto num_stripes = static_cast<uint64_t>(footer->stripes_size());
    if ((lastStripe == num_stripes && rowNumber >= footer->numberofrows()) ||
        (lastStripe < num_stripes && rowNumber >= firstRowOfStripe[lastStripe])) {
        currentStripe = num_stripes;
        previousRow = footer->numberofrows();
        return;
    }

    uint64_t seekToStripe = 0;
    while (seekToStripe + 1 < lastStripe && firstRowOfStripe[seekToStripe + 1] <= rowNumber) {
        seekToStripe++;
    }

    // seeking before the first stripe
    if (seekToStripe < firstStripe) {
        currentStripe = num_stripes;
        previousRow = footer->numberofrows();
        return;
    }

    currentStripe = seekToStripe;
    currentRowInStripe = rowNumber - firstRowOfStripe[currentStripe];
    previousRow = rowNumber;
    startNextStripe();

    uint64_t rowsToSkip = currentRowInStripe;
    auto rowIndexStride = footer->rowindexstride();
    // seek to the target row group if row indexes exists
    if (rowIndexStride > 0 && currentStripeInfo.indexlength() > 0) {
        // when predicate push down is enabled, above call to startNextStripe()
        // will move current row to 1st matching row group; here we only need
        // to deal with the case when PPD is not enabled.
        if (!sargsApplier) {
            if (rowIndexes.empty()) {
                loadStripeIndex();
            }
            auto rowGroupId = static_cast<uint32_t>(currentRowInStripe / footer->rowindexstride());
            rowsToSkip -= static_cast<uint64_t>(rowGroupId) * footer->rowindexstride();

            if (rowGroupId != 0) {
                seekToRowGroup(rowGroupId);
            }
        }
        // skip leading rows in the target row group
        rowsToSkip %= rowIndexStride;
    }

    // 'reader' is reset in startNextStripe(). It could be nullptr if 'rowsToSkip' is 0,
    // e.g. when startNextStripe() skips all remaining rows of the file.
    if (rowsToSkip > 0) {
        reader->skip(rowsToSkip);
    }
}

void RowReaderImpl::loadStripeIndex() {
    // reset all previous row indexes
    rowIndexes.clear();
    bloomFilterIndex.clear();

    // obtain row indexes for selected columns
    uint64_t offset = currentStripeInfo.offset();
    for (int i = 0; i < currentStripeFooter.streams_size(); ++i) {
        const proto::Stream& pbStream = currentStripeFooter.streams(i);
        uint64_t colId = pbStream.column();
        if (selectedColumns[colId] && pbStream.has_kind() &&
            (pbStream.kind() == proto::Stream_Kind_ROW_INDEX ||
             pbStream.kind() == proto::Stream_Kind_BLOOM_FILTER_UTF8)) {
            std::unique_ptr<SeekableInputStream> inStream =
                    createDecompressor(getCompression(),
                                       std::unique_ptr<SeekableInputStream>(new SeekableFileInputStream(
                                               contents->stream.get(), offset, pbStream.length(), *contents->pool)),
                                       getCompressionSize(), *contents->pool, contents->readerMetrics);

            if (pbStream.kind() == proto::Stream_Kind_ROW_INDEX) {
                proto::RowIndex rowIndex;
                if (!rowIndex.ParseFromZeroCopyStream(inStream.get())) {
                    throw ParseError("Failed to parse the row index");
                }
                rowIndexes[colId] = rowIndex;
            } else if (!skipBloomFilters) { // Stream_Kind_BLOOM_FILTER_UTF8
                proto::BloomFilterIndex pbBFIndex;
                if (!pbBFIndex.ParseFromZeroCopyStream(inStream.get())) {
                    throw ParseError("Failed to parse bloom filter index");
                }
                BloomFilterIndex bfIndex;
                for (int j = 0; j < pbBFIndex.bloomfilter_size(); j++) {
                    bfIndex.entries.push_back(BloomFilterUTF8Utils::deserialize(
                            pbStream.kind(), currentStripeFooter.columns(static_cast<int>(pbStream.column())),
                            pbBFIndex.bloomfilter(j)));
                }
                // add bloom filters to result for one column
                bloomFilterIndex[pbStream.column()] = bfIndex;
            }
        }
        offset += pbStream.length();
    }
}

void RowReaderImpl::getRowGroupPosition(uint32_t rowGroupEntryId, PositionProviderMap* map) {
    for (const auto& rowIndexe : rowIndexes) {
        uint64_t colId = rowIndexe.first;
        const proto::RowIndexEntry& entry = rowIndexe.second.entry(static_cast<int32_t>(rowGroupEntryId));
        map->positions.emplace_back();
        auto& position = map->positions.back();
        for (int pos = 0; pos != entry.positions_size(); ++pos) {
            position.push_back(entry.positions(pos));
        }
        map->providers.insert(std::make_pair(colId, PositionProvider(position)));
    }
}

void RowReaderImpl::seekToRowGroup(uint32_t rowGroupEntryId) {
    PositionProviderMap map;
    getRowGroupPosition(rowGroupEntryId, &map);
    reader->seekToRowGroup(&map);
}

const FileContents& RowReaderImpl::getFileContents() const {
    return *contents;
}

bool RowReaderImpl::getThrowOnHive11DecimalOverflow() const {
    return throwOnHive11DecimalOverflow;
}

bool RowReaderImpl::getIsDecimalAsLong() const {
    return contents->isDecimalAsLong;
}

int32_t RowReaderImpl::getForcedScaleOnHive11Decimal() const {
    return forcedScaleOnHive11Decimal;
}

bool RowReaderImpl::getUseWriterTimezone() const {
    return useWriterTimezone;
}

DataBuffer<char>* RowReaderImpl::getSharedBuffer() const {
    return &sharedBuffer;
}
proto::StripeFooter getStripeFooter(const proto::StripeInformation& info, const FileContents& contents) {
    uint64_t stripeFooterStart = info.offset() + info.indexlength() + info.datalength();
    uint64_t stripeFooterLength = info.footerlength();
    std::unique_ptr<SeekableInputStream> pbStream =
            createDecompressor(contents.compression,
                               std::unique_ptr<SeekableInputStream>(new SeekableFileInputStream(
                                       contents.stream.get(), stripeFooterStart, stripeFooterLength, *contents.pool)),
                               contents.blockSize, *contents.pool, contents.readerMetrics);
    proto::StripeFooter result;
    if (!result.ParseFromZeroCopyStream(pbStream.get())) {
        throw ParseError(std::string("bad StripeFooter from ") + pbStream->getName());
    }
    // Verify StripeFooter in case it's corrupt
    if (result.columns_size() != contents.footer->types_size()) {
        std::stringstream msg;
        msg << "bad number of ColumnEncodings in StripeFooter: expected=" << contents.footer->types_size()
            << ", actual=" << result.columns_size();
        throw ParseError(msg.str());
    }
    return result;
}

ReaderImpl::ReaderImpl(std::shared_ptr<FileContents> _contents, const ReaderOptions& opts, uint64_t _fileLength,
                       uint64_t _postscriptLength)
        : contents(std::move(_contents)),
          options(opts),
          fileLength(_fileLength),
          postscriptLength(_postscriptLength),
          footer(contents->footer.get()) {
    isMetadataLoaded = false;
    checkOrcVersion();
    numberOfStripes = static_cast<uint64_t>(footer->stripes_size());
    contents->schema = REDUNDANT_MOVE(convertType(footer->types(0), *footer));
    contents->blockSize = getCompressionBlockSize(*contents->postscript);
    contents->compression = convertCompressionKind(*contents->postscript);
}

std::string ReaderImpl::getSerializedFileTail() const {
    proto::FileTail tail;
    proto::PostScript* mutable_ps = tail.mutable_postscript();
    mutable_ps->CopyFrom(*contents->postscript);
    proto::Footer* mutableFooter = tail.mutable_footer();
    mutableFooter->CopyFrom(*footer);
    tail.set_filelength(fileLength);
    tail.set_postscriptlength(postscriptLength);
    std::string result;
    if (!tail.SerializeToString(&result)) {
        throw ParseError("Failed to serialize file tail");
    }
    return result;
}

const ReaderOptions& ReaderImpl::getReaderOptions() const {
    return options;
}

CompressionKind ReaderImpl::getCompression() const {
    return contents->compression;
}

uint64_t ReaderImpl::getCompressionSize() const {
    return contents->blockSize;
}

uint64_t ReaderImpl::getNumberOfStripes() const {
    return numberOfStripes;
}

uint64_t ReaderImpl::getNumberOfStripeStatistics() const {
    if (!isMetadataLoaded) {
        readMetadata();
    }
    return contents->metadata == nullptr ? 0 : static_cast<uint64_t>(contents->metadata->stripestats_size());
}

std::unique_ptr<StripeInformation> ReaderImpl::getStripe(uint64_t stripeIndex) const {
    if (stripeIndex > getNumberOfStripes()) {
        throw std::logic_error("stripe index out of range");
    }
    proto::StripeInformation stripeInfo = footer->stripes(static_cast<int>(stripeIndex));

    return std::unique_ptr<StripeInformation>(new StripeInformationImpl(
            stripeInfo.offset(), stripeInfo.indexlength(), stripeInfo.datalength(), stripeInfo.footerlength(),
            stripeInfo.numberofrows(), contents->stream.get(), *contents->pool, contents->compression,
            contents->blockSize, contents->readerMetrics));
}

FileVersion ReaderImpl::getFormatVersion() const {
    if (contents->postscript->version_size() != 2) {
        return FileVersion::v_0_11();
    }
    return {contents->postscript->version(0), contents->postscript->version(1)};
}

uint64_t ReaderImpl::getNumberOfRows() const {
    return footer->numberofrows();
}

WriterId ReaderImpl::getWriterId() const {
    if (footer->has_writer()) {
        uint32_t id = footer->writer();
        if (id > WriterId::TRINO_WRITER) {
            return WriterId::UNKNOWN_WRITER;
        } else {
            return static_cast<WriterId>(id);
        }
    }
    return WriterId::ORC_JAVA_WRITER;
}

uint32_t ReaderImpl::getWriterIdValue() const {
    if (footer->has_writer()) {
        return footer->writer();
    } else {
        return WriterId::ORC_JAVA_WRITER;
    }
}

std::string ReaderImpl::getSoftwareVersion() const {
    std::ostringstream buffer;
    buffer << writerIdToString(getWriterIdValue());
    if (footer->has_softwareversion()) {
        buffer << " " << footer->softwareversion();
    }
    return buffer.str();
}

WriterVersion ReaderImpl::getWriterVersion() const {
    return getWriterVersionImpl(contents.get());
}

uint64_t ReaderImpl::getContentLength() const {
    return footer->contentlength();
}

uint64_t ReaderImpl::getStripeStatisticsLength() const {
    return contents->postscript->metadatalength();
}

uint64_t ReaderImpl::getFileFooterLength() const {
    return contents->postscript->footerlength();
}

uint64_t ReaderImpl::getFilePostscriptLength() const {
    return postscriptLength;
}

uint64_t ReaderImpl::getFileLength() const {
    return fileLength;
}

uint64_t ReaderImpl::getRowIndexStride() const {
    return footer->rowindexstride();
}

const std::string& ReaderImpl::getStreamName() const {
    return contents->stream->getName();
}

std::list<std::string> ReaderImpl::getMetadataKeys() const {
    std::list<std::string> result;
    for (int i = 0; i < footer->metadata_size(); ++i) {
        result.push_back(footer->metadata(i).name());
    }
    return result;
}

std::string ReaderImpl::getMetadataValue(const std::string& key) const {
    for (int i = 0; i < footer->metadata_size(); ++i) {
        if (footer->metadata(i).name() == key) {
            return footer->metadata(i).value();
        }
    }
    throw std::range_error("key not found");
}

void ReaderImpl::getRowIndexStatistics(const proto::StripeInformation& stripeInfo, uint64_t stripeIndex,
                                       const proto::StripeFooter& currentStripeFooter,
                                       std::vector<std::vector<proto::ColumnStatistics>>* indexStats) const {
    int num_streams = currentStripeFooter.streams_size();
    uint64_t offset = stripeInfo.offset();
    uint64_t indexEnd = stripeInfo.offset() + stripeInfo.indexlength();
    for (int i = 0; i < num_streams; i++) {
        const proto::Stream& stream = currentStripeFooter.streams(i);
        auto streamKind = static_cast<StreamKind>(stream.kind());
        auto length = static_cast<uint64_t>(stream.length());
        if (streamKind == StreamKind::StreamKind_ROW_INDEX) {
            if (offset + length > indexEnd) {
                std::stringstream msg;
                msg << "Malformed RowIndex stream meta in stripe " << stripeIndex << ": streamOffset=" << offset
                    << ", streamLength=" << length << ", stripeOffset=" << stripeInfo.offset()
                    << ", stripeIndexLength=" << stripeInfo.indexlength();
                throw ParseError(msg.str());
            }
            std::unique_ptr<SeekableInputStream> pbStream =
                    createDecompressor(contents->compression,
                                       std::unique_ptr<SeekableInputStream>(new SeekableFileInputStream(
                                               contents->stream.get(), offset, length, *contents->pool)),
                                       contents->blockSize, *(contents->pool), contents->readerMetrics);

            proto::RowIndex rowIndex;
            if (!rowIndex.ParseFromZeroCopyStream(pbStream.get())) {
                throw ParseError("Failed to parse RowIndex from stripe footer");
            }
            int num_entries = rowIndex.entry_size();
            auto column = static_cast<size_t>(stream.column());
            for (int j = 0; j < num_entries; j++) {
                const proto::RowIndexEntry& entry = rowIndex.entry(j);
                (*indexStats)[column].push_back(entry.statistics());
            }
        }
        offset += length;
    }
}

bool ReaderImpl::hasMetadataValue(const std::string& key) const {
    for (int i = 0; i < footer->metadata_size(); ++i) {
        if (footer->metadata(i).name() == key) {
            return true;
        }
    }
    return false;
}

const Type& ReaderImpl::getType() const {
    return *(contents->schema.get());
}

std::unique_ptr<StripeStatistics> ReaderImpl::getStripeStatistics(uint64_t stripeIndex) const {
    if (!isMetadataLoaded) {
        readMetadata();
    }
    if (contents->metadata == nullptr) {
        throw std::logic_error("No stripe statistics in file");
    }
    auto num_cols = static_cast<size_t>(contents->metadata->stripestats(static_cast<int>(stripeIndex)).colstats_size());
    std::vector<std::vector<proto::ColumnStatistics>> indexStats(num_cols);

    proto::StripeInformation currentStripeInfo = footer->stripes(static_cast<int>(stripeIndex));
    proto::StripeFooter currentStripeFooter = getStripeFooter(currentStripeInfo, *contents);

    getRowIndexStatistics(currentStripeInfo, stripeIndex, currentStripeFooter, &indexStats);

    const Timezone& writerTZ = currentStripeFooter.has_writertimezone()
                                       ? getTimezoneByName(currentStripeFooter.writertimezone())
                                       : getLocalTimezone();
    StatContext statContext(hasCorrectStatistics(), &writerTZ);
    return std::unique_ptr<StripeStatistics>(new StripeStatisticsImpl(
            contents->metadata->stripestats(static_cast<int>(stripeIndex)), indexStats, statContext));
}

std::unique_ptr<Statistics> ReaderImpl::getStatistics() const {
    StatContext statContext(hasCorrectStatistics());
    return std::unique_ptr<Statistics>(new StatisticsImpl(*footer, statContext));
}

std::unique_ptr<ColumnStatistics> ReaderImpl::getColumnStatistics(uint32_t index) const {
    if (index >= static_cast<uint64_t>(footer->statistics_size())) {
        throw std::logic_error("column index out of range");
    }
    proto::ColumnStatistics col = footer->statistics(static_cast<int32_t>(index));

    StatContext statContext(hasCorrectStatistics());
    return std::unique_ptr<ColumnStatistics>(convertColumnStatistics(col, statContext));
}

void ReaderImpl::readMetadata() const {
    uint64_t metadataSize = contents->postscript->metadatalength();
    uint64_t footerLength = contents->postscript->footerlength();
    if (fileLength < metadataSize + footerLength + postscriptLength + 1) {
        std::stringstream msg;
        msg << "Invalid Metadata length: fileLength=" << fileLength << ", metadataLength=" << metadataSize
            << ", footerLength=" << footerLength << ", postscriptLength=" << postscriptLength;
        throw ParseError(msg.str());
    }
    uint64_t metadataStart = fileLength - metadataSize - footerLength - postscriptLength - 1;
    if (metadataSize != 0) {
        std::unique_ptr<SeekableInputStream> pbStream =
                createDecompressor(contents->compression,
                                   std::unique_ptr<SeekableInputStream>(new SeekableFileInputStream(
                                           contents->stream.get(), metadataStart, metadataSize, *contents->pool)),
                                   contents->blockSize, *contents->pool, contents->readerMetrics);
        contents->metadata.reset(new proto::Metadata());
        if (!contents->metadata->ParseFromZeroCopyStream(pbStream.get())) {
            throw ParseError("Failed to parse the metadata");
        }
    }
    isMetadataLoaded = true;
}

bool ReaderImpl::hasCorrectStatistics() const {
    return !WriterVersionImpl::VERSION_HIVE_8732().compareGT(getWriterVersion());
}

void ReaderImpl::checkOrcVersion() {
    FileVersion version = getFormatVersion();
    if (version != FileVersion(0, 11) && version != FileVersion(0, 12)) {
        *(options.getErrorStream()) << "Warning: ORC file " << contents->stream->getName()
                                    << " was written in an unknown format version " << version.toString() << "\n";
    }
}

std::unique_ptr<RowReader> ReaderImpl::createRowReader() const {
    RowReaderOptions defaultOpts;
    return createRowReader(defaultOpts);
}

std::unique_ptr<RowReader> ReaderImpl::createRowReader(const RowReaderOptions& opts) const {
    if (opts.getSearchArgument() && !isMetadataLoaded) {
        // load stripe statistics for PPD
        readMetadata();
    }
    return std::unique_ptr<RowReader>(new RowReaderImpl(contents, opts));
}

uint64_t maxStreamsForType(const proto::Type& type) {
    switch (static_cast<int64_t>(type.kind())) {
    case proto::Type_Kind_STRUCT:
        return 1;
    case proto::Type_Kind_INT:
    case proto::Type_Kind_LONG:
    case proto::Type_Kind_SHORT:
    case proto::Type_Kind_FLOAT:
    case proto::Type_Kind_DOUBLE:
    case proto::Type_Kind_BOOLEAN:
    case proto::Type_Kind_BYTE:
    case proto::Type_Kind_DATE:
    case proto::Type_Kind_LIST:
    case proto::Type_Kind_MAP:
    case proto::Type_Kind_UNION:
        return 2;
    case proto::Type_Kind_BINARY:
    case proto::Type_Kind_DECIMAL:
    case proto::Type_Kind_TIMESTAMP:
    case proto::Type_Kind_TIMESTAMP_INSTANT:
        return 3;
    case proto::Type_Kind_CHAR:
    case proto::Type_Kind_STRING:
    case proto::Type_Kind_VARCHAR:
        return 4;
    default:
        return 0;
    }
}

uint64_t ReaderImpl::getMemoryUse(int stripeIx) {
    std::vector<bool> selectedColumns;
    selectedColumns.assign(static_cast<size_t>(contents->footer->types_size()), true);
    return getMemoryUse(stripeIx, selectedColumns);
}

uint64_t ReaderImpl::getMemoryUseByFieldId(const std::list<uint64_t>& include, int stripeIx) {
    std::vector<bool> selectedColumns;
    selectedColumns.assign(static_cast<size_t>(contents->footer->types_size()), false);
    ColumnSelector column_selector(contents.get());
    if (contents->schema->getKind() == STRUCT && include.begin() != include.end()) {
        for (unsigned long field : include) {
            column_selector.updateSelectedByFieldId(selectedColumns, field);
        }
    } else {
        // default is to select all columns
        std::fill(selectedColumns.begin(), selectedColumns.end(), true);
    }
    column_selector.selectParents(selectedColumns, *contents->schema);
    selectedColumns[0] = true; // column 0 is selected by default
    return getMemoryUse(stripeIx, selectedColumns);
}

uint64_t ReaderImpl::getMemoryUseByName(const std::list<std::string>& names, int stripeIx) {
    std::vector<bool> selectedColumns;
    selectedColumns.assign(static_cast<size_t>(contents->footer->types_size()), false);
    ColumnSelector column_selector(contents.get());
    if (contents->schema->getKind() == STRUCT && names.begin() != names.end()) {
        for (const auto& name : names) {
            column_selector.updateSelectedByName(selectedColumns, name);
        }
    } else {
        // default is to select all columns
        std::fill(selectedColumns.begin(), selectedColumns.end(), true);
    }
    column_selector.selectParents(selectedColumns, *contents->schema);
    selectedColumns[0] = true; // column 0 is selected by default
    return getMemoryUse(stripeIx, selectedColumns);
}

uint64_t ReaderImpl::getMemoryUseByTypeId(const std::list<uint64_t>& include, int stripeIx) {
    std::vector<bool> selectedColumns;
    selectedColumns.assign(static_cast<size_t>(contents->footer->types_size()), false);
    ColumnSelector column_selector(contents.get());
    if (include.begin() != include.end()) {
        for (unsigned long field : include) {
            column_selector.updateSelectedByTypeId(selectedColumns, field);
        }
    } else {
        // default is to select all columns
        std::fill(selectedColumns.begin(), selectedColumns.end(), true);
    }
    column_selector.selectParents(selectedColumns, *contents->schema);
    selectedColumns[0] = true; // column 0 is selected by default
    return getMemoryUse(stripeIx, selectedColumns);
}

uint64_t ReaderImpl::getMemoryUse(int stripeIx, std::vector<bool>& selectedColumns) {
    uint64_t maxDataLength = 0;

    if (stripeIx >= 0 && stripeIx < footer->stripes_size()) {
        uint64_t stripe = footer->stripes(stripeIx).datalength();
        if (maxDataLength < stripe) {
            maxDataLength = stripe;
        }
    } else {
        for (int i = 0; i < footer->stripes_size(); i++) {
            uint64_t stripe = footer->stripes(i).datalength();
            if (maxDataLength < stripe) {
                maxDataLength = stripe;
            }
        }
    }

    bool hasStringColumn = false;
    uint64_t nSelectedStreams = 0;
    for (int i = 0; !hasStringColumn && i < footer->types_size(); i++) {
        if (selectedColumns[static_cast<size_t>(i)]) {
            const proto::Type& type = footer->types(i);
            nSelectedStreams += maxStreamsForType(type);
            switch (static_cast<int64_t>(type.kind())) {
            case proto::Type_Kind_CHAR:
            case proto::Type_Kind_STRING:
            case proto::Type_Kind_VARCHAR:
            case proto::Type_Kind_BINARY: {
                hasStringColumn = true;
                break;
            }
            default: {
                break;
            }
            }
        }
    }

    /* If a string column is read, use stripe datalength as a memory estimate
     * because we don't know the dictionary size. Multiply by 2 because
     * a string column requires two buffers:
     * in the input stream and in the seekable input stream.
     * If no string column is read, estimate from the number of streams.
     */
    uint64_t memory = hasStringColumn ? 2 * maxDataLength
                                      : std::min(uint64_t(maxDataLength),
                                                 nSelectedStreams * contents->stream->getNaturalReadSize());

    // Do we need even more memory to read the footer or the metadata?
    if (memory < contents->postscript->footerlength() + DIRECTORY_SIZE_GUESS) {
        memory = contents->postscript->footerlength() + DIRECTORY_SIZE_GUESS;
    }
    if (memory < contents->postscript->metadatalength()) {
        memory = contents->postscript->metadatalength();
    }

    // Account for firstRowOfStripe.
    memory += static_cast<uint64_t>(footer->stripes_size()) * sizeof(uint64_t);

    // Decompressors need buffers for each stream
    uint64_t decompressorMemory = 0;
    if (contents->compression != CompressionKind_NONE) {
        for (int i = 0; i < footer->types_size(); i++) {
            if (selectedColumns[static_cast<size_t>(i)]) {
                const proto::Type& type = footer->types(i);
                decompressorMemory += maxStreamsForType(type) * contents->blockSize;
            }
        }
        if (contents->compression == CompressionKind_SNAPPY) {
            decompressorMemory *= 2; // Snappy decompressor uses a second buffer
        }
    }

    return memory + decompressorMemory;
}

// Update fields to indicate we've reached the end of file
void RowReaderImpl::markEndOfFile() {
    currentStripe = lastStripe;
    currentRowInStripe = 0;
    rowsInCurrentStripe = 0;
    if (lastStripe == 0) {
        // Empty file
        previousRow = 0;
    } else {
        previousRow =
                firstRowOfStripe[lastStripe - 1] + footer->stripes(static_cast<int>(lastStripe - 1)).numberofrows();
    }
}

void RowReaderImpl::buildIORanges(std::vector<InputStream::IORange>* io_ranges) {
    // column streams: index & data
    uint64_t offset = currentStripeInfo.offset();
    for (const proto::Stream& stream : currentStripeFooter.streams()) {
        uint32_t columnId = stream.column();
        uint64_t length = stream.length();
        if (selectedColumns[columnId] || lazyLoadColumns[columnId]) {
            io_ranges->emplace_back(InputStream::IORange{.offset = offset, .size = length});
        }
        offset += length;
    }
}

void RowReaderImpl::startNextStripe() {
    reader.reset(); // ColumnReaders use lots of memory; free old memory first
    rowIndexes.clear();
    bloomFilterIndex.clear();
    bool streamIORangesEnabled = contents->stream->isIORangesEnabled();

    // evaluate file statistics if it exists
    if (sargsApplier && !sargsApplier->evaluateFileStatistics(*footer, numRowGroupsInStripeRange)) {
        // skip the entire file
        markEndOfFile();
        return;
    }

    while (currentStripe < lastStripe) {
        currentStripeInfo = footer->stripes(static_cast<int>(currentStripe));
        uint64_t fileLength = contents->stream->getLength();
        size_t stripeSize =
                currentStripeInfo.indexlength() + currentStripeInfo.datalength() + currentStripeInfo.footerlength();
        if ((currentStripeInfo.offset() + stripeSize) >= fileLength) {
            std::stringstream msg;
            msg << "Malformed StripeInformation at stripe index " << currentStripe << ": fileLength=" << fileLength
                << ", StripeInfo=(offset=" << currentStripeInfo.offset()
                << ", indexLength=" << currentStripeInfo.indexlength()
                << ", dataLength=" << currentStripeInfo.datalength()
                << ", footerLength=" << currentStripeInfo.footerlength() << ")";
            throw ParseError(msg.str());
        }

        bool skipStripe = false;
        if (sargsApplier && sargsApplier->getRowReaderFilter()) {
            if (sargsApplier->getRowReaderFilter()->filterOnOpeningStripe(currentStripe, &currentStripeInfo)) {
                skipStripe = true;
                goto end;
            }
        }

        if (streamIORangesEnabled) {
            contents->stream->clearIORanges();
        } else {
            contents->stream->prepareCache(InputStream::PrepareCacheScope::READ_FULL_STRIPE, currentStripeInfo.offset(),
                                           stripeSize);
        }

        currentStripeFooter = getStripeFooter(currentStripeInfo, *contents);
        rowsInCurrentStripe = currentStripeInfo.numberofrows();
        if (streamIORangesEnabled) {
            std::vector<InputStream::IORange> io_ranges;
            buildIORanges(&io_ranges);
            contents->stream->setIORanges(io_ranges);
        }

        if (sargsApplier) {
            bool isStripeNeeded = true;
            if (contents->metadata) {
                const auto& currentStripeStats = contents->metadata->stripestats(static_cast<int>(currentStripe));
                // skip this stripe after stats fail to satisfy sargs
                uint64_t stripeRowGroupCount =
                        (rowsInCurrentStripe + footer->rowindexstride() - 1) / footer->rowindexstride();
                isStripeNeeded = sargsApplier->evaluateStripeStatistics(currentStripeStats, stripeRowGroupCount);
            }
            if (!isStripeNeeded) {
                skipStripe = true;
                goto end;
            }

            // read row group statistics and bloom filters of current stripe
            loadStripeIndex();

            if (sargsApplier->getRowReaderFilter()) {
                sargsApplier->getRowReaderFilter()->setWriterTimezone(
                        currentStripeFooter.has_writertimezone() ? currentStripeFooter.writertimezone() : "");
            }

            // select row groups to read in the current stripe
            sargsApplier->pickRowGroups(rowsInCurrentStripe, rowIndexes, bloomFilterIndex);
            if (!sargsApplier->hasSelectedFrom(currentRowInStripe)) {
                skipStripe = true;
                goto end;
            }
        }
        {
            // get writer timezone info from stripe footer to help understand timestamp values.
            const Timezone& writerTimezone = currentStripeFooter.has_writertimezone()
                                                     ? getTimezoneByName(currentStripeFooter.writertimezone())
                                                     : getLocalTimezone();

            StripeStreamsImpl stripeStreams(*this, currentStripe, currentStripeInfo, currentStripeFooter,
                                            currentStripeInfo.offset(), *contents->stream, writerTimezone,
                                            readerTimezone);
            reader = buildReader(*contents->schema, stripeStreams);

            if (sargsApplier) {
                if (sargsApplier->getRowReaderFilter()) {
                    std::unordered_map<uint64_t, StringDictionary*> sdicts;
                    collectStringDictionary(reader.get(), sdicts);
                    if (sargsApplier->getRowReaderFilter()->filterOnPickStringDictionary(sdicts)) {
                        skipStripe = true;
                        reader.reset();
                        goto end;
                    }
                }

                // move to the 1st selected row group when PPD is enabled.
                currentRowInStripe =
                        advanceToNextRowGroup(currentRowInStripe, rowsInCurrentStripe, footer->rowindexstride(),
                                              sargsApplier->getNextSkippedRows());
                previousRow = firstRowOfStripe[currentStripe] + currentRowInStripe - 1;
                if (currentRowInStripe > 0) {
                    seekToRowGroup(static_cast<uint32_t>(currentRowInStripe / footer->rowindexstride()));
                }
            }
        }

    end:
        if (skipStripe) {
            // advance to next stripe when current stripe has no matching rows
            currentStripe += 1;
            currentRowInStripe = 0;
        } else {
            break;
        }
    }

    if (currentStripe == lastStripe) {
        markEndOfFile();
    }
}

bool RowReaderImpl::next(ColumnVectorBatch& data, ReadPosition* pos) {
    SCOPED_STOPWATCH(contents->readerMetrics, ReaderInclusiveLatencyUs, ReaderCall);
    if (currentStripe >= lastStripe) {
        data.numElements = 0;
        markEndOfFile();
        return false;
    }
    if (currentRowInStripe == 0) {
        startNextStripe();
        lazyLoadLastUsedRowInStripe = 0;
        if (pos != nullptr) {
            pos->start_new_stripe = true;
        }
    }
    uint64_t rowsToRead = std::min(static_cast<uint64_t>(data.capacity), rowsInCurrentStripe - currentRowInStripe);
    if (currentStripe >= lastStripe) {
        rowsToRead = 0;
    } else if (sargsApplier && rowsToRead > 0) {
        rowsToRead = computeBatchSize(rowsToRead, currentRowInStripe, rowsInCurrentStripe, footer->rowindexstride(),
                                      sargsApplier->getNextSkippedRows());
    }
    data.numElements = rowsToRead;
    if (rowsToRead == 0) {
        markEndOfFile();
        return false;
    }
    if (enableEncodedBlock) {
        reader->nextEncoded(data, rowsToRead, nullptr);
    } else {
        reader->next(data, rowsToRead, nullptr);
    }

    if (pos != nullptr) {
        pos->stripe_index = currentStripe;
        pos->num_values = rowsToRead;
        pos->row_in_stripe = currentRowInStripe;
    }

    // update row number
    previousRow = firstRowOfStripe[currentStripe] + currentRowInStripe;
    currentRowInStripe += rowsToRead;

    // check if we need to advance to next selected row group
    if (sargsApplier) {
        uint64_t nextRowToRead = advanceToNextRowGroup(currentRowInStripe, rowsInCurrentStripe,
                                                       footer->rowindexstride(), sargsApplier->getNextSkippedRows());
        if (currentRowInStripe != nextRowToRead) {
            // it is guaranteed to be at start of a row group
            currentRowInStripe = nextRowToRead;
            if (currentRowInStripe < rowsInCurrentStripe) {
                seekToRowGroup(static_cast<uint32_t>(currentRowInStripe / footer->rowindexstride()));
            }
        }
    }

    if (currentRowInStripe >= rowsInCurrentStripe) {
        currentStripe += 1;
        currentRowInStripe = 0;
    }
    return rowsToRead != 0;
}

void RowReaderImpl::lazyLoadNext(ColumnVectorBatch& data, uint64_t numValues) {
    if (enableEncodedBlock) {
        reader->lazyLoadNextEncoded(data, numValues, nullptr);
    } else {
        reader->lazyLoadNext(data, numValues, nullptr);
    }
    lazyLoadLastUsedRowInStripe += numValues;
}

void RowReaderImpl::lazyLoadSeekTo(uint64_t toRow) {
    uint64_t costDirectSkip = (toRow - lazyLoadLastUsedRowInStripe);
    if (costDirectSkip == 0) {
        return;
    }
    // we have two options to locate `toRow`:
    // 1. use seek to row group, and skip rest rows(# = toRow % rowindexstripe)
    // 2. or skip rest rows directly(# = toRow - lazyLoadLastUsedRowInStripe)
    // we can assume `seek to row group` costs skipping X rows
    // then #1 cost is X + toRow % rowindexstripe
    // #2 cost is (row - lazyLoadLastUsedRowInStripe)
    const uint64_t ROW_INDEX_STRIDE = footer->rowindexstride();
    if (ROW_INDEX_STRIDE == 0) {
        // Orc can only have one RowGroup in each stipe
        reader->lazyLoadSkip(costDirectSkip);
    } else {
        static const uint64_t SEEK_TO_ROW_GROUP_COST = 4096;
        uint64_t costIndirectSkip = toRow % ROW_INDEX_STRIDE;
        uint64_t toRowGroupNumber = toRow / ROW_INDEX_STRIDE;
        uint64_t fromRowGroupNumber = lazyLoadLastUsedRowInStripe / ROW_INDEX_STRIDE;

        if ((fromRowGroupNumber != toRowGroupNumber) && (SEEK_TO_ROW_GROUP_COST + costIndirectSkip) < costDirectSkip) {
            PositionProviderMap map;
            getRowGroupPosition(static_cast<uint32_t>(toRowGroupNumber), &map);
            reader->lazyLoadSeekToRowGroup(&map);
            reader->lazyLoadSkip(costIndirectSkip);
        } else {
            reader->lazyLoadSkip(costDirectSkip);
        }
    }

    lazyLoadLastUsedRowInStripe = toRow;
}

uint64_t RowReaderImpl::computeBatchSize(uint64_t requestedSize, uint64_t currentRowInStripe,
                                         uint64_t rowsInCurrentStripe, uint64_t rowIndexStride,
                                         const std::vector<uint64_t>& nextSkippedRows) {
    // In case of PPD, batch size should be aware of row group boundaries. If only a subset of row
    // groups are selected then marker position is set to the end of range (subset of row groups
    // within stripe).
    uint64_t endRowInStripe = rowsInCurrentStripe;
    uint64_t groupsInStripe = nextSkippedRows.size();
    if (groupsInStripe > 0) {
        auto rg = static_cast<uint32_t>(currentRowInStripe / rowIndexStride);
        if (rg >= groupsInStripe) return 0;
        uint64_t nextSkippedRow = nextSkippedRows[rg];
        if (nextSkippedRow == 0) return 0;
        endRowInStripe = nextSkippedRow;
    }
    return std::min(requestedSize, endRowInStripe - currentRowInStripe);
}

uint64_t RowReaderImpl::advanceToNextRowGroup(uint64_t currentRowInStripe, uint64_t rowsInCurrentStripe,
                                              uint64_t rowIndexStride, const std::vector<uint64_t>& nextSkippedRows) {
    auto groupsInStripe = nextSkippedRows.size();
    if (groupsInStripe == 0) {
        // No PPD, keeps using the current row in stripe
        return std::min(currentRowInStripe, rowsInCurrentStripe);
    }
    auto rg = static_cast<uint32_t>(currentRowInStripe / rowIndexStride);
    if (rg >= groupsInStripe) {
        // Points to the end of the stripe
        return rowsInCurrentStripe;
    }
    if (nextSkippedRows[rg] != 0) {
        // Current row group is selected
        return currentRowInStripe;
    }
    // Advance to the next selected row group
    while (rg < groupsInStripe && nextSkippedRows[rg] == 0) ++rg;
    if (rg < groupsInStripe) {
        return rg * rowIndexStride;
    }
    return rowsInCurrentStripe;
}

std::unique_ptr<ColumnVectorBatch> RowReaderImpl::createRowBatch(uint64_t capacity) const {
    return getSelectedType().createRowBatch(capacity, *contents->pool, enableEncodedBlock);
}

void ensureOrcFooter(InputStream* stream, DataBuffer<char>* buffer, uint64_t postscriptLength) {
    const std::string MAGIC("ORC");
    const uint64_t magicLength = MAGIC.length();
    const char* const bufferStart = buffer->data();
    const uint64_t bufferLength = buffer->size();

    if (postscriptLength < magicLength || bufferLength < magicLength) {
        throw ParseError("Invalid ORC postscript length");
    }
    const char* magicStart = bufferStart + bufferLength - 1 - magicLength;

    // Look for the magic string at the end of the postscript.
    if (memcmp(magicStart, MAGIC.c_str(), magicLength) != 0) {
        // If there is no magic string at the end, check the beginning.
        // Only files written by Hive 0.11.0 don't have the tail ORC string.
        std::unique_ptr<char[]> frontBuffer(new char[magicLength]);
        stream->read(frontBuffer.get(), magicLength, 0);
        bool foundMatch = memcmp(frontBuffer.get(), MAGIC.c_str(), magicLength) == 0;

        if (!foundMatch) {
            throw ParseError("Not an ORC file");
        }
    }
}

/**
   * Read the file's postscript from the given buffer.
   * @param stream the file stream
   * @param buffer the buffer with the tail of the file.
   * @param postscriptSize the length of postscript in bytes
   */
std::unique_ptr<proto::PostScript> readPostscript(InputStream* stream, DataBuffer<char>* buffer,
                                                  uint64_t postscriptSize) {
    char* ptr = buffer->data();
    uint64_t readSize = buffer->size();

    ensureOrcFooter(stream, buffer, postscriptSize);

    std::unique_ptr<proto::PostScript> postscript = std::unique_ptr<proto::PostScript>(new proto::PostScript());
    if (readSize < 1 + postscriptSize) {
        std::stringstream msg;
        msg << "Invalid ORC postscript length: " << postscriptSize << ", file length = " << stream->getLength();
        throw ParseError(msg.str());
    }
    if (!postscript->ParseFromArray(ptr + readSize - 1 - postscriptSize, static_cast<int>(postscriptSize))) {
        throw ParseError("Failed to parse the postscript from " + stream->getName());
    }
    return REDUNDANT_MOVE(postscript);
}

/**
   * Check that proto Types are valid. Indices in the type tree should be valid,
   * so we won't crash when we convert the proto::Types to TypeImpls (ORC-317).
   * For STRUCT types, fieldName size should match subTypes size (ORC-581).
   */
void checkProtoTypes(const proto::Footer& footer) {
    std::stringstream msg;
    int maxId = footer.types_size();
    if (maxId <= 0) {
        throw ParseError("Footer is corrupt: no types found");
    }
    for (int i = 0; i < maxId; ++i) {
        const proto::Type& type = footer.types(i);
        if (type.kind() == proto::Type_Kind_STRUCT && type.subtypes_size() != type.fieldnames_size()) {
            msg << "Footer is corrupt: STRUCT type " << i << " has " << type.subtypes_size() << " subTypes, but has "
                << type.fieldnames_size() << " fieldNames";
            throw ParseError(msg.str());
        }
        for (int j = 0; j < type.subtypes_size(); ++j) {
            int subTypeId = static_cast<int>(type.subtypes(j));
            if (subTypeId <= i) {
                msg << "Footer is corrupt: malformed link from type " << i << " to " << subTypeId;
                throw ParseError(msg.str());
            }
            if (subTypeId >= maxId) {
                msg << "Footer is corrupt: types(" << subTypeId << ") not exists";
                throw ParseError(msg.str());
            }
            if (j > 0 && static_cast<int>(type.subtypes(j - 1)) >= subTypeId) {
                msg << "Footer is corrupt: subType(" << (j - 1) << ") >= subType(" << j << ") in types(" << i << "). ("
                    << type.subtypes(j - 1) << " >= " << subTypeId << ")";
                throw ParseError(msg.str());
            }
        }
    }
}

/**
   * Parse the footer from the given buffer.
   * @param stream the file's stream
   * @param buffer the buffer to parse the footer from
   * @param footerOffset the offset within the buffer that contains the footer
   * @param ps the file's postscript
   * @param memoryPool the memory pool to use
   */
std::unique_ptr<proto::Footer> readFooter(InputStream* stream, const DataBuffer<char>* buffer, uint64_t footerOffset,
                                          const proto::PostScript& ps, MemoryPool& memoryPool, ReaderMetrics* metrics) {
    const char* footerPtr = buffer->data() + footerOffset;

    std::unique_ptr<SeekableInputStream> pbStream = createDecompressor(
            convertCompressionKind(ps),
            std::unique_ptr<SeekableInputStream>(new SeekableArrayInputStream(footerPtr, ps.footerlength())),
            getCompressionBlockSize(ps), memoryPool, metrics);

    std::unique_ptr<proto::Footer> footer = std::unique_ptr<proto::Footer>(new proto::Footer());
    if (!footer->ParseFromZeroCopyStream(pbStream.get())) {
        throw ParseError("Failed to parse the footer from " + stream->getName());
    }

    checkProtoTypes(*footer);
    return REDUNDANT_MOVE(footer);
}

std::unique_ptr<Reader> createReader(std::unique_ptr<InputStream> stream, const ReaderOptions& options) {
    std::shared_ptr<FileContents> contents = std::make_shared<FileContents>();
    contents->pool = options.getMemoryPool();
    contents->errorStream = options.getErrorStream();
    contents->readerMetrics = options.getReaderMetrics();
    std::string serializedFooter = options.getSerializedFileTail();
    uint64_t fileLength;
    uint64_t postscriptLength;
    if (serializedFooter.length() != 0) {
        // Parse the file tail from the serialized one.
        proto::FileTail tail;
        if (!tail.ParseFromString(serializedFooter)) {
            throw ParseError("Failed to parse the file tail from string");
        }
        contents->postscript.reset(new proto::PostScript(tail.postscript()));
        contents->footer.reset(new proto::Footer(tail.footer()));
        fileLength = tail.filelength();
        postscriptLength = tail.postscriptlength();
    } else {
        // figure out the size of the file using the option or filesystem
        fileLength = std::min(options.getTailLocation(), static_cast<uint64_t>(stream->getLength()));

        // read last bytes into buffer to get PostScript
        uint64_t readSize = std::min(fileLength, DIRECTORY_SIZE_GUESS);
        if (readSize < 4) {
            throw ParseError("File size too small");
        }
        std::unique_ptr<DataBuffer<char>> buffer(new DataBuffer<char>(*contents->pool, readSize));
        stream->prepareCache(InputStream::PrepareCacheScope::READ_FULL_FILE, 0, fileLength);
        stream->read(buffer->data(), readSize, fileLength - readSize);

        postscriptLength = buffer->data()[readSize - 1] & 0xff;
        contents->postscript = REDUNDANT_MOVE(readPostscript(stream.get(), buffer.get(), postscriptLength));
        uint64_t footerSize = contents->postscript->footerlength();
        uint64_t tailSize = 1 + postscriptLength + footerSize;
        if (tailSize >= fileLength) {
            std::stringstream msg;
            msg << "Invalid ORC tailSize=" << tailSize << ", fileLength=" << fileLength;
            throw ParseError(msg.str());
        }
        uint64_t footerOffset;

        if (tailSize > readSize) {
            buffer->resize(footerSize);
            stream->read(buffer->data(), footerSize, fileLength - tailSize);
            footerOffset = 0;
        } else {
            footerOffset = readSize - tailSize;
        }

        contents->footer = REDUNDANT_MOVE(readFooter(stream.get(), buffer.get(), footerOffset, *contents->postscript,
                                                     *contents->pool, contents->readerMetrics));

        contents->isDecimalAsLong = false;
        if (contents->postscript->version_size() == 2) {
            FileVersion v(contents->postscript->version(0), contents->postscript->version(1));
            if (v == FileVersion::UNSTABLE_PRE_2_0()) {
                contents->isDecimalAsLong = true;
            }
        }
    }
    contents->stream = std::move(stream);
    return std::unique_ptr<Reader>(new ReaderImpl(std::move(contents), options, fileLength, postscriptLength));
}

std::map<uint32_t, BloomFilterIndex> ReaderImpl::getBloomFilters(uint32_t stripeIndex,
                                                                 const std::set<uint32_t>& included) const {
    std::map<uint32_t, BloomFilterIndex> ret;

    // find stripe info
    if (stripeIndex >= static_cast<uint32_t>(footer->stripes_size())) {
        throw std::logic_error("Illegal stripe index: " + to_string(static_cast<int64_t>(stripeIndex)));
    }
    const proto::StripeInformation currentStripeInfo = footer->stripes(static_cast<int>(stripeIndex));
    const proto::StripeFooter currentStripeFooter = getStripeFooter(currentStripeInfo, *contents);

    // iterate stripe footer to get stream of bloomfilter
    auto offset = static_cast<uint64_t>(currentStripeInfo.offset());
    for (int i = 0; i < currentStripeFooter.streams_size(); i++) {
        const proto::Stream& stream = currentStripeFooter.streams(i);
        auto column = static_cast<uint32_t>(stream.column());
        auto length = static_cast<uint64_t>(stream.length());

        // a bloom filter stream from a selected column is found
        if (stream.kind() == proto::Stream_Kind_BLOOM_FILTER_UTF8 &&
            (included.empty() || included.find(column) != included.end())) {
            std::unique_ptr<SeekableInputStream> pbStream =
                    createDecompressor(contents->compression,
                                       std::unique_ptr<SeekableInputStream>(new SeekableFileInputStream(
                                               contents->stream.get(), offset, length, *contents->pool)),
                                       contents->blockSize, *(contents->pool), contents->readerMetrics);

            proto::BloomFilterIndex pbBFIndex;
            if (!pbBFIndex.ParseFromZeroCopyStream(pbStream.get())) {
                throw ParseError("Failed to parse BloomFilterIndex");
            }

            BloomFilterIndex bfIndex;
            for (int j = 0; j < pbBFIndex.bloomfilter_size(); j++) {
                std::unique_ptr<BloomFilter> entry = BloomFilterUTF8Utils::deserialize(
                        stream.kind(), currentStripeFooter.columns(static_cast<int>(stream.column())),
                        pbBFIndex.bloomfilter(j));
                bfIndex.entries.push_back(std::shared_ptr<BloomFilter>(std::move(entry)));
            }

            // add bloom filters to result for one column
            ret[column] = bfIndex;
        }

        offset += length;
    }

    return ret;
}

RowReader::~RowReader() {
    // PASS
}

bool RowReader::next(ColumnVectorBatch& cvb) {
    return this->next(cvb, nullptr);
}

Reader::~Reader() {
    // PASS
}

InputStream::~InputStream(){
        // PASS
};

uint64_t InputStream::getNaturalReadSizeAfterSeek() const {
    return 128 * 1024;
}

void InputStream::prepareCache(PrepareCacheScope scope, uint64_t offset, uint64_t length) {}

bool InputStream::isIORangesEnabled() const {
    return false;
}

void InputStream::clearIORanges() {}

void InputStream::setIORanges(std::vector<InputStream::IORange>& io_ranges) {}

} // namespace orc
