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
//   https://github.com/apache/orc/tree/main/c++/src/Options.hh

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

// clang-format off
#pragma once

#include "orc/Int128.hh"
#include "orc/OrcFile.hh"
#include "orc/Reader.hh"

#include <limits>
#include <utility>
// clang-format on

namespace orc {

enum ColumnSelection {
    ColumnSelection_NONE = 0,
    ColumnSelection_NAMES = 1,
    ColumnSelection_FIELD_IDS = 2,
    ColumnSelection_TYPE_IDS = 3,
};

/**
 * ReaderOptions Implementation
 */
struct ReaderOptionsPrivate {
    uint64_t tailLocation;
    std::ostream* errorStream;
    MemoryPool* memoryPool;
    std::string serializedTail;
    ReaderMetrics* metrics;

    ReaderOptionsPrivate() {
        tailLocation = std::numeric_limits<uint64_t>::max();
        errorStream = &std::cerr;
        memoryPool = getDefaultPool();
        metrics = nullptr;
    }
};

ReaderOptions::ReaderOptions() : privateBits(std::unique_ptr<ReaderOptionsPrivate>(new ReaderOptionsPrivate())) {
    // PASS
}

ReaderOptions::ReaderOptions(const ReaderOptions& rhs)
        : privateBits(std::unique_ptr<ReaderOptionsPrivate>(new ReaderOptionsPrivate(*(rhs.privateBits.get())))) {
    // PASS
}

ReaderOptions::ReaderOptions(ReaderOptions& rhs) {
    // swap privateBits with rhs
    privateBits.swap(rhs.privateBits);
}

ReaderOptions& ReaderOptions::operator=(const ReaderOptions& rhs) {
    if (this != &rhs) {
        privateBits.reset(new ReaderOptionsPrivate(*(rhs.privateBits.get())));
    }
    return *this;
}

ReaderOptions::~ReaderOptions() {
    // PASS
}

ReaderOptions& ReaderOptions::setMemoryPool(MemoryPool& pool) {
    privateBits->memoryPool = &pool;
    return *this;
}

MemoryPool* ReaderOptions::getMemoryPool() const {
    return privateBits->memoryPool;
}

ReaderOptions& ReaderOptions::setReaderMetrics(ReaderMetrics* metrics) {
    privateBits->metrics = metrics;
    return *this;
}

ReaderMetrics* ReaderOptions::getReaderMetrics() const {
    return privateBits->metrics;
}

ReaderOptions& ReaderOptions::setTailLocation(uint64_t offset) {
    privateBits->tailLocation = offset;
    return *this;
}

uint64_t ReaderOptions::getTailLocation() const {
    return privateBits->tailLocation;
}

ReaderOptions& ReaderOptions::setSerializedFileTail(const std::string& value) {
    privateBits->serializedTail = value;
    return *this;
}

std::string ReaderOptions::getSerializedFileTail() const {
    return privateBits->serializedTail;
}

ReaderOptions& ReaderOptions::setErrorStream(std::ostream& stream) {
    privateBits->errorStream = &stream;
    return *this;
}

std::ostream* ReaderOptions::getErrorStream() const {
    return privateBits->errorStream;
}

/**
 * RowReaderOptions Implementation
 */

struct RowReaderOptionsPrivate {
    ColumnSelection selection;
    std::list<uint64_t> includedColumnIndexes;
    std::list<uint64_t> lazyLoadColumnIndexes;
    std::list<std::string> includedColumnNames;
    std::list<std::string> lazyLoadColumnNames;
    uint64_t dataStart;
    uint64_t dataLength;
    bool throwOnHive11DecimalOverflow;
    int32_t forcedScaleOnHive11Decimal;
    bool enableLazyDecoding;
    std::shared_ptr<SearchArgument> sargs;
    std::shared_ptr<RowReaderFilter> filter;
    std::string readerTimezone;
    bool useWriterTimezone;

    RowReaderOptionsPrivate() {
        selection = ColumnSelection_NONE;
        dataStart = 0;
        dataLength = std::numeric_limits<uint64_t>::max();
        throwOnHive11DecimalOverflow = true;
        forcedScaleOnHive11Decimal = 6;
        enableLazyDecoding = false;
        readerTimezone = "GMT";
        useWriterTimezone = false;
    }
};

RowReaderOptions::RowReaderOptions()
        : privateBits(std::unique_ptr<RowReaderOptionsPrivate>(new RowReaderOptionsPrivate())) {
    // PASS
}

RowReaderOptions::RowReaderOptions(const RowReaderOptions& rhs)
        : privateBits(std::unique_ptr<RowReaderOptionsPrivate>(new RowReaderOptionsPrivate(*(rhs.privateBits.get())))) {
    // PASS
}

RowReaderOptions::RowReaderOptions(RowReaderOptions& rhs) {
    // swap privateBits with rhs
    privateBits.swap(rhs.privateBits);
}

RowReaderOptions& RowReaderOptions::operator=(const RowReaderOptions& rhs) {
    if (this != &rhs) {
        privateBits.reset(new RowReaderOptionsPrivate(*(rhs.privateBits.get())));
    }
    return *this;
}

RowReaderOptions::~RowReaderOptions() {
    // PASS
}

RowReaderOptions& RowReaderOptions::include(const std::list<uint64_t>& include) {
    privateBits->selection = ColumnSelection_FIELD_IDS;
    privateBits->includedColumnIndexes.assign(include.begin(), include.end());
    privateBits->includedColumnNames.clear();
    return *this;
}

RowReaderOptions& RowReaderOptions::include(const std::list<std::string>& include) {
    privateBits->selection = ColumnSelection_NAMES;
    privateBits->includedColumnNames.assign(include.begin(), include.end());
    privateBits->includedColumnIndexes.clear();
    return *this;
}

RowReaderOptions& RowReaderOptions::includeLazyLoadColumnNames(const std::list<std::string>& include) {
    privateBits->lazyLoadColumnNames.assign(include.begin(), include.end());
    privateBits->lazyLoadColumnIndexes.clear();
    return *this;
}

RowReaderOptions& RowReaderOptions::includeLazyLoadColumnIndexes(const std::list<std::uint64_t>& include) {
    privateBits->lazyLoadColumnIndexes.assign(include.begin(), include.end());
    privateBits->lazyLoadColumnNames.clear();
    return *this;
}

RowReaderOptions& RowReaderOptions::includeTypes(const std::list<uint64_t>& types) {
    privateBits->selection = ColumnSelection_TYPE_IDS;
    privateBits->includedColumnIndexes.assign(types.begin(), types.end());
    privateBits->includedColumnNames.clear();
    return *this;
}

RowReaderOptions& RowReaderOptions::range(uint64_t offset, uint64_t length) {
    privateBits->dataStart = offset;
    privateBits->dataLength = length;
    return *this;
}

bool RowReaderOptions::getIndexesSet() const {
    return privateBits->selection == ColumnSelection_FIELD_IDS;
}

bool RowReaderOptions::getTypeIdsSet() const {
    return privateBits->selection == ColumnSelection_TYPE_IDS;
}

const std::list<uint64_t>& RowReaderOptions::getInclude() const {
    return privateBits->includedColumnIndexes;
}

bool RowReaderOptions::getNamesSet() const {
    return privateBits->selection == ColumnSelection_NAMES;
}

const std::list<std::string>& RowReaderOptions::getIncludeNames() const {
    return privateBits->includedColumnNames;
}

const std::list<std::string>& RowReaderOptions::getLazyLoadColumnNames() const {
    return privateBits->lazyLoadColumnNames;
}

const std::list<uint64_t>& RowReaderOptions::getLazyLoadColumnIndexes() const {
    return privateBits->lazyLoadColumnIndexes;
}

uint64_t RowReaderOptions::getOffset() const {
    return privateBits->dataStart;
}

uint64_t RowReaderOptions::getLength() const {
    return privateBits->dataLength;
}

RowReaderOptions& RowReaderOptions::throwOnHive11DecimalOverflow(bool shouldThrow) {
    privateBits->throwOnHive11DecimalOverflow = shouldThrow;
    return *this;
}

bool RowReaderOptions::getThrowOnHive11DecimalOverflow() const {
    return privateBits->throwOnHive11DecimalOverflow;
}

RowReaderOptions& RowReaderOptions::forcedScaleOnHive11Decimal(int32_t forcedScale) {
    privateBits->forcedScaleOnHive11Decimal = forcedScale;
    return *this;
}

int32_t RowReaderOptions::getForcedScaleOnHive11Decimal() const {
    return privateBits->forcedScaleOnHive11Decimal;
}

bool RowReaderOptions::getEnableLazyDecoding() const {
    return privateBits->enableLazyDecoding;
}

RowReaderOptions& RowReaderOptions::setEnableLazyDecoding(bool enable) {
    privateBits->enableLazyDecoding = enable;
    return *this;
}

RowReaderOptions& RowReaderOptions::searchArgument(std::unique_ptr<SearchArgument> sargs) {
    privateBits->sargs = std::move(sargs);
    return *this;
}

std::shared_ptr<SearchArgument> RowReaderOptions::getSearchArgument() const {
    return privateBits->sargs;
}

RowReaderOptions& RowReaderOptions::rowReaderFilter(std::shared_ptr<RowReaderFilter> filter) {
    privateBits->filter = std::move(filter);
    return *this;
}

std::shared_ptr<RowReaderFilter> RowReaderOptions::getRowReaderFilter() const {
    return privateBits->filter;
}

RowReaderOptions& RowReaderOptions::setTimezoneName(const std::string& zoneName) {
    privateBits->readerTimezone = zoneName;
    return *this;
}

const std::string& RowReaderOptions::getTimezoneName() const {
    return privateBits->readerTimezone;
}
RowReaderOptions& RowReaderOptions::useWriterTimezone() {
    privateBits->useWriterTimezone = true;
    return *this;
}

bool RowReaderOptions::getUseWriterTimezone() const {
    return privateBits->useWriterTimezone;
}
} // namespace orc
