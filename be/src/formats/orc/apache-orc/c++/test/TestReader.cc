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

#include "Adaptor.hh"
#include "Reader.hh"
#include "orc/Reader.hh"
#include "wrap/gmock.h"
#include "wrap/gtest-wrapper.h"

namespace orc {

TEST(TestReader, testWriterVersions) {
    EXPECT_EQ("original", writerVersionToString(WriterVersion_ORIGINAL));
    EXPECT_EQ("HIVE-8732", writerVersionToString(WriterVersion_HIVE_8732));
    EXPECT_EQ("HIVE-4243", writerVersionToString(WriterVersion_HIVE_4243));
    EXPECT_EQ("HIVE-12055", writerVersionToString(WriterVersion_HIVE_12055));
    EXPECT_EQ("HIVE-13083", writerVersionToString(WriterVersion_HIVE_13083));
    EXPECT_EQ("future - 99", writerVersionToString(static_cast<WriterVersion>(99)));
}

TEST(TestReader, testCompressionNames) {
    EXPECT_EQ("none", compressionKindToString(CompressionKind_NONE));
    EXPECT_EQ("zlib", compressionKindToString(CompressionKind_ZLIB));
    EXPECT_EQ("snappy", compressionKindToString(CompressionKind_SNAPPY));
    EXPECT_EQ("lzo", compressionKindToString(CompressionKind_LZO));
    EXPECT_EQ("lz4", compressionKindToString(CompressionKind_LZ4));
    EXPECT_EQ("zstd", compressionKindToString(CompressionKind_ZSTD));
    EXPECT_EQ("unknown - 99", compressionKindToString(static_cast<CompressionKind>(99)));
}

TEST(TestRowReader, computeBatchSize) {
    uint64_t rowIndexStride = 100;
    uint64_t rowsInCurrentStripe = 100 * 8 + 50;
    std::vector<uint64_t> nextSkippedRows = {0, 0, 400, 400, 0, 0, 800, 800, 0};

    EXPECT_EQ(0, RowReaderImpl::computeBatchSize(1024, 0, rowsInCurrentStripe, rowIndexStride, nextSkippedRows));
    EXPECT_EQ(0, RowReaderImpl::computeBatchSize(1024, 50, rowsInCurrentStripe, rowIndexStride, nextSkippedRows));
    EXPECT_EQ(200, RowReaderImpl::computeBatchSize(1024, 200, rowsInCurrentStripe, rowIndexStride, nextSkippedRows));
    EXPECT_EQ(150, RowReaderImpl::computeBatchSize(1024, 250, rowsInCurrentStripe, rowIndexStride, nextSkippedRows));
    EXPECT_EQ(0, RowReaderImpl::computeBatchSize(1024, 550, rowsInCurrentStripe, rowIndexStride, nextSkippedRows));
    EXPECT_EQ(100, RowReaderImpl::computeBatchSize(1024, 700, rowsInCurrentStripe, rowIndexStride, nextSkippedRows));
    EXPECT_EQ(50, RowReaderImpl::computeBatchSize(50, 700, rowsInCurrentStripe, rowIndexStride, nextSkippedRows));
    EXPECT_EQ(0, RowReaderImpl::computeBatchSize(50, 810, rowsInCurrentStripe, rowIndexStride, nextSkippedRows));
    EXPECT_EQ(0, RowReaderImpl::computeBatchSize(50, 900, rowsInCurrentStripe, rowIndexStride, nextSkippedRows));
}

TEST(TestRowReader, advanceToNextRowGroup) {
    uint64_t rowIndexStride = 100;
    uint64_t rowsInCurrentStripe = 100 * 8 + 50;
    std::vector<uint64_t> nextSkippedRows = {0, 0, 400, 400, 0, 0, 800, 800, 0};
    EXPECT_EQ(200, RowReaderImpl::advanceToNextRowGroup(0, rowsInCurrentStripe, rowIndexStride, nextSkippedRows));
    EXPECT_EQ(200, RowReaderImpl::advanceToNextRowGroup(150, rowsInCurrentStripe, rowIndexStride, nextSkippedRows));
    EXPECT_EQ(250, RowReaderImpl::advanceToNextRowGroup(250, rowsInCurrentStripe, rowIndexStride, nextSkippedRows));
    EXPECT_EQ(350, RowReaderImpl::advanceToNextRowGroup(350, rowsInCurrentStripe, rowIndexStride, nextSkippedRows));
    EXPECT_EQ(350, RowReaderImpl::advanceToNextRowGroup(350, rowsInCurrentStripe, rowIndexStride, nextSkippedRows));
    EXPECT_EQ(600, RowReaderImpl::advanceToNextRowGroup(500, rowsInCurrentStripe, rowIndexStride, nextSkippedRows));
    EXPECT_EQ(699, RowReaderImpl::advanceToNextRowGroup(699, rowsInCurrentStripe, rowIndexStride, nextSkippedRows));
    EXPECT_EQ(799, RowReaderImpl::advanceToNextRowGroup(799, rowsInCurrentStripe, rowIndexStride, nextSkippedRows));
    EXPECT_EQ(850, RowReaderImpl::advanceToNextRowGroup(800, rowsInCurrentStripe, rowIndexStride, nextSkippedRows));
    EXPECT_EQ(850, RowReaderImpl::advanceToNextRowGroup(900, rowsInCurrentStripe, rowIndexStride, nextSkippedRows));
}

void CheckFileWithSargs(const char* fileName, const char* softwareVersion) {
    std::stringstream ss;
    if (const char* example_dir = std::getenv("ORC_EXAMPLE_DIR")) {
        ss << example_dir;
    } else {
        ss << "../../../examples";
    }
    // Read a file with bloom filters written by CPP writer in version 1.6.11.
    ss << "/" << fileName;
    ReaderOptions readerOpts;
    std::unique_ptr<Reader> reader = createReader(readLocalFile(ss.str().c_str()), readerOpts);
    EXPECT_EQ(WriterId::ORC_CPP_WRITER, reader->getWriterId());
    EXPECT_EQ(softwareVersion, reader->getSoftwareVersion());

    // Create SearchArgument with a EQUALS predicate which can leverage the bloom filters.
    RowReaderOptions rowReaderOpts;
    std::unique_ptr<SearchArgumentBuilder> sarg = SearchArgumentFactory::newBuilder();
    // Integer value 18000000000 has an inconsistent hash before the fix of ORC-1024.
    sarg->equals(1, PredicateDataType::LONG, Literal(static_cast<int64_t>(18000000000L)));
    std::unique_ptr<SearchArgument> final_sarg = sarg->build();
    rowReaderOpts.searchArgument(std::move(final_sarg));
    std::unique_ptr<RowReader> rowReader = reader->createRowReader(rowReaderOpts);

    // Make sure bad bloom filters won't affect the results.
    std::unique_ptr<ColumnVectorBatch> batch = rowReader->createRowBatch(1024);
    EXPECT_TRUE(rowReader->next(*batch));
    EXPECT_EQ(5, batch->numElements);
    EXPECT_FALSE(rowReader->next(*batch));
}

TEST(TestRowReader, testSkipBadBloomFilters) {
    CheckFileWithSargs("bad_bloom_filter_1.6.11.orc", "ORC C++ 1.6.11");
    CheckFileWithSargs("bad_bloom_filter_1.6.0.orc", "ORC C++");
}

} // namespace orc
