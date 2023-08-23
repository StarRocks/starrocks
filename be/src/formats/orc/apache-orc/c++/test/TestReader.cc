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

TEST(TestReadIntent, testSeekOverEmptyPresentStream) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    uint64_t rowCount = 5000;
    {
        auto type = std::unique_ptr<Type>(
                Type::buildTypeFromString(
                        "struct<col1:struct<col2:int>,col3:struct<col4:int>,"
                        "col5:array<int>,col6:map<int,int>>"));
        WriterOptions options;
        options.setStripeSize(1024 * 1024)
                .setCompressionBlockSize(1024)
                .setCompression(CompressionKind_NONE)
                .setMemoryPool(pool)
                .setRowIndexStride(1000);

        // the child columns of the col3,col5,col6 have the empty present stream
        auto writer = createWriter(*type, &memStream, options);
        auto batch = writer->createRowBatch(rowCount);
        auto& structBatch = dynamic_cast<StructVectorBatch&>(*batch);
        auto& structBatch1 = dynamic_cast<StructVectorBatch&>(*structBatch.fields[0]);
        auto& structBatch2 = dynamic_cast<StructVectorBatch&>(*structBatch.fields[1]);
        auto& listBatch = dynamic_cast<ListVectorBatch&>(*structBatch.fields[2]);
        auto& mapBatch = dynamic_cast<MapVectorBatch&>(*structBatch.fields[3]);

        auto& longBatch1 = dynamic_cast<LongVectorBatch&>(*structBatch1.fields[0]);
        auto& longBatch2 = dynamic_cast<LongVectorBatch&>(*structBatch2.fields[0]);
        auto& longBatch3 = dynamic_cast<LongVectorBatch&>(*listBatch.elements);
        auto& longKeyBatch = dynamic_cast<LongVectorBatch&>(*mapBatch.keys);
        auto& longValueBatch = dynamic_cast<LongVectorBatch&>(*mapBatch.elements);

        structBatch.numElements = rowCount;
        structBatch1.numElements = rowCount;
        structBatch2.numElements = rowCount;
        listBatch.numElements = rowCount;
        mapBatch.numElements = rowCount;
        longBatch1.numElements = rowCount;
        longBatch2.numElements = rowCount;
        longBatch3.numElements = rowCount;
        longKeyBatch.numElements = rowCount;
        longValueBatch.numElements = rowCount;

        structBatch1.hasNulls = false;
        structBatch2.hasNulls = true;
        listBatch.hasNulls = true;
        mapBatch.hasNulls = true;
        longBatch1.hasNulls = false;
        longBatch2.hasNulls = true;
        longBatch3.hasNulls = true;
        longKeyBatch.hasNulls = true;
        longValueBatch.hasNulls = true;
        for (uint64_t i = 0; i < rowCount; ++i) {
            longBatch1.data[i] = static_cast<int64_t>(i);
            longBatch1.notNull[i] = 1;

            structBatch2.notNull[i] = 0;
            listBatch.notNull[i] = 0;
            listBatch.offsets[i] = 0;
            mapBatch.notNull[i] = 0;
            longBatch2.notNull[i] = 0;
            longBatch3.notNull[i] = 0;
            longKeyBatch.notNull[i] = 0;
            longValueBatch.notNull[i] = 0;
        }
        writer->add(*batch);
        writer->close();
    }
    {
        std::unique_ptr<InputStream> inStream(
                new MemoryInputStream(memStream.getData(), memStream.getLength()));
        ReaderOptions readerOptions;
        readerOptions.setMemoryPool(*pool);
        std::unique_ptr<Reader> reader =
                createReader(std::move(inStream), readerOptions);
        EXPECT_EQ(rowCount, reader->getNumberOfRows());
        std::unique_ptr<RowReader> rowReader =
                reader->createRowReader(RowReaderOptions());
        auto batch = rowReader->createRowBatch(1000);
        // seek over the empty present stream
        rowReader->seekToRow(2000);
        EXPECT_TRUE(rowReader->next(*batch));
        EXPECT_EQ(1000, batch->numElements);
        auto& structBatch = dynamic_cast<StructVectorBatch&>(*batch);
        auto& structBatch1 = dynamic_cast<StructVectorBatch&>(*structBatch.fields[0]);
        auto& structBatch2 = dynamic_cast<StructVectorBatch&>(*structBatch.fields[1]);
        auto& listBatch = dynamic_cast<ListVectorBatch&>(*structBatch.fields[2]);
        auto& mapBatch = dynamic_cast<MapVectorBatch&>(*structBatch.fields[3]);

        auto& longBatch1 = dynamic_cast<LongVectorBatch&>(*structBatch1.fields[0]);
        for (uint64_t i = 0; i < 1000; ++i) {
            EXPECT_EQ(longBatch1.data[i], static_cast<int64_t>(i + 2000));
            EXPECT_TRUE(longBatch1.notNull[i]);
            EXPECT_FALSE(structBatch2.notNull[i]);
            EXPECT_FALSE(listBatch.notNull[i]);
            EXPECT_FALSE(mapBatch.notNull[i]);
        }
    }
}
} // namespace orc
