// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/orc/tree/main/c++/test/TestReader.cc

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

using ::testing::ElementsAreArray;

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
    std::vector<bool> includedRowGroups = {false, false, true, true, false, false, true, true, false};

    EXPECT_EQ(0, RowReaderImpl::computeBatchSize(1024, 0, rowsInCurrentStripe, rowIndexStride, includedRowGroups));
    EXPECT_EQ(0, RowReaderImpl::computeBatchSize(1024, 50, rowsInCurrentStripe, rowIndexStride, includedRowGroups));
    EXPECT_EQ(200, RowReaderImpl::computeBatchSize(1024, 200, rowsInCurrentStripe, rowIndexStride, includedRowGroups));
    EXPECT_EQ(150, RowReaderImpl::computeBatchSize(1024, 250, rowsInCurrentStripe, rowIndexStride, includedRowGroups));
    EXPECT_EQ(0, RowReaderImpl::computeBatchSize(1024, 550, rowsInCurrentStripe, rowIndexStride, includedRowGroups));
    EXPECT_EQ(100, RowReaderImpl::computeBatchSize(1024, 700, rowsInCurrentStripe, rowIndexStride, includedRowGroups));
    EXPECT_EQ(50, RowReaderImpl::computeBatchSize(50, 700, rowsInCurrentStripe, rowIndexStride, includedRowGroups));
    EXPECT_EQ(0, RowReaderImpl::computeBatchSize(50, 810, rowsInCurrentStripe, rowIndexStride, includedRowGroups));
    EXPECT_EQ(0, RowReaderImpl::computeBatchSize(50, 900, rowsInCurrentStripe, rowIndexStride, includedRowGroups));
}

TEST(TestRowReader, advanceToNextRowGroup) {
    uint64_t rowIndexStride = 100;
    uint64_t rowsInCurrentStripe = 100 * 8 + 50;
    std::vector<bool> includedRowGroups = {false, false, true, true, false, false, true, true, false};

    EXPECT_EQ(200, RowReaderImpl::advanceToNextRowGroup(0, rowsInCurrentStripe, rowIndexStride, includedRowGroups));
    EXPECT_EQ(200, RowReaderImpl::advanceToNextRowGroup(150, rowsInCurrentStripe, rowIndexStride, includedRowGroups));
    EXPECT_EQ(250, RowReaderImpl::advanceToNextRowGroup(250, rowsInCurrentStripe, rowIndexStride, includedRowGroups));
    EXPECT_EQ(350, RowReaderImpl::advanceToNextRowGroup(350, rowsInCurrentStripe, rowIndexStride, includedRowGroups));
    EXPECT_EQ(350, RowReaderImpl::advanceToNextRowGroup(350, rowsInCurrentStripe, rowIndexStride, includedRowGroups));
    EXPECT_EQ(600, RowReaderImpl::advanceToNextRowGroup(500, rowsInCurrentStripe, rowIndexStride, includedRowGroups));
    EXPECT_EQ(699, RowReaderImpl::advanceToNextRowGroup(699, rowsInCurrentStripe, rowIndexStride, includedRowGroups));
    EXPECT_EQ(799, RowReaderImpl::advanceToNextRowGroup(799, rowsInCurrentStripe, rowIndexStride, includedRowGroups));
    EXPECT_EQ(850, RowReaderImpl::advanceToNextRowGroup(800, rowsInCurrentStripe, rowIndexStride, includedRowGroups));
    EXPECT_EQ(850, RowReaderImpl::advanceToNextRowGroup(900, rowsInCurrentStripe, rowIndexStride, includedRowGroups));
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

std::unique_ptr<Reader> createExampleReader(std::string fileName) {
    std::stringstream ss;
    if (const char* example_dir = std::getenv("ORC_EXAMPLE_DIR")) {
        ss << example_dir;
    } else {
        ss << "../../../examples";
    }
    ss << "/" << fileName;
    ReaderOptions readerOpts;
    return createReader(readLocalFile(ss.str().c_str()), readerOpts);
}

/**
   * Read TestOrcFile.nestedList.orc and verify the resolved selections.
   *
   * The ORC file has the following content:
   * {
   *   "int_array": [-1, -2],
   *   "int_array_array_array": [[[1], [2], [3]], [[4, 5], [6]], [[7, 8, 9]]]
   * }
   *
   * @param idReadIntentMap IdReadIntentMap describing the selections.
   * @param expectedSelection expected TypeIds that will be selected from given
   * idReadIntentMap.
   */
void verifySelection(const RowReaderOptions::IdReadIntentMap& idReadIntentMap,
                     const std::vector<uint32_t>& expectedSelection) {
    std::string fileName = "TestOrcFile.nestedList.orc";
    std::unique_ptr<Reader> reader = createExampleReader(fileName);

    RowReaderOptions rowReaderOpts;
    rowReaderOpts.includeTypesWithIntents(idReadIntentMap);
    std::unique_ptr<RowReader> rowReader = reader->createRowReader(rowReaderOpts);
    std::vector<bool> expected(reader->getType().getMaximumColumnId() + 1, false);
    for (auto id : expectedSelection) {
        expected[id] = true;
    }
    ASSERT_THAT(rowReader->getSelectedColumns(), ElementsAreArray(expected));
}

TEST(TestReadIntent, testListAll) {
    // select all of int_array.
    verifySelection({{1, ReadIntent_ALL}}, {0, 1, 2});
}

TEST(TestReadIntent, testListOffsets) {
    // select only the offsets of int_array.
    verifySelection({{1, ReadIntent_OFFSETS}}, {0, 1});

    // select only the offsets of int_array and the outermost offsets of
    // int_array_array_array.
    verifySelection({{1, ReadIntent_OFFSETS}, {3, ReadIntent_OFFSETS}}, {0, 1, 3});

    // select the entire offsets of int_array_array_array without the elements.
    verifySelection({{3, ReadIntent_OFFSETS}, {5, ReadIntent_OFFSETS}}, {0, 3, 4, 5});
}

TEST(TestReadIntent, testListAllAndOffsets) {
    // select all of int_array and only the outermost offsets of int_array_array_array.
    verifySelection({{1, ReadIntent_ALL}, {3, ReadIntent_OFFSETS}}, {0, 1, 2, 3});
}

TEST(TestReadIntent, testListConflictingIntent) {
    // test conflicting ReadIntent on nested list.
    verifySelection({{3, ReadIntent_OFFSETS}, {5, ReadIntent_ALL}}, {0, 3, 4, 5, 6});
    verifySelection({{3, ReadIntent_ALL}, {5, ReadIntent_OFFSETS}}, {0, 3, 4, 5, 6});
}

TEST(TestReadIntent, testRowBatchContent) {
    std::unique_ptr<Reader> reader = createExampleReader("TestOrcFile.nestedList.orc");

    // select all of int_array and only the offsets of int_array_array.
    RowReaderOptions::IdReadIntentMap idReadIntentMap = {{1, ReadIntent_ALL}, {3, ReadIntent_OFFSETS}};
    RowReaderOptions rowReaderOpts;
    rowReaderOpts.includeTypesWithIntents(idReadIntentMap);
    std::unique_ptr<RowReader> rowReader = reader->createRowReader(rowReaderOpts);

    // Read a row batch.
    std::unique_ptr<ColumnVectorBatch> batch = rowReader->createRowBatch(1024);
    EXPECT_TRUE(rowReader->next(*batch));
    EXPECT_EQ(1, batch->numElements);
    auto& structBatch = dynamic_cast<StructVectorBatch&>(*batch);

    // verify content of int_array selection.
    auto& intArrayBatch = dynamic_cast<ListVectorBatch&>(*structBatch.fields[0]);
    auto& innerLongBatch = dynamic_cast<LongVectorBatch&>(*intArrayBatch.elements);
    EXPECT_EQ(1, intArrayBatch.numElements);
    EXPECT_EQ(0, intArrayBatch.offsets.data()[0]);
    EXPECT_EQ(2, intArrayBatch.offsets.data()[1]);
    EXPECT_EQ(2, innerLongBatch.numElements);
    EXPECT_EQ(-1, innerLongBatch.data.data()[0]);
    EXPECT_EQ(-2, innerLongBatch.data.data()[1]);

    // verify content of int_array_array_array selection.
    auto& intArrayArrayArrayBatch = dynamic_cast<ListVectorBatch&>(*structBatch.fields[1]);
    EXPECT_EQ(1, intArrayArrayArrayBatch.numElements);
    EXPECT_EQ(0, intArrayArrayArrayBatch.offsets.data()[0]);
    EXPECT_EQ(3, intArrayArrayArrayBatch.offsets.data()[1]);
    EXPECT_EQ(nullptr, intArrayArrayArrayBatch.elements.get());
}
} // namespace orc
