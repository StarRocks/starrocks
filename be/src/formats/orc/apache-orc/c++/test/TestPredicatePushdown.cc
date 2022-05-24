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

#include "MemoryInputStream.hh"
#include "MemoryOutputStream.hh"
#include "orc/OrcFile.hh"
#include "orc/sargs/SearchArgument.hh"
#include "wrap/gtest-wrapper.h"

namespace orc {

static const int DEFAULT_MEM_STREAM_SIZE = 10 * 1024 * 1024; // 10M

TEST(TestPredicatePushdown, testPredicatePushdown) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    auto type = std::unique_ptr<Type>(Type::buildTypeFromString("struct<int1:bigint,string1:string>"));
    WriterOptions options;
    options.setStripeSize(1024 * 1024)
            .setCompressionBlockSize(1024)
            .setCompression(CompressionKind_NONE)
            .setMemoryPool(pool)
            .setRowIndexStride(1000);

    auto writer = createWriter(*type, &memStream, options);
    auto batch = writer->createRowBatch(3500);
    auto& structBatch = dynamic_cast<StructVectorBatch&>(*batch);
    auto& longBatch = dynamic_cast<LongVectorBatch&>(*structBatch.fields[0]);
    auto& strBatch = dynamic_cast<StringVectorBatch&>(*structBatch.fields[1]);

    // row group stride is 1000, here 3500 rows of data constitute 4 row groups.
    // the min/max pair of each row group is as below:
    // int1: 0/299700, 300000/599700, 600000/899700, 900000/1049700
    // string1: "0"/"9990", "10000"/"19990", "20000"/"29990", "30000"/"34990"
    char buffer[3500 * 5];
    uint64_t offset = 0;
    for (uint64_t i = 0; i < 3500; ++i) {
        longBatch.data[i] = static_cast<int64_t>(i * 300);

        std::ostringstream ss;
        ss << 10 * i;
        std::string str = ss.str();
        memcpy(buffer + offset, str.c_str(), str.size());
        strBatch.data[i] = buffer + offset;
        strBatch.length[i] = static_cast<int64_t>(str.size());
        offset += str.size();
    }
    structBatch.numElements = 3500;
    longBatch.numElements = 3500;
    strBatch.numElements = 3500;
    writer->add(*batch);
    writer->close();

    std::unique_ptr<InputStream> inStream(new MemoryInputStream(memStream.getData(), memStream.getLength()));
    ReaderOptions readerOptions;
    options.setMemoryPool(pool);
    auto reader = createReader(std::move(inStream), readerOptions);
    EXPECT_EQ(3500, reader->getNumberOfRows());

    // build search argument (x >= 300000 AND x < 600000)
    {
        std::unique_ptr<SearchArgument> sarg =
                SearchArgumentFactory::newBuilder()
                        ->startAnd()
                        .startNot()
                        .lessThan("int1", PredicateDataType::LONG, Literal(static_cast<int64_t>(300000L)))
                        .end()
                        .lessThan("int1", PredicateDataType::LONG, Literal(static_cast<int64_t>(600000L)))
                        .end()
                        .build();

        RowReaderOptions rowReaderOpts;
        rowReaderOpts.searchArgument(std::move(sarg));
        auto rowReader = reader->createRowReader(rowReaderOpts);

        auto readBatch = rowReader->createRowBatch(2000);
        auto& batch0 = dynamic_cast<StructVectorBatch&>(*readBatch);
        auto& batch1 = dynamic_cast<LongVectorBatch&>(*batch0.fields[0]);
        auto& batch2 = dynamic_cast<StringVectorBatch&>(*batch0.fields[1]);

        EXPECT_EQ(true, rowReader->next(*readBatch));
        EXPECT_EQ(1000, readBatch->numElements);
        EXPECT_EQ(1000, rowReader->getRowNumber());
        for (uint64_t i = 1000; i < 2000; ++i) {
            EXPECT_EQ(300 * i, batch1.data[i - 1000]);
            EXPECT_EQ(std::to_string(10 * i),
                      std::string(batch2.data[i - 1000], static_cast<size_t>(batch2.length[i - 1000])));
        }
        EXPECT_EQ(false, rowReader->next(*readBatch));
        EXPECT_EQ(3500, rowReader->getRowNumber());
    }

    // look through the file with no rows selected: x < 0
    {
        std::unique_ptr<SearchArgument> sarg =
                SearchArgumentFactory::newBuilder()
                        ->startAnd()
                        .lessThan("int1", PredicateDataType::LONG, Literal(static_cast<int64_t>(0)))
                        .end()
                        .build();

        RowReaderOptions rowReaderOpts;
        rowReaderOpts.searchArgument(std::move(sarg));
        auto rowReader = reader->createRowReader(rowReaderOpts);

        auto readBatch = rowReader->createRowBatch(2000);
        EXPECT_EQ(false, rowReader->next(*readBatch));
        EXPECT_EQ(3500, rowReader->getRowNumber());
    }

    // select first 1000 and last 500 rows: x < 30000 OR x >= 1020000
    {
        std::unique_ptr<SearchArgument> sarg =
                SearchArgumentFactory::newBuilder()
                        ->startOr()
                        .lessThan("int1", PredicateDataType::LONG, Literal(static_cast<int64_t>(300 * 100)))
                        .startNot()
                        .lessThan("int1", PredicateDataType::LONG, Literal(static_cast<int64_t>(300 * 3400)))
                        .end()
                        .end()
                        .build();

        RowReaderOptions rowReaderOpts;
        rowReaderOpts.searchArgument(std::move(sarg));
        auto rowReader = reader->createRowReader(rowReaderOpts);

        auto readBatch = rowReader->createRowBatch(2000);
        auto& batch0 = dynamic_cast<StructVectorBatch&>(*readBatch);
        auto& batch1 = dynamic_cast<LongVectorBatch&>(*batch0.fields[0]);
        auto& batch2 = dynamic_cast<StringVectorBatch&>(*batch0.fields[1]);

        EXPECT_EQ(true, rowReader->next(*readBatch));
        EXPECT_EQ(1000, readBatch->numElements);
        EXPECT_EQ(0, rowReader->getRowNumber());
        for (uint64_t i = 0; i < 1000; ++i) {
            EXPECT_EQ(300 * i, batch1.data[i]);
            EXPECT_EQ(std::to_string(10 * i), std::string(batch2.data[i], static_cast<size_t>(batch2.length[i])));
        }

        EXPECT_EQ(true, rowReader->next(*readBatch));
        EXPECT_EQ(500, readBatch->numElements);
        EXPECT_EQ(3000, rowReader->getRowNumber());
        for (uint64_t i = 3000; i < 3500; ++i) {
            EXPECT_EQ(300 * i, batch1.data[i - 3000]);
            EXPECT_EQ(std::to_string(10 * i),
                      std::string(batch2.data[i - 3000], static_cast<size_t>(batch2.length[i - 3000])));
        }

        EXPECT_EQ(false, rowReader->next(*readBatch));
        EXPECT_EQ(3500, rowReader->getRowNumber());

        // test seek to 3rd row group but is adjusted to 4th row group
        rowReader->seekToRow(2500);
        EXPECT_EQ(true, rowReader->next(*readBatch));
        EXPECT_EQ(3000, rowReader->getRowNumber());
        EXPECT_EQ(500, readBatch->numElements);
        for (uint64_t i = 3000; i < 3500; ++i) {
            EXPECT_EQ(300 * i, batch1.data[i - 3000]);
            EXPECT_EQ(std::to_string(10 * i),
                      std::string(batch2.data[i - 3000], static_cast<size_t>(batch2.length[i - 3000])));
        }
        EXPECT_EQ(false, rowReader->next(*readBatch));
        EXPECT_EQ(3500, rowReader->getRowNumber());
    }
}

} // namespace orc
