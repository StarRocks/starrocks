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

#include <gtest/gtest.h>

#include <orc/Writer.hh>

#include "orc_test_util/MemoryInputStream.hh"
#include "orc_test_util/MemoryOutputStream.hh"

namespace starrocks {

TEST(OrcLazyLoadTest, TestNormal) {
    MemoryOutputStream buffer(1024000);
    size_t batchSize = 1024;
    size_t batchNum = 128;

    // prepare data.
    {
        orc::WriterOptions writerOptions;
        // force to make stripe every time.
        writerOptions.setStripeSize(0);
        writerOptions.setRowIndexStride(10);
        ORC_UNIQUE_PTR<orc::Type> schema(orc::Type::buildTypeFromString("struct<c0:int,c1:int>"));
        ORC_UNIQUE_PTR<orc::Writer> writer = createWriter(*schema, &buffer, writerOptions);

        ORC_UNIQUE_PTR<orc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
        auto* root = dynamic_cast<orc::StructVectorBatch*>(batch.get());
        auto* c0 = dynamic_cast<orc::LongVectorBatch*>(root->fields[0]);
        auto* c1 = dynamic_cast<orc::LongVectorBatch*>(root->fields[1]);

        size_t index = 0;
        for (size_t k = 0; k < batchNum; k++) {
            for (size_t i = 0; i < batchSize; i++) {
                c0->data[i] = index;
                c1->data[i] = index * 10;
                index += 1;
            }
            c0->numElements = batchSize;
            c1->numElements = batchSize;
            root->numElements = batchSize;
            writer->add(*batch);
        }
        writer->close();
    }

    // read data.
    {
        orc::ReaderOptions readerOptions;
        ORC_UNIQUE_PTR<orc::InputStream> inputStream(new MemoryInputStream(buffer.getData(), buffer.getLength()));
        ORC_UNIQUE_PTR<orc::Reader> reader = createReader(std::move(inputStream), readerOptions);

        orc::RowReaderOptions options;
        std::list<std::string> columns = {"c0", "c1"};
        std::list<std::string> lazyColumns = {"c1"};
        options.include(columns);
        options.includeLazyLoadColumnNames(lazyColumns);
        ORC_UNIQUE_PTR<orc::RowReader> rr = reader->createRowReader(options);

        ORC_UNIQUE_PTR<orc::ColumnVectorBatch> batch = rr->createRowBatch(batchSize);
        auto* root = dynamic_cast<orc::StructVectorBatch*>(batch.get());
        auto* c0 = dynamic_cast<orc::LongVectorBatch*>(root->fields[0]);
        auto* c1 = dynamic_cast<orc::LongVectorBatch*>(root->fields[1]);

        size_t index = 0;
        for (size_t k = 0; k < batchNum; k++) {
            // clear memory.
            std::memset(c0->data.data(), 0x0, sizeof((c0->data[0])) * batchSize);
            std::memset(c1->data.data(), 0x0, sizeof((c1->data[0])) * batchSize);
            orc::RowReader::ReadPosition pos;
            EXPECT_EQ(rr->next(*batch, &pos), true);
            EXPECT_EQ(batch->numElements, batchSize);

            if ((k & 0x1) == 0) {
                for (size_t i = 0; i < batchSize; i++) {
                    ASSERT_EQ(c0->data[i], index);
                    // since c1 is lazy loaded, we don't read actual data.
                    ASSERT_EQ(c1->data[i], 0);
                    index += 1;
                }
                rr->lazyLoadSeekTo(pos.row_in_stripe);
            } else {
                rr->lazyLoadNext(*batch, batch->numElements);
                for (size_t i = 0; i < batchSize; i++) {
                    ASSERT_EQ(c0->data[i], index);
                    ASSERT_EQ(c1->data[i], index * 10);
                    index += 1;
                }
            }
        }

        EXPECT_EQ(rr->next(*batch), false);
    }
}

TEST(OrcLazyLoadTest, TestStructSubField) {
    MemoryOutputStream buffer(1024000);
    const size_t batchSize = 1024;
    const size_t batchNum = 128;

    // prepare data.
    {
        orc::WriterOptions writerOptions;
        // force to make stripe every time.
        writerOptions.setStripeSize(0);
        writerOptions.setRowIndexStride(10);
        ORC_UNIQUE_PTR<orc::Type> schema(orc::Type::buildTypeFromString("struct<c0:int,c1:struct<c11:int,c12:int>>"));
        ORC_UNIQUE_PTR<orc::Writer> writer = createWriter(*schema, &buffer, writerOptions);

        ORC_UNIQUE_PTR<orc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
        auto* root = dynamic_cast<orc::StructVectorBatch*>(batch.get());
        auto* c0 = dynamic_cast<orc::LongVectorBatch*>(root->fields[0]);
        auto* c1 = dynamic_cast<orc::StructVectorBatch*>(root->fields[1]);
        auto* c11 = dynamic_cast<orc::LongVectorBatch*>(c1->fields[0]);
        auto* c12 = dynamic_cast<orc::LongVectorBatch*>(c1->fields[1]);

        size_t index = 0;
        for (size_t k = 0; k < batchNum; k++) {
            for (size_t i = 0; i < batchSize; i++) {
                c0->data[i] = index;
                c11->data[i] = index * 5;
                c12->data[i] = index * 10;
                index += 1;
            }
            c0->numElements = batchSize;
            c1->numElements = batchSize;
            c11->numElements = batchSize;
            c12->numElements = batchSize;
            root->numElements = batchSize;
            writer->add(*batch);
        }
        writer->close();
    }

    // read data.
    {
        orc::ReaderOptions readerOptions;
        ORC_UNIQUE_PTR<orc::InputStream> inputStream(new MemoryInputStream(buffer.getData(), buffer.getLength()));
        ORC_UNIQUE_PTR<orc::Reader> reader = createReader(std::move(inputStream), readerOptions);

        orc::RowReaderOptions options;
        std::list<uint64_t> include_type{0, 1, 2, 3, 4};
        // lazy load c0, c12
        std::list<uint64_t> include_lazy_type{1, 4};
        options.includeTypes(include_type);
        options.includeLazyLoadColumnIndexes(include_lazy_type);
        ORC_UNIQUE_PTR<orc::RowReader> rr = reader->createRowReader(options);

        ORC_UNIQUE_PTR<orc::ColumnVectorBatch> batch = rr->createRowBatch(batchSize);
        auto* root = dynamic_cast<orc::StructVectorBatch*>(batch.get());
        auto* c0 = dynamic_cast<orc::LongVectorBatch*>(root->fields[0]);
        auto* c1 = dynamic_cast<orc::StructVectorBatch*>(root->fields[1]);
        auto* c11 = dynamic_cast<orc::LongVectorBatch*>(c1->fields[0]);
        auto* c12 = dynamic_cast<orc::LongVectorBatch*>(c1->fields[1]);

        size_t index = 0;
        for (size_t k = 0; k < batchNum; k++) {
            // clear memory.
            std::memset(c0->data.data(), 0x0, sizeof((c0->data[0])) * batchSize);
            std::memset(c11->data.data(), 0x0, sizeof((c11->data[0])) * batchSize);
            std::memset(c12->data.data(), 0x0, sizeof((c12->data[0])) * batchSize);

            orc::RowReader::ReadPosition pos;
            EXPECT_EQ(rr->next(*batch, &pos), true);
            EXPECT_EQ(batch->numElements, batchSize);

            if ((k & 0x1) == 0) {
                for (size_t i = 0; i < batchSize; i++) {
                    ASSERT_EQ(c0->data[i], 0);
                    ASSERT_EQ(c11->data[i], index * 5);
                    ASSERT_EQ(c12->data[i], 0);
                    index += 1;
                }
                rr->lazyLoadSeekTo(pos.row_in_stripe);
            } else {
                rr->lazyLoadNext(*batch, batch->numElements);
                for (size_t i = 0; i < batchSize; i++) {
                    ASSERT_EQ(c0->data[i], index);
                    ASSERT_EQ(c11->data[i], index * 5);
                    ASSERT_EQ(c12->data[i], index * 10);
                    index += 1;
                }
            }
        }

        EXPECT_EQ(rr->next(*batch), false);
    }
}

TEST(OrcLazyLoadTest, TestWithSearchArgument) {
    MemoryOutputStream buffer(1024000);
    size_t batchSize = 1024;
    size_t batchNum = 2;
    size_t readSize = 256;
    EXPECT_EQ(batchSize % readSize == 0, true);
    size_t repeatRead = batchSize / readSize;
    EXPECT_EQ(repeatRead > 2, true);

    // prepare data.
    {
        orc::WriterOptions writerOptions;
        // force to make stripe every time.
        writerOptions.setStripeSize(0);
        ORC_UNIQUE_PTR<orc::Type> schema(orc::Type::buildTypeFromString("struct<c0:int,c1:int>"));
        ORC_UNIQUE_PTR<orc::Writer> writer = createWriter(*schema, &buffer, writerOptions);

        ORC_UNIQUE_PTR<orc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
        auto* root = dynamic_cast<orc::StructVectorBatch*>(batch.get());
        auto* c0 = dynamic_cast<orc::LongVectorBatch*>(root->fields[0]);
        auto* c1 = dynamic_cast<orc::LongVectorBatch*>(root->fields[1]);

        size_t index = 0;
        for (size_t k = 0; k < batchNum; k++) {
            for (size_t i = 0; i < batchSize; i++) {
                c0->data[i] = index;
                c1->data[i] = index * 10;
                index += 1;
            }
            c0->numElements = batchSize;
            c1->numElements = batchSize;
            root->numElements = batchSize;
            writer->add(*batch);
        }
        writer->close();
    }

    // read data with predicates.
    // c0 == batchSize + readSize, at stripe 2 and second read.
    {
        auto builder = orc::SearchArgumentFactory::newBuilder();
        builder->equals("c0", orc::PredicateDataType::LONG, orc::Literal((int64_t)(batchSize + readSize)));
        auto sarg = builder->build();

        orc::ReaderOptions readerOptions;
        ORC_UNIQUE_PTR<orc::InputStream> inputStream(new MemoryInputStream(buffer.getData(), buffer.getLength()));
        ORC_UNIQUE_PTR<orc::Reader> reader = createReader(std::move(inputStream), readerOptions);

        orc::RowReaderOptions options;
        options.searchArgument(std::move(sarg));
        std::list<std::string> columns = {"c0", "c1"};
        std::list<std::string> lazyColumns = {"c1"};
        options.include(columns);
        options.includeLazyLoadColumnNames(lazyColumns);
        ORC_UNIQUE_PTR<orc::RowReader> rr = reader->createRowReader(options);

        ORC_UNIQUE_PTR<orc::ColumnVectorBatch> batch = rr->createRowBatch(readSize);
        auto* root = dynamic_cast<orc::StructVectorBatch*>(batch.get());
        auto* c0 = dynamic_cast<orc::LongVectorBatch*>(root->fields[0]);
        auto* c1 = dynamic_cast<orc::LongVectorBatch*>(root->fields[1]);

        std::memset(c0->data.data(), 0x0, sizeof((c0->data[0])) * readSize);
        std::memset(c1->data.data(), 0x0, sizeof((c1->data[0])) * readSize);

        // stripe #1 be filtered by search argument.
        // // stripe #1
        // for (size_t i = 0; i < repeatRead; i++) {
        //     EXPECT_EQ(rr->next(*batch), true);
        //     EXPECT_EQ(batch->numElements, readSize);
        // }

        orc::RowReader::ReadPosition pos;
        // stripe #2.
        EXPECT_EQ(rr->next(*batch, &pos), true);
        EXPECT_EQ(batch->numElements, readSize);

        // we don't need to skip first stripe.
        EXPECT_EQ(rr->next(*batch, &pos), true);
        EXPECT_EQ(batch->numElements, readSize);
        rr->lazyLoadSeekTo(pos.row_in_stripe);
        rr->lazyLoadNext(*batch, readSize);

        size_t index = batchSize + readSize;
        for (size_t i = 0; i < readSize; i++) {
            ASSERT_EQ(c0->data[i], index);
            // since c1 is lazy loaded, we don't read actual data.
            ASSERT_EQ(c1->data[i], index * 10);
            index += 1;
        }

        for (size_t i = 2; i < repeatRead; i++) {
            EXPECT_EQ(rr->next(*batch), true);
            EXPECT_EQ(batch->numElements, readSize);
        }
        EXPECT_EQ(rr->next(*batch), false);
    }
}

} // namespace starrocks