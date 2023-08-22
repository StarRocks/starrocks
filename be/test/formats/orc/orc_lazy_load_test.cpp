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

#include "formats/orc/memory_stream/MemoryInputStream.hh"
#include "formats/orc/memory_stream/MemoryOutputStream.hh"

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

TEST(OrcLazyLoadTest, TestStructSubFieldLoad) {
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

TEST(OrcLazyLoadTest, TestStructLazyLoad) {
    MemoryOutputStream buffer(1024000);
    size_t batchSize = 1024;
    size_t batchNum = 4;
    size_t numRows = 4096;

    // prepare data.
    {
        orc::WriterOptions writerOptions;
        writerOptions.setStripeSize(1024);
        writerOptions.setRowIndexStride(0);
        ORC_UNIQUE_PTR<orc::Type> schema(orc::Type::buildTypeFromString("struct<c0:int,c1:struct<c1_1:int,c1_2:int>>"));
        ORC_UNIQUE_PTR<orc::Writer> writer = createWriter(*schema, &buffer, writerOptions);

        ORC_UNIQUE_PTR<orc::ColumnVectorBatch> rowBatch = writer->createRowBatch(batchSize);
        auto* root = dynamic_cast<orc::StructVectorBatch*>(rowBatch.get());
        auto* c0 = dynamic_cast<orc::LongVectorBatch*>(root->fields[0]);
        auto* c1 = dynamic_cast<orc::StructVectorBatch*>(root->fields[1]);
        auto* c1_1 = dynamic_cast<orc::LongVectorBatch*>(c1->fields[0]);
        auto* c1_2 = dynamic_cast<orc::LongVectorBatch*>(c1->fields[1]);

        size_t rowIndex = 0;
        for (size_t k = 0; k < batchNum; k++) {
            for (size_t i = 0; i < batchSize; i++) {
                c0->data[i] = rowIndex;
                c1_1->data[i] = rowIndex * 10;
                c1_2->data[i] = rowIndex * 20;
                rowIndex++;
            }
            root->numElements = batchSize;
            c0->numElements = batchSize;
            c1->numElements = batchSize;
            c1_1->numElements = batchSize;
            c1_2->numElements = batchSize;

            // add null
            root->hasNulls = true;
            c0->hasNulls = true;
            c1->hasNulls = true;
            c1_1->hasNulls = true;
            c1_2->hasNulls = true;
            for (size_t i = 0; i < batchSize; i++) {
                size_t notNull = (i % 9 != 0) ? 1 : 0;
                root->notNull[i] = notNull;
                c0->notNull[i] = notNull;
                c1->notNull[i] = notNull;
                c1_1->notNull[i] = notNull;
                c1_2->notNull[i] = notNull;
            }

            writer->add(*rowBatch);
        }
        writer->close();
        ASSERT_EQ(numRows, rowIndex);
    }

    // read data.
    {
        orc::ReaderOptions readerOptions;
        ORC_UNIQUE_PTR<orc::InputStream> inputStream(new MemoryInputStream(buffer.getData(), buffer.getLength()));
        ORC_UNIQUE_PTR<orc::Reader> reader = createReader(std::move(inputStream), readerOptions);

        orc::RowReaderOptions options;
        std::list<std::string> includeColumns = {"c0", "c1"};
        std::list<std::string> lazyColumns = {"c1"};
        options.include(includeColumns);
        options.includeLazyLoadColumnNames(lazyColumns);
        ORC_UNIQUE_PTR<orc::RowReader> rr = reader->createRowReader(options);

        ORC_UNIQUE_PTR<orc::ColumnVectorBatch> rowBatch = rr->createRowBatch(batchSize);

        size_t rowIndex = 0;
        for (size_t k = 0; k < batchNum; k++) {
            orc::RowReader::ReadPosition pos;
            ASSERT_TRUE(rr->next(*rowBatch, &pos));
            EXPECT_EQ(rowBatch->numElements, batchSize);

            size_t lazyRowIndex = rowIndex;

            {
                // read active first
                auto* root = dynamic_cast<orc::StructVectorBatch*>(rowBatch.get());
                auto* c0 = dynamic_cast<orc::LongVectorBatch*>(root->fields[0]);

                // check values
                for (size_t i = 0; i < batchSize; i++) {
                    c0->data[i] = rowIndex;
                    rowIndex++;
                }

                ASSERT_EQ(batchSize, root->numElements);
                ASSERT_EQ(batchSize, c0->numElements);

                // check null
                for (size_t i = 0; i < batchSize; i++) {
                    size_t notNull = (i % 9 != 0) ? 1 : 0;
                    ASSERT_EQ(notNull, root->notNull[i]);
                    ASSERT_EQ(notNull, c0->notNull[i]);
                }

                ASSERT_EQ(true, root->hasNulls);
                ASSERT_EQ(true, c0->hasNulls);
            }

            {
                // read lazy next
                rr->lazyLoadSeekTo(lazyRowIndex);
                rr->lazyLoadNext(*rowBatch, batchSize);

                auto* root = dynamic_cast<orc::StructVectorBatch*>(rowBatch.get());
                auto* c1 = dynamic_cast<orc::StructVectorBatch*>(root->fields[1]);
                auto* c1_1 = dynamic_cast<orc::LongVectorBatch*>(c1->fields[0]);
                auto* c1_2 = dynamic_cast<orc::LongVectorBatch*>(c1->fields[1]);

                for (size_t i = 0; i < batchSize; i++) {
                    c1_1->data[i] = rowIndex * 10;
                    c1_2->data[i] = rowIndex * 20;
                    lazyRowIndex++;
                }

                ASSERT_EQ(batchSize, root->numElements);
                ASSERT_EQ(batchSize, c1->numElements);
                ASSERT_EQ(batchSize, c1_1->numElements);
                ASSERT_EQ(batchSize, c1_2->numElements);

                // check null
                for (size_t i = 0; i < batchSize; i++) {
                    size_t notNull = (i % 9 != 0) ? 1 : 0;
                    ASSERT_EQ(notNull, root->notNull[i]);
                    ASSERT_EQ(notNull, c1->notNull[i]);
                    ASSERT_EQ(notNull, c1_1->notNull[i]);
                    ASSERT_EQ(notNull, c1_2->notNull[i]);
                }

                ASSERT_EQ(true, root->hasNulls);
                ASSERT_EQ(true, c1->hasNulls);
                ASSERT_EQ(true, c1_1->hasNulls);
                ASSERT_EQ(true, c1_2->hasNulls);
            }
        }

        EXPECT_EQ(rr->next(*rowBatch), false);
    }
}

TEST(OrcLazyLoadTest, TestArrayStructLazyLoad) {
    MemoryOutputStream buffer(1024000);
    // Write data demo:
    // {c0: 0, c1: [{c1_1: 100}, {c1_1: 101}]}
    // {c0: 1, c1: null}
    // {c0: 2, c1: [{c1_1: 102}]}
    // {c0: 3, c1: [{c1_1: 103}, null, {c1_1: null}, {c1_1: 106}]}
    // {c0: 4, c1: null}
    // {c0: 5, c1: [{c1_1: 107}]}
    // {null}

    // prepare data.
    {
        orc::WriterOptions writerOptions;
        writerOptions.setStripeSize(3);
        writerOptions.setRowIndexStride(0);
        ORC_UNIQUE_PTR<orc::Type> schema(orc::Type::buildTypeFromString("struct<c0:int,c1:array<struct<c1_1:int>>>"));
        ORC_UNIQUE_PTR<orc::Writer> writer = createWriter(*schema, &buffer, writerOptions);

        ORC_UNIQUE_PTR<orc::ColumnVectorBatch> rowBatch = writer->createRowBatch(7);
        auto* root = dynamic_cast<orc::StructVectorBatch*>(rowBatch.get());
        auto* c0 = dynamic_cast<orc::LongVectorBatch*>(root->fields[0]);
        auto* c1 = dynamic_cast<orc::ListVectorBatch*>(root->fields[1]);
        auto* c1_element = dynamic_cast<orc::StructVectorBatch*>(c1->elements.get());
        auto* c1_1 = dynamic_cast<orc::LongVectorBatch*>(c1_element->fields[0]);

        // handle root / c0 / c1
        root->numElements = 7;
        c0->numElements = 7;
        c1->numElements = 7;
        for (size_t i = 0; i < 7; i++) {
            root->notNull[i] = 1;
            c0->notNull[i] = 1;
            c1->notNull[i] = 1;
            c0->data[i] = i;
        }
        root->notNull[6] = 0;
        c0->notNull[6] = 0;

        // set c1's offset
        int64_t* offsets = c1->offsets.data();
        offsets[0] = 0;
        offsets[1] = 2;
        offsets[2] = 2;
        offsets[3] = 3;
        offsets[4] = 7;
        offsets[5] = 7;
        offsets[6] = 8;
        offsets[7] = 8;

        c1->hasNulls = true;
        c1->notNull[1] = 0;
        c1->notNull[4] = 0;
        c1->notNull[6] = 0;

        // set c1_element
        c1_element->hasNulls = true;
        c1_element->numElements = 8;
        c1_element->notNull.resize(8);
        for (size_t i = 0; i < 8; i++) {
            c1_element->notNull[i] = 1;
        }
        c1_element->notNull[4] = 0;

        // set c1_1
        c1_1->numElements = 8;
        c1_1->hasNulls = true;
        c1_1->data.resize(8);
        c1_1->notNull.resize(8);
        for (size_t i = 0; i < 8; i++) {
            c1_1->data[i] = 100 + i;
            c1_1->notNull[i] = 1;
        }
        c1_1->notNull[4] = 0;
        c1_1->notNull[5] = 0;

        writer->add(*rowBatch);
        writer->close();
    }

    // read all
    {
        orc::ReaderOptions readerOptions;
        ORC_UNIQUE_PTR<orc::InputStream> inputStream(new MemoryInputStream(buffer.getData(), buffer.getLength()));
        ORC_UNIQUE_PTR<orc::Reader> reader = createReader(std::move(inputStream), readerOptions);

        orc::RowReaderOptions options;
        std::list<std::string> includeColumns = {"c0", "c1"};
        options.include(includeColumns);
        ORC_UNIQUE_PTR<orc::RowReader> rr = reader->createRowReader(options);

        ORC_UNIQUE_PTR<orc::ColumnVectorBatch> rowBatch = rr->createRowBatch(7);

        orc::RowReader::ReadPosition pos;
        ASSERT_TRUE(rr->next(*rowBatch, &pos));

        auto* root = dynamic_cast<orc::StructVectorBatch*>(rowBatch.get());
        auto* c0 = dynamic_cast<orc::LongVectorBatch*>(root->fields[0]);
        auto* c1 = dynamic_cast<orc::ListVectorBatch*>(root->fields[1]);
        auto* c1_element = dynamic_cast<orc::StructVectorBatch*>(c1->elements.get());
        auto* c1_1 = dynamic_cast<orc::LongVectorBatch*>(c1_element->fields[0]);

        EXPECT_EQ(7, c0->numElements);
        EXPECT_EQ(7, c1->numElements);
        EXPECT_EQ(8, c1_1->numElements);
    }

    // read data with lazy load
    {
        orc::ReaderOptions readerOptions;
        ORC_UNIQUE_PTR<orc::InputStream> inputStream(new MemoryInputStream(buffer.getData(), buffer.getLength()));
        ORC_UNIQUE_PTR<orc::Reader> reader = createReader(std::move(inputStream), readerOptions);

        orc::RowReaderOptions options;
        std::list<std::string> includeColumns = {"c0", "c1"};
        std::list<std::string> lazyColumns = {"c1"};
        options.include(includeColumns);
        options.includeLazyLoadColumnNames(lazyColumns);
        ORC_UNIQUE_PTR<orc::RowReader> rr = reader->createRowReader(options);

        orc::StructVectorBatch* root = nullptr;
        orc::LongVectorBatch* c0 = nullptr;
        orc::ListVectorBatch* c1 = nullptr;
        orc::StructVectorBatch* c1_element = nullptr;
        orc::LongVectorBatch* c1_1 = nullptr;

        orc::RowReader::ReadPosition pos;
        {
            ORC_UNIQUE_PTR<orc::ColumnVectorBatch> rowBatch = rr->createRowBatch(3);
            // read first rowBatch
            ASSERT_TRUE(rr->next(*rowBatch, &pos));
            EXPECT_EQ(3, rowBatch->numElements);
            EXPECT_EQ(0, pos.row_in_stripe);
            EXPECT_EQ(3, pos.num_values);

            // check active
            root = dynamic_cast<orc::StructVectorBatch*>(rowBatch.get());
            c0 = dynamic_cast<orc::LongVectorBatch*>(root->fields[0]);
            EXPECT_EQ(3, root->numElements);
            EXPECT_EQ(3, c0->numElements);
            EXPECT_FALSE(root->hasNulls);
            EXPECT_FALSE(c0->hasNulls);

            EXPECT_EQ(0, c0->data[0]);
            EXPECT_EQ(1, c0->data[1]);
            EXPECT_EQ(2, c0->data[2]);
            EXPECT_EQ(1, c0->notNull[0]);
            EXPECT_EQ(1, c0->notNull[1]);
            EXPECT_EQ(1, c0->notNull[2]);

            // make sure lazy column didn't load now
            c1 = dynamic_cast<orc::ListVectorBatch*>(root->fields[1]);
            EXPECT_EQ(0, c1->numElements);
        }
        {
            ORC_UNIQUE_PTR<orc::ColumnVectorBatch> rowBatch = rr->createRowBatch(3);

            // read second rowBatch
            ASSERT_TRUE(rr->next(*rowBatch, &pos));
            EXPECT_EQ(3, rowBatch->numElements);
            EXPECT_EQ(3, pos.row_in_stripe);
            EXPECT_EQ(3, pos.num_values);

            root = dynamic_cast<orc::StructVectorBatch*>(rowBatch.get());

            // check active
            c0 = dynamic_cast<orc::LongVectorBatch*>(root->fields[0]);
            EXPECT_EQ(3, root->numElements);
            EXPECT_EQ(3, c0->numElements);
            EXPECT_FALSE(root->hasNulls);
            EXPECT_FALSE(c0->hasNulls);

            EXPECT_EQ(3, c0->data[0]);
            EXPECT_EQ(4, c0->data[1]);
            EXPECT_EQ(5, c0->data[2]);
            EXPECT_EQ(1, c0->notNull[0]);
            EXPECT_EQ(1, c0->notNull[1]);
            EXPECT_EQ(1, c0->notNull[2]);

            // check lazy
            // skip to line #3
            rr->lazyLoadSeekTo(pos.row_in_stripe);
            rr->lazyLoadNext(*rowBatch, 3);

            c1 = dynamic_cast<orc::ListVectorBatch*>(root->fields[1]);
            c1_element = dynamic_cast<orc::StructVectorBatch*>(c1->elements.get());
            c1_1 = dynamic_cast<orc::LongVectorBatch*>(c1_element->fields[0]);

            EXPECT_EQ(3, c1->numElements);
            EXPECT_TRUE(c1->hasNulls);
            c1->notNull[0] = 1;
            c1->notNull[1] = 1;
            c1->notNull[2] = 0;

            // check c1_element
            EXPECT_EQ(5, c1_element->numElements);
            EXPECT_TRUE(c1_element->hasNulls);
            EXPECT_EQ(0, c1_element->notNull[1]);

            // check c1_1
            EXPECT_EQ(5, c1_1->numElements);
            EXPECT_EQ(0, c1_1->notNull[1]);
            EXPECT_EQ(0, c1_1->notNull[2]);
            EXPECT_EQ(107, c1_1->data[4]);
        }
        {
            ORC_UNIQUE_PTR<orc::ColumnVectorBatch> rowBatch = rr->createRowBatch(3);

            // read third rowBatch
            ASSERT_TRUE(rr->next(*rowBatch, &pos));
            EXPECT_EQ(1, rowBatch->numElements);
            EXPECT_EQ(6, pos.row_in_stripe);
            EXPECT_EQ(1, pos.num_values);

            // check active
            root = dynamic_cast<orc::StructVectorBatch*>(rowBatch.get());
            c0 = dynamic_cast<orc::LongVectorBatch*>(root->fields[0]);
            EXPECT_EQ(1, root->numElements);
            EXPECT_EQ(1, c0->numElements);
            EXPECT_TRUE(root->hasNulls);
            EXPECT_TRUE(c0->hasNulls);

            // make sure lazy column didn't load now
            c1 = dynamic_cast<orc::ListVectorBatch*>(root->fields[1]);
            EXPECT_EQ(0, c1->numElements);
        }
        {
            ORC_UNIQUE_PTR<orc::ColumnVectorBatch> rowBatch = rr->createRowBatch(3);

            // read EOF
            EXPECT_EQ(false, rr->next(*rowBatch));
        }
    }
    // read all with filter
    {
        orc::ReaderOptions readerOptions;
        ORC_UNIQUE_PTR<orc::InputStream> inputStream(new MemoryInputStream(buffer.getData(), buffer.getLength()));
        ORC_UNIQUE_PTR<orc::Reader> reader = createReader(std::move(inputStream), readerOptions);

        orc::RowReaderOptions options;
        std::list<std::string> includeColumns = {"c0", "c1"};
        options.include(includeColumns);
        ORC_UNIQUE_PTR<orc::RowReader> rr = reader->createRowReader(options);

        ORC_UNIQUE_PTR<orc::ColumnVectorBatch> rowBatch = rr->createRowBatch(7);

        orc::RowReader::ReadPosition pos;
        ASSERT_TRUE(rr->next(*rowBatch, &pos));

        auto* root = dynamic_cast<orc::StructVectorBatch*>(rowBatch.get());
        auto* c0 = dynamic_cast<orc::LongVectorBatch*>(root->fields[0]);
        auto* c1 = dynamic_cast<orc::ListVectorBatch*>(root->fields[1]);
        auto* c1_element = dynamic_cast<orc::StructVectorBatch*>(c1->elements.get());
        auto* c1_1 = dynamic_cast<orc::LongVectorBatch*>(c1_element->fields[0]);

        EXPECT_EQ(7, root->numElements);

        std::vector<uint8_t> filter;
        filter.resize(7, 1);
        filter[1] = 0;

        root->filterOnFields(filter.data(), 7, 6, {0, 1}, true);

        // check root
        EXPECT_TRUE(root->hasNulls);
        EXPECT_EQ(6, root->numElements);
        for (size_t i = 0; i < root->numElements - 1; i++) {
            EXPECT_EQ(1, root->notNull[i]);
        }
        EXPECT_EQ(0, root->notNull[root->numElements - 1]);

        // check c0
        EXPECT_TRUE(c0->hasNulls);
        EXPECT_EQ(6, c0->numElements);
        EXPECT_EQ(0, c0->data[0]);
        EXPECT_EQ(5, c0->data[4]);

        // check c1_1
        EXPECT_EQ(8, c1_1->numElements);
        EXPECT_TRUE(c1_1->hasNulls);
        for (size_t i = 0; i < c1_1->numElements; i++) {
            if (i == 4 || i == 5) {
                EXPECT_EQ(0, c1_1->notNull[i]);
            } else {
                EXPECT_EQ(1, c1_1->notNull[i]);
                EXPECT_EQ(100 + i, c1_1->data[i]);
            }
        }
    }
}

} // namespace starrocks