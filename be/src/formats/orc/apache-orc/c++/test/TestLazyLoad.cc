// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include <iostream>
#include <vector>

#include "MemoryInputStream.hh"
#include "MemoryOutputStream.hh"
#include "OrcTest.hh"
#include "wrap/gtest-wrapper.h"

namespace orc {

TEST(TestLazyLoad, TestNormal) {
    orc::MemoryOutputStream buffer(102400);
    size_t batchSize = 1024;

    // prepare data.
    // size = 2 * batchSize
    // c0 -> [0, 1, 2, i ... ]
    // c1 -> [i * 10]
    {
        ORC_UNIQUE_PTR<orc::Type> schema(orc::Type::buildTypeFromString("struct<c0:int,c1:int>"));
        ORC_UNIQUE_PTR<orc::Writer> writer = createWriter(*schema, &buffer, orc::WriterOptions{});

        size_t numValues = batchSize * 2;
        ORC_UNIQUE_PTR<orc::ColumnVectorBatch> batch = writer->createRowBatch(numValues);
        auto* root = dynamic_cast<orc::StructVectorBatch*>(batch.get());
        auto* c0 = dynamic_cast<orc::LongVectorBatch*>(root->fields[0]);
        auto* c1 = dynamic_cast<orc::LongVectorBatch*>(root->fields[1]);
        for (size_t i = 0; i < numValues; i++) {
            c0->data[i] = i;
            c1->data[i] = i * 10;
        }
        c0->numElements = numValues;
        c1->numElements = numValues;
        root->numElements = numValues;
        writer->add(*batch);
        writer->close();
    }

    // read data.
    // we read first batch, and discard c1 field
    // then read second batch, and verify c1 values.
    {
        orc::ReaderOptions readerOptions;
        ORC_UNIQUE_PTR<orc::InputStream> inputStream(new orc::MemoryInputStream(buffer.getData(), buffer.getLength()));
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

        // clear memory.
        std::memset(c0->data.data(), 0x0, sizeof((c0->data[0])) * batchSize);
        std::memset(c1->data.data(), 0x0, sizeof((c1->data[0])) * batchSize);

        // there are 2 * batchSize, but we are going to read twice, each time read batchSize.
        EXPECT_EQ(rr->next(*batch), true);
        EXPECT_EQ(batch->numElements, batchSize);
        for (size_t i = 0; i < batchSize; i++) {
            ASSERT_EQ(c0->data[i], i);
            // since c1 is lazy loaded, we don't read actual data.
            ASSERT_EQ(c1->data[i], 0);
        }
        rr->lazyLoadSkip(batch->numElements);

        EXPECT_EQ(rr->next(*batch), true);
        EXPECT_EQ(batch->numElements, batchSize);
        rr->lazyLoadNext(*batch, batch->numElements);
        for (size_t i = 0, j = batchSize; i < batchSize; i++, j++) {
            ASSERT_EQ(c0->data[i], j);
            // since c1 is lazy loaded, we don't read actual data.
            ASSERT_EQ(c1->data[i], j * 10);
        }

        EXPECT_EQ(rr->next(*batch), false);
    }
}

} // namespace orc