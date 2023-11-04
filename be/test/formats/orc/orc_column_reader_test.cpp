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

#include "formats/orc/column_reader.h"
#include "formats/orc/memory_stream/MemoryInputStream.hh"
#include "formats/orc/memory_stream/MemoryOutputStream.hh"
#include "formats/orc/orc_chunk_reader.h"
#include "formats/orc/orc_mapping.h"

namespace starrocks {

// 100mb buffer
const static size_t bufferSize = 100 * 1024 * 1024;

TEST(OrcColumnReaderTest, TestDateColumn) {
    const static size_t batchSize = 3;

    MemoryOutputStream buffer(bufferSize);
    ORC_UNIQUE_PTR<orc::Type> schema(orc::Type::buildTypeFromString("struct<c0:date>"));
    const orc::Type* orcType = schema->getSubtype(0);

    // prepare data.
    {
        orc::WriterOptions writerOptions;
        ORC_UNIQUE_PTR<orc::Writer> writer = createWriter(*schema, &buffer, writerOptions);

        ORC_UNIQUE_PTR<orc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
        auto* root = dynamic_cast<orc::StructVectorBatch*>(batch.get());
        auto* c0 = dynamic_cast<orc::LongVectorBatch*>(root->fields[0]);

        for (size_t i = 0; i < batchSize; i++) {
            c0->data[i] = i;
        }
        c0->notNull[0] = 1;
        c0->notNull[1] = 0;
        c0->notNull[2] = 1;

        c0->numElements = batchSize;
        root->numElements = batchSize;
        writer->add(*batch);
        writer->close();
    }

    // read
    {
        orc::ReaderOptions readerOptions;
        ORC_UNIQUE_PTR<orc::InputStream> inputStream(new MemoryInputStream(buffer.getData(), buffer.getLength()));
        ORC_UNIQUE_PTR<orc::Reader> reader = createReader(std::move(inputStream), readerOptions);

        orc::RowReaderOptions options;
        std::list<std::string> columns = {"c0"};
        options.include(columns);
        ORC_UNIQUE_PTR<orc::RowReader> rr = reader->createRowReader(options);

        // Set for OrcMapping and OrcChunkReader, just used it to pass arguments in function, actually it's not used
        OrcMappingContext orc_mapping_context{nullptr, orcType, nullptr};
        OrcChunkReader orcChunkReader(batchSize, {});
        orcChunkReader.disable_broker_load_mode();

        TypeDescriptor c0Type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DATE);

        std::unique_ptr<ORCColumnReader> orcColumnReader =
                ORCColumnReader::create(c0Type, orc_mapping_context, true, &orcChunkReader).value();

        ORC_UNIQUE_PTR<orc::ColumnVectorBatch> batch = rr->createRowBatch(batchSize);
        auto* root = dynamic_cast<orc::StructVectorBatch*>(batch.get());
        auto* c0 = dynamic_cast<orc::LongVectorBatch*>(root->fields[0]);
        orc::RowReader::ReadPosition pos;
        EXPECT_TRUE(rr->next(*batch, &pos));
        ColumnPtr column = ColumnHelper::create_column(c0Type, true);
        EXPECT_TRUE(orcColumnReader->get_next(c0, column, 0, batchSize).ok());
        EXPECT_EQ(batchSize, column->size());

        EXPECT_EQ("1970-01-01", column->debug_item(0));
        EXPECT_EQ("NULL", column->debug_item(1));
        EXPECT_EQ("1970-01-02", column->debug_item(2));
    }
}

} // namespace starrocks