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

#include <random>

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
        const OrcMappingPtr orcMapping = nullptr;
        OrcChunkReader orcChunkReader(batchSize, {});
        orcChunkReader.disable_broker_load_mode();

        TypeDescriptor c0Type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DATE);

        std::unique_ptr<ORCColumnReader> orcColumnReader =
                ORCColumnReader::create(c0Type, orcType, true, orcMapping, &orcChunkReader).value();

        ORC_UNIQUE_PTR<orc::ColumnVectorBatch> batch = rr->createRowBatch(batchSize);
        auto* root = dynamic_cast<orc::StructVectorBatch*>(batch.get());
        auto* c0 = dynamic_cast<orc::LongVectorBatch*>(root->fields[0]);
        orc::RowReader::ReadPosition pos;
        EXPECT_TRUE(rr->next(*batch, &pos));
        MutableColumnPtr column = ColumnHelper::create_column(c0Type, true);
        EXPECT_TRUE(orcColumnReader->get_next(c0, column, 0, batchSize).ok());
        EXPECT_EQ(batchSize, column->size());

        EXPECT_EQ("1970-01-01", column->debug_item(0));
        EXPECT_EQ("NULL", column->debug_item(1));
        EXPECT_EQ("1970-01-02", column->debug_item(2));
    }
}

// TIMESTAMP_INSTANT values read in the reader zone with the offset at each instant, and plain TIMESTAMP values as
// they are, match the cctz conversion: values across DST changes and before 1970, null rows, and reads that start
// at a non-zero batch position into a column that already holds rows.
TEST(OrcColumnReaderTest, TestTimestampColumnsInDstZone) {
    std::mt19937_64 rng(41);
    const size_t n = 3000;
    orc::TimestampVectorBatch batch(n, *orc::getDefaultPool());
    std::vector<int64_t> fixed = {0, -1, 1711846800 - 1, 1711846800, 1729990800 - 1, 1729990800};
    for (size_t i = 0; i < n; i++) {
        batch.data[i] = i < fixed.size() ? fixed[i] : static_cast<int64_t>(rng() % 4000000000ULL) - 1000000000LL;
        batch.nanoseconds[i] = static_cast<int64_t>(rng() % 1000000000ULL);
        batch.notNull[i] = i % 7 != 3;
    }
    batch.hasNulls = true;
    batch.numElements = n;

    for (const std::string zone : {"America/Los_Angeles", "Europe/London", "UTC"}) {
        for (auto kind : {orc::TypeKind::TIMESTAMP_INSTANT, orc::TypeKind::TIMESTAMP}) {
            SCOPED_TRACE(zone + (kind == orc::TypeKind::TIMESTAMP_INSTANT ? " instant" : " timestamp"));
            OrcChunkReader chunk_reader(n, {});
            chunk_reader.disable_broker_load_mode();
            ASSERT_TRUE(chunk_reader.set_timezone(zone).ok());
            std::unique_ptr<orc::Type> orc_type = orc::createPrimitiveType(kind);
            TypeDescriptor type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME);
            auto reader = ORCColumnReader::create(type, orc_type.get(), true, nullptr, &chunk_reader).value();

            const size_t leading = 5;
            MutableColumnPtr column = ColumnHelper::create_column(type, true);
            column->append_nulls(leading);
            // two reads: [0, 1000) and [1000, n), each into column positions shifted by `leading`
            ASSERT_TRUE(reader->get_next(&batch, column.get(), 0, 1000).ok());
            ASSERT_TRUE(reader->get_next(&batch, column.get(), 1000, n - 1000).ok());
            ASSERT_EQ(leading + n, column->size());

            const cctz::time_zone& tz =
                    kind == orc::TypeKind::TIMESTAMP_INSTANT ? chunk_reader.tzinfo() : cctz::utc_time_zone();
            const bool use_ns = chunk_reader.use_nanoseconds_in_datetime();
            for (size_t i = 0; i < n; i++) {
                if (!batch.notNull[i]) {
                    ASSERT_EQ("NULL", column->debug_item(leading + i)) << i;
                    continue;
                }
                TimestampValue want;
                want.from_unixtime(batch.data[i], use_ns ? batch.nanoseconds[i] / 1000 : 0, tz);
                ASSERT_EQ(want.to_string(), column->debug_item(leading + i)) << "seconds=" << batch.data[i];
            }
        }
    }
}

} // namespace starrocks