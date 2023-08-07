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

#include <benchmark/benchmark.h>

#include "formats/orc/memory_stream/MemoryInputStream.hh"
#include "formats/orc/memory_stream/MemoryOutputStream.hh"
#include "formats/orc/orc_chunk_reader.h"
#include <glog/logging.h>
#include "formats/orc/column_reader.h"
#include "formats/orc/orc_mapping.h"

namespace starrocks {

const static size_t benchmarkIterationTimes = 500;

// 2G buffer
const static size_t bufferSize = 2L * 1024 * 1024 * 1024;
const static size_t batchSize = 4096;
const static size_t batchNum = 15000;
const static size_t SRChunkSize = 4096;

template<bool IsUsingOld>
static void BM_boolean_nullable(benchmark::State& state) {
    MemoryOutputStream buffer(bufferSize);
    const orc::Type* orcType = nullptr;

    // prepare data.
    {
        orc::WriterOptions writerOptions;
        ORC_UNIQUE_PTR<orc::Type> schema(orc::Type::buildTypeFromString("struct<c0:boolean>"));
        orcType = schema->getSubtype(0);
        ORC_UNIQUE_PTR<orc::Writer> writer = createWriter(*schema, &buffer, writerOptions);

        ORC_UNIQUE_PTR<orc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
        auto* root = dynamic_cast<orc::StructVectorBatch*>(batch.get());
        auto* c0 = dynamic_cast<orc::LongVectorBatch*>(root->fields[0]);

        for (size_t k = 0; k < batchNum; k++) {
            for (size_t i = 0; i < batchSize; i++) {
                c0->data[i] = ((k % 2) == 0);
            }
            c0->numElements = batchSize;
            root->numElements = batchSize;
            writer->add(*batch);
        }
        writer->close();
    }

    // read data.
    {
        TypeDescriptor c0Type = TypeDescriptor::from_logical_type(LogicalType::TYPE_BOOLEAN);

        orc::ReaderOptions readerOptions;
        ORC_UNIQUE_PTR<orc::InputStream> inputStream(new MemoryInputStream(buffer.getData(), buffer.getLength()));
        ORC_UNIQUE_PTR<orc::Reader> reader = createReader(std::move(inputStream), readerOptions);

        orc::RowReaderOptions options;
        std::list<std::string> columns = {"c0"};
        options.include(columns);
        ORC_UNIQUE_PTR<orc::RowReader> rr = reader->createRowReader(options);

        const OrcMappingPtr orcMapping = nullptr;
        OrcChunkReader orcChunkReader(SRChunkSize, {});
        orcChunkReader.set_broker_load_mode(false);
        std::unique_ptr<ORCColumnReader> orcColumnReader = ORCColumnReader::create(TypeDescriptor::from_logical_type(LogicalType::TYPE_BOOLEAN), orcType, true, orcMapping, &orcChunkReader).value();

        for (auto _ : state) {
            for (size_t k = 0; k < batchNum; k++) {
                ColumnPtr column = ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_BOOLEAN), true);
                ORC_UNIQUE_PTR<orc::ColumnVectorBatch> batch = rr->createRowBatch(batchSize);
                auto* root = dynamic_cast<orc::StructVectorBatch*>(batch.get());
                auto* c0 = dynamic_cast<orc::LongVectorBatch*>(root->fields[0]);

                orc::RowReader::ReadPosition pos;
                bool res = rr->next(*batch, &pos);
                DCHECK_EQ(true, res);

                if constexpr (!IsUsingOld) {
                    orcColumnReader->get_next(c0, column, 0, batchSize);
                } else {
                    orcColumnReader->old_get_next(c0, column, 0, batchSize);
                }
                DCHECK_EQ(SRChunkSize, column->size());
            }
        }
    }
}

BENCHMARK_TEMPLATE(BM_boolean_nullable, false)->Unit(benchmark::kMillisecond)->Iterations(benchmarkIterationTimes);
BENCHMARK_TEMPLATE(BM_boolean_nullable, true)->Unit(benchmark::kMillisecond)->Iterations(benchmarkIterationTimes);


} // namespace starrocks

BENCHMARK_MAIN();