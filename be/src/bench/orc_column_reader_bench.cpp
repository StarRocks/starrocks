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
#include <glog/logging.h>

#include "formats/orc/column_reader.h"
#include "formats/orc/memory_stream/MemoryInputStream.hh"
#include "formats/orc/memory_stream/MemoryOutputStream.hh"
#include "formats/orc/orc_chunk_reader.h"
#include "formats/orc/orc_mapping.h"

namespace starrocks {

// Run command: ./be/build_Release/src/bench/output/orc_column_reader_bench 2>>/dev/null

const static size_t benchmarkIterationTimes = 5;

// 2G buffer
const static size_t bufferSize = 2L * 1024 * 1024 * 1024;
const static size_t batchSize = 1024;
const static size_t batchNum = 60000;
const static size_t columnSize = 4096;

const static size_t decimal64Precision = 10;
const static size_t decimal64Scale = 2;
const static size_t decimal128Precision = 20;
const static size_t decimal128Scale = 10;

template <LogicalType logicType>
struct OrcColumnVectorBatch {};

template <>
struct OrcColumnVectorBatch<TYPE_BOOLEAN> {
    using BatchType = orc::LongVectorBatch;
};

template <>
struct OrcColumnVectorBatch<TYPE_VARCHAR> {
    using BatchType = orc::StringVectorBatch;
};

template <>
struct OrcColumnVectorBatch<TYPE_CHAR> {
    using BatchType = orc::StringVectorBatch;
};

template <>
struct OrcColumnVectorBatch<TYPE_VARBINARY> {
    using BatchType = orc::StringVectorBatch;
};

template <>
struct OrcColumnVectorBatch<TYPE_INT> {
    using BatchType = orc::LongVectorBatch;
};

template <>
struct OrcColumnVectorBatch<TYPE_DOUBLE> {
    using BatchType = orc::DoubleVectorBatch;
};

template <>
struct OrcColumnVectorBatch<TYPE_DECIMAL64> {
    using BatchType = orc::Decimal64VectorBatch;
};

template <>
struct OrcColumnVectorBatch<TYPE_DECIMAL128> {
    using BatchType = orc::Decimal128VectorBatch;
};

template <>
struct OrcColumnVectorBatch<TYPE_DATE> {
    using BatchType = orc::LongVectorBatch;
};

template <>
struct OrcColumnVectorBatch<TYPE_DATETIME> {
    using BatchType = orc::TimestampVectorBatch;
};

template <LogicalType Type>
using RunTimeOrcColumnVectorBatch = typename OrcColumnVectorBatch<Type>::BatchType;

static inline std::string getOrcSchemaString(const LogicalType& logicalType) {
    switch (logicalType) {
    case TYPE_BOOLEAN:
        return "struct<c0:boolean>";
    case TYPE_VARCHAR:
        return "struct<c0:string>";
    case TYPE_CHAR:
        return "struct<c0:char(30)>";
    case TYPE_VARBINARY:
        return "struct<c0:binary>";
    case TYPE_INT:
        return "struct<c0:int>";
    case TYPE_DOUBLE:
        return "struct<c0:double>";
    case TYPE_DECIMAL64:
        return strings::Substitute("struct<c0:decimal($0,$1)>", decimal64Precision, decimal64Scale);
    case TYPE_DECIMAL128:
        return strings::Substitute("struct<c0:decimal($0,$1)>", decimal128Precision, decimal128Scale);
    case TYPE_DATE:
        return "struct<c0:date>";
    case TYPE_DATETIME:
        return "struct<c0:timestamp with local time zone>";
    default:
        return "";
    }
}

template <bool nullable>
static inline void handleNull(orc::ColumnVectorBatch* vb, size_t pos, size_t kBatchNum) {
    if (nullable && kBatchNum % 3 == 0) {
        vb->notNull[pos] = 0;
        vb->hasNulls = true;
    }
}

template <bool nullable>
static inline void insertBoolean(orc::LongVectorBatch* vb, size_t pos, size_t kBatchNum) {
    vb->data[pos] = ((pos % 2) == 0);
    handleNull<nullable>(vb, pos, kBatchNum);
}

template <bool nullable>
static inline void insertString(orc::StringVectorBatch* vb, size_t pos, size_t kBatchNum, ObjectPool& pool) {
    std::string* data;
    if (kBatchNum % 2 == 0) {
        data = pool.add(new std::string(strings::Substitute("Hello, ORC! %ld", pos)));
    } else {
        data = pool.add(new std::string("Hello, ORC!"));
    }
    vb->data[pos] = data->data();
    vb->length[pos] = data->length();
    handleNull<nullable>(vb, pos, kBatchNum);
}

template <bool nullable>
static inline void insertInt(orc::LongVectorBatch* vb, size_t pos, size_t kBatchNum) {
    vb->data[pos] = pos;
    handleNull<nullable>(vb, pos, kBatchNum);
}

template <bool nullable>
static inline void insertDouble(orc::DoubleVectorBatch* vb, size_t pos, size_t kBatchNum) {
    vb->data[pos] = pos;
    handleNull<nullable>(vb, pos, kBatchNum);
}

template <bool nullable>
static inline void insertDecimal64(orc::Decimal64VectorBatch* vb, size_t pos, size_t kBatchNum) {
    vb->values[pos] = pos * 10;
    handleNull<nullable>(vb, pos, kBatchNum);
}

template <bool nullable>
static inline void insertDecimal128(orc::Decimal128VectorBatch* vb, size_t pos, size_t kBatchNum) {
    vb->values[pos] = orc::Int128(100, pos);
    handleNull<nullable>(vb, pos, kBatchNum);
}

template <bool nullable>
static inline void insertTimestamp(orc::TimestampVectorBatch* vb, size_t pos, size_t kBatchNum) {
    vb->data[pos] = pos;
    vb->nanoseconds[pos] = pos * 10;
    handleNull<nullable>(vb, pos, kBatchNum);
}

template <bool isNullable, LogicalType logicalType>
static void BM_primitive(benchmark::State& state) {
    MemoryOutputStream buffer(bufferSize);
    ORC_UNIQUE_PTR<orc::Type> schema(orc::Type::buildTypeFromString(getOrcSchemaString(logicalType)));
    const orc::Type* orcType = schema->getSubtype(0);

    // prepare data.
    {
        orc::WriterOptions writerOptions;
        ORC_UNIQUE_PTR<orc::Writer> writer = createWriter(*schema, &buffer, writerOptions);

        ORC_UNIQUE_PTR<orc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
        auto* root = dynamic_cast<orc::StructVectorBatch*>(batch.get());
        auto* c0 = dynamic_cast<RunTimeOrcColumnVectorBatch<logicalType>*>(root->fields[0]);

        ObjectPool pool{};
        for (size_t k = 0; k < batchNum; k++) {
            for (size_t i = 0; i < batchSize; i++) {
                if constexpr (logicalType == TYPE_BOOLEAN) {
                    insertBoolean<isNullable>(c0, i, k);
                } else if constexpr (logicalType == TYPE_VARCHAR || logicalType == TYPE_CHAR ||
                                     logicalType == TYPE_VARBINARY) {
                    insertString<isNullable>(c0, i, k, pool);
                } else if constexpr (logicalType == TYPE_INT || logicalType == TYPE_DATE) {
                    insertInt<isNullable>(c0, i, k);
                } else if constexpr (logicalType == TYPE_DOUBLE) {
                    insertDouble<isNullable>(c0, i, k);
                } else if constexpr (logicalType == TYPE_DECIMAL64) {
                    insertDecimal64<isNullable>(c0, i, k);
                } else if constexpr (logicalType == TYPE_DECIMAL128) {
                    insertDecimal128<isNullable>(c0, i, k);
                } else if constexpr (logicalType == TYPE_DATETIME) {
                    insertTimestamp<isNullable>(c0, i, k);
                } else {
                    LOG(ERROR) << "No matching fill function";
                    return;
                }
            }
            c0->numElements = batchSize;
            root->numElements = batchSize;
            writer->add(*batch);
        }
        writer->close();
    }

    // Start to read data
    TypeDescriptor c0Type;
    if (logicalType == TYPE_DECIMAL64) {
        c0Type = TypeDescriptor::from_logical_type(TYPE_DECIMAL64, TypeDescriptor::MAX_VARCHAR_LENGTH,
                                                   decimal64Precision, decimal64Scale);
    } else if (logicalType == TYPE_DECIMAL128) {
        c0Type = TypeDescriptor::from_logical_type(TYPE_DECIMAL128, TypeDescriptor::MAX_VARCHAR_LENGTH,
                                                   decimal128Precision, decimal128Scale);
    } else {
        c0Type = TypeDescriptor::from_logical_type(logicalType);
    }

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

    std::unique_ptr<ORCColumnReader> orcColumnReader =
            ORCColumnReader::create(c0Type, orcType, isNullable, orcMapping, &orcChunkReader).value();

    for (auto _ : state) {
        ORC_UNIQUE_PTR<orc::ColumnVectorBatch> batch = rr->createRowBatch(columnSize);
        auto* root = dynamic_cast<orc::StructVectorBatch*>(batch.get());
        auto* c0 = dynamic_cast<RunTimeOrcColumnVectorBatch<logicalType>*>(root->fields[0]);
        orc::RowReader::ReadPosition pos;
        size_t totalNumRows = 0;
        while (rr->next(*batch, &pos)) {
            ColumnPtr column = ColumnHelper::create_column(c0Type, isNullable);
            CHECK(orcColumnReader->get_next(c0, column, 0, columnSize).ok());
            DCHECK_EQ(columnSize, column->size());
            totalNumRows += columnSize;
        }
        DCHECK_EQ(batchSize * batchNum, totalNumRows);
    }
}

#define NULLABLE true
#define NON_NULLABLE false

// Boolean
BENCHMARK_TEMPLATE(BM_primitive, NULLABLE, LogicalType::TYPE_BOOLEAN)
        ->Unit(benchmark::kMillisecond)
        ->Iterations(benchmarkIterationTimes);
BENCHMARK_TEMPLATE(BM_primitive, NON_NULLABLE, LogicalType::TYPE_BOOLEAN)
        ->Unit(benchmark::kMillisecond)
        ->Iterations(benchmarkIterationTimes);

// String
BENCHMARK_TEMPLATE(BM_primitive, NULLABLE, LogicalType::TYPE_VARCHAR)
        ->Unit(benchmark::kMillisecond)
        ->Iterations(benchmarkIterationTimes);
BENCHMARK_TEMPLATE(BM_primitive, NON_NULLABLE, LogicalType::TYPE_VARCHAR)
        ->Unit(benchmark::kMillisecond)
        ->Iterations(benchmarkIterationTimes);

// Char
BENCHMARK_TEMPLATE(BM_primitive, NULLABLE, LogicalType::TYPE_CHAR)
        ->Unit(benchmark::kMillisecond)
        ->Iterations(benchmarkIterationTimes);
BENCHMARK_TEMPLATE(BM_primitive, NON_NULLABLE, LogicalType::TYPE_CHAR)
        ->Unit(benchmark::kMillisecond)
        ->Iterations(benchmarkIterationTimes);

// TinyInt / SmallInt / Int / BigInt / LargeInt
BENCHMARK_TEMPLATE(BM_primitive, NULLABLE, LogicalType::TYPE_INT)
        ->Unit(benchmark::kMillisecond)
        ->Iterations(benchmarkIterationTimes);
BENCHMARK_TEMPLATE(BM_primitive, NON_NULLABLE, LogicalType::TYPE_INT)
        ->Unit(benchmark::kMillisecond)
        ->Iterations(benchmarkIterationTimes);

// Double / Float
BENCHMARK_TEMPLATE(BM_primitive, NULLABLE, LogicalType::TYPE_DOUBLE)
        ->Unit(benchmark::kMillisecond)
        ->Iterations(benchmarkIterationTimes);
BENCHMARK_TEMPLATE(BM_primitive, NON_NULLABLE, LogicalType::TYPE_DOUBLE)
        ->Unit(benchmark::kMillisecond)
        ->Iterations(benchmarkIterationTimes);

// Decimal32 / Decimal64, test with Decimal64VectorBatch
BENCHMARK_TEMPLATE(BM_primitive, NULLABLE, LogicalType::TYPE_DECIMAL64)
        ->Unit(benchmark::kMillisecond)
        ->Iterations(benchmarkIterationTimes);
BENCHMARK_TEMPLATE(BM_primitive, NON_NULLABLE, LogicalType::TYPE_DECIMAL64)
        ->Unit(benchmark::kMillisecond)
        ->Iterations(benchmarkIterationTimes);

// Decimal128, test with Decimal128VectorBatch
BENCHMARK_TEMPLATE(BM_primitive, NULLABLE, LogicalType::TYPE_DECIMAL128)
        ->Unit(benchmark::kMillisecond)
        ->Iterations(benchmarkIterationTimes);
BENCHMARK_TEMPLATE(BM_primitive, NON_NULLABLE, LogicalType::TYPE_DECIMAL128)
        ->Unit(benchmark::kMillisecond)
        ->Iterations(benchmarkIterationTimes);

// Varbinary, test with StringVectorBatch
BENCHMARK_TEMPLATE(BM_primitive, NULLABLE, LogicalType::TYPE_VARBINARY)
        ->Unit(benchmark::kMillisecond)
        ->Iterations(benchmarkIterationTimes);
BENCHMARK_TEMPLATE(BM_primitive, NON_NULLABLE, LogicalType::TYPE_VARBINARY)
        ->Unit(benchmark::kMillisecond)
        ->Iterations(benchmarkIterationTimes);

//  Date, test with LongVectorBatch
BENCHMARK_TEMPLATE(BM_primitive, NULLABLE, LogicalType::TYPE_DATE)
        ->Unit(benchmark::kMillisecond)
        ->Iterations(benchmarkIterationTimes);
BENCHMARK_TEMPLATE(BM_primitive, NON_NULLABLE, LogicalType::TYPE_DATE)
        ->Unit(benchmark::kMillisecond)
        ->Iterations(benchmarkIterationTimes);

//  DateTime, test with TimestampVectorBatch
BENCHMARK_TEMPLATE(BM_primitive, NULLABLE, LogicalType::TYPE_DATETIME)
        ->Unit(benchmark::kMillisecond)
        ->Iterations(benchmarkIterationTimes);
BENCHMARK_TEMPLATE(BM_primitive, NON_NULLABLE, LogicalType::TYPE_DATETIME)
        ->Unit(benchmark::kMillisecond)
        ->Iterations(benchmarkIterationTimes);

} // namespace starrocks

BENCHMARK_MAIN();
