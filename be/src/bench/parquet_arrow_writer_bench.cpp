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
#include <gtest/gtest.h>

#include <random>

#include "bench.h"
#include "column/column_helper.h"
#include "exprs/runtime_filter.h"
#include "exprs/runtime_filter_bank.h"
#include "simd/simd.h"
#include "util/time.h"
#include "formats/parquet/file_writer.h"
#include "types/logical_type.h"
#include "formats/parquet/file_writer.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <memory>

#include "column/chunk.h"
#include "common/statusor.h"
#include "formats/parquet/file_reader.h"
#include "formats/parquet/parquet_test_util/util.h"
#include "fs/fs.h"
#include "fs/fs_memory.h"
#include "gutil/casts.h"
#include "runtime/descriptor_helper.h"
#include "testutil/assert.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include "fs/fs_posix.h"

#include <iostream>



namespace starrocks {
namespace parquet {

//class ParquetArrowWriterBench {
//public:
//    static std::unique_ptr<Chunk> Setup(int32_t num_rows, int32_t num_columns) {
//        auto chunk = std::make_unique<Chunk>();
//        for (size_t i = 0; i < num_columns; i++) {
//            auto col = Bench::create_series_column(TypeDescriptor::from_logical_type(TYPE_INT), num_rows);
//            chunk->append_column(col, i);
//        }
//        return chunk;
//    }
//};

// #0 Build dummy data to pass around
// To have some input data, we first create an Arrow Table that holds
// some data.
std::shared_ptr<arrow::Table> generate_table(int num_rows) {
    std::vector<int64_t> values(num_rows);
    std::iota(values.begin(), values.end(), 0);
    arrow::Int64Builder i64builder;
    PARQUET_THROW_NOT_OK(i64builder.AppendValues(values));
    std::shared_ptr<arrow::Array> i64array;
    PARQUET_THROW_NOT_OK(i64builder.Finish(&i64array));

//    arrow::StringBuilder strbuilder;
//    PARQUET_THROW_NOT_OK(strbuilder.Append("some"));
//    PARQUET_THROW_NOT_OK(strbuilder.Append("string"));
//    PARQUET_THROW_NOT_OK(strbuilder.Append("content"));
//    PARQUET_THROW_NOT_OK(strbuilder.Append("in"));
//    PARQUET_THROW_NOT_OK(strbuilder.Append("rows"));
//    std::shared_ptr<arrow::Array> strarray;
//    PARQUET_THROW_NOT_OK(strbuilder.Finish(&strarray));

    std::shared_ptr<arrow::Schema> schema = arrow::schema(
            {arrow::field("int", arrow::int64())});

    return arrow::Table::Make(schema, {i64array});
}

// #1 Write out the data as a Parquet file
void write_parquet_file(const arrow::Table& table) {
    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    PARQUET_ASSIGN_OR_THROW(
            outfile, arrow::io::FileOutputStream::Open("parquet-arrow-example.parquet"));
    // The last argument to the function call is the size of the RowGroup in
    // the parquet file. Normally you would choose this to be rather large but
    // for the example, we use a small value to have multiple RowGroups.
    PARQUET_THROW_NOT_OK(
            ::parquet::arrow::WriteTable(table, arrow::default_memory_pool(), outfile, 3));
}

static void BenchmarkParquetArrowWriterArgs(benchmark::internal::Benchmark *b) {
    std::vector<int64_t> bm_num_rows = {100000, 1000000, 10000000};
    for (auto &num_rows: bm_num_rows) {
        b->Args({num_rows});
    }
}

static void Benchmark_ParquetArrowWriter(benchmark::State &state) {
    auto fs = new_fs_posix();
    const std::string file_path = "./be/test/exec/test_data/parquet_scanner/bench.parquet";

    auto num_rows = state.range(0);
    std::shared_ptr<arrow::Table> table = generate_table(num_rows);

    auto chunk_size = 128 * 1024 * 1024;

    for (int i = 0; i < 100; i++) {
        ASSIGN_OR_ABORT(auto file, fs->new_writable_file(file_path));
        auto outstream = std::make_shared<ParquetOutputStream>(std::move(file));

        PARQUET_THROW_NOT_OK(
                ::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outstream, chunk_size));
        auto st = outstream->Close();
        ASSERT_TRUE(st.ok());
        fs->delete_file(file_path);
    }

    for (auto _: state) {
        state.PauseTiming();
        ASSIGN_OR_ABORT(auto file, fs->new_writable_file(file_path));
        auto outstream = std::make_shared<ParquetOutputStream>(std::move(file));

        state.ResumeTiming();
        PARQUET_THROW_NOT_OK(
                ::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outstream, chunk_size));
        auto st = outstream->Close();
        ASSERT_TRUE(st.ok());

        state.PauseTiming();
        fs->delete_file(file_path);
    }
}

BENCHMARK(Benchmark_ParquetArrowWriter)->Apply(BenchmarkParquetArrowWriterArgs)->Iterations(100);

} // namespace parquet
} // namespace starrocks

BENCHMARK_MAIN();
