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
#include "fs/fs_posix.h"
#include "fs/fs_memory.h"
#include "gutil/casts.h"
#include "runtime/descriptor_helper.h"
#include "testutil/assert.h"

namespace starrocks {
namespace parquet {

class ParquetWriterBench {
public:
    static std::unique_ptr<Chunk> Setup(int32_t num_rows, int32_t num_columns) {
        auto chunk = std::make_unique<Chunk>();
        for (size_t i = 0; i < num_columns; i++) {
            auto col = Bench::create_series_column(TypeDescriptor::from_logical_type(TYPE_INT), num_rows);
            chunk->append_column(col, i);
        }
        return chunk;
    }
};

static void BenchmarkParquetWriterArgs(benchmark::internal::Benchmark *b) {
    std::vector<int64_t> bm_num_rows = {100000, 1000000, 10000000};
    for (auto &num_rows: bm_num_rows) {
        b->Args({num_rows});
    }
}

static void Benchmark_ParquetWriter(benchmark::State &state) {
    auto fs = new_fs_posix();
    const std::string file_path = "./be/test/exec/test_data/parquet_scanner/bench.parquet";

    std::vector<LogicalType> integral_types{TYPE_INT};
    std::vector<TypeDescriptor> type_descs{TypeDescriptor::from_logical_type(TYPE_INT)};
    std::vector<std::string> type_names{"INT"};

    auto ret = ParquetBuildHelper::make_schema(type_names, type_descs);
    ASSERT_TRUE(ret.ok());
    auto schema = ret.ValueOrDie();
    auto properties = ParquetBuildHelper::make_properties(ParquetBuilderOptions());

    auto num_rows = state.range(0);
    auto chunk = ParquetWriterBench::Setup(num_rows, 1);

    for (int i = 0; i < 100; i++) {
        ASSIGN_OR_ABORT(auto file, fs->new_writable_file(file_path));
        auto file_writer = std::make_shared<SyncFileWriter>(std::move(file), properties, schema, type_descs);
        file_writer->init();

        file_writer->write(chunk.get());
        auto st = file_writer->close();

        ASSERT_TRUE(st.ok());
        fs->delete_file(file_path);
    }

    for (auto _: state) {
        state.PauseTiming();
        ASSIGN_OR_ABORT(auto file, fs->new_writable_file(file_path));
        auto file_writer = std::make_shared<SyncFileWriter>(std::move(file), properties, schema, type_descs);
        file_writer->init();

        state.ResumeTiming();
        file_writer->write(chunk.get());
        auto st = file_writer->close();
        state.PauseTiming();

        ASSERT_TRUE(st.ok());
        fs->delete_file(file_path);
    }
}

BENCHMARK(Benchmark_ParquetWriter)->Apply(BenchmarkParquetWriterArgs)->Iterations(100);

} // namespace parquet
} // namespace starrocks

BENCHMARK_MAIN();
