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

#include <memory>
#include <random>

#include "bench.h"
#include "column/chunk.h"
#include "common/statusor.h"
#include "exprs/runtime_filter.h"
#include "exprs/runtime_filter_bank.h"
#include "formats/parquet/file_writer.h"
#include "formats/parquet/parquet_test_util/util.h"
#include "fs/fs.h"
#include "fs/fs_posix.h"
#include "runtime/descriptor_helper.h"
#include "simd/simd.h"
#include "testutil/assert.h"
#include "util/time.h"

namespace starrocks {
namespace parquet {
namespace {

const int null_prop = 50;
const int max_array_len = 10;

inline std::shared_ptr<Chunk> make_chunk(int num_rows) {
    auto chunk = std::make_shared<Chunk>();

    auto type_array = TypeDescriptor::from_logical_type(TYPE_ARRAY);
    auto type_int = TypeDescriptor::from_logical_type(TYPE_INT);
    type_array.children.push_back(type_int);
    auto col = ColumnHelper::create_column(type_array, /*nullable*/ true);

    for (int i = 0; i < num_rows; i++) {
        Datum datum;
        if ((rand() % 100) < null_prop) {
            datum.set_null();
        } else {
            std::vector<Datum> datum_array;
            int len = (rand() % (max_array_len + 1)); // [0, max_array_len]
            for (int j = 0; j < len; j++) {
                Datum datum_element;
                datum_element.set_int32(rand());
                datum_array.push_back(datum);
            }
            datum.set_array(datum_array);
        }
        col->append_datum(datum);
    }

    chunk->append_column(col, 0);
    return chunk;
}

inline std::shared_ptr<arrow::Table> make_arrow_table(int num_rows) {
    std::vector<int32_t> values(num_rows);
    std::iota(values.begin(), values.end(), 0);
    arrow::Int32Builder i32builder;
    PARQUET_THROW_NOT_OK(i32builder.AppendValues(values));
    std::shared_ptr<arrow::Array> i32array;
    PARQUET_THROW_NOT_OK(i32builder.Finish(&i32array));

    std::shared_ptr<arrow::Schema> schema = arrow::schema({arrow::field("int", arrow::int32())});

    return arrow::Table::Make(schema, {i32array});
}

inline std::shared_ptr<::parquet::WriterProperties> make_property() {
    ::parquet::WriterProperties::Builder builder;
    builder.disable_dictionary();
    return builder.build();
}

inline std::vector<TypeDescriptor> make_type_descs() {
    auto type_array = TypeDescriptor::from_logical_type(TYPE_ARRAY);
    auto type_int = TypeDescriptor::from_logical_type(TYPE_INT);
    type_array.children.push_back(type_int);
    return {type_array};
}

inline std::shared_ptr<::parquet::schema::GroupNode> make_schema() {
    auto type_descs = make_type_descs();
    std::vector<std::string> type_names{"array"};
    auto ret = ParquetBuildHelper::make_schema(type_names, type_descs);
    EXPECT_TRUE(ret.ok());
    auto schema = ret.ValueOrDie();
    return schema;
}

inline std::shared_ptr<arrow::Schema> make_arrow_schema() {
    std::shared_ptr<arrow::Schema> schema = arrow::schema({arrow::field("int", arrow::int32())});
    return schema;
}

inline std::unique_ptr<SyncFileWriter> make_starrocks_writer(std::unique_ptr<WritableFile> file) {
    auto property = make_property();
    auto schema = make_schema();
    auto type_descs = make_type_descs();
    auto file_writer = std::make_unique<SyncFileWriter>(std::move(file), property, schema, type_descs);
    return file_writer;
}

inline std::unique_ptr<::parquet::arrow::FileWriter> make_arrow_writer(std::shared_ptr<ParquetOutputStream> sink) {
    auto schema = make_arrow_schema();
    auto property = make_property();
    std::unique_ptr<::parquet::arrow::FileWriter> writer;
    auto st = ::parquet::arrow::FileWriter::Open(*schema, ::arrow::default_memory_pool(), sink, property, &writer);
    if (!st.ok()) {
        return nullptr;
    }
    return writer;
}

static void Benchmark_ParquetWriterArgs(benchmark::internal::Benchmark* b) {
    std::vector<int64_t> bm_num_rows = {100000, 1000000, 10000000};
    for (auto& num_rows : bm_num_rows) {
        b->Args({num_rows});
    }
}

static void Benchmark_StarRocksParquetWriter(benchmark::State& state) {
    auto fs = new_fs_posix();
    const std::string file_path = "./be/test/exec/test_data/parquet_scanner/starrocks_writer.parquet";
    fs->delete_file(file_path);

    auto num_rows = state.range(0);
    auto chunk = make_chunk(num_rows);

    for (int i = 0; i < 10; i++) {
        ASSIGN_OR_ABORT(auto file, fs->new_writable_file(file_path));
        auto writer = make_starrocks_writer(std::move(file));
        writer->init();

        writer->write(chunk.get());
        auto st = writer->close();
        ASSERT_TRUE(st.ok());

        fs->delete_file(file_path);
    }

    for (auto _ : state) {
        state.PauseTiming();
        ASSIGN_OR_ABORT(auto file, fs->new_writable_file(file_path));
        auto writer = make_starrocks_writer(std::move(file));
        writer->init();

        state.ResumeTiming();
        writer->write(chunk.get());
        auto st = writer->close();
        ASSERT_TRUE(st.ok());
        state.PauseTiming();

        fs->delete_file(file_path);
    }

    // leave output for analysis
    {
        ASSIGN_OR_ABORT(auto file, fs->new_writable_file(file_path));
        auto writer = make_starrocks_writer(std::move(file));
        writer->init();

        writer->write(chunk.get());
        auto st = writer->close();
        ASSERT_TRUE(st.ok());

        fs->delete_file(file_path);
    }
}

[[maybe_unused]] static void Benchmark_ArrowParquetWriter(benchmark::State& state) {
    auto fs = new_fs_posix();
    const std::string file_path = "./be/test/exec/test_data/parquet_scanner/arrow_writer.parquet";
    fs->delete_file(file_path);

    auto num_rows = state.range(0);
    std::shared_ptr<arrow::Table> table = make_arrow_table(num_rows);

    for (int i = 0; i < 10; i++) {
        ASSIGN_OR_ABORT(auto file, fs->new_writable_file(file_path));
        auto outstream = std::make_shared<ParquetOutputStream>(std::move(file));
        auto writer = make_arrow_writer(outstream);

        PARQUET_THROW_NOT_OK(writer->WriteTable(*table, num_rows));

        auto st = outstream->Close();
        ASSERT_TRUE(st.ok());
        fs->delete_file(file_path);
    }

    for (auto _ : state) {
        state.PauseTiming();
        ASSIGN_OR_ABORT(auto file, fs->new_writable_file(file_path));
        auto outstream = std::make_shared<ParquetOutputStream>(std::move(file));
        auto writer = make_arrow_writer(outstream);

        state.ResumeTiming();
        PARQUET_THROW_NOT_OK(writer->WriteTable(*table, num_rows));
        auto st = outstream->Close();
        ASSERT_TRUE(st.ok());
        state.PauseTiming();

        fs->delete_file(file_path);
    }

    // leave output for analysis
    {
        ASSIGN_OR_ABORT(auto file, fs->new_writable_file(file_path));
        auto outstream = std::make_shared<ParquetOutputStream>(std::move(file));
        auto writer = make_arrow_writer(outstream);

        PARQUET_THROW_NOT_OK(writer->WriteTable(*table, num_rows));

        auto st = outstream->Close();
        ASSERT_TRUE(st.ok());
    }
}

BENCHMARK(Benchmark_StarRocksParquetWriter)
        ->Apply(Benchmark_ParquetWriterArgs)
        ->Unit(benchmark::kMillisecond)
        ->MinTime(30);
//BENCHMARK(Benchmark_ArrowParquetWriter)->Apply(Benchmark_ParquetWriterArgs)->Unit(benchmark::kMillisecond)->MinTime(30);

} // namespace
} // namespace parquet
} // namespace starrocks

BENCHMARK_MAIN();

//-----------------------------------------------------------------------------------------------------
//Benchmark                                                           Time             CPU   Iterations
//-----------------------------------------------------------------------------------------------------
//Benchmark_StarRocksParquetWriter/100000/min_time:30.000          16.9 ms         9.07 ms         4582
//Benchmark_StarRocksParquetWriter/1000000/min_time:30.000          118 ms         67.8 ms          619
//Benchmark_StarRocksParquetWriter/10000000/min_time:30.000        1030 ms          579 ms           74
//Benchmark_StarRocksParquetWriter/100000000/min_time:30.000      10828 ms         5568 ms            7
//Benchmark_ArrowParquetWriter/100000/min_time:30.000              14.8 ms         6.91 ms         5948
//Benchmark_ArrowParquetWriter/1000000/min_time:30.000             90.1 ms         40.3 ms          978
//Benchmark_ArrowParquetWriter/10000000/min_time:30.000             662 ms          185 ms          228
//Benchmark_ArrowParquetWriter/100000000/min_time:30.000           6956 ms         1619 ms           26
