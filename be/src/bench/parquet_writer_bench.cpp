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
#include <vector>

#include "bench.h"
#include "column/chunk.h"
#include "common/statusor.h"
#include "formats/parquet/parquet_file_writer.h"
#include "formats/parquet/parquet_test_util/util.h"
#include "fs/fs.h"
#include "fs/fs_posix.h"
#include "runtime/descriptor_helper.h"
#include "testutil/assert.h"
#include "util/time.h"

namespace starrocks {
namespace parquet {
namespace {

const int random_seed = 42;

inline std::shared_ptr<Chunk> make_chunk(int num_rows, int null_percent) {
    std::srand(random_seed);

    std::vector<int32_t> values(num_rows);
    std::vector<uint8_t> is_null(num_rows, 0);

    for (int i = 0; i < num_rows; i++) {
        if (std::rand() % 100 < null_percent) {
            is_null[i] = 1;
        } else {
            values[i] = std::rand();
        }
    }

    auto chunk = std::make_shared<Chunk>();
    auto data_column = Int32Column::create();
    data_column->append_numbers(values.data(), values.size() * sizeof(int32));
    auto null_column = UInt8Column::create();
    null_column->append_numbers(is_null.data(), is_null.size());
    auto col = NullableColumn::create(data_column, null_column);

    chunk->append_column(col, 0);
    return chunk;
}

inline std::shared_ptr<arrow::Table> make_arrow_table(int num_rows, int null_percent) {
    std::srand(random_seed);

    std::vector<int32_t> values(num_rows);
    std::vector<bool> is_valid(num_rows, 1);

    for (int i = 0; i < num_rows; i++) {
        if (std::rand() % 100 < null_percent) {
            is_valid[i] = 0;
        } else {
            values[i] = std::rand();
        }
    }

    arrow::Int32Builder i32builder;
    PARQUET_THROW_NOT_OK(i32builder.AppendValues(values, is_valid));
    std::shared_ptr<arrow::Array> i32array;
    PARQUET_THROW_NOT_OK(i32builder.Finish(&i32array));

    std::shared_ptr<arrow::Schema> schema = arrow::schema({arrow::field("int32", arrow::int32())});
    return arrow::Table::Make(schema, {i32array});
}

inline std::shared_ptr<::parquet::WriterProperties> make_property() {
    ::parquet::WriterProperties::Builder builder;
    builder.disable_dictionary();
    return builder.build();
}

inline std::vector<TypeDescriptor> make_type_descs() {
    return {TypeDescriptor::from_logical_type(TYPE_INT)};
}

inline std::shared_ptr<::parquet::schema::GroupNode> make_schema() {
    auto type_descs = make_type_descs();
    std::vector<std::string> type_names{"int32"};
    std::vector<FileColumnId> file_column_ids{FileColumnId{0}};
    auto ret = ParquetBuildHelper::make_schema(type_names, type_descs, file_column_ids);
    EXPECT_TRUE(ret.ok());
    auto schema = ret.ValueOrDie();
    return schema;
}

inline std::shared_ptr<arrow::Schema> make_arrow_schema() {
    std::shared_ptr<arrow::Schema> schema = arrow::schema({arrow::field("int32", arrow::int32())});
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
    std::vector<int> bm_null_percent = {0, 5, 25, 50, 75}; // percentage of null values
    for (auto& num_rows : bm_num_rows) {
        for (auto& null_percent : bm_null_percent) {
            b->Args({num_rows, null_percent});
        }
    }
}

static void Benchmark_StarRocksParquetWriter(benchmark::State& state) {
    auto fs = new_fs_posix();
    const std::string file_path = "./be/test/exec/test_data/parquet_scanner/starrocks_writer.parquet";
    fs->delete_file(file_path);

    auto num_rows = state.range(0);
    auto null_percent = state.range(1);
    auto chunk = make_chunk(num_rows, null_percent);

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
    }
}

[[maybe_unused]] static void Benchmark_ArrowParquetWriter(benchmark::State& state) {
    auto fs = new_fs_posix();
    const std::string file_path = "./be/test/exec/test_data/parquet_scanner/arrow_writer.parquet";
    fs->delete_file(file_path);

    auto num_rows = state.range(0);
    auto null_percent = state.range(1);
    std::shared_ptr<arrow::Table> table = make_arrow_table(num_rows, null_percent);

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
        ->MinTime(10);
BENCHMARK(Benchmark_ArrowParquetWriter)->Apply(Benchmark_ParquetWriterArgs)->Unit(benchmark::kMillisecond)->MinTime(10);

} // namespace
} // namespace parquet
} // namespace starrocks

BENCHMARK_MAIN();
