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
#include <gtest/gtest.h>

#include "fs/fs_memory.h"
#include "storage/rowset/bitmap_index_writer.h"
#include "storage/types.h"
#include "types/logical_type.h"
#include "testutil/assert.h"

namespace starrocks {
    

static void BM_bitmap_index_write(benchmark::State& state) {
    const std::string dir_name = "bitmap_bench";
    const std::string file_name = "bitmap_bench_file";
    auto fs = std::make_shared<MemoryFileSystem>();
    fs->create_dir(dir_name);
    
    ColumnIndexMetaPB meta;
    TypeInfoPtr type_info = get_type_info(TYPE_VARCHAR);
    
    std::vector<int> values;
    for (int i = 0; i < 4096; i++) {
        values.push_back(i);
    }
    for (auto _ : state) {
        ASSIGN_OR_ABORT(auto file,  fs->new_writable_file(file_name));
        std::unique_ptr<BitmapIndexWriter> writer;
        BitmapIndexWriter::create(type_info, &writer);
        for (auto _ : state) {
            writer->add_values(values.data(), values.size());
        }

        writer->finish(file.get(), &meta);
        file->close();
        fs->delete_file(file_name);
    }
    
}

BENCHMARK(BM_bitmap_index_write);

    
} // namespace starrocks
