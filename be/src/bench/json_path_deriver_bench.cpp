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

#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "column/json_column.h"
#include "gutil/casts.h"
#include "util/json.h"
#include "util/json_flattener.h"

namespace starrocks {

class JsonPathDeriverBench {
public:
    JsonPathDeriverBench() = default;

    void SetUp(const std::string& filepath) {
        auto json_column = JsonColumn::create();

        std::ifstream file(filepath);
        if (!file.is_open()) {
            std::cerr << "Could not open file: " << filepath << ", using empty column" << std::endl;
            return;
        }

        std::string line;
        while (std::getline(file, line)) {
            if (line.empty()) continue;
            auto json_value = JsonValue::parse(line);
            if (json_value.ok()) {
                json_column->append(std::move(json_value.value()));
            }
        }
        _json_column = std::move(json_column);
        std::cout << "Loaded " << _json_column->size() << " rows from " << filepath << std::endl;
    }

    const JsonColumn* get_column() const { return down_cast<JsonColumn*>(_json_column.get()); }

private:
    MutableColumnPtr _json_column;
};

static void BM_JsonPathDeriver(benchmark::State& state) {
    int num_fields = state.range(0);
    std::string path;
    if (num_fields == 0) {
        // read from env
        const char* bench_file = std::getenv("BENCH_FILE");
        path = bench_file ? bench_file : "";
        if (path.empty()) {
            state.SkipWithError("empty BENCH_FILE");
            return;
        }
    } else {
        path = fmt::format("test_{}.json", num_fields);
    }

    JsonPathDeriverBench bench;
    bench.SetUp(path);

    const JsonColumn* json_col = bench.get_column();
    if (json_col->size() == 0) {
        state.SkipWithError("No data to benchmark");
        return;
    }
    std::vector<const Column*> columns{json_col};

    for (auto _ : state) {
        JsonPathDeriver deriver;
        deriver.derived(columns);
    }
}

// BENCHMARK(BM_JsonPathDeriver)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_JsonPathDeriver)->Unit(benchmark::kMillisecond)->Arg(10)->Arg(20)->Arg(40)->Arg(80);

} // namespace starrocks

BENCHMARK_MAIN();
