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

#include <random>
#include <string>
#include <vector>

#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "exprs/function_context.h"
#include "exprs/json_functions.h"
#include "types/logical_type.h"

namespace starrocks {

namespace {

// Build a JSON document of approximately `target_bytes`, with the `target` field
// nested `depth` levels deep. `position` controls whether the target field appears
// near the start (Early), middle (Middle), or end (Late) of each object.
enum class FieldPosition { Early, Middle, Late };

// Generate a noise field entry suitable for prepending or appending inside an object.
// Returns: `"noise_<id>":"lorem ipsum..."` (no separator).
std::string noise_field(int id) {
    return std::string("\"noise_") + std::to_string(id) +
           R"(":"lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor")";
}

std::string build_payload(int depth, size_t target_bytes, FieldPosition pos) {
    // Decide how many noise fields to insert per level, on the head side (before the target
    // descent) and on the tail side (after the target descent).
    const size_t per_level = target_bytes / static_cast<size_t>(depth + 1);
    const size_t one_field_size = noise_field(0).size() + 1; // +1 for comma separator
    size_t heads_n = 0;
    size_t tails_n = 0;
    switch (pos) {
    case FieldPosition::Early:
        // target near the start: noise lives AFTER (tails)
        tails_n = per_level / one_field_size;
        break;
    case FieldPosition::Late:
        // target near the end: noise lives BEFORE (heads)
        heads_n = per_level / one_field_size;
        break;
    case FieldPosition::Middle:
        // split noise evenly across both sides
        heads_n = per_level / (2 * one_field_size);
        tails_n = per_level / (2 * one_field_size);
        break;
    }

    int field_id = 0;
    std::string out = "{";
    for (int i = 0; i < depth; ++i) {
        for (size_t k = 0; k < heads_n; ++k) {
            out += noise_field(field_id++);
            out += ",";
        }
        out += "\"l" + std::to_string(i) + "\":{";
    }
    out += "\"target\":\"hello-world-target-string\"";
    for (int i = depth - 1; i >= 0; --i) {
        for (size_t k = 0; k < tails_n; ++k) {
            out += ",";
            out += noise_field(field_id++);
        }
        out += "}";
    }
    out += "}";
    return out;
}

std::string build_path(int depth) {
    std::string p = "$";
    for (int i = 0; i < depth; ++i) {
        p += ".l" + std::to_string(i);
    }
    p += ".target";
    return p;
}

class JsonExtractBench {
public:
    JsonExtractBench(int depth, size_t bytes, FieldPosition pos, int rows, bool fast_path)
            : _depth(depth), _bytes(bytes), _pos(pos), _rows(rows), _fast_path(fast_path) {}

    void setup() {
        std::string payload = build_payload(_depth, _bytes, _pos);
        std::string path = build_path(_depth);

        auto json_col = BinaryColumn::create();
        for (int r = 0; r < _rows; ++r) {
            json_col->append(payload);
        }
        auto path_col_data = BinaryColumn::create();
        path_col_data->append(path);

        _columns.clear();
        _columns.emplace_back(json_col);
        _columns.emplace_back(ConstColumn::create(path_col_data, _rows));

        _ctx.reset(FunctionContext::create_test_context());
        _ctx->set_constant_columns(_columns);

        CHECK(JsonFunctions::native_json_path_prepare(_ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                      .ok());
        CHECK(JsonFunctions::native_json_path_prepare(_ctx.get(), FunctionContext::FunctionStateScope::THREAD_LOCAL)
                      .ok());
        set_json_fast_path_disabled_for_test(_ctx.get(), !_fast_path);
    }

    void teardown() {
        CHECK(JsonFunctions::native_json_path_close(_ctx.get(), FunctionContext::FunctionStateScope::THREAD_LOCAL)
                      .ok());
        CHECK(JsonFunctions::native_json_path_close(_ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                      .ok());
    }

    void run_one() {
        auto result = JsonFunctions::get_json_string(_ctx.get(), _columns).value();
        benchmark::DoNotOptimize(result);
    }

private:
    int _depth;
    size_t _bytes;
    FieldPosition _pos;
    int _rows;
    bool _fast_path;
    Columns _columns;
    std::unique_ptr<FunctionContext> _ctx;
};

void run_bench(benchmark::State& state, bool fast_path) {
    const int depth = state.range(0);
    const size_t bytes = state.range(1);
    const int rows = state.range(2);
    const FieldPosition pos = static_cast<FieldPosition>(state.range(3));

    JsonExtractBench bench(depth, bytes, pos, rows, fast_path);
    bench.setup();

    for (auto _ : state) {
        bench.run_one();
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(rows));
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(rows) * static_cast<int64_t>(bytes));

    bench.teardown();
}

void BM_legacy(benchmark::State& s) {
    run_bench(s, /*fast=*/false);
}
void BM_fast(benchmark::State& s) {
    run_bench(s, /*fast=*/true);
}

// Args: {depth, payload_bytes, rows, position(0=Early,1=Middle,2=Late)}
BENCHMARK(BM_legacy)
        ->Args({4, 1024, 4096, 0})
        ->Args({4, 1024, 4096, 2})
        ->Args({4, 10240, 4096, 0})
        ->Args({4, 10240, 4096, 2})
        ->Args({8, 10240, 4096, 2})
        ->Args({4, 102400, 256, 2});

BENCHMARK(BM_fast)
        ->Args({4, 1024, 4096, 0})
        ->Args({4, 1024, 4096, 2})
        ->Args({4, 10240, 4096, 0})
        ->Args({4, 10240, 4096, 2})
        ->Args({8, 10240, 4096, 2})
        ->Args({4, 102400, 256, 2});

} // namespace

} // namespace starrocks

BENCHMARK_MAIN();
