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

#include <memory>
#include <random>

#include "formats/parquet/encoding_dict.h"
#include "formats/parquet/encoding_plain.h"

namespace starrocks {
namespace parquet {

static const int kDictSize = 20;
static const int kDictLength = 10;
static const int kTestChunkSize = 4096;
static std::string kAlphaNumber =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

static void BM_DictDecoder(benchmark::State& state) {
    DictDecoder<Slice> dict_decoder;
    static constexpr bool debug = false;
    {
        // create encoder and dict values.
        PlainEncoder<Slice> encoder;
        std::vector<Slice> dict_values;
        for (int i = 0; i < kDictSize; i++) {
            dict_values.emplace_back(Slice(kAlphaNumber.data() + i, kDictLength));
        }
        encoder.append((const uint8_t*)dict_values.data(), kDictSize);
        Slice data = encoder.build();

        // create decoder
        PlainDecoder<Slice> decoder;
        decoder.set_data(data);
        dict_decoder.set_dict(kTestChunkSize, kDictSize, &decoder);

        if (debug) {
            ColumnPtr column = ColumnHelper::create_column(TypeDescriptor{TYPE_VARCHAR}, true);
            dict_decoder.get_dict_values(column.get());
            std::cout << column->debug_string() << "\n";
        }
    }

    auto null_score = state.range(0);
    auto nulls_ptr =
            ColumnHelper::as_column<NullableColumn>(ColumnHelper::create_column(TypeDescriptor{TYPE_INT}, true));
    NullableColumn* nulls = nulls_ptr.get();
    nulls->resize(kTestChunkSize);
    uint8_t* null_data = nulls->mutable_null_column()->mutable_raw_data();

    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_int_distribution<int> dist(0, 99);
    std::vector<int32_t> dict_codes;
    int count = 0;

    for (int i = 0; i < kTestChunkSize; i++) {
        int random_number = dist(rng);
        null_data[i] = 0;
        if (random_number < null_score) {
            null_data[i] = 1;
            count += 1;
        }
        dict_codes.push_back(random_number % kDictSize);
    }
    nulls->update_has_null();
    if (debug) {
        std::cout << "nulls. has_null = " << nulls->has_null() << ", null rate = " << count << "/" << kTestChunkSize
                  << ".\n";
    }

    ColumnPtr column = ColumnHelper::create_column(TypeDescriptor{TYPE_VARCHAR}, true);
    for (auto _ : state) {
        state.PauseTiming();
        column->reset_column();
        state.ResumeTiming();
        Status st = dict_decoder.get_dict_values(dict_codes, *nulls, column.get());
        if (debug && !st.ok()) {
            std::cout << "Fail to call `get_dict_values`: " << st.get_error_msg() << "\n";
        }
    }
}

BENCHMARK(BM_DictDecoder)->DenseRange(0, 100, 10)->Unit(benchmark::kMillisecond);

} // namespace parquet
} // namespace starrocks

BENCHMARK_MAIN();