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
#include <fmt/core.h>

#include <limits>
#include <memory>

#include "bench/bench_util.h"
#include "formats/parquet/encoding.h"
#include "util/compression/block_compression.h"

namespace starrocks::parquet {

inline int kTestChunkSize = 4096;

static void BMRandomInt64(benchmark::State& state) {
    auto f = [&]() {
        tparquet::Type::type PT = tparquet::Type::INT64;
        using T = int64_t;
        tparquet::Encoding::type encoding_type = (tparquet::Encoding::type)state.range(0);
        const EncodingInfo* encoding_info;
        RETURN_IF_ERROR(EncodingInfo::get(PT, encoding_type, &encoding_info));

        int64_t num_rows = state.range(1);
        int64_t min_value = state.range(2);
        int64_t range_value = state.range(3);
        std::vector<T> elements = BenchUtil::create_random_values<T>(num_rows, min_value, min_value + range_value);

        std::unique_ptr<Encoder> encoder;
        RETURN_IF_ERROR(encoding_info->create_encoder(&encoder));
        RETURN_IF_ERROR(encoder->append((const uint8_t*)elements.data(), num_rows));
        Slice encoded_data = encoder->build();

        {
            // see compressed size using zstd
            std::vector<char> compressed_buffer(encoded_data.size * 2 + 20);
            const BlockCompressionCodec* codec = nullptr;
            RETURN_IF_ERROR(get_block_compression_codec(CompressionTypePB::ZSTD, &codec));
            Slice compressed_slice(compressed_buffer.data(), compressed_buffer.size());
            RETURN_IF_ERROR(codec->compress(encoded_data, &compressed_slice));

            std::string label = fmt::format("enc={},rows={},sz={},cpsz={}", to_string(encoding_type), num_rows,
                                            encoded_data.size, compressed_slice.size);
            state.SetLabel(label);
        }

        std::unique_ptr<Decoder> decoder;
        std::vector<T> output(num_rows);
        RETURN_IF_ERROR(encoding_info->create_decoder(&decoder));
        for (auto _ : state) {
            RETURN_IF_ERROR(decoder->set_data(encoded_data));
            RETURN_IF_ERROR(decoder->next_batch(num_rows, (uint8_t*)output.data()));
        }
        return Status::OK();
    };
    Status st = f();
    if (!st.ok()) {
        state.SkipWithError(st.to_string().c_str());
    }
}

static void CustomArgsInt64(benchmark::internal::Benchmark* b) {
    std::vector<int64_t> encodings = {(int64_t)tparquet::Encoding::PLAIN,
                                      (int64_t)tparquet::Encoding::DELTA_BINARY_PACKED,
                                      (int64_t)tparquet::Encoding::BYTE_STREAM_SPLIT};
    std::vector<int64_t> min_values = {std::numeric_limits<int64_t>::min() + 1000000, 0,
                                       std::numeric_limits<int64_t>::max() - 1000000};
    std::vector<int64_t> ranges = {100, 10000, 1000000};

    for (auto min_value : min_values) {
        for (auto range : ranges) {
            for (auto encoding : encodings) {
                b->Args({encoding, kTestChunkSize, min_value, range});
            }
        }
    }
}

BENCHMARK(BMRandomInt64)->Apply(CustomArgsInt64);

} // namespace starrocks::parquet

BENCHMARK_MAIN();
