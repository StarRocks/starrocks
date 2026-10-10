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

#include "base/compression/block_compression.h"
#include "base/testutil/assert.h"
#include "bench/bench_util.h"
#include "column/fixed_length_column.h"
#include "formats/parquet/encoding.h"
#include "formats/parquet/types.h"

namespace starrocks::parquet {

inline int kTestChunkSize = 4096;

enum TestMode {
    DECOMPRESS = -2,
    SKIP = -1,
    RANDOM = 0,
    SERIES = 1,
    REPEAT = 2,
    PREFIX = 3,
    LOWCARD = 4,
};

std::string to_string(TestMode mode) {
    switch (mode) {
    case RANDOM:
        return "RANDOM";
    case SERIES:
        return "SERIES";
    case PREFIX:
        return "PREFIX";
    case SKIP:
        return "SKIP";
    case DECOMPRESS:
        return "DECOMPRESS";
    case REPEAT:
        return "REPEAT";
    case LOWCARD:
        return "LOWCARD";
    default:
        return "UNKNOWN";
    }
}

// I just find the Status::is_moved_from and Status::moved_from_state take too much time.
// so after I'm sure that there is no mistake in the code, I just ignore the status check.
#undef RETURN_IF_ERROR
#define RETURN_IF_ERROR(stmt) (void)(stmt)

template <tparquet::Type::type PT, TestMode test_mode>
static void BMTestValue(benchmark::State& state) {
    auto f = [&]() {
        using T = typename PhysicalTypeTraits<PT>::CppType;
        tparquet::Encoding::type encoding_type = (tparquet::Encoding::type)state.range(0);
        const EncodingInfo* encoding_info;
        RETURN_IF_ERROR(EncodingInfo::get(PT, encoding_type, &encoding_info));

        int64_t num_rows = state.range(1);
        std::vector<T> elements;
        if constexpr (test_mode == RANDOM) {
            int64_t min_value = state.range(2);
            int64_t max_value = state.range(3);
            elements = BenchUtil::create_random_values<T>(num_rows, min_value, max_value);
        } else if constexpr (test_mode == SERIES) {
            int64_t init_value = state.range(2);
            int64_t delta_value = state.range(3);
            elements = BenchUtil::create_series_values<T>(num_rows, init_value, delta_value);
        } else if constexpr (test_mode == SKIP || test_mode == DECOMPRESS) {
            elements = BenchUtil::create_random_values<T>(num_rows, std::numeric_limits<T>::lowest(),
                                                          std::numeric_limits<T>::max());
        } else if constexpr (test_mode == REPEAT) {
            elements = BenchUtil::create_series_values<T>(num_rows, 100, 0);
        }

        std::unique_ptr<Encoder> encoder;
        RETURN_IF_ERROR(encoding_info->create_encoder(&encoder));
        RETURN_IF_ERROR(encoder->append((const uint8_t*)elements.data(), num_rows));
        Slice encoded_data = encoder->build();

        // see compressed size using zstd
        std::vector<char> compressed_buffer(encoded_data.size * 2 + 20);
        const BlockCompressionCodec* codec = nullptr;
        RETURN_IF_ERROR(get_block_compression_codec(CompressionTypePB::ZSTD, &codec));
        Slice compressed_slice(compressed_buffer.data(), compressed_buffer.size());
        RETURN_IF_ERROR(codec->compress(encoded_data, &compressed_slice));

        std::string label = fmt::format("enc={},mode={},rows={},sz={},cpsz={}", to_string(encoding_type),
                                        to_string(test_mode), num_rows, encoded_data.size, compressed_slice.size);
        state.SetLabel(label);

        if constexpr (test_mode != DECOMPRESS) {
            std::unique_ptr<Decoder> decoder;
            std::vector<T> output(num_rows);
            RETURN_IF_ERROR(encoding_info->create_decoder(&decoder));
            for (auto _ : state) {
                RETURN_IF_ERROR(decoder->set_data(encoded_data));
                if constexpr (test_mode == SKIP) {
                    RETURN_IF_ERROR(decoder->skip(num_rows));
                } else {
                    RETURN_IF_ERROR(decoder->next_batch(num_rows, (uint8_t*)output.data()));
                }
            }
        } else {
            std::vector<char> decompressed_buffer(encoded_data.size);
            for (auto _ : state) {
                Slice output(decompressed_buffer.data(), decompressed_buffer.size());
                RETURN_IF_ERROR(codec->decompress(compressed_slice, &output));
            }
        }
        return Status::OK();
    };
    Status st = f();
    if (!st.ok()) {
        state.SkipWithError(st.to_string().c_str());
    }
}

static void CustomArgsRandomInt(benchmark::internal::Benchmark* b) {
    using T = int32_t;
    std::vector<int64_t> encodings = {(int64_t)tparquet::Encoding::PLAIN,
                                      (int64_t)tparquet::Encoding::DELTA_BINARY_PACKED,
                                      (int64_t)tparquet::Encoding::BYTE_STREAM_SPLIT};
    std::vector<pair<int64_t, int64_t>> ranges = {{std::numeric_limits<T>::lowest(), std::numeric_limits<T>::max()}};

    for (auto encoding : encodings) {
        for (auto range : ranges) {
            b->Args({encoding, kTestChunkSize, range.first, range.second});
        }
    }
}

static void CustomArgsSeriesInt(benchmark::internal::Benchmark* b) {
    using T = int32_t;
    std::vector<int64_t> encodings = {(int64_t)tparquet::Encoding::PLAIN,
                                      (int64_t)tparquet::Encoding::DELTA_BINARY_PACKED,
                                      (int64_t)tparquet::Encoding::BYTE_STREAM_SPLIT};
    std::vector<int64_t> delta_values = {127};

    for (auto encoding : encodings) {
        for (auto delta_value : delta_values) {
            b->Args({encoding, kTestChunkSize, 0, delta_value});
        }
    }
}

static void CustomArgsSkipInt(benchmark::internal::Benchmark* b) {
    using T = int32_t;
    std::vector<int64_t> encodings = {(int64_t)tparquet::Encoding::PLAIN,
                                      (int64_t)tparquet::Encoding::DELTA_BINARY_PACKED,
                                      (int64_t)tparquet::Encoding::BYTE_STREAM_SPLIT};
    for (auto encoding : encodings) {
        b->Args({encoding, kTestChunkSize});
    }
}

static void CustomArgsRandomFloat(benchmark::internal::Benchmark* b) {
    using T = int32_t;
    std::vector<int64_t> encodings = {(int64_t)tparquet::Encoding::PLAIN,
                                      (int64_t)tparquet::Encoding::BYTE_STREAM_SPLIT};
    std::vector<pair<int64_t, int64_t>> ranges = {{std::numeric_limits<T>::lowest(), std::numeric_limits<T>::max()}};

    for (auto encoding : encodings) {
        for (auto range : ranges) {
            b->Args({encoding, kTestChunkSize, range.first, range.second});
        }
    }
}

static void CustomArgsSkipFloat(benchmark::internal::Benchmark* b) {
    using T = int32_t;
    std::vector<int64_t> encodings = {(int64_t)tparquet::Encoding::PLAIN,
                                      (int64_t)tparquet::Encoding::BYTE_STREAM_SPLIT};
    for (auto encoding : encodings) {
        b->Args({encoding, kTestChunkSize});
    }
}

BENCHMARK_TEMPLATE(BMTestValue, tparquet::Type::INT32, DECOMPRESS)->Apply(CustomArgsSkipInt);
BENCHMARK_TEMPLATE(BMTestValue, tparquet::Type::INT32, SKIP)->Apply(CustomArgsSkipInt);
BENCHMARK_TEMPLATE(BMTestValue, tparquet::Type::INT32, REPEAT)->Apply(CustomArgsSkipInt);
BENCHMARK_TEMPLATE(BMTestValue, tparquet::Type::INT32, RANDOM)->Apply(CustomArgsRandomInt);
BENCHMARK_TEMPLATE(BMTestValue, tparquet::Type::INT32, SERIES)->Apply(CustomArgsSeriesInt);

BENCHMARK_TEMPLATE(BMTestValue, tparquet::Type::INT64, DECOMPRESS)->Apply(CustomArgsSkipInt);
BENCHMARK_TEMPLATE(BMTestValue, tparquet::Type::INT64, SKIP)->Apply(CustomArgsSkipInt);
BENCHMARK_TEMPLATE(BMTestValue, tparquet::Type::INT64, REPEAT)->Apply(CustomArgsSkipInt);
BENCHMARK_TEMPLATE(BMTestValue, tparquet::Type::INT64, RANDOM)->Apply(CustomArgsRandomInt);
BENCHMARK_TEMPLATE(BMTestValue, tparquet::Type::INT64, SERIES)->Apply(CustomArgsSeriesInt);

BENCHMARK_TEMPLATE(BMTestValue, tparquet::Type::FLOAT, DECOMPRESS)->Apply(CustomArgsSkipFloat);
BENCHMARK_TEMPLATE(BMTestValue, tparquet::Type::FLOAT, SKIP)->Apply(CustomArgsSkipFloat);
BENCHMARK_TEMPLATE(BMTestValue, tparquet::Type::FLOAT, RANDOM)->Apply(CustomArgsRandomFloat);

BENCHMARK_TEMPLATE(BMTestValue, tparquet::Type::DOUBLE, DECOMPRESS)->Apply(CustomArgsSkipFloat);
BENCHMARK_TEMPLATE(BMTestValue, tparquet::Type::DOUBLE, SKIP)->Apply(CustomArgsSkipFloat);
BENCHMARK_TEMPLATE(BMTestValue, tparquet::Type::DOUBLE, RANDOM)->Apply(CustomArgsRandomFloat);

template <tparquet::Type::type PT, TestMode test_mode>
static void BMTestString(benchmark::State& state) {
    auto f = [&]() {
        using T = typename PhysicalTypeTraits<PT>::CppType;
        tparquet::Encoding::type encoding_type = (tparquet::Encoding::type)state.range(0);
        const EncodingInfo* encoding_info;
        RETURN_IF_ERROR(EncodingInfo::get(PT, encoding_type, &encoding_info));

        int64_t num_rows = state.range(1);
        std::vector<std::string> elements;
        if constexpr (test_mode == RANDOM) {
            int64_t min_value = state.range(2);
            int64_t max_value = state.range(3);
            elements = BenchUtil::create_random_string(num_rows, min_value, max_value);
        } else if constexpr (test_mode == PREFIX) {
            int64_t cardinality = state.range(2);
            int64_t length = state.range(3);
            elements = BenchUtil::create_lowcard_string(num_rows, cardinality, length, length);
            for (auto& str : elements) {
                str = "prefix_" + str;
            }
        } else if constexpr (test_mode == LOWCARD) {
            int64_t cardinality = state.range(2);
            int64_t length = state.range(3);
            elements = BenchUtil::create_lowcard_string(num_rows, cardinality, length, length);
        } else if constexpr (test_mode == SKIP || test_mode == DECOMPRESS) {
            elements = BenchUtil::create_random_string(num_rows, 10, 20);
        }

        std::vector<Slice> slices;
        for (auto& str : elements) {
            slices.emplace_back(str.data(), str.size());
        }

        std::unique_ptr<Encoder> encoder;
        RETURN_IF_ERROR(encoding_info->create_encoder(&encoder));
        RETURN_IF_ERROR(encoder->append((const uint8_t*)(slices.data()), num_rows));
        Slice encoded_data = encoder->build();

        // see compressed size using zstd
        std::vector<char> compressed_buffer(encoded_data.size * 2 + 20);
        const BlockCompressionCodec* codec = nullptr;
        RETURN_IF_ERROR(get_block_compression_codec(CompressionTypePB::ZSTD, &codec));
        Slice compressed_slice(compressed_buffer.data(), compressed_buffer.size());
        RETURN_IF_ERROR(codec->compress(encoded_data, &compressed_slice));

        std::string label = fmt::format("enc={},mode={},rows={},sz={},cpsz={}", to_string(encoding_type),
                                        to_string(test_mode), num_rows, encoded_data.size, compressed_slice.size);
        state.SetLabel(label);

        if constexpr (test_mode != DECOMPRESS) {
            std::unique_ptr<Decoder> decoder;
            std::vector<T> output(num_rows);
            RETURN_IF_ERROR(encoding_info->create_decoder(&decoder));
            // all strings are in the same length
            if constexpr (PT == tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
                RETURN_IF_ERROR(decoder->set_type_length(slices[0].size));
            }
            for (auto _ : state) {
                RETURN_IF_ERROR(decoder->set_data(encoded_data));
                if constexpr (test_mode == SKIP) {
                    RETURN_IF_ERROR(decoder->skip(num_rows));
                } else {
                    RETURN_IF_ERROR(decoder->next_batch(num_rows, (uint8_t*)output.data()));
                }
            }
        } else {
            std::vector<char> decompressed_buffer(encoded_data.size);
            for (auto _ : state) {
                Slice output(decompressed_buffer.data(), decompressed_buffer.size());
                RETURN_IF_ERROR(codec->decompress(compressed_slice, &output));
            }
        }
        return Status::OK();
    };
    Status st = f();
    if (!st.ok()) {
        state.SkipWithError(st.to_string().c_str());
    }
}

static void CustomArgsRandomString(benchmark::internal::Benchmark* b) {
    using T = int32_t;
    std::vector<int64_t> encodings = {(int64_t)tparquet::Encoding::PLAIN,
                                      (int64_t)tparquet::Encoding::DELTA_LENGTH_BYTE_ARRAY,
                                      (int64_t)tparquet::Encoding::DELTA_BYTE_ARRAY};

    std::vector<pair<int64_t, int64_t>> ranges = {{10, 20}};

    for (auto encoding : encodings) {
        for (auto range : ranges) {
            b->Args({encoding, kTestChunkSize, range.first, range.second});
        }
    }
}

static void CustomArgsLowcardString(benchmark::internal::Benchmark* b) {
    using T = int32_t;
    std::vector<int64_t> encodings = {(int64_t)tparquet::Encoding::PLAIN,
                                      (int64_t)tparquet::Encoding::DELTA_LENGTH_BYTE_ARRAY,
                                      (int64_t)tparquet::Encoding::DELTA_BYTE_ARRAY};

    std::vector<pair<int64_t, int64_t>> ranges = {{10, 10}};

    for (auto encoding : encodings) {
        for (auto range : ranges) {
            b->Args({encoding, kTestChunkSize, range.first, range.second});
        }
    }
}

static void CustomArgsSkipString(benchmark::internal::Benchmark* b) {
    using T = int32_t;
    std::vector<int64_t> encodings = {(int64_t)tparquet::Encoding::PLAIN,
                                      (int64_t)tparquet::Encoding::DELTA_LENGTH_BYTE_ARRAY,
                                      (int64_t)tparquet::Encoding::DELTA_BYTE_ARRAY};
    for (auto encoding : encodings) {
        b->Args({encoding, kTestChunkSize});
    }
}

BENCHMARK_TEMPLATE(BMTestString, tparquet::Type::BYTE_ARRAY, DECOMPRESS)->Apply(CustomArgsSkipString);
BENCHMARK_TEMPLATE(BMTestString, tparquet::Type::BYTE_ARRAY, SKIP)->Apply(CustomArgsSkipString);
BENCHMARK_TEMPLATE(BMTestString, tparquet::Type::BYTE_ARRAY, LOWCARD)->Apply(CustomArgsLowcardString);
BENCHMARK_TEMPLATE(BMTestString, tparquet::Type::BYTE_ARRAY, PREFIX)->Apply(CustomArgsLowcardString);
BENCHMARK_TEMPLATE(BMTestString, tparquet::Type::BYTE_ARRAY, RANDOM)->Apply(CustomArgsRandomString);

BENCHMARK_TEMPLATE(BMTestString, tparquet::Type::FIXED_LEN_BYTE_ARRAY, DECOMPRESS)->Apply(CustomArgsSkipString);
BENCHMARK_TEMPLATE(BMTestString, tparquet::Type::FIXED_LEN_BYTE_ARRAY, SKIP)->Apply(CustomArgsSkipString);
BENCHMARK_TEMPLATE(BMTestString, tparquet::Type::FIXED_LEN_BYTE_ARRAY, LOWCARD)->Apply(CustomArgsLowcardString);
BENCHMARK_TEMPLATE(BMTestString, tparquet::Type::FIXED_LEN_BYTE_ARRAY, PREFIX)->Apply(CustomArgsLowcardString);

// Compare selective materialization with the unchanged bulk path on identical
// inputs. This measures value decoding/copying only, not page IO or decompression.
template <tparquet::Type::type PT>
static void BMPlainSelection(benchmark::State& state) {
    using T = typename PhysicalTypeTraits<PT>::CppType;
    const size_t rows = state.range(0);
    const size_t retained_percent = state.range(1);
    const bool clustered = state.range(2);
    const bool use_filter = state.range(3);
    std::vector<T> values(rows);
    Filter filter(rows);
    size_t retained_rows = 0;
    for (size_t i = 0; i < rows; ++i) {
        values[i] = static_cast<T>(1 + i % 127);
        filter[i] = clustered ? i < rows * retained_percent / 100 : (i * 13 % 100) < retained_percent;
        retained_rows += filter[i];
    }
    const EncodingInfo* encoding = nullptr;
    CHECK_OK(EncodingInfo::get(PT, tparquet::Encoding::PLAIN, &encoding));
    std::unique_ptr<Encoder> encoder;
    CHECK_OK(encoding->create_encoder(&encoder));
    CHECK_OK(encoder->append(reinterpret_cast<const uint8_t*>(values.data()), rows));
    const Slice encoded = encoder->build();
    std::unique_ptr<Decoder> decoder;
    CHECK_OK(encoding->create_decoder(&decoder));
    auto column = FixedLengthColumn<T>::create();
    column->reserve(rows);
    for (auto _ : state) {
        column->reset_column();
        CHECK_OK(decoder->set_data(encoded));
        CHECK_OK(decoder->next_batch(rows, ColumnContentType::VALUE, column.get(),
                                     use_filter ? filter.data() : nullptr));
        benchmark::DoNotOptimize(column->immutable_data().data());
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * rows);
    state.SetBytesProcessed(state.iterations() * rows * sizeof(T));
    state.counters["retained_rows"] = retained_rows;
}

static void PlainSelectionArgs(benchmark::internal::Benchmark* benchmark) {
    benchmark->ArgNames({"rows", "retained_percent", "clustered", "use_filter"});
    for (int rows : {4096, 65536}) {
        for (int retained : {0, 1, 5, 10, 19, 100}) {
            for (int clustered : {0, 1}) {
                for (int filtered : {0, 1}) {
                    benchmark->Args({rows, retained, clustered, filtered});
                }
            }
        }
    }
}

BENCHMARK_TEMPLATE(BMPlainSelection, tparquet::Type::INT32)->Apply(PlainSelectionArgs);
BENCHMARK_TEMPLATE(BMPlainSelection, tparquet::Type::INT64)->Apply(PlainSelectionArgs);
BENCHMARK_TEMPLATE(BMPlainSelection, tparquet::Type::FLOAT)->Apply(PlainSelectionArgs);
BENCHMARK_TEMPLATE(BMPlainSelection, tparquet::Type::DOUBLE)->Apply(PlainSelectionArgs);

} // namespace starrocks::parquet

BENCHMARK_MAIN();
