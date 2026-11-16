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

// End-to-end StarRocks SQL function & scanner benchmark suite for simdutf
// (requested in https://github.com/StarRocks/starrocks/pull/78568).
//
// Exercises the exact 4,096-row vectorized BinaryColumn (16-byte aligned
// contiguous byte buffer + uint32_t offsets) and per-record scanner hot paths
// that invoke simdutf in StarRocks:
//   * StringFunctions::utf8_length (char_length / character_length)
//   * StringFunctions::substring   (substr / substring)
//   * StringFunctions::reverse     (reverse)
//   * StringFunctions::locate      (locate / instr)
//   * CsvScanner / HdfsTextScanner / SegmentIterator / MysqlRowBuffer
//     per-record validate_utf8
//   * EncryptionFunctions::to_base64 / from_base64 (legacy scalar vs simdutf)

#include <benchmark/benchmark.h>

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "base/base64.h"
#include "base/string/utf8.h"
#include "base/string/utf8_check.h"
#include "simdutf.h"

namespace starrocks {

namespace {

constexpr size_t kNumRows = 4096;

// StarRocks BinaryColumn::Bytes (raw::RawVectorPad16<uint8_t>) allocates via
// standard 16-byte aligned allocator (typically 16 mod 64, i.e., not 64-byte
// aligned), which exercises simdutf's unaligned prologue + 64B loop + tail.
struct MockBinaryColumn {
    std::vector<uint8_t> storage;
    uint8_t* bytes = nullptr;
    size_t total_bytes = 0;
    std::vector<uint32_t> offsets;

    void init_ascii(size_t num_rows, size_t row_len) {
        total_bytes = num_rows * row_len;
        storage.assign(total_bytes + 80, 'a');
        // Offset by +16 from a 64-byte boundary to match jemalloc/glibc 16B alignment.
        uintptr_t base = reinterpret_cast<uintptr_t>(storage.data());
        uintptr_t aligned64 = (base + 63) & ~uintptr_t(63);
        bytes = reinterpret_cast<uint8_t*>(aligned64 + 16);
        offsets.resize(num_rows + 1);
        offsets[0] = 0;
        for (size_t i = 0; i < num_rows; ++i) {
            uint8_t* row_ptr = bytes + i * row_len;
            for (size_t j = 0; j < row_len; ++j) {
                row_ptr[j] = static_cast<uint8_t>('a' + ((i + j) % 26));
            }
            offsets[i + 1] = static_cast<uint32_t>((i + 1) * row_len);
        }
    }

    void init_utf8(size_t num_rows, size_t target_row_bytes) {
        // Mix of ASCII and 3-byte UTF-8 CJK characters ("中" = E4 B8 AD)
        std::string sample_row;
        sample_row.reserve(target_row_bytes + 4);
        while (sample_row.size() + 4 <= target_row_bytes) {
            sample_row.append("a\xE4\xB8\xAD");
        }
        while (sample_row.size() < target_row_bytes) {
            sample_row.push_back('b');
        }
        size_t row_len = sample_row.size();
        total_bytes = num_rows * row_len;
        storage.assign(total_bytes + 80, 0);
        uintptr_t base = reinterpret_cast<uintptr_t>(storage.data());
        uintptr_t aligned64 = (base + 63) & ~uintptr_t(63);
        bytes = reinterpret_cast<uint8_t*>(aligned64 + 16);
        offsets.resize(num_rows + 1);
        offsets[0] = 0;
        for (size_t i = 0; i < num_rows; ++i) {
            std::memcpy(bytes + i * row_len, sample_row.data(), row_len);
            offsets[i + 1] = static_cast<uint32_t>((i + 1) * row_len);
        }
    }
};

// ---------------------------------------------------------------------------
// 1. StringFunctions::utf8_length (SELECT sum(char_length(col)))
// ---------------------------------------------------------------------------
static void BM_SqlUtf8Length_Ascii(benchmark::State& state) {
    const size_t row_len = static_cast<size_t>(state.range(0));
    MockBinaryColumn col;
    col.init_ascii(kNumRows, row_len);
    std::vector<int32_t> dst(kNumRows, 0);

    for (auto _ : state) {
        const char* base = reinterpret_cast<const char*>(col.bytes);
        for (size_t i = 0; i < kNumRows; ++i) {
            const char* begin = base + col.offsets[i];
            const char* end = base + col.offsets[i + 1];
            dst[i] = utf8_len(begin, end);
        }
        benchmark::DoNotOptimize(dst.data());
    }
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(col.total_bytes));
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(kNumRows));
}

static void BM_SqlUtf8Length_Utf8(benchmark::State& state) {
    const size_t row_len = static_cast<size_t>(state.range(0));
    MockBinaryColumn col;
    col.init_utf8(kNumRows, row_len);
    std::vector<int32_t> dst(kNumRows, 0);

    for (auto _ : state) {
        const char* base = reinterpret_cast<const char*>(col.bytes);
        for (size_t i = 0; i < kNumRows; ++i) {
            const char* begin = base + col.offsets[i];
            const char* end = base + col.offsets[i + 1];
            dst[i] = utf8_len(begin, end);
        }
        benchmark::DoNotOptimize(dst.data());
    }
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(col.total_bytes));
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(kNumRows));
}

// ---------------------------------------------------------------------------
// 2. StringFunctions::substring (SELECT count(substr(col, 2, 12)))
// ---------------------------------------------------------------------------
static void BM_SqlSubstr_Ascii(benchmark::State& state) {
    const size_t row_len = static_cast<size_t>(state.range(0));
    MockBinaryColumn col;
    col.init_ascii(kNumRows, row_len);
    const size_t pos = 1; // 0-based offset for SQL pos=2
    const size_t sub_len = std::min<size_t>(12, row_len > pos ? row_len - pos : 0);
    std::vector<uint8_t> dst_bytes(kNumRows * sub_len);
    std::vector<uint32_t> dst_offsets(kNumRows + 1, 0);

    for (auto _ : state) {
        bool is_ascii = validate_ascii_fast(reinterpret_cast<const char*>(col.bytes), col.total_bytes);
        if (is_ascii) {
            uint8_t* out = dst_bytes.data();
            for (size_t i = 0; i < kNumRows; ++i) {
                const uint8_t* row_ptr = col.bytes + col.offsets[i] + pos;
                std::memcpy(out, row_ptr, sub_len);
                out += sub_len;
                dst_offsets[i + 1] = static_cast<uint32_t>((i + 1) * sub_len);
            }
        }
        benchmark::DoNotOptimize(is_ascii);
        benchmark::DoNotOptimize(dst_bytes.data());
        benchmark::DoNotOptimize(dst_offsets.data());
    }
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(col.total_bytes));
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(kNumRows));
}

// ---------------------------------------------------------------------------
// 3. StringFunctions::reverse (SELECT count(reverse(col)))
// ---------------------------------------------------------------------------
static void BM_SqlReverse_Ascii(benchmark::State& state) {
    const size_t row_len = static_cast<size_t>(state.range(0));
    MockBinaryColumn col;
    col.init_ascii(kNumRows, row_len);
    std::vector<uint8_t> dst_bytes(col.total_bytes);

    for (auto _ : state) {
        bool is_ascii = validate_ascii_fast(reinterpret_cast<const char*>(col.bytes), col.total_bytes);
        if (is_ascii) {
            char* dst_curr = reinterpret_cast<char*>(dst_bytes.data());
            const char* src_base = reinterpret_cast<const char*>(col.bytes);
            for (size_t i = 0; i < kNumRows; ++i) {
                const char* begin = src_base + col.offsets[i];
                const char* end = src_base + col.offsets[i + 1];
                dst_curr += (end - begin);
                char* p = dst_curr;
                for (const char* s = begin; s < end; ++s) {
                    --p;
                    *p = *s;
                }
            }
        }
        benchmark::DoNotOptimize(is_ascii);
        benchmark::DoNotOptimize(dst_bytes.data());
    }
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(col.total_bytes));
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(kNumRows));
}

// ---------------------------------------------------------------------------
// 4. StringFunctions::locate (SELECT sum(locate(needle, col)))
// ---------------------------------------------------------------------------
static void BM_SqlLocate_Utf8(benchmark::State& state) {
    const size_t row_len = static_cast<size_t>(state.range(0));
    MockBinaryColumn col;
    col.init_utf8(kNumRows, row_len);
    // Place needle "TARGET" near 75% of each row so prefix length is < 64B for 32/48B rows
    const std::string needle = "TARGET";
    for (size_t i = 0; i < kNumRows; ++i) {
        uint8_t* row_ptr = col.bytes + col.offsets[i];
        size_t len = col.offsets[i + 1] - col.offsets[i];
        size_t target_pos = ((len * 3) / 4) & ~size_t(3);
        if (target_pos + needle.size() <= len) {
            std::memcpy(row_ptr + target_pos, needle.data(), needle.size());
        }
    }
    std::vector<int32_t> result(kNumRows, 0);

    for (auto _ : state) {
        const char* base = reinterpret_cast<const char*>(col.bytes);
        for (size_t i = 0; i < kNumRows; ++i) {
            const char* hay = base + col.offsets[i];
            size_t hay_len = col.offsets[i + 1] - col.offsets[i];
            const void* match = memmem(hay, hay_len, needle.data(), needle.size());
            if (match != nullptr) {
                const char* match_ptr = static_cast<const char*>(match);
                result[i] = utf8_len(hay, match_ptr) + 1;
            } else {
                result[i] = 0;
            }
        }
        benchmark::DoNotOptimize(result.data());
    }
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(col.total_bytes));
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(kNumRows));
}

// ---------------------------------------------------------------------------
// 5. Scanner & Storage per-record validate_utf8 (CSV / HDFS Text / Segment / MySQL)
// ---------------------------------------------------------------------------
static void BM_ScannerValidateUtf8_PerRecord(benchmark::State& state) {
    const size_t record_len = static_cast<size_t>(state.range(0));
    const bool is_multibyte = state.range(1) != 0;
    MockBinaryColumn col;
    if (is_multibyte) {
        col.init_utf8(kNumRows, record_len);
    } else {
        col.init_ascii(kNumRows, record_len);
    }

    for (auto _ : state) {
        size_t valid_count = 0;
        const char* base = reinterpret_cast<const char*>(col.bytes);
        for (size_t i = 0; i < kNumRows; ++i) {
            const char* rec = base + col.offsets[i];
            size_t len = col.offsets[i + 1] - col.offsets[i];
            valid_count += validate_utf8(rec, len) ? 1 : 0;
        }
        benchmark::DoNotOptimize(valid_count);
    }
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(col.total_bytes));
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(kNumRows));
}

// ---------------------------------------------------------------------------
// 6. EncryptionFunctions::to_base64 / from_base64 (Legacy Scalar vs simdutf)
// ---------------------------------------------------------------------------
static void BM_SqlBase64Encode_Legacy(benchmark::State& state) {
    const size_t row_len = static_cast<size_t>(state.range(0));
    MockBinaryColumn col;
    col.init_ascii(kNumRows, row_len);
    size_t max_encoded = simdutf::base64_length_from_binary(row_len) + 4;
    std::vector<uint8_t> out(max_encoded);

    for (auto _ : state) {
        size_t total_out = 0;
        for (size_t i = 0; i < kNumRows; ++i) {
            const uint8_t* src = col.bytes + col.offsets[i];
            size_t len = col.offsets[i + 1] - col.offsets[i];
            int64_t written = base64_encode2(src, len, out.data());
            total_out += static_cast<size_t>(written);
        }
        benchmark::DoNotOptimize(total_out);
        benchmark::DoNotOptimize(out.data());
    }
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(col.total_bytes));
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(kNumRows));
}

static void BM_SqlBase64Encode_Simdutf(benchmark::State& state) {
    const size_t row_len = static_cast<size_t>(state.range(0));
    MockBinaryColumn col;
    col.init_ascii(kNumRows, row_len);
    size_t max_encoded = simdutf::base64_length_from_binary(row_len) + 4;
    std::vector<char> out(max_encoded);

    for (auto _ : state) {
        size_t total_out = 0;
        for (size_t i = 0; i < kNumRows; ++i) {
            const char* src = reinterpret_cast<const char*>(col.bytes + col.offsets[i]);
            size_t len = col.offsets[i + 1] - col.offsets[i];
            total_out += simdutf::binary_to_base64(src, len, out.data());
        }
        benchmark::DoNotOptimize(total_out);
        benchmark::DoNotOptimize(out.data());
    }
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(col.total_bytes));
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(kNumRows));
}

static void BM_SqlBase64Decode_Legacy(benchmark::State& state) {
    const size_t encoded_len = static_cast<size_t>(state.range(0));
    const size_t raw_len = (encoded_len / 4) * 3;
    std::string raw(raw_len, 'x');
    std::string encoded(simdutf::base64_length_from_binary(raw_len), '\0');
    simdutf::binary_to_base64(raw.data(), raw.size(), encoded.data());

    MockBinaryColumn col;
    col.total_bytes = kNumRows * encoded.size();
    col.storage.assign(col.total_bytes + 80, 0);
    col.bytes = col.storage.data() + 16;
    col.offsets.resize(kNumRows + 1, 0);
    for (size_t i = 0; i < kNumRows; ++i) {
        std::memcpy(col.bytes + i * encoded.size(), encoded.data(), encoded.size());
        col.offsets[i + 1] = static_cast<uint32_t>((i + 1) * encoded.size());
    }
    std::vector<char> out(encoded.size() + 16);

    for (auto _ : state) {
        int64_t total_out = 0;
        const char* base = reinterpret_cast<const char*>(col.bytes);
        for (size_t i = 0; i < kNumRows; ++i) {
            const char* src = base + col.offsets[i];
            size_t len = col.offsets[i + 1] - col.offsets[i];
            total_out += base64_decode2(src, len, out.data());
        }
        benchmark::DoNotOptimize(total_out);
        benchmark::DoNotOptimize(out.data());
    }
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(col.total_bytes));
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(kNumRows));
}

static void BM_SqlBase64Decode_Simdutf(benchmark::State& state) {
    const size_t encoded_len = static_cast<size_t>(state.range(0));
    const size_t raw_len = (encoded_len / 4) * 3;
    std::string raw(raw_len, 'x');
    std::string encoded(simdutf::base64_length_from_binary(raw_len), '\0');
    simdutf::binary_to_base64(raw.data(), raw.size(), encoded.data());

    MockBinaryColumn col;
    col.total_bytes = kNumRows * encoded.size();
    col.storage.assign(col.total_bytes + 80, 0);
    col.bytes = col.storage.data() + 16;
    col.offsets.resize(kNumRows + 1, 0);
    for (size_t i = 0; i < kNumRows; ++i) {
        std::memcpy(col.bytes + i * encoded.size(), encoded.data(), encoded.size());
        col.offsets[i + 1] = static_cast<uint32_t>((i + 1) * encoded.size());
    }
    std::vector<char> out(encoded.size() + 16);

    for (auto _ : state) {
        size_t total_out = 0;
        const char* base = reinterpret_cast<const char*>(col.bytes);
        for (size_t i = 0; i < kNumRows; ++i) {
            const char* src = base + col.offsets[i];
            size_t len = col.offsets[i + 1] - col.offsets[i];
            simdutf::result r = simdutf::base64_to_binary(src, len, out.data());
            total_out += r.count;
        }
        benchmark::DoNotOptimize(total_out);
        benchmark::DoNotOptimize(out.data());
    }
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(col.total_bytes));
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(kNumRows));
}

BENCHMARK(BM_SqlUtf8Length_Ascii)->Arg(16)->Arg(48)->Arg(100)->Arg(256);
BENCHMARK(BM_SqlUtf8Length_Utf8)->Arg(16)->Arg(48)->Arg(100)->Arg(256);
BENCHMARK(BM_SqlSubstr_Ascii)->Arg(24)->Arg(64)->Arg(192);
BENCHMARK(BM_SqlReverse_Ascii)->Arg(24)->Arg(64)->Arg(192);
BENCHMARK(BM_SqlLocate_Utf8)->Arg(32)->Arg(48)->Arg(96);
BENCHMARK(BM_ScannerValidateUtf8_PerRecord)
        ->Args({32, 0})
        ->Args({96, 0})
        ->Args({160, 0})
        ->Args({240, 0})
        ->Args({2048, 0})
        ->Args({96, 1})
        ->Args({160, 1})
        ->Args({240, 1});
BENCHMARK(BM_SqlBase64Encode_Legacy)->Arg(32)->Arg(128)->Arg(1024);
BENCHMARK(BM_SqlBase64Encode_Simdutf)->Arg(32)->Arg(128)->Arg(1024);
BENCHMARK(BM_SqlBase64Decode_Legacy)->Arg(44)->Arg(172)->Arg(1368);
BENCHMARK(BM_SqlBase64Decode_Simdutf)->Arg(44)->Arg(172)->Arg(1368);

} // namespace
} // namespace starrocks

BENCHMARK_MAIN();
