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

// ============================================================================
// Empirical Results on GCP n4-standard-32 (Intel Xeon Platinum 8581C @ 2.10GHz
// Emerald Rapids, GCC 14, taskset -c 2, 4,096 rows per BinaryColumn chunk):
//
// Benchmark (AVX-512 icelake)            5.2.8 (4.1.7)    9.1.0 (HEAD)     9.4.0 (PR)   Speedup (vs 9.1.0)
// --------------------------------------------------------------------------------------------------------
// BM_SqlUtf8Length_Ascii/16                 39,281 ns        36,134 ns      15,224 ns     2.37x (4.01 GiB/s)
// BM_SqlUtf8Length_Ascii/48                124,778 ns       121,899 ns      15,221 ns     8.01x (12.03 GiB/s)
// BM_SqlUtf8Length_Ascii/100                68,623 ns        82,803 ns      22,770 ns     3.64x (16.75 GiB/s)
// BM_SqlUtf8Length_Ascii/256                35,919 ns        32,811 ns      31,782 ns     1.03x (30.73 GiB/s)
// BM_SqlUtf8Length_Utf8/16                  60,662 ns        60,626 ns      15,160 ns     4.00x (4.03 GiB/s)
// BM_SqlUtf8Length_Utf8/48                 156,981 ns       160,360 ns      15,135 ns    10.60x (12.10 GiB/s)
// BM_SqlUtf8Length_Utf8/100                143,263 ns       138,552 ns      21,543 ns     6.43x (17.71 GiB/s)
// BM_SqlUtf8Length_Utf8/256                 34,297 ns        30,937 ns      29,818 ns     1.04x (32.75 GiB/s)
// BM_SqlSubstr_Ascii/24                      7,111 ns         7,118 ns       6,538 ns     1.09x (14.00 GiB/s)
// BM_SqlSubstr_Ascii/64                      9,504 ns         9,446 ns       8,326 ns     1.13x (29.32 GiB/s)
// BM_SqlSubstr_Ascii/192                    16,322 ns        16,342 ns      12,981 ns     1.26x (56.42 GiB/s)
// BM_SqlReverse_Ascii/64                    14,494 ns        14,355 ns      12,856 ns     1.12x (18.99 GiB/s)
// BM_SqlReverse_Ascii/192                   41,335 ns        40,605 ns      33,374 ns     1.22x (21.95 GiB/s)
// BM_SqlLocate_Utf8/32                     154,541 ns       154,954 ns     107,469 ns     1.44x (1.14 GiB/s)
// BM_SqlLocate_Utf8/48                     219,663 ns       219,649 ns     112,525 ns     1.95x (1.63 GiB/s)
// BM_SqlLocate_Utf8/96                     153,685 ns       148,768 ns     137,274 ns     1.08x (2.67 GiB/s)
//
// Benchmark (AVX2 haswell)               5.2.8 (4.1.7)    9.1.0 (HEAD)     9.4.0 (PR)   Speedup (vs 9.1.0)
// --------------------------------------------------------------------------------------------------------
// BM_ScannerValidateUtf8_PerRecord/96/0     24,182 ns        26,133 ns      16,315 ns     1.60x (22.45 GiB/s)
// BM_ScannerValidateUtf8_PerRecord/160/0    30,835 ns        29,672 ns      19,824 ns     1.50x (30.79 GiB/s)
// BM_ScannerValidateUtf8_PerRecord/240/0    54,222 ns        52,999 ns      25,001 ns     2.12x (36.62 GiB/s)
// BM_ScannerValidateUtf8_PerRecord/240/1   114,642 ns       116,600 ns     105,792 ns     1.10x (8.65 GiB/s)
//
// Base64 Legacy Scalar vs simdutf 9.4.0 on x86-64 (4,096 rows/chunk):
// --------------------------------------------------------------------------------------------------------
// BM_SqlBase64Encode (32B / 128B / 1024B):  119,593 / 396,195 / 2,894,851 ns -> 24,275 / 35,628 / 178,706 ns (4.93x / 11.12x / 16.20x)
// BM_SqlBase64Decode (44B / 172B / 1368B):  331,374 / 1,112,093 / 8,418,747 ns -> 57,308 / 76,722 / 327,377 ns (5.78x / 14.50x / 25.72x)
//
// Empirical Results on GCP c4a-standard-4 (Google Axion ARM Neoverse-V2 @ 2.0GHz
// ARM64/NEON, GCC 14, taskset -c 2, 4,096 rows per BinaryColumn chunk):
//
// Benchmark (ARM64 NEON)                 5.2.8 (4.1.7)    9.1.0 (HEAD)     9.4.0 (PR)   Speedup (vs 9.1.0)
// --------------------------------------------------------------------------------------------------------
// BM_SqlUtf8Length_Ascii/16                 17,843 ns        17,839 ns      17,838 ns     1.00x (3.42 GiB/s)
// BM_SqlUtf8Length_Ascii/48                 32,089 ns        32,143 ns      32,130 ns     1.00x (5.70 GiB/s)
// BM_SqlUtf8Length_Ascii/100                34,328 ns        34,323 ns      34,324 ns     1.00x (11.11 GiB/s)
// BM_SqlUtf8Length_Ascii/256                32,680 ns        33,522 ns      33,331 ns     1.01x (29.30 GiB/s)
// BM_SqlUtf8Length_Utf8/16                  17,837 ns        17,838 ns      17,838 ns     1.00x (3.42 GiB/s)
// BM_SqlUtf8Length_Utf8/48                  31,660 ns        31,674 ns      31,661 ns     1.00x (5.78 GiB/s)
// BM_SqlUtf8Length_Utf8/100                 32,938 ns        32,943 ns      32,946 ns     1.00x (11.58 GiB/s)
// BM_SqlUtf8Length_Utf8/256                 31,282 ns        32,374 ns      31,976 ns     1.01x (30.54 GiB/s)
// BM_SqlSubstr_Ascii/192                    16,441 ns        16,956 ns      16,667 ns     1.02x (43.94 GiB/s)
// BM_SqlReverse_Ascii/192                   31,742 ns        32,520 ns      30,711 ns     1.06x (23.85 GiB/s)
// BM_ScannerValidateUtf8_PerRecord/96/0     27,529 ns        26,705 ns      19,366 ns     1.38x (18.91 GiB/s)
// BM_ScannerValidateUtf8_PerRecord/160/0    30,123 ns        29,187 ns      22,151 ns     1.32x (27.55 GiB/s)
// BM_ScannerValidateUtf8_PerRecord/240/0    35,157 ns        34,352 ns      25,839 ns     1.33x (35.43 GiB/s)
// BM_ScannerValidateUtf8_PerRecord/2048/0  164,734 ns       170,458 ns     158,502 ns     1.08x (49.29 GiB/s)
// BM_ScannerValidateUtf8_PerRecord/96/1     81,567 ns        81,319 ns      77,516 ns     1.05x (4.72 GiB/s)
// BM_ScannerValidateUtf8_PerRecord/160/1   113,694 ns       113,131 ns     107,711 ns     1.05x (5.67 GiB/s)
// BM_ScannerValidateUtf8_PerRecord/240/1   143,907 ns       142,453 ns     135,498 ns     1.05x (6.76 GiB/s)
//
// Base64 Legacy Scalar vs simdutf 9.4.0 on ARM64/NEON (4,096 rows/chunk):
// --------------------------------------------------------------------------------------------------------
// BM_SqlBase64Encode (32B / 128B / 1024B):   88,585 / 300,835 / 2,293,064 ns ->  35,711 /  74,077 / 425,829 ns (2.48x / 4.06x / 5.38x)
// BM_SqlBase64Decode (44B / 172B / 1368B):  246,123 / 974,210 / 7,570,223 ns -> 117,497 / 179,080 / 724,495 ns (2.09x / 5.44x / 10.45x)
// ============================================================================
