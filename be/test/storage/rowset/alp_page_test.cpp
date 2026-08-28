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

#include "storage/rowset/alp_page.h"

#include <gtest/gtest.h>

#include <chrono>
#include <cmath>
#include <limits>
#include <memory>
#include <vector>

#include "column/chunk_factory.h"
#include "column/column_helper.h"
#include "column/datum_convert.h"
#include "common/config_rowset_fwd.h"
#include "storage/chunk_helper.h"
#include "storage/rowset/bitshuffle_page.h"
#include "storage/rowset/encoding_info.h"
#include "storage/rowset/options.h"
#include "storage/rowset/page_decoder.h"
#include "storage/rowset/storage_page_decoder.h"

namespace starrocks {

namespace {

// Deterministic LCG matching the offline helium/ALP investigation so results
// stay comparable run to run.
struct Lcg {
    uint64_t s;
    explicit Lcg(uint64_t seed) : s(seed) {}
    uint32_t next_u32() {
        s = s * 6364136223846793005ULL + 1442695040888963407ULL;
        return (uint32_t)(s >> 32);
    }
};

// Sensor-style f64: slowly varying, rounded to 2 decimals (ALP's main case).
std::vector<double> gen_sensor_f64(size_t n) {
    Lcg rng(0x5e0501);
    double v = 21.5;
    std::vector<double> out(n);
    for (auto& x : out) {
        v += ((double)rng.next_u32() / (double)UINT32_MAX - 0.5) * 0.1;
        x = std::round(v * 100.0) / 100.0;
    }
    return out;
}

// CPU-utilisation-style f32 in [0, 100], rounded to 1 decimal.
std::vector<float> gen_cpu_f32(size_t n) {
    Lcg rng(0xc90001);
    float v = 30.0f;
    std::vector<float> out(n);
    for (auto& x : out) {
        float step = ((float)rng.next_u32() / (float)UINT32_MAX - 0.5f) * 4.0f;
        v = std::min(std::max(v + step, 0.0f), 100.0f);
        x = std::roundf(v * 10.0f) / 10.0f;
    }
    return out;
}

// Random-mantissa doubles: incompressible for ALP -> raw fallback vectors.
std::vector<double> gen_random_f64(size_t n) {
    Lcg rng(0xfacefeed);
    std::vector<double> out(n);
    for (auto& x : out) {
        uint64_t bits = ((uint64_t)rng.next_u32() << 32) | rng.next_u32();
        // Keep the exponent sane so the values stay finite.
        bits = (bits & 0x800FFFFFFFFFFFFFULL) | 0x3FF0000000000000ULL;
        memcpy(&x, &bits, sizeof(x));
    }
    return out;
}

} // namespace

class AlpPageTest : public testing::Test {
public:
    ~AlpPageTest() override = default;

    static Slice pre_decode(Slice encoded_data, std::unique_ptr<std::vector<uint8_t>>* page) {
        PageFooterPB footer;
        footer.set_type(DATA_PAGE);
        footer.mutable_data_page_footer()->set_nullmap_size(0);
        Status st = StoragePageDecoder::decode_page(&footer, 0, ALP_ENCODING, page, &encoded_data);
        EXPECT_TRUE(st.ok()) << st.to_string();
        return encoded_data;
    }

    template <LogicalType Type>
    void test_roundtrip(const StorageCppType<Type>* src, size_t size) {
        using CppType = StorageCppType<Type>;
        PageBuilderOptions options;
        options.data_page_size = 256 * 1024;
        AlpPageBuilder<Type> page_builder(options);

        size = page_builder.add(reinterpret_cast<const uint8_t*>(src), size);
        OwnedSlice s = page_builder.finish()->build();

        CppType first_value;
        ASSERT_TRUE(page_builder.get_first_value(&first_value).ok());
        ASSERT_EQ(memcmp(&src[0], &first_value, sizeof(CppType)), 0);
        CppType last_value;
        ASSERT_TRUE(page_builder.get_last_value(&last_value).ok());
        ASSERT_EQ(memcmp(&src[size - 1], &last_value, sizeof(CppType)), 0);

        std::unique_ptr<std::vector<uint8_t>> page;
        Slice decoded = pre_decode(s.slice(), &page);

        AlpPageDecoder<Type> page_decoder(decoded);
        ASSERT_TRUE(page_decoder.init().ok());
        ASSERT_EQ(size, page_decoder.count());

        // Element-wise check (bit-exact, including NaN / -0.0).
        for (uint32_t i = 0; i < size; i++) {
            CppType out;
            page_decoder.at_index(i, &out);
            ASSERT_EQ(memcmp(&src[i], &out, sizeof(CppType)), 0) << "mismatch at index " << i;
        }

        // Batch read through Column.
        auto column = ChunkFactory::column_from_field_type(Type, false);
        size_t n = size;
        ASSERT_TRUE(page_decoder.seek_to_position_in_page(0).ok());
        ASSERT_TRUE(page_decoder.next_batch(&n, column.get()).ok());
        ASSERT_EQ(size, n);
        const auto values = GetStorageContainer<Type>::get_data(column);
        ASSERT_EQ(memcmp(values.data(), src, size * sizeof(CppType)), 0);

        // Sparse-range read.
        if (size >= 3) {
            AlpPageDecoder<Type> range_decoder(decoded);
            ASSERT_TRUE(range_decoder.init().ok());
            SparseRange<> read_range;
            read_range.add(Range<>(0, size / 3));
            read_range.add(Range<>(size / 2, size * 2 / 3));
            auto dst = ChunkFactory::column_from_field_type(Type, false);
            ASSERT_TRUE(range_decoder.next_batch(read_range, dst.get()).ok());
            ASSERT_EQ(read_range.span_size(), dst->size());
            const auto range_values = GetStorageContainer<Type>::get_data(dst);
            size_t off = 0;
            SparseRangeIterator<> iter = read_range.new_iterator();
            while (iter.has_more()) {
                Range<> r = iter.next(size);
                ASSERT_EQ(memcmp(&range_values[off], &src[r.begin()], r.span_size() * sizeof(CppType)), 0);
                off += r.span_size();
            }
        }

        // read_by_rowids.
        {
            AlpPageDecoder<Type> rowid_decoder(decoded);
            ASSERT_TRUE(rowid_decoder.init().ok());
            rowid_t rowids[] = {0, (rowid_t)(size / 2), (rowid_t)(size - 1)};
            size_t num_read = 3;
            auto dst = ChunkFactory::column_from_field_type(Type, false);
            ASSERT_TRUE(rowid_decoder.read_by_rowids(0, rowids, &num_read, dst.get()).ok());
            ASSERT_EQ(3, num_read);
            const auto rowid_values = GetStorageContainer<Type>::get_data(dst);
            ASSERT_EQ(memcmp(&rowid_values[0], &src[0], sizeof(CppType)), 0);
            ASSERT_EQ(memcmp(&rowid_values[1], &src[size / 2], sizeof(CppType)), 0);
            ASSERT_EQ(memcmp(&rowid_values[2], &src[size - 1], sizeof(CppType)), 0);
        }
    }
};

// NOLINTNEXTLINE
TEST_F(AlpPageTest, RoundtripSensorDouble) {
    auto data = gen_sensor_f64(10000);
    test_roundtrip<TYPE_DOUBLE>(data.data(), data.size());
}

// NOLINTNEXTLINE
TEST_F(AlpPageTest, RoundtripCpuFloat) {
    auto data = gen_cpu_f32(10000);
    test_roundtrip<TYPE_FLOAT>(data.data(), data.size());
}

// NOLINTNEXTLINE
TEST_F(AlpPageTest, RoundtripEqualDouble) {
    std::vector<double> data(10000, 19880217.19890323);
    test_roundtrip<TYPE_DOUBLE>(data.data(), data.size());
}

// NOLINTNEXTLINE
TEST_F(AlpPageTest, RoundtripSequenceDouble) {
    std::vector<double> data(10000);
    double base = 19880217.25;
    for (auto& v : data) {
        base += 13.25;
        v = base;
    }
    test_roundtrip<TYPE_DOUBLE>(data.data(), data.size());
}

// Special values go through the ALP exception path and must round-trip
// bit-exactly.
// NOLINTNEXTLINE
TEST_F(AlpPageTest, RoundtripSpecialValues) {
    auto data = gen_sensor_f64(5000);
    data[7] = std::numeric_limits<double>::quiet_NaN();
    data[100] = std::numeric_limits<double>::infinity();
    data[1024] = -std::numeric_limits<double>::infinity();
    data[2049] = -0.0;
    data[3000] = std::numeric_limits<double>::max();
    data[4001] = std::numeric_limits<double>::denorm_min();
    test_roundtrip<TYPE_DOUBLE>(data.data(), data.size());
}

// Random-mantissa doubles are incompressible for ALP and must fall back to
// per-vector raw storage while still round-tripping.
// NOLINTNEXTLINE
TEST_F(AlpPageTest, RoundtripIncompressibleFallback) {
    auto data = gen_random_f64(10000);
    test_roundtrip<TYPE_DOUBLE>(data.data(), data.size());
}

// Page sizes that are not multiples of the 1024-value ALP vector exercise the
// padded tail vector.
// NOLINTNEXTLINE
TEST_F(AlpPageTest, RoundtripPartialVector) {
    for (size_t n : {1UL, 5UL, 1023UL, 1025UL, 4097UL}) {
        auto data = gen_sensor_f64(n);
        test_roundtrip<TYPE_DOUBLE>(data.data(), data.size());
    }
}

// NOLINTNEXTLINE
TEST_F(AlpPageTest, SeekAtOrAfterValue) {
    const size_t size = 1000;
    std::vector<double> data(size);
    for (size_t i = 0; i < size; i++) {
        data[i] = 100.25 + (double)i;
    }
    PageBuilderOptions options;
    options.data_page_size = 256 * 1024;
    AlpPageBuilder<TYPE_DOUBLE> page_builder(options);
    ASSERT_EQ(size, page_builder.add(reinterpret_cast<const uint8_t*>(data.data()), size));
    OwnedSlice s = page_builder.finish()->build();

    std::unique_ptr<std::vector<uint8_t>> page;
    Slice decoded = pre_decode(s.slice(), &page);
    AlpPageDecoder<TYPE_DOUBLE> page_decoder(decoded);
    ASSERT_TRUE(page_decoder.init().ok());

    bool exact_match = false;
    double seek_value = data[123];
    ASSERT_TRUE(page_decoder.seek_at_or_after_value(&seek_value, &exact_match).ok());
    ASSERT_EQ(123, page_decoder.current_index());
    ASSERT_TRUE(exact_match);

    double small = 1.0;
    ASSERT_TRUE(page_decoder.seek_at_or_after_value(&small, &exact_match).ok());
    ASSERT_EQ(0, page_decoder.current_index());
    ASSERT_FALSE(exact_match);

    double big = 1e18;
    ASSERT_EQ(TStatusCode::NOT_FOUND, page_decoder.seek_at_or_after_value(&big, &exact_match).code());
}

// A corrupted or truncated ALP page header must be rejected by the page-load
// pre-decoder with a clear error instead of being misread.
// NOLINTNEXTLINE
TEST_F(AlpPageTest, CorruptedPageRejected) {
    auto data = gen_sensor_f64(3000);
    PageBuilderOptions options;
    options.data_page_size = 256 * 1024;
    AlpPageBuilder<TYPE_DOUBLE> page_builder(options);
    ASSERT_EQ(data.size(), page_builder.add(reinterpret_cast<const uint8_t*>(data.data()), data.size()));
    OwnedSlice s = page_builder.finish()->build();
    std::string good(s.slice().data, s.slice().size);

    auto decode = [](std::string page_bytes, uint32_t footer_size = 0) {
        Slice slice(page_bytes.data(), page_bytes.size());
        std::unique_ptr<std::vector<uint8_t>> page;
        PageFooterPB footer;
        footer.set_type(DATA_PAGE);
        footer.mutable_data_page_footer()->set_nullmap_size(0);
        return StoragePageDecoder::decode_page(&footer, footer_size, ALP_ENCODING, &page, &slice);
    };

    // Sanity: the untouched page decodes fine.
    ASSERT_TRUE(decode(good).ok());

    // Page smaller than the 16-byte header.
    ASSERT_FALSE(decode(good.substr(0, ALP_PAGE_HEADER_SIZE - 1)).ok());

    // Invalid size_of_element (header bytes [12,16)).
    {
        std::string bad = good;
        encode_fixed32_le(reinterpret_cast<uint8_t*>(bad.data()) + 12, 5);
        ASSERT_FALSE(decode(bad).ok());
    }

    // Padded element count that does not match num_elements (header bytes [8,12)).
    {
        std::string bad = good;
        encode_fixed32_le(reinterpret_cast<uint8_t*>(bad.data()) + 8, 12345);
        ASSERT_FALSE(decode(bad).ok());
    }

    // Encoded size larger than the page (header bytes [4,8)).
    {
        std::string bad = good;
        encode_fixed32_le(reinterpret_cast<uint8_t*>(bad.data()) + 4, good.size() + 100);
        ASSERT_FALSE(decode(bad).ok());
    }

    // Corrupted vector meta in the body: an impossible bit width.
    {
        std::string bad = good;
        bad[ALP_PAGE_HEADER_SIZE] = (char)0xFE;
        ASSERT_FALSE(decode(bad).ok());
    }

    // Corrupted vector meta in the body: scale indexes out of the supported
    // range must be rejected before they reach the FALP constant tables.
    {
        std::string bad = good;
        bad[ALP_PAGE_HEADER_SIZE + 2] = (char)0x7F; // exponent index
        ASSERT_FALSE(decode(bad).ok());
    }
    {
        std::string bad = good;
        bad[ALP_PAGE_HEADER_SIZE + 1] = (char)0x7F; // factor index > exponent
        ASSERT_FALSE(decode(bad).ok());
    }

    // Consistent but implausibly large element counts (more vectors than the
    // encoded body could possibly hold metadata for) must be rejected before
    // they drive the allocation.
    {
        std::string bad = good;
        encode_fixed32_le(reinterpret_cast<uint8_t*>(bad.data()) + 0, 100000000);
        encode_fixed32_le(reinterpret_cast<uint8_t*>(bad.data()) + 8,
                          (uint32_t)alppage::padded_element_count(100000000));
        ASSERT_FALSE(decode(bad).ok());
    }

    // Even with a body large enough to satisfy the per-vector metadata bound,
    // a decoded size past the writer's int32 data_page_size limit must be
    // rejected before the (multi-GiB) allocation.
    {
        const uint32_t huge_count = 536872960; // 4 GiB / 8, 1024-aligned
        std::string bad;
        bad.resize(ALP_PAGE_HEADER_SIZE + (size_t)huge_count / ALP_PAGE_VECTOR_SIZE * ALP_PAGE_VECTOR_META_SIZE, 0);
        encode_fixed32_le(reinterpret_cast<uint8_t*>(bad.data()) + 0, huge_count);
        encode_fixed32_le(reinterpret_cast<uint8_t*>(bad.data()) + 4, bad.size());
        encode_fixed32_le(reinterpret_cast<uint8_t*>(bad.data()) + 8, huge_count);
        encode_fixed32_le(reinterpret_cast<uint8_t*>(bad.data()) + 12, 8);
        ASSERT_FALSE(decode(bad).ok());
    }

    // A trailer (nullmap/footer) claimed to be larger than the bytes actually
    // present after the encoded body must be rejected, not copied.
    ASSERT_FALSE(decode(good, /*footer_size=*/8).ok());

    // Surplus bytes between the encoded body and the trailer must be rejected
    // too: the input has to be consumed exactly, otherwise the cached page
    // (whose footer is parsed from its end) is polluted.
    ASSERT_FALSE(decode(good + std::string(3, 'x')).ok());
}

// ============================================================================
// Performance comparison: ALP_ENCODING vs BIT_SHUFFLE on the same data,
// through the production page path (PageBuilder -> StoragePageDecoder ->
// PageDecoder -> Column). Prints a markdown report.
// ============================================================================

namespace {

struct PerfResult {
    size_t raw_bytes = 0;
    size_t encoded_bytes = 0;
    double encode_s = 0;
    double decode_s = 0; // page-load pre-decode (StoragePageDecoder)
    double scan_s = 0;   // PageDecoder next_batch into Column
};

template <typename F>
double time_min(F&& fn, int iters) {
    double best = 1e300;
    for (int i = 0; i < iters; i++) {
        auto t0 = std::chrono::steady_clock::now();
        fn();
        auto t1 = std::chrono::steady_clock::now();
        best = std::min(best, std::chrono::duration<double>(t1 - t0).count());
    }
    return best;
}

template <LogicalType Type, class BuilderT, class DecoderT>
PerfResult run_perf(EncodingTypePB encoding, const StorageCppType<Type>* data, size_t n) {
    using CppType = StorageCppType<Type>;
    constexpr int ENC_ITERS = 5;
    constexpr int DEC_ITERS = 10;

    PerfResult res;
    res.raw_bytes = n * sizeof(CppType);

    PageBuilderOptions options; // default 64KB data pages, same as production
    options.data_page_size = config::data_page_size;

    // Build all pages once, keeping the encoded bytes.
    std::vector<std::string> pages;
    {
        BuilderT builder(options);
        size_t added = 0;
        while (added < n) {
            added += builder.add(reinterpret_cast<const uint8_t*>(data + added), n - added);
            if (builder.is_page_full() || added == n) {
                faststring* fs = builder.finish();
                pages.emplace_back(reinterpret_cast<const char*>(fs->data()), fs->size());
                res.encoded_bytes += fs->size();
                builder.reset();
            }
        }
    }

    // Encode throughput: rebuild every page.
    res.encode_s = time_min(
            [&] {
                BuilderT builder(options);
                size_t added = 0;
                while (added < n) {
                    added += builder.add(reinterpret_cast<const uint8_t*>(data + added), n - added);
                    if (builder.is_page_full() || added == n) {
                        builder.finish();
                        builder.reset();
                    }
                }
            },
            ENC_ITERS);

    // Page-load decode (StoragePageDecoder pre-decode): the real scan-path
    // decode cost that runs once per page read from disk.
    res.decode_s = time_min(
            [&] {
                for (auto& p : pages) {
                    Slice slice(p.data(), p.size());
                    std::unique_ptr<std::vector<uint8_t>> page;
                    PageFooterPB footer;
                    footer.set_type(DATA_PAGE);
                    footer.mutable_data_page_footer()->set_nullmap_size(0);
                    Status st = StoragePageDecoder::decode_page(&footer, 0, encoding, &page, &slice);
                    CHECK(st.ok()) << st.to_string();
                }
            },
            DEC_ITERS);

    // Scan: pre-decode once, then repeatedly next_batch into a Column.
    std::vector<std::unique_ptr<std::vector<uint8_t>>> decoded_pages;
    std::vector<Slice> decoded_slices;
    for (auto& p : pages) {
        Slice slice(p.data(), p.size());
        auto page = std::make_unique<std::vector<uint8_t>>();
        PageFooterPB footer;
        footer.set_type(DATA_PAGE);
        footer.mutable_data_page_footer()->set_nullmap_size(0);
        CHECK(StoragePageDecoder::decode_page(&footer, 0, encoding, &page, &slice).ok());
        decoded_pages.emplace_back(std::move(page));
        decoded_slices.emplace_back(slice);
    }
    auto column = ChunkFactory::column_from_field_type(Type, false);
    column->reserve(n);
    res.scan_s = time_min(
            [&] {
                column->resize(0);
                for (auto& slice : decoded_slices) {
                    DecoderT decoder(slice);
                    CHECK(decoder.init().ok());
                    size_t cnt = decoder.count();
                    CHECK(decoder.next_batch(&cnt, column.get()).ok());
                }
            },
            DEC_ITERS);

    // Round-trip verification through the timed path.
    CHECK_EQ(n, column->size());
    const auto values = GetStorageContainer<Type>::get_data(column);
    CHECK_EQ(0, memcmp(values.data(), data, res.raw_bytes)) << "perf path round-trip mismatch";
    return res;
}

void print_perf_row(const char* column, const char* codec, const PerfResult& r) {
    printf("| %s | %s | %.2fx | %.0f | %.0f | %.0f |\n", column, codec, (double)r.raw_bytes / (double)r.encoded_bytes,
           r.raw_bytes / r.encode_s / 1e6, r.raw_bytes / r.decode_s / 1e6, r.raw_bytes / r.scan_s / 1e6);
    fflush(stdout);
}

} // namespace

// NOLINTNEXTLINE
TEST_F(AlpPageTest, PerfCompareWithBitshuffle) {
    constexpr size_t N = 1000000;
    auto f64_data = gen_sensor_f64(N);
    auto f32_data = gen_cpu_f32(N);
    auto rnd_data = gen_random_f64(N);

    printf("\n=== ALP vs BIT_SHUFFLE page path performance (%zu rows, %d-byte pages) ===\n", N,
           (int)config::data_page_size);
    printf("| Column | Encoding | Ratio | Enc MB/s | PageDecode MB/s | Scan MB/s |\n");
    printf("|---|---|---:|---:|---:|---:|\n");

    auto bs_f64 = run_perf<TYPE_DOUBLE, BitshufflePageBuilder<TYPE_DOUBLE>, BitShufflePageDecoder<TYPE_DOUBLE>>(
            BIT_SHUFFLE, f64_data.data(), N);
    print_perf_row("SensorValue(f64)", "BIT_SHUFFLE", bs_f64);
    auto alp_f64 = run_perf<TYPE_DOUBLE, AlpPageBuilder<TYPE_DOUBLE>, AlpPageDecoder<TYPE_DOUBLE>>(ALP_ENCODING,
                                                                                                   f64_data.data(), N);
    print_perf_row("SensorValue(f64)", "ALP_ENCODING", alp_f64);

    auto bs_f32 = run_perf<TYPE_FLOAT, BitshufflePageBuilder<TYPE_FLOAT>, BitShufflePageDecoder<TYPE_FLOAT>>(
            BIT_SHUFFLE, f32_data.data(), N);
    print_perf_row("CpuPct(f32)", "BIT_SHUFFLE", bs_f32);
    auto alp_f32 = run_perf<TYPE_FLOAT, AlpPageBuilder<TYPE_FLOAT>, AlpPageDecoder<TYPE_FLOAT>>(ALP_ENCODING,
                                                                                                f32_data.data(), N);
    print_perf_row("CpuPct(f32)", "ALP_ENCODING", alp_f32);

    auto bs_rnd = run_perf<TYPE_DOUBLE, BitshufflePageBuilder<TYPE_DOUBLE>, BitShufflePageDecoder<TYPE_DOUBLE>>(
            BIT_SHUFFLE, rnd_data.data(), N);
    print_perf_row("RandomMantissa(f64)", "BIT_SHUFFLE", bs_rnd);
    auto alp_rnd = run_perf<TYPE_DOUBLE, AlpPageBuilder<TYPE_DOUBLE>, AlpPageDecoder<TYPE_DOUBLE>>(ALP_ENCODING,
                                                                                                   rnd_data.data(), N);
    print_perf_row("RandomMantissa(f64)", "ALP_ENCODING", alp_rnd);

    // ALP must beat bitshuffle on compression for decimal-like floats.
    EXPECT_LT(alp_f64.encoded_bytes, bs_f64.encoded_bytes);
    // The raw fallback must not blow up incompressible data (allow 2% meta overhead).
    EXPECT_LT(alp_rnd.encoded_bytes, rnd_data.size() * sizeof(double) * 102 / 100);
}

// The write-side config gate: default encoding stays BIT_SHUFFLE unless
// enable_alp_float_encoding is set.
// NOLINTNEXTLINE
TEST_F(AlpPageTest, DefaultEncodingGatedByConfig) {
    ASSERT_EQ(BIT_SHUFFLE, EncodingInfo::get_default_encoding(TYPE_DOUBLE, false));
    ASSERT_EQ(BIT_SHUFFLE, EncodingInfo::get_default_encoding(TYPE_FLOAT, false));

    config::enable_alp_float_encoding = true;
    ASSERT_EQ(ALP_ENCODING, EncodingInfo::get_default_encoding(TYPE_DOUBLE, false));
    ASSERT_EQ(ALP_ENCODING, EncodingInfo::get_default_encoding(TYPE_FLOAT, false));
    // Types other than FLOAT/DOUBLE are unaffected.
    ASSERT_EQ(BIT_SHUFFLE, EncodingInfo::get_default_encoding(TYPE_BIGINT, false));
    config::enable_alp_float_encoding = false;

    ASSERT_EQ(BIT_SHUFFLE, EncodingInfo::get_default_encoding(TYPE_DOUBLE, false));

    // Read side is registered regardless of the config.
    const EncodingInfo* info = nullptr;
    ASSERT_TRUE(EncodingInfo::get(TYPE_DOUBLE, ALP_ENCODING, &info).ok());
    ASSERT_NE(nullptr, info);
    ASSERT_TRUE(EncodingInfo::get(TYPE_FLOAT, ALP_ENCODING, &info).ok());
    ASSERT_NE(nullptr, info);
    // ...but not for non-float types.
    ASSERT_FALSE(EncodingInfo::get(TYPE_BIGINT, ALP_ENCODING, &info).ok());
}

} // namespace starrocks
