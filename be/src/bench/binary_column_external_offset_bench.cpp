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

#include <arrow/api.h>
#include <arrow/util/bit_util.h>
#include <parquet/schema.h>
#include <parquet/types.h>

#include <algorithm>
#include <cstring>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "bench/binary_column_regression_bench.h"
#include "column/append_with_mask.h"
#include "column/chunk.h"
#include "column/column_builder.h"
#include "column/column_hash/column_hash.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/schema.h"
#include "common/config.h"
#include "common/memory/mem_hook_allocator.h"
#include "common/object_pool.h"
#include "common/system/cpu_info.h"
#include "exec/arrow_to_starrocks_converter.h"
#include "exec/join/join_hash_table.h"
#include "exec/sorting/sort_permute.h"
#include "exprs/agg/aggregate_state_allocator.h"
#include "exprs/agg/aggregate_factory.h"
#include "exprs/array_functions.h"
#include "exprs/cast_expr.h"
#include "exprs/expr.h"
#include "exprs/function_context.h"
#include "exprs/like_predicate.h"
#include "exprs/string_functions.h"
#include "formats/orc/column_reader.h"
#include "formats/orc/orc_chunk_reader.h"
#include "formats/parquet/encoding_dict.h"
#include "formats/parquet/encoding_plain.h"
#include "formats/parquet/level_builder.h"
#include "gen_cpp/olap_file.pb.h"
#include "gutil/strings/fastmem.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"
#include "serde/column_array_serde.h"
#include "storage/chunk_helper.h"
#include "storage/column_predicate.h"
#include "storage/row_store_encoder_simple.h"
#include "storage/rowset/binary_plain_page.h"
#include "storage/types.h"

namespace starrocks::bench {
namespace {

static constexpr const char* kSuite = "BinaryColumnExternalOffset";

class StaticColumnExpr final : public Expr {
public:
    StaticColumnExpr(TypeDescriptor type, ColumnPtr column)
            : Expr(std::move(type), false), _column(std::move(column)) {}

    StatusOr<ColumnPtr> evaluate_checked(ExprContext*, Chunk*) override { return _column; }

    Expr* clone(ObjectPool* pool) const override { return pool->add(new StaticColumnExpr(*this)); }

    bool is_constant() const override { return false; }

private:
    ColumnPtr _column;
};

enum class HashMode {
    RANGE,
    SELECTION_RANDOM_50,
    SELECTIVE_CLUSTERED,
};

enum class PermutePattern {
    SEQ,
    REVERSE,
    RANDOM,
    CLUSTERED,
};

enum class SparsePattern {
    CONTIGUOUS,
    CLUSTERED_50,
};

enum class ArrowStringKind {
    STRING,
    LARGE_STRING,
    BINARY,
    LARGE_BINARY,
    FIXED_SIZE_BINARY,
};

enum class JoinKeyMode {
    FIXED_SIZE,
    SERIALIZED_VARCHAR,
};

static const char* hash_mode_name(HashMode mode) {
    switch (mode) {
    case HashMode::RANGE:
        return "range";
    case HashMode::SELECTION_RANDOM_50:
        return "selection_random50";
    case HashMode::SELECTIVE_CLUSTERED:
        return "selective_clustered_full_count";
    }
    return "unknown";
}

static const char* permute_pattern_name(PermutePattern pattern) {
    switch (pattern) {
    case PermutePattern::SEQ:
        return "seq";
    case PermutePattern::REVERSE:
        return "reverse";
    case PermutePattern::RANDOM:
        return "random";
    case PermutePattern::CLUSTERED:
        return "clustered";
    }
    return "unknown";
}

static const char* sparse_pattern_name(SparsePattern pattern) {
    switch (pattern) {
    case SparsePattern::CONTIGUOUS:
        return "contiguous";
    case SparsePattern::CLUSTERED_50:
        return "clustered50";
    }
    return "unknown";
}

static const char* arrow_string_kind_name(ArrowStringKind kind) {
    switch (kind) {
    case ArrowStringKind::STRING:
        return "string";
    case ArrowStringKind::LARGE_STRING:
        return "large_string";
    case ArrowStringKind::BINARY:
        return "binary";
    case ArrowStringKind::LARGE_BINARY:
        return "large_binary";
    case ArrowStringKind::FIXED_SIZE_BINARY:
        return "fixed_size_binary";
    }
    return "unknown";
}

static const char* join_key_mode_name(JoinKeyMode mode) {
    switch (mode) {
    case JoinKeyMode::FIXED_SIZE:
        return "fixed_size";
    case JoinKeyMode::SERIALIZED_VARCHAR:
        return "serialized_varchar";
    }
    return "unknown";
}

static const char* bool_name(bool value) {
    return value ? "true" : "false";
}

static std::string ext_case_name(const std::string& priority, const std::string& op, const std::string& params) {
    return std::string(kSuite) + "/" + priority + "/" + op + "/" + params;
}

static size_t count_selected(const std::vector<uint8_t>& mask, bool positive) {
    size_t selected = 0;
    for (uint8_t value : mask) {
        selected += positive ? (value != 0) : (value == 0);
    }
    return selected;
}

static std::vector<uint8_t> make_u8_mask(size_t rows, FilterPattern pattern) {
    auto filter = make_filter(rows, pattern);
    return std::vector<uint8_t>(filter.begin(), filter.end());
}

static std::vector<uint8_t> make_selection(size_t rows, FilterPattern pattern) {
    return make_u8_mask(rows, pattern);
}

static std::vector<uint16_t> make_u16_selection(size_t rows, IndexPattern pattern) {
    CHECK_LE(rows, static_cast<size_t>(std::numeric_limits<uint16_t>::max()));
    std::vector<uint16_t> sel(rows);
    for (size_t i = 0; i < rows; ++i) {
        uint32_t idx = 0;
        switch (pattern) {
        case IndexPattern::SEQ:
            idx = i;
            break;
        case IndexPattern::REVERSE:
            idx = rows - 1 - i;
            break;
        case IndexPattern::RANDOM:
        case IndexPattern::RANDOM_10:
        case IndexPattern::DUPLICATE:
            idx = pseudo_random(i) % rows;
            break;
        case IndexPattern::CLUSTERED:
            idx = ((i / 64) * 128 + i % 64) % rows;
            break;
        }
        sel[i] = static_cast<uint16_t>(idx);
    }
    return sel;
}

static SmallPermutation make_small_permutation(size_t rows, PermutePattern pattern) {
    SmallPermutation perm(rows);

    std::vector<uint32_t> indexes(rows);
    std::iota(indexes.begin(), indexes.end(), 0);
    switch (pattern) {
    case PermutePattern::SEQ:
        break;
    case PermutePattern::REVERSE:
        std::reverse(indexes.begin(), indexes.end());
        break;
    case PermutePattern::RANDOM:
        for (size_t i = rows; i > 1; --i) {
            const size_t j = pseudo_random(i) % i;
            std::swap(indexes[i - 1], indexes[j]);
        }
        break;
    case PermutePattern::CLUSTERED:
        for (size_t begin = 0; begin < rows; begin += 128) {
            const size_t mid = std::min(begin + 64, rows);
            const size_t end = std::min(begin + 128, rows);
            std::rotate(indexes.begin() + begin, indexes.begin() + mid, indexes.begin() + end);
        }
        break;
    }

    for (size_t i = 0; i < rows; ++i) {
        perm[i].index_in_chunk = indexes[i];
    }
    return perm;
}

static void fill_unique_string(std::string* str, size_t len, size_t value) {
    static constexpr char kAlphabet[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    CHECK_GT(len, 0);
    str->assign(len, '0');
    for (size_t i = 0; i < len; ++i) {
        (*str)[len - 1 - i] = kAlphabet[value % (sizeof(kAlphabet) - 1)];
        value /= sizeof(kAlphabet) - 1;
    }
}

static BinaryColumn::MutablePtr make_unique_fixed_column(size_t rows, size_t len, size_t seed = 0) {
    auto column = BinaryColumn::create();
    column->reserve(rows, rows * len);

    std::string value(len, 'x');
    for (size_t i = 0; i < rows; ++i) {
        fill_unique_string(&value, len, seed + i);
        column->append(Slice(value));
    }
    CHECK_LT(column->get_bytes().size(), static_cast<size_t>(std::numeric_limits<uint32_t>::max()));
    return column;
}

static BinaryColumn::MutablePtr make_column_with_empty_ratio(size_t rows, size_t len, int empty_percent) {
    CHECK_LE(empty_percent, 100);
    auto column = BinaryColumn::create();
    const size_t non_empty_rows = empty_percent == 100 ? 0 : rows;
    column->reserve(rows, non_empty_rows * len);

    std::string value(len, 'x');
    for (size_t i = 0; i < rows; ++i) {
        const bool empty = empty_percent == 100 || (empty_percent != 0 && pseudo_random(i) % 100 < empty_percent);
        if (empty) {
            column->append(Slice());
        } else {
            fill_string(&value, len, i, false);
            column->append(Slice(value));
        }
    }
    CHECK_LT(column->get_bytes().size(), static_cast<size_t>(std::numeric_limits<uint32_t>::max()));
    return column;
}

static LargeBinaryColumn::MutablePtr make_large_fixed_column(size_t rows, size_t len) {
    auto column = LargeBinaryColumn::create();
    column->reserve(rows, rows * len);

    std::string value(len, 'x');
    for (size_t i = 0; i < rows; ++i) {
        fill_string(&value, len, i, false);
        column->append(Slice(value));
    }
    CHECK_LT(column->get_bytes().size(), static_cast<size_t>(std::numeric_limits<uint32_t>::max()));
    return column;
}

static MutableColumnPtr make_predicate_column(size_t rows, size_t len, int empty_percent, bool nullable) {
    auto data = make_column_with_empty_ratio(rows, len, empty_percent);
    if (!nullable) {
        return data;
    }

    auto nulls = NullColumn::create(rows, 0);
    auto& null_data = nulls->get_data();
    for (size_t i = 0; i < rows; ++i) {
        null_data[i] = static_cast<uint8_t>(pseudo_random(i + 17) % 10 == 0);
    }
    auto column = NullableColumn::create(std::move(data), std::move(nulls));
    column->set_has_null(true);
    return column;
}

static SparseRange<> make_sparse_range(size_t rows, SparsePattern pattern) {
    SparseRange<> range;
    switch (pattern) {
    case SparsePattern::CONTIGUOUS:
        range.add(Range<>(0, rows));
        break;
    case SparsePattern::CLUSTERED_50:
        for (size_t begin = 0; begin < rows; begin += 128) {
            range.add(Range<>(begin, std::min(rows, begin + 64)));
        }
        break;
    }
    return range;
}

struct BinaryPageFixture {
    OwnedSlice page;
    std::vector<size_t> prefix_bytes;
    size_t rows = 0;
    size_t bytes = 0;
};

static BinaryPageFixture make_binary_page(size_t rows, size_t len, LenPattern pattern) {
    auto block = make_slice_block(rows, len, pattern);

    PageBuilderOptions options;
    options.data_page_size = static_cast<uint32_t>(block.total_bytes + rows * sizeof(uint32_t) + 4096);
    BinaryPlainPageBuilder builder(options);
    const auto added = builder.add(reinterpret_cast<const uint8_t*>(block.slices.data()), block.slices.size());
    CHECK_EQ(added, block.slices.size());

    BinaryPageFixture fixture;
    fixture.page = builder.finish()->build();
    fixture.prefix_bytes.resize(rows + 1);
    for (size_t i = 0; i < rows; ++i) {
        fixture.prefix_bytes[i + 1] = fixture.prefix_bytes[i] + block.slices[i].size;
    }
    fixture.rows = rows;
    fixture.bytes = block.total_bytes;
    return fixture;
}

static size_t bytes_for_sparse_range(const BinaryPageFixture& fixture, const SparseRange<>& range) {
    size_t bytes = 0;
    size_t to_read = range.span_size();
    auto iter = range.new_iterator();
    while (to_read > 0) {
        Range<> r = iter.next(to_read);
        bytes += fixture.prefix_bytes[r.end()] - fixture.prefix_bytes[r.begin()];
        to_read -= r.span_size();
    }
    return bytes;
}

struct ParquetPlainFixture {
    std::string encoded;
    parquet::NullInfos null_infos;
    size_t rows = 0;
    size_t non_null_rows = 0;
    size_t bytes = 0;
};

static ParquetPlainFixture make_parquet_plain_fixture(size_t rows, size_t len, bool nullable,
                                                      FilterPattern null_pattern) {
    auto block = make_slice_block(rows, len, LenPattern::FIXED);
    auto nulls = nullable ? make_filter(rows, null_pattern) : Filter(rows);

    parquet::PlainEncoder<Slice> encoder;
    std::vector<Slice> non_null_slices;
    non_null_slices.reserve(rows);

    ParquetPlainFixture fixture;
    fixture.rows = rows;
    if (nullable) {
        fixture.null_infos.reset_with_capacity(rows);
    }

    uint8_t prev_null = 0;
    for (size_t i = 0; i < rows; ++i) {
        const uint8_t is_null = nullable ? nulls[i] : 0;
        if (nullable) {
            fixture.null_infos.nulls_data()[i] = is_null;
            fixture.null_infos.num_nulls += is_null;
            fixture.null_infos.num_ranges += (i == 0 || is_null != prev_null);
            prev_null = is_null;
        }
        if (!is_null) {
            non_null_slices.push_back(block.slices[i]);
            fixture.non_null_rows++;
            fixture.bytes += block.slices[i].size;
        }
    }

    auto st = encoder.append(reinterpret_cast<const uint8_t*>(non_null_slices.data()), non_null_slices.size());
    CHECK(st.ok()) << st.to_string();
    auto encoded = encoder.build();
    fixture.encoded.assign(encoded.data, encoded.size);
    return fixture;
}

static ParquetPlainFixture make_parquet_flba_fixture(size_t rows, size_t len, bool nullable,
                                                     FilterPattern null_pattern) {
    auto block = make_slice_block(rows, len, LenPattern::FIXED);
    auto nulls = nullable ? make_filter(rows, null_pattern) : Filter(rows);

    parquet::FLBAPlainEncoder encoder;
    std::vector<Slice> non_null_slices;
    non_null_slices.reserve(rows);

    ParquetPlainFixture fixture;
    fixture.rows = rows;
    if (nullable) {
        fixture.null_infos.reset_with_capacity(rows);
    }

    uint8_t prev_null = 0;
    for (size_t i = 0; i < rows; ++i) {
        const uint8_t is_null = nullable ? nulls[i] : 0;
        if (nullable) {
            fixture.null_infos.nulls_data()[i] = is_null;
            fixture.null_infos.num_nulls += is_null;
            fixture.null_infos.num_ranges += (i == 0 || is_null != prev_null);
            prev_null = is_null;
        }
        if (!is_null) {
            non_null_slices.push_back(block.slices[i]);
            fixture.non_null_rows++;
            fixture.bytes += block.slices[i].size;
        }
    }

    auto st = encoder.append(reinterpret_cast<const uint8_t*>(non_null_slices.data()), non_null_slices.size());
    CHECK(st.ok()) << st.to_string();
    auto encoded = encoder.build();
    fixture.encoded.assign(encoded.data, encoded.size);
    return fixture;
}

struct ParquetDictFixture {
    std::string dict_encoded;
    std::string data_encoded;
    parquet::NullInfos null_infos;
    size_t rows = 0;
    size_t non_null_rows = 0;
    size_t bytes = 0;
    size_t num_dicts = 0;
};

static ParquetDictFixture make_parquet_dict_fixture(size_t rows, size_t len, size_t cardinality, bool nullable,
                                                    FilterPattern null_pattern) {
    auto nulls = nullable ? make_filter(rows, null_pattern) : Filter(rows);
    std::vector<std::string> dict_values(cardinality);
    std::vector<Slice> dict_slices;
    dict_slices.reserve(cardinality);
    for (size_t i = 0; i < cardinality; ++i) {
        fill_unique_string(&dict_values[i], len, i);
        dict_slices.emplace_back(dict_values[i]);
    }

    parquet::DictEncoder<Slice> encoder;
    std::vector<Slice> non_null_slices;
    non_null_slices.reserve(rows);

    ParquetDictFixture fixture;
    fixture.rows = rows;
    if (nullable) {
        fixture.null_infos.reset_with_capacity(rows);
    }

    uint8_t prev_null = 0;
    for (size_t i = 0; i < rows; ++i) {
        const uint8_t is_null = nullable ? nulls[i] : 0;
        if (nullable) {
            fixture.null_infos.nulls_data()[i] = is_null;
            fixture.null_infos.num_nulls += is_null;
            fixture.null_infos.num_ranges += (i == 0 || is_null != prev_null);
            prev_null = is_null;
        }
        if (!is_null) {
            const auto& slice = dict_slices[(fixture.non_null_rows * 1315423911ULL) % cardinality];
            non_null_slices.push_back(slice);
            fixture.non_null_rows++;
            fixture.bytes += slice.size;
        }
    }

    auto st = encoder.append(reinterpret_cast<const uint8_t*>(non_null_slices.data()), non_null_slices.size());
    CHECK(st.ok()) << st.to_string();

    parquet::PlainEncoder<Slice> dict_encoder;
    st = encoder.encode_dict(&dict_encoder, &fixture.num_dicts);
    CHECK(st.ok()) << st.to_string();
    CHECK_EQ(fixture.num_dicts, cardinality);

    auto dict_encoded = dict_encoder.build();
    auto data_encoded = encoder.build();
    fixture.dict_encoded.assign(dict_encoded.data, dict_encoded.size);
    fixture.data_encoded.assign(data_encoded.data, data_encoded.size);
    return fixture;
}

template <bool PositiveSelect>
static void bench_append_with_mask_external(benchmark::State& state, size_t rows, size_t len, FilterPattern pattern,
                                            LenPattern len_pattern = LenPattern::FIXED) {
    auto src = make_column(rows, len, len_pattern);
    auto mask = make_u8_mask(rows, pattern);
    const size_t selected = count_selected(mask, PositiveSelect);
    const size_t input_bytes = src->get_bytes().size() + rows * sizeof(uint8_t);
    size_t selected_bytes = 0;
    for (size_t i = 0; i < rows; ++i) {
        const bool keep = PositiveSelect ? (mask[i] != 0) : (mask[i] == 0);
        if (keep) {
            selected_bytes += src->get_slice(i).size;
        }
    }

    for (auto _ : state) {
        state.PauseTiming();
        {
            auto dst = BinaryColumn::create();
            dst->reserve(selected, selected_bytes);
            state.ResumeTiming();

            auto st = append_with_mask<PositiveSelect>(dst.get(), *src, mask.data(), rows);
            CHECK(st.ok()) << st.to_string();
            benchmark::DoNotOptimize(dst.get());
            benchmark::ClobberMemory();
            state.PauseTiming();
        }
        state.ResumeTiming();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * input_bytes));
    state.counters["selected_rows_per_iter"] = static_cast<double>(selected);
    state.counters["selected_bytes_per_iter"] = static_cast<double>(selected_bytes);
}

static void bench_column_hash_external(benchmark::State& state, size_t rows, size_t len, HashMode mode,
                                       LenPattern len_pattern = LenPattern::FIXED) {
    CHECK(mode == HashMode::RANGE || rows <= static_cast<size_t>(std::numeric_limits<uint16_t>::max()));
    auto column = make_column(rows, len, len_pattern);
    std::vector<uint32_t> hashes(rows, 0);
    std::vector<uint8_t> selection;
    std::vector<uint16_t> sel;
    size_t hashed_rows = rows;
    size_t hashed_bytes = column->get_bytes().size();
    if (mode == HashMode::SELECTION_RANDOM_50) {
        selection = make_selection(rows, FilterPattern::RANDOM_50);
        hashed_rows = count_selected(selection, true);
        hashed_bytes = 0;
        for (size_t i = 0; i < rows; ++i) {
            if (selection[i] != 0) {
                hashed_bytes += column->get_slice(i).size;
            }
        }
    } else if (mode == HashMode::SELECTIVE_CLUSTERED) {
        sel = make_u16_selection(rows, IndexPattern::CLUSTERED);
        hashed_rows = sel.size();
        hashed_bytes = 0;
        for (uint16_t idx : sel) {
            hashed_bytes += column->get_slice(idx).size;
        }
    }
    size_t processed_bytes = hashed_bytes;
    if (mode == HashMode::SELECTION_RANDOM_50) {
        processed_bytes += rows * sizeof(uint8_t);
    } else if (mode == HashMode::SELECTIVE_CLUSTERED) {
        processed_bytes += sel.size() * sizeof(uint16_t);
    }

    for (auto _ : state) {
        state.PauseTiming();
        std::fill(hashes.begin(), hashes.end(), 0);
        state.ResumeTiming();
        switch (mode) {
        case HashMode::RANGE:
            column->crc32_hash(hashes.data(), 0, rows);
            break;
        case HashMode::SELECTION_RANDOM_50:
            column->crc32_hash_with_selection(hashes.data(), selection.data(), 0, static_cast<uint16_t>(rows));
            break;
        case HashMode::SELECTIVE_CLUSTERED:
            column->crc32_hash_selective(hashes.data(), sel.data(), static_cast<uint16_t>(sel.size()));
            break;
        }
        benchmark::DoNotOptimize(hashes.data());
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * processed_bytes));
    state.counters["hashed_rows_per_iter"] = static_cast<double>(hashed_rows);
    state.counters["hashed_bytes_per_iter"] = static_cast<double>(hashed_bytes);
}

static void bench_sort_permute_external(benchmark::State& state, size_t rows, size_t len, PermutePattern pattern,
                                        LenPattern len_pattern = LenPattern::FIXED) {
    auto src = make_column(rows, len, len_pattern);
    auto perm = make_small_permutation(rows, pattern);

    for (auto _ : state) {
        state.PauseTiming();
        {
            auto dst = BinaryColumn::create();
            dst->reserve(rows, rows * len);
            state.ResumeTiming();

            materialize_column_by_permutation_single(dst.get(), src.get(), SmallPermutationView(perm.data(), rows));
            benchmark::DoNotOptimize(dst.get());
            benchmark::ClobberMemory();
            state.PauseTiming();
        }
        state.ResumeTiming();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * src->get_bytes().size()));
}

static void bench_column_predicate_empty_ne(benchmark::State& state, size_t rows, size_t len, int empty_percent,
                                            bool nullable) {
    CHECK_LE(rows, static_cast<size_t>(std::numeric_limits<uint16_t>::max()));
    auto column = make_predicate_column(rows, len, empty_percent, nullable);
    std::unique_ptr<ColumnPredicate> predicate(new_column_ne_predicate(get_type_info(TYPE_VARCHAR), 0, Slice()));
    std::vector<uint16_t> base_sel(rows);
    std::iota(base_sel.begin(), base_sel.end(), 0);
    std::vector<uint16_t> sel(rows);

    for (auto _ : state) {
        state.PauseTiming();
        std::memcpy(sel.data(), base_sel.data(), rows * sizeof(uint16_t));
        state.ResumeTiming();
        auto result = predicate->evaluate_branchless(column.get(), sel.data(), static_cast<uint16_t>(rows));
        CHECK(result.ok()) << result.status().to_string();
        benchmark::DoNotOptimize(result.value());
        benchmark::DoNotOptimize(sel.data());
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
    const size_t input_bytes = rows * len + rows * sizeof(uint16_t);
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * input_bytes));
    state.counters["input_bytes_per_iter"] = static_cast<double>(input_bytes);
}

static void bench_binary_plain_page_append_range(benchmark::State& state, size_t rows, size_t len, SparsePattern pattern,
                                                 LenPattern len_pattern = LenPattern::FIXED) {
    auto fixture = make_binary_page(rows, len, len_pattern);
    BinaryPlainPageDecoder<TYPE_VARCHAR> decoder(fixture.page.slice());
    auto st = decoder.init();
    CHECK(st.ok()) << st.to_string();
    auto range = make_sparse_range(rows, pattern);
    const size_t range_bytes = bytes_for_sparse_range(fixture, range);

    for (auto _ : state) {
        state.PauseTiming();
        {
            auto dst = BinaryColumn::create();
            dst->reserve(range.span_size(), range_bytes);
            auto seek_st = decoder.seek_to_position_in_page(0);
            CHECK(seek_st.ok()) << seek_st.to_string();
            state.ResumeTiming();

            auto next_st = decoder.next_batch(range, dst.get());
            CHECK(next_st.ok()) << next_st.to_string();
            benchmark::DoNotOptimize(dst.get());
            benchmark::ClobberMemory();
            state.PauseTiming();
        }
        state.ResumeTiming();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * range.span_size()));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * range_bytes));
}

static void bench_binary_plain_page_with_filter(benchmark::State& state, size_t rows, size_t len, SparsePattern pattern,
                                                bool nullable, LenPattern len_pattern = LenPattern::FIXED) {
    CHECK_LE(rows, static_cast<size_t>(std::numeric_limits<uint16_t>::max()));
    auto fixture = make_binary_page(rows, len, len_pattern);
    BinaryPlainPageDecoder<TYPE_VARCHAR> decoder(fixture.page.slice());
    auto st = decoder.init();
    CHECK(st.ok()) << st.to_string();
    auto range = make_sparse_range(rows, pattern);
    const size_t range_bytes = bytes_for_sparse_range(fixture, range);

    std::string threshold(len, 'm');
    std::unique_ptr<ColumnPredicate> predicate(
            new_column_ge_predicate(get_type_info(TYPE_VARCHAR), 0, Slice(threshold)));
    std::vector<const ColumnPredicate*> predicates{predicate.get()};
    auto nulls = nullable ? make_u8_mask(range.span_size(), FilterPattern::RANDOM_10) : std::vector<uint8_t>();
    std::vector<uint8_t> selection(range.span_size());
    std::vector<uint16_t> selected_idx(range.span_size());

    for (auto _ : state) {
        state.PauseTiming();
        {
            MutableColumnPtr dst;
            if (nullable) {
                dst = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
            } else {
                dst = BinaryColumn::create();
            }
            auto seek_st = decoder.seek_to_position_in_page(0);
            CHECK(seek_st.ok()) << seek_st.to_string();
            state.ResumeTiming();

            auto next_st =
                    decoder.next_batch_with_filter(dst.get(), range, predicates, nullable ? nulls.data() : nullptr,
                                                   selection.data(), selected_idx.data());
            CHECK(next_st.ok()) << next_st.to_string();
            benchmark::DoNotOptimize(dst.get());
            benchmark::ClobberMemory();
            state.PauseTiming();
        }
        state.ResumeTiming();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * range.span_size()));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * range_bytes));
}

static void bench_binary_plain_page_dict_filter_selection(benchmark::State& state, size_t rows, size_t len) {
    auto fixture = make_binary_page(rows, len, LenPattern::FIXED);
    std::string threshold(len, 'm');
    std::unique_ptr<ColumnPredicate> predicate(
            new_column_ge_predicate(get_type_info(TYPE_VARCHAR), 0, Slice(threshold)));
    std::vector<const ColumnPredicate*> predicates{predicate.get()};

    for (auto _ : state) {
        state.PauseTiming();
        {
            BinaryPlainPageDecoder<TYPE_VARCHAR> decoder(fixture.page.slice());
            auto init_st = decoder.init();
            CHECK(init_st.ok()) << init_st.to_string();
            state.ResumeTiming();

            const uint8_t* selection = nullptr;
            uint32_t dict_size = 0;
            uint32_t selected_count = 0;
            auto st = decoder.get_dict_filter_selection(predicates, &selection, &dict_size, &selected_count);
            CHECK(st.ok()) << st.to_string();
            benchmark::DoNotOptimize(selection);
            benchmark::DoNotOptimize(dict_size);
            benchmark::DoNotOptimize(selected_count);
            benchmark::ClobberMemory();
            state.PauseTiming();
        }
        state.ResumeTiming();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
}

static void bench_parquet_plain_decode(benchmark::State& state, size_t rows, size_t len, bool nullable,
                                       FilterPattern null_pattern) {
    auto fixture = make_parquet_plain_fixture(rows, len, nullable, null_pattern);

    for (auto _ : state) {
        state.PauseTiming();
        {
            parquet::PlainDecoder<Slice> decoder;
            auto set_st = decoder.set_data(Slice(fixture.encoded.data(), fixture.encoded.size()));
            CHECK(set_st.ok()) << set_st.to_string();
            MutableColumnPtr dst;
            if (nullable) {
                dst = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
            } else {
                dst = BinaryColumn::create();
            }
            state.ResumeTiming();

            Status st;
            if (nullable) {
                st = decoder.next_batch_with_nulls(rows, fixture.null_infos, parquet::VALUE, dst.get(), nullptr);
            } else {
                st = decoder.next_batch(rows, parquet::VALUE, dst.get(), nullptr);
            }
            CHECK(st.ok()) << st.to_string();
            benchmark::DoNotOptimize(dst.get());
            benchmark::ClobberMemory();
            state.PauseTiming();
        }
        state.ResumeTiming();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * fixture.bytes));
}

static void bench_parquet_plain_flba_decode(benchmark::State& state, size_t rows, size_t len, bool nullable,
                                            FilterPattern null_pattern) {
    auto fixture = make_parquet_flba_fixture(rows, len, nullable, null_pattern);

    for (auto _ : state) {
        state.PauseTiming();
        {
            parquet::FLBAPlainDecoder decoder;
            decoder.set_type_length(static_cast<int32_t>(len));
            auto set_st = decoder.set_data(Slice(fixture.encoded.data(), fixture.encoded.size()));
            CHECK(set_st.ok()) << set_st.to_string();
            MutableColumnPtr dst;
            if (nullable) {
                dst = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
            } else {
                dst = BinaryColumn::create();
            }
            state.ResumeTiming();

            Status st;
            if (nullable) {
                st = decoder.next_batch_with_nulls(rows, fixture.null_infos, parquet::VALUE, dst.get(), nullptr);
            } else {
                st = decoder.next_batch(rows, parquet::VALUE, dst.get(), nullptr);
            }
            CHECK(st.ok()) << st.to_string();
            benchmark::DoNotOptimize(dst.get());
            benchmark::ClobberMemory();
            state.PauseTiming();
        }
        state.ResumeTiming();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * fixture.bytes));
}

static void bench_parquet_dict_decode(benchmark::State& state, size_t rows, size_t len, size_t cardinality,
                                      bool nullable, FilterPattern null_pattern) {
    auto fixture = make_parquet_dict_fixture(rows, len, cardinality, nullable, null_pattern);
    parquet::PlainDecoder<Slice> dict_decoder;
    auto dict_st = dict_decoder.set_data(Slice(fixture.dict_encoded.data(), fixture.dict_encoded.size()));
    CHECK(dict_st.ok()) << dict_st.to_string();

    parquet::DictDecoder<Slice> decoder;
    auto set_dict_st = decoder.set_dict(config::vector_chunk_size, fixture.num_dicts, &dict_decoder);
    CHECK(set_dict_st.ok()) << set_dict_st.to_string();

    for (auto _ : state) {
        state.PauseTiming();
        {
            auto set_data_st = decoder.set_data(Slice(fixture.data_encoded.data(), fixture.data_encoded.size()));
            CHECK(set_data_st.ok()) << set_data_st.to_string();
            MutableColumnPtr dst;
            if (nullable) {
                dst = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
            } else {
                dst = BinaryColumn::create();
            }
            state.ResumeTiming();

            Status st;
            if (nullable) {
                st = decoder.next_batch_with_nulls(rows, fixture.null_infos, parquet::VALUE, dst.get(), nullptr);
            } else {
                st = decoder.next_batch(rows, parquet::VALUE, dst.get(), nullptr);
            }
            CHECK(st.ok()) << st.to_string();
            benchmark::DoNotOptimize(dst.get());
            benchmark::ClobberMemory();
            state.PauseTiming();
        }
        state.ResumeTiming();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * fixture.bytes));
}

static void bench_binary_plain_page_zero_copy_offsets_with_alloc(benchmark::State& state, size_t rows, size_t len) {
    auto fixture = make_binary_page(rows, len, LenPattern::FIXED);
    BinaryPlainPageDecoder<TYPE_VARCHAR> decoder(fixture.page.slice());
    auto st = decoder.init();
    CHECK(st.ok()) << st.to_string();
    const size_t offsets_bytes = (rows + 1) * sizeof(uint32_t);
    bool validated = false;

    for (auto _ : state) {
        state.PauseTiming();
        {
            BinaryColumn::Offsets offsets;
            state.ResumeTiming();
            decoder.get_offsets_for_zero_copy(offsets);
            state.PauseTiming();
            benchmark::DoNotOptimize(offsets.back());
            benchmark::ClobberMemory();
            if (!validated) {
                CHECK_EQ(offsets.size(), rows + 1);
                CHECK_EQ(offsets[0], 0);
                CHECK_EQ(offsets.back(), fixture.bytes);
                validated = true;
            }
        }
        state.ResumeTiming();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * offsets_bytes));
}

static void bench_binary_plain_page_zero_copy_offsets_reuse(benchmark::State& state, size_t rows, size_t len) {
    auto fixture = make_binary_page(rows, len, LenPattern::FIXED);
    BinaryPlainPageDecoder<TYPE_VARCHAR> decoder(fixture.page.slice());
    auto st = decoder.init();
    CHECK(st.ok()) << st.to_string();
    const size_t offsets_bytes = (rows + 1) * sizeof(uint32_t);

    BinaryColumn::Offsets offsets;
    offsets.reserve(rows + 1);
    bool validated = false;
    for (auto _ : state) {
        decoder.get_offsets_for_zero_copy(offsets);
        benchmark::DoNotOptimize(offsets.back());
        benchmark::ClobberMemory();
        if (!validated) {
            state.PauseTiming();
            CHECK_EQ(offsets.size(), rows + 1);
            CHECK_EQ(offsets[0], 0);
            CHECK_EQ(offsets.back(), fixture.bytes);
            validated = true;
            state.ResumeTiming();
        }
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * offsets_bytes));
}

static void bench_binary_plain_page_zero_copy_column_hash_consumer_only(benchmark::State& state, size_t rows, size_t len) {
    const bool old_zero_copy_config = config::enable_zero_copy_from_page_cache;
    config::enable_zero_copy_from_page_cache = true;

    auto fixture = make_binary_page(rows, len, LenPattern::FIXED);
    BinaryPlainPageDecoder<TYPE_VARCHAR> decoder(fixture.page.slice());
    auto st = decoder.init();
    CHECK(st.ok()) << st.to_string();
    std::vector<uint32_t> hashes(rows, 0);
    bool validated = false;

    for (auto _ : state) {
        state.PauseTiming();
        {
            BinaryColumn::Offsets offsets;
            decoder.get_offsets_for_zero_copy(offsets);
            if (!validated) {
                CHECK_EQ(offsets.size(), rows + 1);
                CHECK_EQ(offsets[0], 0);
                CHECK_EQ(offsets.back(), fixture.bytes);
            }
            ContainerResource resource;
            resource.set_data(decoder.get_raw_data());
            resource.set_length(decoder.get_data_length());
            auto column = BinaryColumn::create(std::move(resource), std::move(offsets));
            if (!validated) {
                CHECK_EQ(column->size(), rows);
                validated = true;
            }
            std::fill(hashes.begin(), hashes.end(), 0);
            state.ResumeTiming();

            column->crc32_hash(hashes.data(), 0, rows);
            benchmark::DoNotOptimize(column.get());
            benchmark::DoNotOptimize(hashes.data());
            benchmark::ClobberMemory();
            state.PauseTiming();
        }
        state.ResumeTiming();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * fixture.bytes));
    config::enable_zero_copy_from_page_cache = old_zero_copy_config;
}

static MutableColumnPtr make_serde_binary_column(size_t rows, size_t len, bool large_column) {
    if (large_column) {
        return make_large_fixed_column(rows, len);
    }
    return make_fixed_column(rows, len);
}

static MutableColumnPtr make_empty_serde_binary_column(bool large_column) {
    if (large_column) {
        return LargeBinaryColumn::create();
    }
    return BinaryColumn::create();
}

static void bench_column_array_serde_serialize(benchmark::State& state, size_t rows, size_t len, int encode_level,
                                               bool large_column = false) {
    auto column = make_serde_binary_column(rows, len, large_column);
    const int64_t max_size = serde::ColumnArraySerde::max_serialized_size(*column, encode_level);
    CHECK_GT(max_size, 0);
    std::vector<uint8_t> buffer(max_size);
    size_t serialized_bytes = 0;

    for (auto _ : state) {
        auto result = serde::ColumnArraySerde::serialize(*column, buffer.data(), false, encode_level);
        CHECK(result.ok()) << result.status().to_string();
        serialized_bytes = result.value() - buffer.data();
        benchmark::DoNotOptimize(result.value());
        benchmark::DoNotOptimize(buffer.data());
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * rows * len));
    state.counters["logical_bytes_per_iter"] = static_cast<double>(rows * len);
    state.counters["serialized_bytes_per_iter"] = static_cast<double>(serialized_bytes);
}

static void bench_column_array_serde_deserialize(benchmark::State& state, size_t rows, size_t len, int encode_level,
                                                 bool large_column = false) {
    auto column = make_serde_binary_column(rows, len, large_column);
    const int64_t max_size = serde::ColumnArraySerde::max_serialized_size(*column, encode_level);
    CHECK_GT(max_size, 0);
    std::vector<uint8_t> buffer(max_size);
    auto serialized = serde::ColumnArraySerde::serialize(*column, buffer.data(), false, encode_level);
    CHECK(serialized.ok()) << serialized.status().to_string();
    const auto* end = serialized.value();

    for (auto _ : state) {
        state.PauseTiming();
        {
            auto dst = make_empty_serde_binary_column(large_column);
            state.ResumeTiming();

            auto result = serde::ColumnArraySerde::deserialize(buffer.data(), end, dst.get(), false, encode_level);
            CHECK(result.ok()) << result.status().to_string();
            benchmark::DoNotOptimize(result.value());
            benchmark::DoNotOptimize(dst.get());
            benchmark::ClobberMemory();
            state.PauseTiming();
        }
        state.ResumeTiming();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * rows * len));
    state.counters["logical_bytes_per_iter"] = static_cast<double>(rows * len);
    state.counters["serialized_bytes_per_iter"] = static_cast<double>(end - buffer.data());
}

static void bench_nullable_binary_column_builder(benchmark::State& state, size_t rows, size_t len, bool nullable) {
    auto block = make_slice_block(rows, len, LenPattern::FIXED);
    auto nulls = nullable ? make_filter(rows, FilterPattern::RANDOM_10) : Filter();

    for (auto _ : state) {
        state.PauseTiming();
        {
            NullableBinaryColumnBuilder builder;
            builder.resize(rows, block.total_bytes);
            state.ResumeTiming();

            for (size_t i = 0; i < rows; ++i) {
                if (nullable && nulls[i] != 0) {
                    builder.set_null(i);
                    continue;
                }
                builder.append_partial(block.slices[i]);
                builder.append_complete(i);
            }
            auto column = builder.build_nullable_column();
            benchmark::DoNotOptimize(column.get());
            benchmark::ClobberMemory();
            state.PauseTiming();
        }
        state.ResumeTiming();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * block.total_bytes));
}

struct SegmentedBinaryColumnFixture {
    SegmentedChunkPtr chunk;
    SegmentedColumnPtr column;
};

static SegmentedBinaryColumnFixture make_segmented_binary_column(size_t num_segments, size_t segment_size, size_t len) {
    auto segmented_chunk = SegmentedChunk::create(segment_size);
    segmented_chunk->append_column(make_fixed_column(segment_size, len), 0);

    for (size_t i = 1; i < num_segments; ++i) {
        auto chunk = std::make_shared<Chunk>();
        chunk->append_column(make_fixed_column(segment_size, len), 0);
        segmented_chunk->append_chunk(chunk);
    }

    auto& segments = segmented_chunk->segments();
    if (!segments.empty() && segments.back()->num_rows() == 0) {
        segments.pop_back();
    }
    segmented_chunk->build_columns();
    return {segmented_chunk, segmented_chunk->get_column_by_slot_id(0)};
}

static void bench_segmented_column_clone_selective(benchmark::State& state, size_t num_segments, size_t segment_size,
                                                   size_t len, IndexPattern pattern) {
    const size_t rows = num_segments * segment_size;
    auto fixture = make_segmented_binary_column(num_segments, segment_size, len);
    auto indexes = make_indexes(rows, rows, pattern);

    for (auto _ : state) {
        {
            auto column = fixture.column->clone_selective(indexes.data(), 0, indexes.size());
            benchmark::DoNotOptimize(column.get());
            benchmark::ClobberMemory();
            state.PauseTiming();
        }
        state.ResumeTiming();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * rows * len));
}

template <typename ArrowType, bool Nullable>
static std::shared_ptr<arrow::Array> make_arrow_string_array(size_t rows, size_t len) {
    using OffsetType = typename ArrowType::offset_type;
    const size_t offsets_in_bytes = (rows + 1) * sizeof(OffsetType);
    auto offsets_buf = arrow::AllocateBuffer(offsets_in_bytes).ValueOrDie();
    auto* offsets = reinterpret_cast<OffsetType*>(offsets_buf->mutable_data());
    offsets[0] = 0;

    const size_t data_in_bytes = rows * len;
    auto data_buf = arrow::AllocateBuffer(data_in_bytes).ValueOrDie();
    auto* data = data_buf->mutable_data();

    std::shared_ptr<arrow::Buffer> null_bitmap;
    uint8_t* nulls = nullptr;
    if constexpr (Nullable) {
        null_bitmap = arrow::AllocateBuffer((rows + 7) / 8).ValueOrDie();
        nulls = null_bitmap->mutable_data();
    }

    OffsetType data_offset = 0;
    int64_t null_count = 0;
    std::string value(len, 'x');
    for (size_t i = 0; i < rows; ++i) {
        if constexpr (Nullable) {
            if (pseudo_random(i) % 10 == 0) {
                arrow::bit_util::ClearBit(nulls, static_cast<int64_t>(i));
                offsets[i + 1] = data_offset;
                ++null_count;
                continue;
            }
        }
        if constexpr (Nullable) {
            arrow::bit_util::SetBit(nulls, static_cast<int64_t>(i));
        }
        fill_string(&value, len, i, false);
        strings::memcpy_inlined(data + data_offset, value.data(), len);
        data_offset += static_cast<OffsetType>(len);
        offsets[i + 1] = data_offset;
    }

    using ArrayType = typename arrow::TypeTraits<ArrowType>::ArrayType;
    return std::make_shared<ArrayType>(static_cast<int64_t>(rows), std::move(offsets_buf), std::move(data_buf),
                                       std::move(null_bitmap), null_count);
}

template <ArrowTypeId AT, typename ArrowType, bool Nullable>
static void bench_arrow_string_converter_impl(benchmark::State& state, size_t rows, size_t len,
                                              int64_t source_offset = 0, bool use_array_slice = false) {
    auto owner = make_arrow_string_array<ArrowType, Nullable>(rows + source_offset, len);
    std::shared_ptr<arrow::Array> array = owner;
    if (use_array_slice) {
        array = owner->Slice(source_offset, rows);
    }
    using ArrayType = typename arrow::TypeTraits<ArrowType>::ArrayType;
    auto typed_array = std::static_pointer_cast<ArrayType>(array);
    const int64_t convert_start = use_array_slice ? 0 : source_offset;
    auto conv_func = get_arrow_converter(AT, TYPE_VARCHAR, Nullable, false);
    CHECK(conv_func != nullptr);

    bool validated = false;
    for (auto _ : state) {
        state.PauseTiming();
        {
            auto data_column = BinaryColumn::create();
            data_column->reserve(rows, rows * len);
            auto null_column = NullColumn::create();
            Filter filter(rows);
            std::fill(filter.begin(), filter.end(), 1);
            uint8_t* null_data = nullptr;
            if constexpr (Nullable) {
                fill_null_column(array.get(), convert_start, rows, null_column.get(), 0);
                null_data = null_column->get_data().data();
            }
            state.ResumeTiming();

            auto st =
                    conv_func(array.get(), convert_start, rows, data_column.get(), 0, null_data, &filter, nullptr, nullptr);
            CHECK(st.ok()) << st.to_string();
            benchmark::DoNotOptimize(data_column.get());
            benchmark::DoNotOptimize(filter.data());
            benchmark::ClobberMemory();
            state.PauseTiming();
            if (!validated) {
                data_column->check_or_die();
                CHECK_EQ(data_column->size(), rows);
                auto validate_row = [&](size_t row) {
                    if constexpr (Nullable) {
                        if (null_data[row] != 0) {
                            return false;
                        }
                    }
                    typename ArrowType::offset_type arrow_size = 0;
                    const auto* arrow_data = typed_array->GetValue(convert_start + row, &arrow_size);
                    const auto slice = data_column->get_slice(row);
                    CHECK_EQ(slice.size, static_cast<size_t>(arrow_size));
                    CHECK_EQ(std::memcmp(slice.data, arrow_data, slice.size), 0);
                    return true;
                };
                if (rows > 0) {
                    bool verified_non_null = validate_row(0);
                    verified_non_null = validate_row(rows / 2) || verified_non_null;
                    verified_non_null = validate_row(rows - 1) || verified_non_null;
                    if constexpr (Nullable) {
                        for (size_t row = 0; row < rows && !verified_non_null; ++row) {
                            verified_non_null = validate_row(row);
                        }
                        CHECK(verified_non_null);
                        for (size_t row = 0; row < rows; ++row) {
                            if (null_data[row] != 0) {
                                CHECK_EQ(data_column->get_slice(row).size, 0);
                                break;
                            }
                        }
                    }
                }
                validated = true;
            }
        }
        state.ResumeTiming();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * rows * len));
}

template <bool Nullable>
static std::shared_ptr<arrow::Array> make_arrow_fixed_size_binary_array(size_t rows, size_t len) {
    auto type = std::make_shared<arrow::FixedSizeBinaryType>(static_cast<int32_t>(len));
    auto data_buf = arrow::AllocateBuffer(rows * len).ValueOrDie();
    auto* data = data_buf->mutable_data();

    std::shared_ptr<arrow::Buffer> null_bitmap;
    uint8_t* nulls = nullptr;
    if constexpr (Nullable) {
        null_bitmap = arrow::AllocateBuffer((rows + 7) / 8).ValueOrDie();
        nulls = null_bitmap->mutable_data();
    }

    int64_t null_count = 0;
    std::string value(len, 'x');
    for (size_t i = 0; i < rows; ++i) {
        if constexpr (Nullable) {
            if (pseudo_random(i) % 10 == 0) {
                arrow::bit_util::ClearBit(nulls, static_cast<int64_t>(i));
                ++null_count;
                continue;
            }
        }
        if constexpr (Nullable) {
            arrow::bit_util::SetBit(nulls, static_cast<int64_t>(i));
        }
        fill_string(&value, len, i, false);
        strings::memcpy_inlined(data + i * len, value.data(), len);
    }

    return std::make_shared<arrow::FixedSizeBinaryArray>(std::move(type), static_cast<int64_t>(rows),
                                                         std::move(data_buf), std::move(null_bitmap), null_count);
}

template <bool Nullable>
static void bench_arrow_fixed_size_binary_converter_impl(benchmark::State& state, size_t rows, size_t len,
                                                         int64_t source_offset = 0, bool use_array_slice = false) {
    auto owner = make_arrow_fixed_size_binary_array<Nullable>(rows + source_offset, len);
    std::shared_ptr<arrow::Array> array = owner;
    if (use_array_slice) {
        array = owner->Slice(source_offset, rows);
    }
    auto typed_array = std::static_pointer_cast<arrow::FixedSizeBinaryArray>(array);
    const int64_t convert_start = use_array_slice ? 0 : source_offset;
    auto conv_func = get_arrow_converter(ArrowTypeId::FIXED_SIZE_BINARY, TYPE_VARCHAR, Nullable, false);
    CHECK(conv_func != nullptr);

    bool validated = false;
    for (auto _ : state) {
        state.PauseTiming();
        {
            auto data_column = BinaryColumn::create();
            data_column->reserve(rows, rows * len);
            auto null_column = NullColumn::create();
            Filter filter(rows);
            std::fill(filter.begin(), filter.end(), 1);
            uint8_t* null_data = nullptr;
            if constexpr (Nullable) {
                fill_null_column(array.get(), convert_start, rows, null_column.get(), 0);
                null_data = null_column->get_data().data();
            }
            state.ResumeTiming();

            auto st = conv_func(array.get(), convert_start, rows, data_column.get(), 0, null_data, &filter, nullptr, nullptr);
            CHECK(st.ok()) << st.to_string();
            benchmark::DoNotOptimize(data_column.get());
            benchmark::DoNotOptimize(filter.data());
            benchmark::ClobberMemory();
            state.PauseTiming();
            if (!validated) {
                data_column->check_or_die();
                CHECK_EQ(data_column->size(), rows);
                auto validate_row = [&](size_t row) {
                    if constexpr (Nullable) {
                        if (null_data[row] != 0) {
                            return false;
                        }
                    }
                    const auto* arrow_data = typed_array->GetValue(convert_start + row);
                    const auto slice = data_column->get_slice(row);
                    CHECK_EQ(slice.size, len);
                    CHECK_EQ(std::memcmp(slice.data, arrow_data, slice.size), 0);
                    return true;
                };
                if (rows > 0) {
                    bool verified_non_null = validate_row(0);
                    verified_non_null = validate_row(rows / 2) || verified_non_null;
                    verified_non_null = validate_row(rows - 1) || verified_non_null;
                    if constexpr (Nullable) {
                        for (size_t row = 0; row < rows && !verified_non_null; ++row) {
                            verified_non_null = validate_row(row);
                        }
                        CHECK(verified_non_null);
                        for (size_t row = 0; row < rows; ++row) {
                            if (null_data[row] != 0) {
                                CHECK_EQ(data_column->get_slice(row).size, 0);
                                break;
                            }
                        }
                    }
                }
                validated = true;
            }
        }
        state.ResumeTiming();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * rows * len));
}

static void bench_arrow_string_converter(benchmark::State& state, ArrowStringKind kind, size_t rows, size_t len,
                                         bool nullable, int64_t source_offset = 0, bool use_array_slice = false) {
    if (kind == ArrowStringKind::STRING) {
        if (nullable) {
            bench_arrow_string_converter_impl<ArrowTypeId::STRING, arrow::StringType, true>(state, rows, len,
                                                                                            source_offset,
                                                                                            use_array_slice);
        } else {
            bench_arrow_string_converter_impl<ArrowTypeId::STRING, arrow::StringType, false>(state, rows, len,
                                                                                             source_offset,
                                                                                             use_array_slice);
        }
    } else if (kind == ArrowStringKind::LARGE_STRING) {
        if (nullable) {
            bench_arrow_string_converter_impl<ArrowTypeId::LARGE_STRING, arrow::LargeStringType, true>(state, rows,
                                                                                                       len,
                                                                                                       source_offset,
                                                                                                       use_array_slice);
        } else {
            bench_arrow_string_converter_impl<ArrowTypeId::LARGE_STRING, arrow::LargeStringType, false>(state, rows,
                                                                                                        len,
                                                                                                        source_offset,
                                                                                                        use_array_slice);
        }
    } else if (kind == ArrowStringKind::BINARY) {
        if (nullable) {
            bench_arrow_string_converter_impl<ArrowTypeId::BINARY, arrow::BinaryType, true>(state, rows, len,
                                                                                            source_offset,
                                                                                            use_array_slice);
        } else {
            bench_arrow_string_converter_impl<ArrowTypeId::BINARY, arrow::BinaryType, false>(state, rows, len,
                                                                                             source_offset,
                                                                                             use_array_slice);
        }
    } else if (kind == ArrowStringKind::LARGE_BINARY) {
        if (nullable) {
            bench_arrow_string_converter_impl<ArrowTypeId::LARGE_BINARY, arrow::LargeBinaryType, true>(state, rows,
                                                                                                       len,
                                                                                                       source_offset,
                                                                                                       use_array_slice);
        } else {
            bench_arrow_string_converter_impl<ArrowTypeId::LARGE_BINARY, arrow::LargeBinaryType, false>(state, rows,
                                                                                                        len,
                                                                                                        source_offset,
                                                                                                        use_array_slice);
        }
    } else {
        if (nullable) {
            bench_arrow_fixed_size_binary_converter_impl<true>(state, rows, len, source_offset, use_array_slice);
        } else {
            bench_arrow_fixed_size_binary_converter_impl<false>(state, rows, len, source_offset, use_array_slice);
        }
    }
}

static void bench_arrow_string_converter_nonzero_offset(benchmark::State& state, ArrowStringKind kind, size_t rows,
                                                        size_t len, bool nullable, bool use_array_slice) {
    bench_arrow_string_converter(state, kind, rows, len, nullable, 7, use_array_slice);
}

struct OrcStringFixture {
    orc::StringVectorBatch batch;
    SliceBlock block;
    Filter nulls;
    size_t rows = 0;
    size_t bytes = 0;

    OrcStringFixture(size_t rows_, size_t len, bool nullable, bool char_with_trailing_spaces)
            : batch(rows_, *orc::getDefaultPool()),
              block(make_slice_block(rows_, len, LenPattern::FIXED)),
              rows(rows_) {
        batch.numElements = rows;
        batch.hasNulls = nullable;
        batch.data.resize(rows);
        batch.length.resize(rows);
        batch.notNull.resize(rows);
        for (size_t i = 0; i < rows; ++i) {
            if (char_with_trailing_spaces && len > 1) {
                block.backing[i][len - 1] = ' ';
                if (len > 2) {
                    block.backing[i][len - 2] = ' ';
                }
            }
            batch.data[i] = block.slices[i].data;
            batch.length[i] = static_cast<int64_t>(block.slices[i].size);
            bytes += block.slices[i].size;
        }
        if (nullable) {
            nulls = make_filter(rows, FilterPattern::RANDOM_10);
            for (size_t i = 0; i < rows; ++i) {
                batch.notNull[i] = !nulls[i];
            }
        } else {
            for (size_t i = 0; i < rows; ++i) {
                batch.notNull[i] = 1;
            }
        }
    }
};

static void bench_orc_string_reader(benchmark::State& state, LogicalType type, size_t rows, size_t len, bool nullable) {
    OrcStringFixture fixture(rows, len, nullable, type == TYPE_CHAR);
    TypeDescriptor type_desc = type == TYPE_CHAR ? TypeDescriptor::create_char_type(static_cast<int>(len))
                                                 : TypeDescriptor::from_logical_type(type);
    OrcChunkReader reader(static_cast<int>(rows), {});
    reader.disable_broker_load_mode();

    for (auto _ : state) {
        state.PauseTiming();
        {
            MutableColumnPtr dst;
            if (nullable) {
                dst = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
            } else {
                dst = BinaryColumn::create();
            }
            std::unique_ptr<ORCColumnReader> column_reader;
            if (type == TYPE_VARBINARY) {
                column_reader = std::make_unique<VarbinaryColumnReader>(type_desc, nullptr, nullable, &reader);
            } else {
                column_reader = std::make_unique<StringColumnReader>(type_desc, nullptr, nullable, &reader);
            }
            state.ResumeTiming();

            auto st = column_reader->get_next(&fixture.batch, dst.get(), 0, rows);
            CHECK(st.ok()) << st.to_string();
            benchmark::DoNotOptimize(dst.get());
            benchmark::ClobberMemory();
            state.PauseTiming();
        }
        state.ResumeTiming();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * fixture.bytes));
}

enum class StringFunctionCase {
    SUBSTRING,
    SUBSTRING_CONST,
    RIGHT,
    CONCAT,
    LOCATE,
    LIKE_PREFIX,
    LIKE_SUBSTRING,
};

enum class AggSerializeCase {
    GROUP_CONCAT,
    MAX_BY,
    PERCENTILE_CONT,
    DS_HLL_COUNT_DISTINCT,
};

enum class AggConvertCase {
    GROUP_CONCAT,
    MAX_BY,
    AVG,
    VARIANCE,
    PERCENTILE_APPROX,
    DS_HLL_COUNT_DISTINCT,
};

static const char* string_function_case_name(StringFunctionCase fn) {
    switch (fn) {
    case StringFunctionCase::SUBSTRING:
        return "substring";
    case StringFunctionCase::SUBSTRING_CONST:
        return "substring_const";
    case StringFunctionCase::RIGHT:
        return "right";
    case StringFunctionCase::CONCAT:
        return "concat";
    case StringFunctionCase::LOCATE:
        return "locate";
    case StringFunctionCase::LIKE_PREFIX:
        return "like_prefix";
    case StringFunctionCase::LIKE_SUBSTRING:
        return "like_substring";
    }
    return "unknown";
}

static const char* agg_serialize_case_name(AggSerializeCase fn) {
    switch (fn) {
    case AggSerializeCase::GROUP_CONCAT:
        return "group_concat";
    case AggSerializeCase::MAX_BY:
        return "max_by";
    case AggSerializeCase::PERCENTILE_CONT:
        return "percentile_cont";
    case AggSerializeCase::DS_HLL_COUNT_DISTINCT:
        return "ds_hll_count_distinct";
    }
    return "unknown";
}

static const char* agg_convert_case_name(AggConvertCase fn) {
    switch (fn) {
    case AggConvertCase::GROUP_CONCAT:
        return "group_concat";
    case AggConvertCase::MAX_BY:
        return "max_by";
    case AggConvertCase::AVG:
        return "avg";
    case AggConvertCase::VARIANCE:
        return "variance";
    case AggConvertCase::PERCENTILE_APPROX:
        return "percentile_approx";
    case AggConvertCase::DS_HLL_COUNT_DISTINCT:
        return "ds_hll_count_distinct";
    }
    return "unknown";
}

static ColumnPtr make_const_varchar(std::string value, size_t size) {
    auto data = BinaryColumn::create();
    data->append(Slice(value));
    return ConstColumn::create(std::move(data), size);
}

static ColumnPtr make_const_int32(int32_t value, size_t size) {
    auto data = Int32Column::create();
    data->append(value);
    return ConstColumn::create(std::move(data), size);
}

static void bench_string_function_external(benchmark::State& state, StringFunctionCase fn, size_t rows, size_t len) {
    auto input = make_fixed_column(rows, len);
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    bool substring_prepared = false;
    bool like_prepared = false;

    switch (fn) {
    case StringFunctionCase::SUBSTRING: {
        auto pos = Int32Column::create();
        auto sub_len = Int32Column::create();
        const int32_t start = 2;
        const int32_t length = static_cast<int32_t>(std::min<size_t>(len - 1, 8));
        pos->append_value_multiple_times(&start, rows);
        sub_len->append_value_multiple_times(&length, rows);
        columns = {input, std::move(pos), std::move(sub_len)};
        break;
    }
    case StringFunctionCase::SUBSTRING_CONST: {
        std::vector<TypeDescriptor> arg_types{TypeDescriptor::from_logical_type(TYPE_VARCHAR),
                                              TypeDescriptor::from_logical_type(TYPE_INT),
                                              TypeDescriptor::from_logical_type(TYPE_INT)};
        ctx.reset(FunctionContext::create_test_context(std::move(arg_types),
                                                       TypeDescriptor::from_logical_type(TYPE_VARCHAR)));
        const int32_t start = 2;
        const int32_t length = static_cast<int32_t>(std::min<size_t>(len - 1, 8));
        columns = {input, make_const_int32(start, rows), make_const_int32(length, rows)};
        ctx->set_constant_columns(columns);
        auto prepare_st = StringFunctions::sub_str_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
        CHECK(prepare_st.ok()) << prepare_st.to_string();
        substring_prepared = true;
        break;
    }
    case StringFunctionCase::RIGHT: {
        auto right_len = Int32Column::create();
        const int32_t length = static_cast<int32_t>(std::min<size_t>(len, 8));
        right_len->append_value_multiple_times(&length, rows);
        columns = {input, std::move(right_len)};
        break;
    }
    case StringFunctionCase::CONCAT: {
        columns = {input, make_fixed_column(rows, len)};
        break;
    }
    case StringFunctionCase::LOCATE: {
        auto pos = Int32Column::create();
        const int32_t start = 1;
        pos->append_value_multiple_times(&start, rows);
        columns = {make_const_varchar("bc", rows), input, std::move(pos)};
        break;
    }
    case StringFunctionCase::LIKE_PREFIX: {
        columns = {input, make_const_varchar("ab%", rows)};
        ctx->set_constant_columns(columns);
        auto prepare_st = LikePredicate::like_prepare(ctx.get(), FunctionContext::FunctionStateScope::THREAD_LOCAL);
        CHECK(prepare_st.ok()) << prepare_st.to_string();
        like_prepared = true;
        break;
    }
    case StringFunctionCase::LIKE_SUBSTRING: {
        columns = {input, make_const_varchar("%bc%", rows)};
        ctx->set_constant_columns(columns);
        auto prepare_st = LikePredicate::like_prepare(ctx.get(), FunctionContext::FunctionStateScope::THREAD_LOCAL);
        CHECK(prepare_st.ok()) << prepare_st.to_string();
        like_prepared = true;
        break;
    }
    }

    for (auto _ : state) {
        {
            auto result = [&]() -> StatusOr<ColumnPtr> {
                switch (fn) {
                case StringFunctionCase::SUBSTRING:
                case StringFunctionCase::SUBSTRING_CONST:
                    return StringFunctions::substring(ctx.get(), columns);
                case StringFunctionCase::RIGHT:
                    return StringFunctions::right(ctx.get(), columns);
                case StringFunctionCase::CONCAT:
                    return StringFunctions::concat(ctx.get(), columns);
                case StringFunctionCase::LOCATE:
                    return StringFunctions::locate_pos(ctx.get(), columns);
                case StringFunctionCase::LIKE_PREFIX:
                case StringFunctionCase::LIKE_SUBSTRING:
                    return LikePredicate::like(ctx.get(), columns);
                }
                __builtin_unreachable();
            }();
            CHECK(result.ok()) << result.status().to_string();
            benchmark::DoNotOptimize(result.value().get());
            benchmark::ClobberMemory();
            state.PauseTiming();
        }
        state.ResumeTiming();
    }

    if (substring_prepared) {
        auto close_st = StringFunctions::sub_str_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
        CHECK(close_st.ok()) << close_st.to_string();
    }
    if (like_prepared) {
        auto close_st = LikePredicate::like_close(ctx.get(), FunctionContext::FunctionStateScope::THREAD_LOCAL);
        CHECK(close_st.ok()) << close_st.to_string();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
    const size_t input_bytes = fn == StringFunctionCase::CONCAT ? rows * len * 2 : rows * len;
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * input_bytes));
    state.counters["input_bytes_per_iter"] = static_cast<double>(input_bytes);
}

static TSlotDescriptor create_join_slot_descriptor(const std::string& column_name, LogicalType column_type,
                                                   int32_t column_pos, bool nullable) {
    TSlotDescriptorBuilder slot_desc_builder;
    if (column_type == TYPE_VARCHAR) {
        return slot_desc_builder.string_type(TypeDescriptor::MAX_VARCHAR_LENGTH)
                .column_name(column_name)
                .column_pos(column_pos)
                .nullable(nullable)
                .build();
    }
    return slot_desc_builder.type(column_type)
            .column_name(column_name)
            .column_pos(column_pos)
            .nullable(nullable)
            .build();
}

static void add_join_tuple_descriptor(TDescriptorTableBuilder* table_desc_builder, LogicalType column_type,
                                      size_t num_columns, bool nullable) {
    TTupleDescriptorBuilder tuple_desc_builder;
    for (size_t i = 0; i < num_columns; ++i) {
        tuple_desc_builder.add_slot(create_join_slot_descriptor("c" + std::to_string(i), column_type,
                                                               static_cast<int32_t>(i), nullable));
    }
    tuple_desc_builder.build(table_desc_builder);
}

static std::shared_ptr<RuntimeState> make_join_runtime_state(bool enable_fixed_size_string = true) {
    TUniqueId fragment_id;
    TQueryOptions query_options;
    query_options.batch_size = config::vector_chunk_size;
    query_options.__isset.enable_hash_join_serialize_fixed_size_string = true;
    query_options.enable_hash_join_serialize_fixed_size_string = enable_fixed_size_string;

    TQueryGlobals query_globals;
    auto runtime_state = std::make_shared<RuntimeState>(fragment_id, query_options, query_globals, nullptr);
    runtime_state->init_instance_mem_tracker();
    return runtime_state;
}

static std::shared_ptr<RowDescriptor> make_join_row_descriptor(RuntimeState* runtime_state, ObjectPool* object_pool,
                                                               TDescriptorTableBuilder& table_desc_builder,
                                                               TTupleId tuple_id) {
    DescriptorTbl* descriptor_table = nullptr;
    auto st = DescriptorTbl::create(runtime_state, object_pool, table_desc_builder.desc_tbl(), &descriptor_table,
                                    config::vector_chunk_size);
    CHECK(st.ok()) << st.to_string();

    std::vector<TTupleId> row_tuples{tuple_id};
    return std::make_shared<RowDescriptor>(*descriptor_table, row_tuples);
}

static BinaryColumn::MutablePtr make_join_data_column(size_t rows, size_t len, size_t seed = 0) {
    auto data = BinaryColumn::create();
    data->reserve(rows, rows * len);
    std::string value(len, 'x');
    for (size_t i = 0; i < rows; ++i) {
        fill_unique_string(&value, len, seed + i);
        data->append(Slice(value));
    }
    CHECK_LT(data->get_bytes().size(), static_cast<size_t>(std::numeric_limits<uint32_t>::max()));
    return data;
}

static ColumnPtr make_join_key_column(size_t rows, size_t len, bool nullable, size_t seed = 0) {
    auto data = make_join_data_column(rows, len, seed);
    if (!nullable) {
        return data;
    }

    auto nulls = NullColumn::create(rows, 0);
    auto& null_data = nulls->get_data();
    for (size_t i = 0; i < rows; ++i) {
        null_data[i] = static_cast<uint8_t>(pseudo_random(seed + i + 41) % 10 == 0);
    }
    auto column = NullableColumn::create(std::move(data), std::move(nulls));
    column->set_has_null(true);
    return column;
}

struct JoinBuildFixture {
    std::shared_ptr<ObjectPool> object_pool = std::make_shared<ObjectPool>();
    std::shared_ptr<RuntimeState> runtime_state;
    RuntimeProfile runtime_profile{"binary_column_external_offset_join"};
    TDescriptorTableBuilder table_desc_builder;
    std::shared_ptr<RowDescriptor> probe_row_desc;
    std::shared_ptr<RowDescriptor> build_row_desc;
    TypeDescriptor varchar_type = TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
    HashTableParam param;
    ChunkPtr build_chunk;
    Columns build_keys;

    JoinBuildFixture(size_t rows, size_t len, size_t num_keys, bool nullable, JoinKeyMode key_mode)
            : runtime_state(make_join_runtime_state(key_mode == JoinKeyMode::FIXED_SIZE)) {
        CHECK_GT(num_keys, 0);
        if (key_mode == JoinKeyMode::SERIALIZED_VARCHAR) {
            CHECK_GT(num_keys, 1) << "single VARCHAR key uses the one-key path, not serialized varchar key builder";
        }

        add_join_tuple_descriptor(&table_desc_builder, TYPE_VARCHAR, num_keys, nullable);
        add_join_tuple_descriptor(&table_desc_builder, TYPE_VARCHAR, num_keys, nullable);
        probe_row_desc = make_join_row_descriptor(runtime_state.get(), object_pool.get(), table_desc_builder, 0);
        build_row_desc = make_join_row_descriptor(runtime_state.get(), object_pool.get(), table_desc_builder, 1);

        param.join_type = TJoinOp::INNER_JOIN;
        param.probe_row_desc = probe_row_desc.get();
        param.build_row_desc = build_row_desc.get();
        for (size_t i = 0; i < num_keys; ++i) {
            param.join_keys.emplace_back(JoinKeyDesc{&varchar_type, false, nullptr});
        }
        param.search_ht_timer = ADD_TIMER(&runtime_profile, "SearchHashTableTime");
        param.output_build_column_timer = ADD_TIMER(&runtime_profile, "OutputBuildColumnTime");
        param.output_probe_column_timer = ADD_TIMER(&runtime_profile, "OutputProbeColumnTime");

        build_chunk = std::make_shared<Chunk>();
        // SERIALIZED_VARCHAR disables fixed-size string optimization and uses multi-key input,
        // which forces the real JoinHashTable serialized varchar key builder.
        const auto& build_slots = build_row_desc->tuple_descriptors()[0]->slots();
        for (size_t i = 0; i < num_keys; ++i) {
            auto key_column = make_join_key_column(rows, len, nullable, i * rows);
            build_chunk->append_column(key_column, build_slots[i]->id());
            build_keys.emplace_back(std::move(key_column));
        }
    }
};

static void bench_join_hash_table_binary_key_build_only(benchmark::State& state, size_t rows, size_t len, bool nullable,
                                                        JoinKeyMode key_mode, size_t num_keys) {
    JoinBuildFixture fixture(rows, len, num_keys, nullable, key_mode);

    for (auto _ : state) {
        state.PauseTiming();
        {
            JoinHashTable hash_table;
            hash_table.create(fixture.param);
            hash_table.append_chunk(fixture.build_chunk, fixture.build_keys);
            state.ResumeTiming();

            auto st = hash_table.build(fixture.runtime_state.get());
            CHECK(st.ok()) << st.to_string();
            benchmark::DoNotOptimize(hash_table.get_row_count());
            benchmark::ClobberMemory();

            state.PauseTiming();
            hash_table.close();
        }
        state.ResumeTiming();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * rows * len * num_keys));
    state.counters["key_columns_per_row"] = static_cast<double>(num_keys);
}

class ManagedAggState {
public:
    ManagedAggState(FunctionContext* ctx, const AggregateFunction* func) : _ctx(ctx), _func(func) {
        _state = _mem_pool.allocate_aligned(_func->size(), _func->alignof_size());
        _func->create(_ctx, _state);
    }
    ~ManagedAggState() { _func->destroy(_ctx, _state); }

    AggDataPtr state() const { return _state; }

private:
    FunctionContext* _ctx = nullptr;
    const AggregateFunction* _func = nullptr;
    MemPool _mem_pool;
    AggDataPtr _state = nullptr;
};

static std::unique_ptr<FunctionContext> make_agg_context(AggSerializeCase fn) {
    switch (fn) {
    case AggSerializeCase::GROUP_CONCAT: {
        std::vector<TypeDescriptor> arg_types{TypeDescriptor::from_logical_type(TYPE_VARCHAR)};
        return std::unique_ptr<FunctionContext>(FunctionContext::create_test_context(
                std::move(arg_types), TypeDescriptor::from_logical_type(TYPE_VARCHAR)));
    }
    case AggSerializeCase::MAX_BY: {
        std::vector<TypeDescriptor> arg_types{TypeDescriptor::from_logical_type(TYPE_INT),
                                              TypeDescriptor::from_logical_type(TYPE_VARCHAR)};
        return std::unique_ptr<FunctionContext>(FunctionContext::create_test_context(
                std::move(arg_types), TypeDescriptor::from_logical_type(TYPE_INT)));
    }
    case AggSerializeCase::PERCENTILE_CONT: {
        std::vector<TypeDescriptor> arg_types{TypeDescriptor::from_logical_type(TYPE_DOUBLE),
                                              TypeDescriptor::from_logical_type(TYPE_DOUBLE)};
        return std::unique_ptr<FunctionContext>(FunctionContext::create_test_context(
                std::move(arg_types), TypeDescriptor::from_logical_type(TYPE_DOUBLE)));
    }
    case AggSerializeCase::DS_HLL_COUNT_DISTINCT: {
        std::vector<TypeDescriptor> arg_types{TypeDescriptor::from_logical_type(TYPE_VARCHAR)};
        return std::unique_ptr<FunctionContext>(FunctionContext::create_test_context(
                std::move(arg_types), TypeDescriptor::from_logical_type(TYPE_BIGINT)));
    }
    }
    __builtin_unreachable();
}

static const AggregateFunction* get_bench_aggregate_function(AggSerializeCase fn) {
    const AggregateFunction* func = nullptr;
    switch (fn) {
    case AggSerializeCase::GROUP_CONCAT:
        func = get_aggregate_function("group_concat", TYPE_VARCHAR, TYPE_VARCHAR, false);
        break;
    case AggSerializeCase::MAX_BY:
        func = get_aggregate_function("max_by", TYPE_VARCHAR, TYPE_INT, false);
        break;
    case AggSerializeCase::PERCENTILE_CONT:
        func = get_aggregate_function("percentile_cont", TYPE_DOUBLE, TYPE_DOUBLE, false);
        break;
    case AggSerializeCase::DS_HLL_COUNT_DISTINCT:
        func = get_aggregate_function("ds_hll_count_distinct", TYPE_VARCHAR, TYPE_BIGINT, false);
        break;
    }
    CHECK(func != nullptr) << "aggregate function not found: " << agg_serialize_case_name(fn);
    return func;
}

static void update_group_concat_state(FunctionContext* ctx, const AggregateFunction* func, AggDataPtr state, size_t len,
                                      size_t seed, size_t values_per_state) {
    auto column = BinaryColumn::create();
    column->reserve(values_per_state, values_per_state * len);
    std::string value(len, 'x');
    for (size_t i = 0; i < values_per_state; ++i) {
        fill_string(&value, len, seed + i, false);
        column->append(Slice(value));
    }
    const Column* raw_column = column.get();
    func->update_batch_single_state(ctx, column->size(), &raw_column, state);
}

static void update_max_by_state(FunctionContext* ctx, const AggregateFunction* func, AggDataPtr state, size_t len,
                                size_t seed, size_t values_per_state) {
    auto values = Int32Column::create();
    auto keys = BinaryColumn::create();
    values->reserve(values_per_state);
    keys->reserve(values_per_state, values_per_state * len);

    std::string value(len, 'x');
    for (size_t i = 0; i < values_per_state; ++i) {
        values->append(static_cast<int32_t>((seed + i) & 0x7FFFFFFF));
        fill_string(&value, len, seed + i, false);
        keys->append(Slice(value));
    }

    const Column* raw_columns[] = {values.get(), keys.get()};
    func->update_batch_single_state(ctx, values->size(), raw_columns, state);
}

static void update_percentile_cont_state(FunctionContext* ctx, const AggregateFunction* func, AggDataPtr state,
                                         size_t seed, size_t values_per_state) {
    auto values = DoubleColumn::create();
    values->reserve(values_per_state);
    for (size_t i = 0; i < values_per_state; ++i) {
        values->append(static_cast<double>((seed + i) % 1000));
    }
    auto percentile = ColumnHelper::create_const_column<TYPE_DOUBLE>(0.5, 1);

    const Column* raw_columns[] = {values.get(), percentile.get()};
    func->update_batch_single_state(ctx, values->size(), raw_columns, state);
}

static void update_ds_hll_count_distinct_state(FunctionContext* ctx, const AggregateFunction* func, AggDataPtr state,
                                               size_t len, size_t seed, size_t values_per_state) {
    auto column = BinaryColumn::create();
    column->reserve(values_per_state, values_per_state * len);
    std::string value(len, 'x');
    for (size_t i = 0; i < values_per_state; ++i) {
        fill_string(&value, len, seed + i, false);
        column->append(Slice(value));
    }
    const Column* raw_column = column.get();
    func->update_batch_single_state(ctx, column->size(), &raw_column, state);
}

static void update_agg_state(AggSerializeCase fn, FunctionContext* ctx, const AggregateFunction* func, AggDataPtr state,
                             size_t len, size_t seed, size_t values_per_state) {
    switch (fn) {
    case AggSerializeCase::GROUP_CONCAT:
        update_group_concat_state(ctx, func, state, len, seed, values_per_state);
        break;
    case AggSerializeCase::MAX_BY:
        update_max_by_state(ctx, func, state, len, seed, values_per_state);
        break;
    case AggSerializeCase::PERCENTILE_CONT:
        update_percentile_cont_state(ctx, func, state, seed, values_per_state);
        break;
    case AggSerializeCase::DS_HLL_COUNT_DISTINCT:
        update_ds_hll_count_distinct_state(ctx, func, state, len, seed, values_per_state);
        break;
    }
}

static size_t agg_serialize_input_bytes(AggSerializeCase fn, size_t rows, size_t len, size_t values_per_state) {
    switch (fn) {
    case AggSerializeCase::GROUP_CONCAT:
    case AggSerializeCase::DS_HLL_COUNT_DISTINCT:
        return rows * len * values_per_state;
    case AggSerializeCase::MAX_BY:
        return rows * values_per_state * (sizeof(int32_t) + len);
    case AggSerializeCase::PERCENTILE_CONT:
        return rows * values_per_state * sizeof(double);
    }
    __builtin_unreachable();
}

static size_t agg_serialize_reserve_bytes(AggSerializeCase fn, size_t rows, size_t len, size_t values_per_state) {
    if (fn == AggSerializeCase::PERCENTILE_CONT) {
        return rows * std::max<size_t>(64, values_per_state * sizeof(double) + 64);
    }
    if (fn == AggSerializeCase::GROUP_CONCAT) {
        return rows * (2 * sizeof(uint32_t) + values_per_state * (len + 2));
    }
    return rows * std::max<size_t>(64, len * values_per_state);
}

static void bench_agg_batch_serialize_external(benchmark::State& state, AggSerializeCase fn, size_t rows, size_t len,
                                               size_t values_per_state) {
    auto ctx = make_agg_context(fn);
    int64_t mem_usage = 0;
    ctx->set_mem_usage_counter(&mem_usage);
    const auto* func = get_bench_aggregate_function(fn);

    MemHookAllocator allocator;
    SCOPED_THREAD_LOCAL_STATE_ALLOCATOR_SETTER(&allocator);
    std::vector<std::unique_ptr<ManagedAggState>> states;
    states.reserve(rows);
    Buffer<AggDataPtr> agg_states(rows);
    for (size_t i = 0; i < rows; ++i) {
        states.emplace_back(std::make_unique<ManagedAggState>(ctx.get(), func));
        update_agg_state(fn, ctx.get(), func, states.back()->state(), len, i * values_per_state, values_per_state);
        agg_states[i] = states.back()->state();
    }

    const size_t reserve_bytes = agg_serialize_reserve_bytes(fn, rows, len, values_per_state);
    for (auto _ : state) {
        state.PauseTiming();
        {
            auto dst = BinaryColumn::create();
            dst->reserve(rows, reserve_bytes);
            state.ResumeTiming();

            func->batch_serialize(ctx.get(), rows, agg_states, 0, dst.get());
            benchmark::DoNotOptimize(dst.get());
            benchmark::ClobberMemory();
            state.PauseTiming();
        }
        state.ResumeTiming();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
    state.SetBytesProcessed(
            static_cast<int64_t>(state.iterations() * agg_serialize_input_bytes(fn, rows, len, values_per_state)));
}

static std::unique_ptr<FunctionContext> make_agg_convert_context(AggConvertCase fn) {
    switch (fn) {
    case AggConvertCase::GROUP_CONCAT: {
        std::vector<TypeDescriptor> arg_types{TypeDescriptor::from_logical_type(TYPE_VARCHAR),
                                              TypeDescriptor::from_logical_type(TYPE_VARCHAR)};
        return std::unique_ptr<FunctionContext>(FunctionContext::create_test_context(
                std::move(arg_types), TypeDescriptor::from_logical_type(TYPE_VARCHAR)));
    }
    case AggConvertCase::MAX_BY: {
        std::vector<TypeDescriptor> arg_types{TypeDescriptor::from_logical_type(TYPE_INT),
                                              TypeDescriptor::from_logical_type(TYPE_VARCHAR)};
        return std::unique_ptr<FunctionContext>(FunctionContext::create_test_context(
                std::move(arg_types), TypeDescriptor::from_logical_type(TYPE_INT)));
    }
    case AggConvertCase::AVG:
    case AggConvertCase::VARIANCE: {
        std::vector<TypeDescriptor> arg_types{TypeDescriptor::from_logical_type(TYPE_DOUBLE)};
        return std::unique_ptr<FunctionContext>(FunctionContext::create_test_context(
                std::move(arg_types), TypeDescriptor::from_logical_type(TYPE_DOUBLE)));
    }
    case AggConvertCase::PERCENTILE_APPROX: {
        std::vector<TypeDescriptor> arg_types{TypeDescriptor::from_logical_type(TYPE_DOUBLE),
                                              TypeDescriptor::from_logical_type(TYPE_DOUBLE)};
        return std::unique_ptr<FunctionContext>(FunctionContext::create_test_context(
                std::move(arg_types), TypeDescriptor::from_logical_type(TYPE_DOUBLE)));
    }
    case AggConvertCase::DS_HLL_COUNT_DISTINCT: {
        std::vector<TypeDescriptor> arg_types{TypeDescriptor::from_logical_type(TYPE_VARCHAR)};
        return std::unique_ptr<FunctionContext>(FunctionContext::create_test_context(
                std::move(arg_types), TypeDescriptor::from_logical_type(TYPE_BIGINT)));
    }
    }
    __builtin_unreachable();
}

static const AggregateFunction* get_bench_agg_convert_function(AggConvertCase fn) {
    const AggregateFunction* func = nullptr;
    switch (fn) {
    case AggConvertCase::GROUP_CONCAT:
        func = get_aggregate_function("group_concat", TYPE_VARCHAR, TYPE_VARCHAR, false);
        break;
    case AggConvertCase::MAX_BY:
        func = get_aggregate_function("max_by", TYPE_VARCHAR, TYPE_INT, false);
        break;
    case AggConvertCase::AVG:
        func = get_aggregate_function("avg", TYPE_DOUBLE, TYPE_DOUBLE, false);
        break;
    case AggConvertCase::VARIANCE:
        func = get_aggregate_function("variance", TYPE_DOUBLE, TYPE_DOUBLE, false);
        break;
    case AggConvertCase::PERCENTILE_APPROX:
        func = get_aggregate_function("percentile_approx", TYPE_DOUBLE, TYPE_DOUBLE, false);
        break;
    case AggConvertCase::DS_HLL_COUNT_DISTINCT:
        func = get_aggregate_function("ds_hll_count_distinct", TYPE_VARCHAR, TYPE_BIGINT, false);
        break;
    }
    CHECK(func != nullptr) << "aggregate function not found: " << agg_convert_case_name(fn);
    return func;
}

static Columns make_agg_convert_sources(AggConvertCase fn, size_t rows, size_t len) {
    switch (fn) {
    case AggConvertCase::GROUP_CONCAT:
        return {make_fixed_column(rows, len), make_fixed_column(rows, 4)};
    case AggConvertCase::MAX_BY: {
        auto values = Int32Column::create();
        values->reserve(rows);
        for (size_t i = 0; i < rows; ++i) {
            values->append(static_cast<int32_t>(i & 0x7FFFFFFF));
        }
        return {std::move(values), make_fixed_column(rows, len)};
    }
    case AggConvertCase::AVG:
    case AggConvertCase::VARIANCE: {
        auto values = DoubleColumn::create();
        values->reserve(rows);
        for (size_t i = 0; i < rows; ++i) {
            values->append(static_cast<double>(pseudo_random(i) % 10000) / 100.0);
        }
        return {std::move(values)};
    }
    case AggConvertCase::PERCENTILE_APPROX: {
        auto values = DoubleColumn::create();
        values->reserve(rows);
        for (size_t i = 0; i < rows; ++i) {
            values->append(static_cast<double>(pseudo_random(i) % 10000) / 100.0);
        }
        return {std::move(values), ColumnHelper::create_const_column<TYPE_DOUBLE>(0.5, rows)};
    }
    case AggConvertCase::DS_HLL_COUNT_DISTINCT:
        return {make_fixed_column(rows, len)};
    }
    __builtin_unreachable();
}

static size_t agg_convert_input_bytes(AggConvertCase fn, size_t rows, size_t len) {
    switch (fn) {
    case AggConvertCase::GROUP_CONCAT:
        return rows * (len + 4);
    case AggConvertCase::MAX_BY:
        return rows * (sizeof(int32_t) + len);
    case AggConvertCase::DS_HLL_COUNT_DISTINCT:
        return rows * len;
    case AggConvertCase::AVG:
    case AggConvertCase::VARIANCE:
    case AggConvertCase::PERCENTILE_APPROX:
        return rows * sizeof(double);
    }
    __builtin_unreachable();
}

static void bench_agg_convert_to_serialize_format_external(benchmark::State& state, AggConvertCase fn, size_t rows,
                                                           size_t len) {
    auto ctx = make_agg_convert_context(fn);
    int64_t mem_usage = 0;
    ctx->set_mem_usage_counter(&mem_usage);
    const auto* func = get_bench_agg_convert_function(fn);
    auto src = make_agg_convert_sources(fn, rows, len);

    for (auto _ : state) {
        state.PauseTiming();
        {
            MutableColumnPtr dst = BinaryColumn::create();
            state.ResumeTiming();

            func->convert_to_serialize_format(ctx.get(), src, rows, dst);
            benchmark::DoNotOptimize(dst.get());
            benchmark::ClobberMemory();
            state.PauseTiming();
        }
        state.ResumeTiming();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * agg_convert_input_bytes(fn, rows, len)));
}

static void bench_parquet_level_builder_byte_array(benchmark::State& state, size_t rows, size_t len, bool nullable) {
    MutableColumnPtr column = make_fixed_column(rows, len);
    if (nullable) {
        auto nulls = NullColumn::create(rows, 0);
        auto& null_data = nulls->get_data();
        for (size_t i = 0; i < rows; ++i) {
            null_data[i] = static_cast<uint8_t>(pseudo_random(i) % 10 == 0);
        }
        auto nullable_column = NullableColumn::create(std::move(column), std::move(nulls));
        nullable_column->set_has_null(true);
        column = std::move(nullable_column);
    }

    const auto repetition = nullable ? ::parquet::Repetition::OPTIONAL : ::parquet::Repetition::REQUIRED;
    auto node = ::parquet::schema::PrimitiveNode::Make("c0", repetition, ::parquet::LogicalType::String(),
                                                       ::parquet::Type::BYTE_ARRAY, -1, -1);
    TypeDescriptor type_desc = TypeDescriptor::from_logical_type(TYPE_VARCHAR);

    for (auto _ : state) {
        state.PauseTiming();
        {
            parquet::LevelBuilder builder(type_desc, node, "UTC", false, false);
            auto init_st = builder.init();
            CHECK(init_st.ok()) << init_st.to_string();
            parquet::LevelBuilderContext ctx(rows);
            state.ResumeTiming();

            auto st = builder.write(ctx, column, [&](const parquet::LevelBuilderResult& result) {
                auto* values = reinterpret_cast<::parquet::ByteArray*>(result.values);
                if (values != nullptr && rows > 0) {
                    benchmark::DoNotOptimize(values[0].len);
                }
                benchmark::DoNotOptimize(values);
            });
            CHECK(st.ok()) << st.to_string();
            benchmark::ClobberMemory();
            state.PauseTiming();
        }
        state.ResumeTiming();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * rows * len));
}

static TabletSchemaCSPtr make_single_char_tablet_schema(size_t char_len, bool nullable) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(DUP_KEYS);
    schema_pb.set_num_short_key_columns(1);
    schema_pb.set_num_rows_per_row_block(1024);
    schema_pb.set_next_column_unique_id(2);

    ColumnPB* column = schema_pb.add_column();
    column->set_unique_id(1);
    column->set_name("c0");
    column->set_type("CHAR");
    column->set_is_key(true);
    column->set_length(static_cast<uint32_t>(char_len));
    column->set_index_length(static_cast<uint32_t>(char_len));
    column->set_is_nullable(nullable);
    column->set_is_bf_column(false);

    return std::make_shared<TabletSchema>(schema_pb);
}

static void bench_chunk_helper_padding_char_column(benchmark::State& state, size_t rows, size_t char_len,
                                                   bool nullable) {
    auto tablet_schema = make_single_char_tablet_schema(char_len, nullable);
    auto schema = ChunkHelper::convert_schema(tablet_schema);
    const size_t input_len = std::max<size_t>(1, char_len - 2);

    for (auto _ : state) {
        state.PauseTiming();
        {
            auto column = make_predicate_column(rows, input_len, 0, nullable);
            state.ResumeTiming();

            ChunkHelper::padding_char_column(tablet_schema, *schema.field(0), column.get());
            benchmark::DoNotOptimize(column.get());
            benchmark::ClobberMemory();
            state.PauseTiming();
        }
        state.ResumeTiming();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * rows * char_len));
}

static ColumnPtr make_array_varchar_column(size_t rows, size_t array_size, size_t len, bool nullable) {
    auto elements = BinaryColumn::create();
    elements->reserve(rows * array_size, rows * array_size * len);
    auto offsets = UInt32Column::create();
    offsets->reserve(rows + 1);
    offsets->append(0);

    std::string value(len, 'x');
    for (size_t row = 0; row < rows; ++row) {
        for (size_t j = 0; j < array_size; ++j) {
            fill_string(&value, len, row * array_size + j, false);
            elements->append(Slice(value));
        }
        offsets->append(elements->size());
    }

    auto element_nulls = NullColumn::create(elements->size(), 0);
    auto nullable_elements = NullableColumn::create(std::move(elements), std::move(element_nulls));
    nullable_elements->set_has_null(false);
    auto array = ArrayColumn::create(std::move(nullable_elements), std::move(offsets));
    if (!nullable) {
        return array;
    }

    auto nulls = NullColumn::create(rows, 0);
    auto& null_data = nulls->get_data();
    for (size_t i = 0; i < rows; ++i) {
        null_data[i] = static_cast<uint8_t>(pseudo_random(i + 73) % 10 == 0);
    }
    auto nullable_array = NullableColumn::create(std::move(array), std::move(nulls));
    nullable_array->set_has_null(true);
    return nullable_array;
}

static void bench_array_reverse_string_external(benchmark::State& state, size_t rows, size_t array_size, size_t len,
                                                bool nullable) {
    auto column = make_array_varchar_column(rows, array_size, len, nullable);
    Columns args{column};

    for (auto _ : state) {
        {
            auto result = ArrayFunctions::array_reverse<TYPE_VARCHAR>(nullptr, args);
            CHECK(result.ok()) << result.status().to_string();
            benchmark::DoNotOptimize(result.value().get());
            benchmark::ClobberMemory();
            state.PauseTiming();
        }
        state.ResumeTiming();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * rows * array_size * len));
}

static ColumnPtr make_cast_input_column(const TypeDescriptor& type, size_t rows) {
    switch (type.type) {
    case TYPE_INT: {
        auto column = Int32Column::create();
        column->reserve(rows);
        for (size_t i = 0; i < rows; ++i) {
            column->append(static_cast<int32_t>(i));
        }
        return column;
    }
    case TYPE_DOUBLE: {
        auto column = DoubleColumn::create();
        column->reserve(rows);
        for (size_t i = 0; i < rows; ++i) {
            column->append(static_cast<double>(i) / 10.0);
        }
        return column;
    }
    case TYPE_DECIMAL64: {
        auto column = Decimal64Column::create(type.precision, type.scale);
        column->reserve(rows);
        int64_t scale_base = 1;
        for (int i = 0; i < type.scale; ++i) {
            scale_base *= 10;
        }
        for (size_t i = 0; i < rows; ++i) {
            column->append(static_cast<int64_t>(i) * scale_base + 42);
        }
        return column;
    }
    default:
        CHECK(false) << "unsupported cast input type: " << type.debug_string();
    }
    return nullptr;
}

static std::string cast_type_case_name(const TypeDescriptor& type) {
    if (type.type == TYPE_DECIMAL64) {
        return type_to_string(type.type) + "(p" + std::to_string(type.precision) + "s" + std::to_string(type.scale) +
               ")";
    }
    return type_to_string(type.type);
}

static size_t cast_input_bytes(const TypeDescriptor& type, size_t rows) {
    switch (type.type) {
    case TYPE_INT:
        return rows * sizeof(int32_t);
    case TYPE_DOUBLE:
        return rows * sizeof(double);
    case TYPE_DECIMAL64:
        return rows * sizeof(int64_t);
    default:
        CHECK(false) << "unsupported cast input type: " << type.debug_string();
    }
    return 0;
}

static void bench_cast_to_string_external(benchmark::State& state, TypeDescriptor from_desc, size_t rows) {
    auto input = make_cast_input_column(from_desc, rows);
    auto to_desc = TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);

    ObjectPool pool;
    auto* child = pool.add(new StaticColumnExpr(from_desc, input));
    Expr* expr = VectorizedCastExprFactory::from_type(from_desc, to_desc, child, &pool, false);
    CHECK(expr != nullptr);

    for (auto _ : state) {
        {
            auto result = expr->evaluate_checked(nullptr, nullptr);
            CHECK(result.ok()) << result.status().to_string();
            benchmark::DoNotOptimize(result.value().get());
            benchmark::ClobberMemory();
            state.PauseTiming();
        }
        state.ResumeTiming();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
    const size_t input_bytes = cast_input_bytes(from_desc, rows);
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * input_bytes));
    state.counters["input_bytes_per_iter"] = static_cast<double>(input_bytes);
}

static Schema make_row_store_schema(size_t num_value_columns, bool nullable) {
    CHECK_GT(num_value_columns, 0);
    Fields fields;
    fields.reserve(num_value_columns + 1);
    fields.emplace_back(std::make_shared<Field>(0, "k0", get_type_info(TYPE_INT), STORAGE_AGGREGATE_NONE, 0, true,
                                                false));
    for (size_t i = 0; i < num_value_columns; ++i) {
        fields.emplace_back(std::make_shared<Field>(static_cast<ColumnId>(i + 1), "v" + std::to_string(i),
                                                    get_type_info(TYPE_VARCHAR), STORAGE_AGGREGATE_NONE, 0, false,
                                                    nullable));
    }
    return Schema(std::move(fields), KeysType::DUP_KEYS, {0});
}

static Columns make_row_store_value_columns(size_t rows, size_t len, size_t num_value_columns, bool nullable) {
    Columns columns;
    columns.reserve(num_value_columns);
    for (size_t i = 0; i < num_value_columns; ++i) {
        auto data = make_unique_fixed_column(rows, len, i * rows);
        if (!nullable) {
            columns.emplace_back(std::move(data));
            continue;
        }

        auto nulls = NullColumn::create(rows, 0);
        auto& null_data = nulls->get_data();
        for (size_t row = 0; row < rows; ++row) {
            null_data[row] = static_cast<uint8_t>(pseudo_random(row + i * 131) % 10 == 0);
        }
        auto column = NullableColumn::create(std::move(data), std::move(nulls));
        column->set_has_null(true);
        columns.emplace_back(std::move(column));
    }
    return columns;
}

static void bench_row_store_encoder_simple_encode(benchmark::State& state, size_t rows, size_t len,
                                                  size_t num_value_columns, bool nullable) {
    CHECK_GT(rows, 0);
    CHECK_GT(len, 0);
    CHECK_GT(num_value_columns, 0);

    auto schema = make_row_store_schema(num_value_columns, nullable);
    auto columns = make_row_store_value_columns(rows, len, num_value_columns, nullable);
    RowStoreEncoderSimple encoder;

    for (auto _ : state) {
        state.PauseTiming();
        {
            auto dest = BinaryColumn::create();
            state.ResumeTiming();

            auto st = encoder.encode_columns_to_full_row_column(schema, columns, *dest);
            CHECK(st.ok()) << st.to_string();
            benchmark::DoNotOptimize(dest.get());
            benchmark::DoNotOptimize(dest->get_bytes().data());
            benchmark::ClobberMemory();
            state.PauseTiming();
        }
        state.ResumeTiming();
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * rows * len * num_value_columns));
    state.counters["value_columns_per_row"] = static_cast<double>(num_value_columns);
}

static void bench_column_predicate_in_branchless_external(benchmark::State& state, size_t rows, size_t len) {
    CHECK_LE(rows, static_cast<size_t>(std::numeric_limits<uint16_t>::max()));
    auto column = make_unique_fixed_column(rows, len);
    auto filter = make_filter(rows, FilterPattern::RANDOM_50);

    std::vector<uint16_t> base_sel;
    base_sel.reserve(rows);
    for (size_t i = 0; i < rows; ++i) {
        if (filter[i] != 0) {
            base_sel.emplace_back(static_cast<uint16_t>(i));
        }
    }
    CHECK(!base_sel.empty());

    std::vector<std::string> operands;
    operands.reserve((base_sel.size() + 1) / 2);
    for (size_t i = 0; i < base_sel.size(); i += 2) {
        const auto slice = column->get_slice(base_sel[i]);
        operands.emplace_back(slice.data, slice.size);
    }
    const uint16_t expected_hits = static_cast<uint16_t>(operands.size());
    std::unique_ptr<ColumnPredicate> predicate(new_column_in_predicate(get_type_info(TYPE_VARCHAR), 0, operands));

    std::vector<uint16_t> sel(base_sel.size());
    bool validated = false;
    for (auto _ : state) {
        state.PauseTiming();
        std::memcpy(sel.data(), base_sel.data(), base_sel.size() * sizeof(uint16_t));
        state.ResumeTiming();

        auto result = predicate->evaluate_branchless(column.get(), sel.data(), static_cast<uint16_t>(sel.size()));
        CHECK(result.ok()) << result.status().to_string();
        benchmark::DoNotOptimize(result.value());
        benchmark::DoNotOptimize(sel.data());
        benchmark::ClobberMemory();

        if (!validated) {
            state.PauseTiming();
            CHECK_EQ(result.value(), expected_hits);
            validated = true;
            state.ResumeTiming();
        }
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * base_sel.size()));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * base_sel.size() * (len + sizeof(uint16_t))));
    state.counters["selected_rows_per_iter"] = static_cast<double>(base_sel.size());
    state.counters["expected_hits_per_iter"] = static_cast<double>(expected_hits);
}

static void register_p0_core_cases() {
    for (size_t rows : {65536UL, 262144UL}) {
        for (size_t len : {4UL, 32UL}) {
            register_case(ext_case_name("P0", "append_with_mask",
                                        "rows:" + std::to_string(rows) + "/len:" + std::to_string(len) +
                                                "/select:random50/positive:true"),
                          [=](benchmark::State& state) {
                              bench_append_with_mask_external<true>(state, rows, len, FilterPattern::RANDOM_50);
                          });
        }
    }
    for (size_t len : {4UL, 32UL, 128UL}) {
        register_case(ext_case_name("P0", "append_with_mask",
                                    "rows:65536/len:" + std::to_string(len) + "/select:random10/positive:true"),
                      [=](benchmark::State& state) {
                          bench_append_with_mask_external<true>(state, 65536, len, FilterPattern::RANDOM_10);
                      });
        register_case(ext_case_name("P0", "append_with_mask",
                                    "rows:65536/len:" + std::to_string(len) + "/select:random50/positive:false"),
                      [=](benchmark::State& state) {
                          bench_append_with_mask_external<false>(state, 65536, len, FilterPattern::RANDOM_50);
                      });
    }
    register_case(ext_case_name("P0", "append_with_mask", "rows:65536/len:32/select:keep_all/positive:true"),
                  [](benchmark::State& state) {
                      bench_append_with_mask_external<true>(state, 65536, 32, FilterPattern::KEEP_ALL);
                  });
    register_case(ext_case_name("P0", "append_with_mask", "rows:65536/len:32/select:keep_none/positive:true"),
                  [](benchmark::State& state) {
                      bench_append_with_mask_external<true>(state, 65536, 32, FilterPattern::KEEP_NONE);
                  });
    register_case(ext_case_name("P0", "append_with_mask",
                                "rows:65536/len:128/dist:uniform/select:random50/positive:true"),
                  [](benchmark::State& state) {
                      bench_append_with_mask_external<true>(state, 65536, 128, FilterPattern::RANDOM_50,
                                                            LenPattern::UNIFORM);
                  });

    for (size_t len : {4UL, 32UL, 128UL}) {
        register_case(ext_case_name("P0", "column_hash", "rows:65536/len:" + std::to_string(len) + "/mode:range"),
                      [=](benchmark::State& state) { bench_column_hash_external(state, 65536, len, HashMode::RANGE); });
        for (HashMode mode : {HashMode::SELECTION_RANDOM_50, HashMode::SELECTIVE_CLUSTERED}) {
            register_case(ext_case_name("P0", "column_hash",
                                        "rows:65535/len:" + std::to_string(len) + "/mode:" + hash_mode_name(mode)),
                          [=](benchmark::State& state) { bench_column_hash_external(state, 65535, len, mode); });
        }
    }
    register_case(ext_case_name("P0", "column_hash", "rows:262144/len:4/mode:range"),
                  [](benchmark::State& state) { bench_column_hash_external(state, 262144, 4, HashMode::RANGE); });
    register_case(ext_case_name("P0", "column_hash", "rows:65536/len:128/dist:uniform/mode:range"),
                  [](benchmark::State& state) {
                      bench_column_hash_external(state, 65536, 128, HashMode::RANGE, LenPattern::UNIFORM);
                  });
    register_case(ext_case_name("P0", "column_hash", "rows:65535/len:128/dist:uniform/mode:selection_random50"),
                  [](benchmark::State& state) {
                      bench_column_hash_external(state, 65535, 128, HashMode::SELECTION_RANDOM_50, LenPattern::UNIFORM);
                  });

    for (size_t len : {4UL, 32UL, 128UL}) {
        for (PermutePattern pattern : {PermutePattern::SEQ, PermutePattern::RANDOM, PermutePattern::CLUSTERED}) {
            register_case(
                    ext_case_name("P0", "sort_permute",
                                  "rows:65536/len:" + std::to_string(len) + "/perm:" + permute_pattern_name(pattern)),
                    [=](benchmark::State& state) { bench_sort_permute_external(state, 65536, len, pattern); });
        }
    }
    register_case(ext_case_name("P0", "sort_permute", "rows:65536/len:4/perm:reverse"), [](benchmark::State& state) {
        bench_sort_permute_external(state, 65536, 4, PermutePattern::REVERSE);
    });
    register_case(ext_case_name("P0", "sort_permute", "rows:65536/len:128/dist:uniform/perm:random"),
                  [](benchmark::State& state) {
                      bench_sort_permute_external(state, 65536, 128, PermutePattern::RANDOM, LenPattern::UNIFORM);
                  });

    for (bool nullable : {false, true}) {
        for (int empty_percent : {0, 50, 100}) {
            register_case(ext_case_name("P0", "column_predicate_ne_empty",
                                        "rows:65535/len:4/nullable:" + std::to_string(nullable) +
                                                "/empty_percent:" + std::to_string(empty_percent)),
                          [=](benchmark::State& state) {
                              bench_column_predicate_empty_ne(state, 65535, 4, empty_percent, nullable);
                          });
        }
    }

    for (size_t len : {4UL, 32UL, 128UL}) {
        for (SparsePattern pattern : {SparsePattern::CONTIGUOUS, SparsePattern::CLUSTERED_50}) {
            register_case(
                    ext_case_name("P0", "binary_plain_page_next_batch",
                                  "rows:65536/len:" + std::to_string(len) + "/range:" + sparse_pattern_name(pattern)),
                    [=](benchmark::State& state) { bench_binary_plain_page_append_range(state, 65536, len, pattern); });
        }
    }
    for (size_t len : {4UL, 32UL, 128UL}) {
        for (SparsePattern pattern : {SparsePattern::CONTIGUOUS, SparsePattern::CLUSTERED_50}) {
            register_case(ext_case_name("P0", "binary_plain_page_next_batch_with_filter",
                                        "rows:65535/len:" + std::to_string(len) +
                                                "/range:" + sparse_pattern_name(pattern) + "/nullable:false"),
                          [=](benchmark::State& state) {
                              bench_binary_plain_page_with_filter(state, 65535, len, pattern, false);
                          });
            register_case(ext_case_name("P0", "binary_plain_page_next_batch_with_filter",
                                        "rows:65535/len:" + std::to_string(len) +
                                                "/range:" + sparse_pattern_name(pattern) + "/nullable:true"),
                          [=](benchmark::State& state) {
                              bench_binary_plain_page_with_filter(state, 65535, len, pattern, true);
                          });
        }
    }
    register_case(ext_case_name("P0", "binary_plain_page_next_batch",
                                "rows:65536/len:128/dist:uniform/range:clustered50"),
                  [](benchmark::State& state) {
                      bench_binary_plain_page_append_range(state, 65536, 128, SparsePattern::CLUSTERED_50,
                                                           LenPattern::UNIFORM);
                  });
    register_case(ext_case_name("P0", "binary_plain_page_next_batch_with_filter",
                                "rows:65535/len:128/dist:uniform/range:clustered50/nullable:true"),
                  [](benchmark::State& state) {
                      bench_binary_plain_page_with_filter(state, 65535, 128, SparsePattern::CLUSTERED_50, true,
                                                          LenPattern::UNIFORM);
                  });
    for (size_t len : {4UL, 32UL}) {
        register_case(ext_case_name("P0", "parquet_plain_decode",
                                    "rows:65536/len:" + std::to_string(len) + "/nullable:false/nulls:none"),
                      [=](benchmark::State& state) {
                          bench_parquet_plain_decode(state, 65536, len, false, FilterPattern::KEEP_NONE);
                      });
        register_case(ext_case_name("P0", "parquet_plain_decode",
                                    "rows:65536/len:" + std::to_string(len) + "/nullable:true/nulls:random50"),
                      [=](benchmark::State& state) {
                          bench_parquet_plain_decode(state, 65536, len, true, FilterPattern::RANDOM_50);
                      });
    }

    for (size_t len : {4UL, 32UL, 128UL}) {
        register_case(ext_case_name("P0", "parquet_plain_flba_decode",
                                    "rows:65536/len:" + std::to_string(len) + "/nullable:false/nulls:none"),
                      [=](benchmark::State& state) {
                          bench_parquet_plain_flba_decode(state, 65536, len, false, FilterPattern::KEEP_NONE);
                      });
        register_case(ext_case_name("P0", "parquet_plain_flba_decode",
                                    "rows:65536/len:" + std::to_string(len) + "/nullable:true/nulls:random50"),
                      [=](benchmark::State& state) {
                          bench_parquet_plain_flba_decode(state, 65536, len, true, FilterPattern::RANDOM_50);
                      });
    }

    for (size_t len : {4UL, 32UL, 128UL}) {
        for (size_t cardinality : {16UL, 1024UL}) {
            register_case(
                    ext_case_name("P0", "parquet_dict_decode",
                                  "rows:65536/len:" + std::to_string(len) +
                                          "/cardinality:" + std::to_string(cardinality) + "/nullable:false/nulls:none"),
                    [=](benchmark::State& state) {
                        bench_parquet_dict_decode(state, 65536, len, cardinality, false, FilterPattern::KEEP_NONE);
                    });
            register_case(ext_case_name("P0", "parquet_dict_decode",
                                        "rows:65536/len:" + std::to_string(len) + "/cardinality:" +
                                                std::to_string(cardinality) + "/nullable:true/nulls:random50"),
                          [=](benchmark::State& state) {
                              bench_parquet_dict_decode(state, 65536, len, cardinality, true, FilterPattern::RANDOM_50);
                          });
        }
    }

    for (LogicalType type : {TYPE_CHAR, TYPE_VARCHAR, TYPE_VARBINARY}) {
        for (size_t len : {4UL, 32UL, 128UL}) {
            for (bool nullable : {false, true}) {
                register_case(
                        ext_case_name("P0", "orc_string_reader",
                                      "rows:65536/len:" + std::to_string(len) + "/type:" + type_to_string(type) +
                                              "/nullable:" + std::to_string(nullable)),
                        [=](benchmark::State& state) { bench_orc_string_reader(state, type, 65536, len, nullable); });
            }
        }
    }
}

static void register_p0_boundary_cases() {
    for (size_t rows : {4096UL, 65536UL}) {
        for (size_t len : {4UL, 32UL, 128UL}) {
            register_case(ext_case_name("P0B", "column_array_serde",
                                        "rows:" + std::to_string(rows) + "/len:" + std::to_string(len) +
                                                "/mode:serialize/encode:0"),
                          [=](benchmark::State& state) { bench_column_array_serde_serialize(state, rows, len, 0); });
            register_case(ext_case_name("P0B", "column_array_serde",
                                        "rows:" + std::to_string(rows) + "/len:" + std::to_string(len) +
                                                "/mode:deserialize/encode:0"),
                          [=](benchmark::State& state) { bench_column_array_serde_deserialize(state, rows, len, 0); });
        }
    }
    register_case(ext_case_name("P0B", "column_array_serde",
                                "rows:262144/len:4/mode:serialize/encode:0"),
                  [](benchmark::State& state) { bench_column_array_serde_serialize(state, 262144, 4, 0); });
    register_case(ext_case_name("P0B", "column_array_serde",
                                "rows:262144/len:4/mode:deserialize/encode:0"),
                  [](benchmark::State& state) { bench_column_array_serde_deserialize(state, 262144, 4, 0); });

    for (size_t len : {4UL, 32UL}) {
        register_case(ext_case_name("P0B", "binary_plain_page_zero_copy_offsets_with_alloc",
                                    "rows:65536/len:" + std::to_string(len)),
                      [=](benchmark::State& state) {
                          bench_binary_plain_page_zero_copy_offsets_with_alloc(state, 65536, len);
                      });
        register_case(ext_case_name("P0B", "binary_plain_page_zero_copy_offsets_reuse",
                                    "rows:65536/len:" + std::to_string(len)),
                      [=](benchmark::State& state) {
                          bench_binary_plain_page_zero_copy_offsets_reuse(state, 65536, len);
                      });
    }
    register_case(ext_case_name("P0B", "binary_plain_page_zero_copy_column_hash_consumer_only", "rows:65536/len:32"),
                  [](benchmark::State& state) {
                      bench_binary_plain_page_zero_copy_column_hash_consumer_only(state, 65536, 32);
                  });

    for (int encode_level : {2, 4, 6}) {
        register_case(
                ext_case_name("P0B_EXTRA", "column_array_serde_encoded",
                              "rows:65536/len:32/mode:serialize/encode:" + std::to_string(encode_level)),
                [=](benchmark::State& state) { bench_column_array_serde_serialize(state, 65536, 32, encode_level); });
        register_case(
                ext_case_name("P0B_EXTRA", "column_array_serde_encoded",
                              "rows:65536/len:32/mode:deserialize/encode:" + std::to_string(encode_level)),
                [=](benchmark::State& state) { bench_column_array_serde_deserialize(state, 65536, 32, encode_level); });
    }
    register_case(ext_case_name("P0B_EXTRA", "column_array_serde_encoded",
                                "rows:65536/len:4/mode:serialize/encode:2"),
                  [](benchmark::State& state) { bench_column_array_serde_serialize(state, 65536, 4, 2); });
    register_case(ext_case_name("P0B_EXTRA", "column_array_serde_encoded",
                                "rows:65536/len:4/mode:deserialize/encode:2"),
                  [](benchmark::State& state) { bench_column_array_serde_deserialize(state, 65536, 4, 2); });

}

static void register_p1_cases() {
    // P1 includes integration/regression coverage. Treat P0/P0B no-promote cases as the primary performance signal.
    // Keep these cases module-level: they call real StarRocks components instead of open-coded scalar offset loops.
    for (const auto& spec : {std::tuple<size_t, size_t, bool>{4, 1, false},
                             std::tuple<size_t, size_t, bool>{32, 1, false},
                             std::tuple<size_t, size_t, bool>{4, 4, false},
                             std::tuple<size_t, size_t, bool>{4, 4, true}}) {
        const auto [len, value_columns, nullable] = spec;
        register_case(ext_case_name("P1_EXTRA", "row_store_encoder_simple_encode",
                                    "rows:65536/len:" + std::to_string(len) +
                                            "/value_cols:" + std::to_string(value_columns) +
                                            "/nullable:" + bool_name(nullable)),
                      [=](benchmark::State& state) {
                          bench_row_store_encoder_simple_encode(state, 65536, len, value_columns, nullable);
                      });
    }

    register_case(ext_case_name("P1_EXTRA", "column_predicate_in_branchless",
                                "rows:65535/len:4/select:random50/hit:50"),
                  [](benchmark::State& state) { bench_column_predicate_in_branchless_external(state, 65535, 4); });
    register_case(ext_case_name("P1_EXTRA", "column_predicate_in_branchless",
                                "rows:65535/len:32/select:random50/hit:50"),
                  [](benchmark::State& state) { bench_column_predicate_in_branchless_external(state, 65535, 32); });

    register_case(ext_case_name("P1_EXTRA", "binary_plain_page_dict_filter_selection",
                                "rows:70000/len:8/predicate:ge_mid"),
                  [](benchmark::State& state) { bench_binary_plain_page_dict_filter_selection(state, 70000, 8); });

    for (size_t rows : {4096UL, 65536UL}) {
        for (size_t len : {4UL, 32UL, 128UL}) {
            for (bool nullable : {false, true}) {
                register_case(ext_case_name("P1", "nullable_binary_column_builder_append_build",
                                            "rows:" + std::to_string(rows) + "/len:" + std::to_string(len) +
                                                    "/nullable:" + bool_name(nullable)),
                              [=](benchmark::State& state) {
                                  bench_nullable_binary_column_builder(state, rows, len, nullable);
                              });
            }
        }
    }

    for (size_t num_segments : {2UL, 8UL}) {
        for (size_t len : {4UL, 32UL}) {
            for (IndexPattern pattern : {IndexPattern::RANDOM, IndexPattern::CLUSTERED}) {
                register_case(ext_case_name("P1", "segmented_column_clone_selective",
                                            "segments:" + std::to_string(num_segments) + "/segment_rows:8192/len:" +
                                                    std::to_string(len) + "/select:" + index_pattern_name(pattern)),
                              [=](benchmark::State& state) {
                                  bench_segmented_column_clone_selective(state, num_segments, 8192, len, pattern);
                              });
            }
        }
    }

    for (ArrowStringKind kind : {ArrowStringKind::STRING, ArrowStringKind::BINARY,
                                 ArrowStringKind::FIXED_SIZE_BINARY}) {
        for (size_t len : {4UL, 32UL}) {
            for (bool nullable : {false, true}) {
                register_case(ext_case_name("P1", "arrow_string_converter",
                                            "rows:65536/len:" + std::to_string(len) +
                                                    "/arrow:" + arrow_string_kind_name(kind) +
                                                    "/nullable:" + std::to_string(nullable)),
                              [=](benchmark::State& state) {
                                  bench_arrow_string_converter(state, kind, 65536, len, nullable);
                              });
            }
        }
    }
    for (ArrowStringKind kind : {ArrowStringKind::STRING, ArrowStringKind::BINARY,
                                 ArrowStringKind::FIXED_SIZE_BINARY}) {
        for (bool nullable : {false, true}) {
            register_case(ext_case_name("P1", "arrow_string_converter_nonzero_source_offset",
                                        "rows:65536/len:32/arrow:" + std::string(arrow_string_kind_name(kind)) +
                                                "/source_offset:7/nullable:" + std::to_string(nullable)),
                          [=](benchmark::State& state) {
                              bench_arrow_string_converter_nonzero_offset(state, kind, 65536, 32, nullable, false);
                          });
            register_case(ext_case_name("P1", "arrow_string_converter_sliced_array_offset",
                                        "rows:65536/len:32/arrow:" + std::string(arrow_string_kind_name(kind)) +
                                                "/array_offset:7/nullable:" + std::to_string(nullable)),
                          [=](benchmark::State& state) {
                              bench_arrow_string_converter_nonzero_offset(state, kind, 65536, 32, nullable, true);
                          });
        }
    }

    for (StringFunctionCase fn : {StringFunctionCase::SUBSTRING, StringFunctionCase::SUBSTRING_CONST,
                                  StringFunctionCase::RIGHT, StringFunctionCase::CONCAT, StringFunctionCase::LOCATE,
                                  StringFunctionCase::LIKE_PREFIX, StringFunctionCase::LIKE_SUBSTRING}) {
        for (size_t len : {4UL, 32UL, 128UL}) {
            register_case(
                    ext_case_name("P1_EXTRA", "string_function",
                                  "rows:65536/len:" + std::to_string(len) + "/fn:" + string_function_case_name(fn)),
                    [=](benchmark::State& state) { bench_string_function_external(state, fn, 65536, len); });
        }
    }

    for (const auto& spec :
         {std::tuple<size_t, size_t, size_t, bool, JoinKeyMode>{65536, 4, 1, false, JoinKeyMode::FIXED_SIZE},
          std::tuple<size_t, size_t, size_t, bool, JoinKeyMode>{65536, 16, 1, true, JoinKeyMode::FIXED_SIZE},
          std::tuple<size_t, size_t, size_t, bool, JoinKeyMode>{262144, 8, 1, false, JoinKeyMode::FIXED_SIZE},
          std::tuple<size_t, size_t, size_t, bool, JoinKeyMode>{65536, 4, 2, false, JoinKeyMode::SERIALIZED_VARCHAR},
          std::tuple<size_t, size_t, size_t, bool, JoinKeyMode>{65536, 32, 2, false, JoinKeyMode::SERIALIZED_VARCHAR},
          std::tuple<size_t, size_t, size_t, bool, JoinKeyMode>{65536, 4, 2, true, JoinKeyMode::SERIALIZED_VARCHAR},
          std::tuple<size_t, size_t, size_t, bool, JoinKeyMode>{65536, 4, 4, false,
                                                                JoinKeyMode::SERIALIZED_VARCHAR}}) {
        const auto [rows, len, key_columns, nullable, key_mode] = spec;
        register_case(
                ext_case_name("P1", "join_hash_table_binary_key_build_only",
                              "rows:" + std::to_string(rows) + "/len:" + std::to_string(len) +
                                      "/key_cols:" + std::to_string(key_columns) +
                                      "/nullable:" + bool_name(nullable) +
                                      "/key_mode:" + join_key_mode_name(key_mode) +
                                      "/fixed_size_opt:" + bool_name(key_mode == JoinKeyMode::FIXED_SIZE) +
                                      "/cardinality:unique"),
                [=](benchmark::State& state) {
                    bench_join_hash_table_binary_key_build_only(state, rows, len, nullable, key_mode, key_columns);
                });
    }

    for (AggConvertCase fn : {AggConvertCase::GROUP_CONCAT, AggConvertCase::MAX_BY}) {
        for (size_t len : {4UL, 32UL, 128UL}) {
            register_case(ext_case_name("P1", "agg_convert_to_serialize_format",
                                        "rows:65536/len:" + std::to_string(len) + "/fn:" + agg_convert_case_name(fn)),
                          [=](benchmark::State& state) {
                              bench_agg_convert_to_serialize_format_external(state, fn, 65536, len);
                          });
        }
    }
    for (AggConvertCase fn : {AggConvertCase::AVG, AggConvertCase::VARIANCE}) {
        register_case(
                ext_case_name("P1", "agg_convert_to_serialize_format",
                              "rows:65536/len:na/fn:" + std::string(agg_convert_case_name(fn))),
                [=](benchmark::State& state) { bench_agg_convert_to_serialize_format_external(state, fn, 65536, 0); });
    }
    register_case(
            ext_case_name("P1_EXTRA", "agg_convert_to_serialize_format", "rows:65536/len:na/fn:percentile_approx"),
            [](benchmark::State& state) {
                bench_agg_convert_to_serialize_format_external(state, AggConvertCase::PERCENTILE_APPROX, 65536, 0);
            });
    register_case(
            ext_case_name("P1_EXTRA", "agg_convert_to_serialize_format", "rows:65536/len:32/fn:ds_hll_count_distinct"),
            [](benchmark::State& state) {
                bench_agg_convert_to_serialize_format_external(state, AggConvertCase::DS_HLL_COUNT_DISTINCT, 65536, 32);
            });

    for (AggSerializeCase fn :
         {AggSerializeCase::GROUP_CONCAT, AggSerializeCase::MAX_BY, AggSerializeCase::DS_HLL_COUNT_DISTINCT}) {
        for (size_t len : {4UL, 32UL, 128UL}) {
            register_case(
                    ext_case_name("P1_EXTRA", "agg_batch_serialize",
                                  "rows:4096/len:" + std::to_string(len) + "/fn:" + agg_serialize_case_name(fn) +
                                          "/values_per_state:4"),
                    [=](benchmark::State& state) { bench_agg_batch_serialize_external(state, fn, 4096, len, 4); });
        }
        if (fn != AggSerializeCase::DS_HLL_COUNT_DISTINCT) {
            register_case(
                    ext_case_name(
                            "P1_EXTRA", "agg_batch_serialize",
                            "rows:65536/len:32/fn:" + std::string(agg_serialize_case_name(fn)) + "/values_per_state:2"),
                    [=](benchmark::State& state) { bench_agg_batch_serialize_external(state, fn, 65536, 32, 2); });
        }
    }
    register_case(
            ext_case_name("P1_EXTRA", "agg_batch_serialize", "rows:4096/len:8/fn:percentile_cont/values_per_state:16"),
            [](benchmark::State& state) {
                bench_agg_batch_serialize_external(state, AggSerializeCase::PERCENTILE_CONT, 4096, 8, 16);
            });
    register_case(
            ext_case_name("P1_EXTRA", "agg_batch_serialize", "rows:65536/len:8/fn:percentile_cont/values_per_state:4"),
            [](benchmark::State& state) {
                bench_agg_batch_serialize_external(state, AggSerializeCase::PERCENTILE_CONT, 65536, 8, 4);
            });

    for (size_t len : {4UL, 32UL, 128UL}) {
        for (bool nullable : {false, true}) {
            register_case(
                    ext_case_name("P1", "parquet_level_builder_byte_array",
                                  "rows:65536/len:" + std::to_string(len) + "/nullable:" + std::to_string(nullable)),
                    [=](benchmark::State& state) {
                        bench_parquet_level_builder_byte_array(state, 65536, len, nullable);
                    });
        }
    }

    for (size_t len : {4UL, 32UL, 128UL}) {
        for (bool nullable : {false, true}) {
            register_case(ext_case_name("P1_EXTRA", "chunk_helper_padding_char_column",
                                        "rows:65536/char_len:" + std::to_string(len) +
                                                "/nullable:" + std::to_string(nullable)),
                          [=](benchmark::State& state) {
                              bench_chunk_helper_padding_char_column(state, 65536, len, nullable);
                          });
        }
    }

    for (size_t array_size : {4UL, 16UL}) {
        for (size_t len : {4UL, 32UL}) {
            register_case(ext_case_name("P1_EXTRA", "array_reverse_string",
                                        "rows:65536/array_size:" + std::to_string(array_size) +
                                                "/len:" + std::to_string(len) + "/nullable:false"),
                          [=](benchmark::State& state) {
                              bench_array_reverse_string_external(state, 65536, array_size, len, false);
                          });
            register_case(ext_case_name("P1_EXTRA", "array_reverse_string",
                                        "rows:65536/array_size:" + std::to_string(array_size) +
                                                "/len:" + std::to_string(len) + "/nullable:true"),
                          [=](benchmark::State& state) {
                              bench_array_reverse_string_external(state, 65536, array_size, len, true);
                          });
        }
    }

    std::vector<TypeDescriptor> cast_from_types{TypeDescriptor::from_logical_type(TYPE_INT),
                                                TypeDescriptor::from_logical_type(TYPE_DOUBLE),
                                                TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 18, 2)};
    for (const auto& from_type : cast_from_types) {
        register_case(ext_case_name("P1_EXTRA", "cast_to_string",
                                    "rows:65536/from:" + cast_type_case_name(from_type) + "/to:VARCHAR"),
                      [=](benchmark::State& state) { bench_cast_to_string_external(state, from_type, 65536); });
    }
}

static void register_binary_column_external_offset() {
    register_p0_core_cases();
    register_p0_boundary_cases();
    register_p1_cases();
}

const bool kRegistered = [] {
    register_binary_column_external_offset();
    return true;
}();

} // namespace
} // namespace starrocks::bench

int main(int argc, char** argv) {
    starrocks::CpuInfo::init();
    benchmark::Initialize(&argc, argv);
    if (benchmark::ReportUnrecognizedArguments(argc, argv)) {
        return 1;
    }
    benchmark::RunSpecifiedBenchmarks();
    benchmark::Shutdown();
    return 0;
}
