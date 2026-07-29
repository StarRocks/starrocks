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

#pragma once

// Shared data generator for the spill serde micro-benchmarks (P1 of the plan).
//
// Design goals:
//   - Reproducible: every dataset is produced by a fixed-seed PRNG, so a given
//     (dataset, config) pair yields byte-identical chunks across runs and across
//     StarRocks code versions. This makes the L1/L2/L3 benches comparable A/B slots.
//   - Explainable: the dataset matrix is designed around the "four kinds of
//     redundancy" taxonomy, so each variant maps to a codec that is expected to
//     win or lose on it. See kAllDatasets below.
//   - Self-contained: only depends on Column/Chunk + type descriptors, so it can
//     be included by the lowest-dependency L1 bench without any Spiller/RuntimeState.

#include <cstdint>
#include <cstdio>
#include <random>
#include <string>
#include <vector>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "types/date_value.h"
#include "types/datum.h"
#include "types/logical_type.h"
#include "types/timestamp_value.h"
#include "types/type_descriptor.h"

namespace starrocks::spill_bench {

// The dataset matrix. Each entry targets one leaf of the codec decision tree
// (type -> data shape -> algorithm), so that per-dataset results are directly
// interpretable and each future codec has a dedicated acceptance probe.
enum class DataSet {
    // bool
    BOOL_RUNS,   // long constant runs (sorted/state flags) -> RLE leaf
    BOOL_RANDOM, // random 0/1 -> bitmap bit-packing leaf
    // int
    INT64_CONST,   // constant column -> extreme skew / RLE leaf
    INT64_SEQ,     // monotonically increasing -> delta(+zigzag) leaf
    INT64_NARROW,  // random over ~1e4 domain, clean -> FOR/bit-packing leaf
    INT64_OUTLIER, // narrow domain + ~1% full-domain outliers -> PFOR leaf
    INT64_RANDOM,  // random over full domain -> no redundancy / raw leaf
    // decimal (= scaled integer)
    DECIMAL64_MONEY, // DECIMAL64(18,2) amounts -> scaled-int, joins the int tree
    // float
    DOUBLE_DECIMAL, // 2-decimal values stored as double -> ALP leaf
    DOUBLE_SMOOTH,  // random-walk sensor series -> smooth, XOR-of-consecutive-values friendly
    DOUBLE_RANDOM,  // random mantissa -> ALP_rd / raw leaf
    // timestamp / date
    TS_MONOTONIC, // near-monotonic event time (jittered) -> delta-of-delta leaf
    DATE_LOWRES,  // long runs of the same date -> RLE/dict leaf
    // string
    STR_LOWCARD,     // ~1e3 distinct values -> dictionary leaf
    STR_TEMPLATE,    // url/email templates -> substring repetition, FSST leaf
    STR_SORTED,      // sorted keys with shared prefixes -> front-coding leaf
    STR_UUID,        // hex-UUID -> structural skew, FSST-vs-binary boundary
    STR_HIGHENTROPY, // random chars -> no redundancy / raw leaf
    STR_LONGTEXT,    // 0.4-1.4KB sentences over a small vocabulary -> block Zstd/LZ4 leaf
    // realistic mix
    WIDE_MIXED, // 10-column fact-table-ish chunk (join-build spill shape)
};

struct DataSetInfo {
    DataSet id;
    const char* name;
    const char* redundancy; // short human tag, echoed into the manifest
};

// Canonical order + stable string names (used for bench labels, on-disk file names,
// and the manifest). Keep names stable: they are the join key of the whole pipeline.
inline const std::vector<DataSetInfo>& all_datasets() {
    static const std::vector<DataSetInfo> kAll = {
            {DataSet::BOOL_RUNS, "bool_runs", "rle"},
            {DataSet::BOOL_RANDOM, "bool_random", "bitmap"},
            {DataSet::INT64_CONST, "int64_const", "rle"},
            {DataSet::INT64_SEQ, "int64_seq", "delta"},
            {DataSet::INT64_NARROW, "int64_narrow", "for-bitpack"},
            {DataSet::INT64_OUTLIER, "int64_outlier", "pfor"},
            {DataSet::INT64_RANDOM, "int64_random", "raw"},
            {DataSet::DECIMAL64_MONEY, "decimal64_money", "scaled-int"},
            {DataSet::DOUBLE_DECIMAL, "double_decimal", "alp"},
            {DataSet::DOUBLE_SMOOTH, "double_smooth", "gorilla-xor"},
            {DataSet::DOUBLE_RANDOM, "double_random", "alp-rd/raw"},
            {DataSet::TS_MONOTONIC, "ts_monotonic", "delta-of-delta"},
            {DataSet::DATE_LOWRES, "date_lowres", "rle/dict"},
            {DataSet::STR_LOWCARD, "str_lowcard", "dict"},
            {DataSet::STR_TEMPLATE, "str_template", "fsst"},
            {DataSet::STR_SORTED, "str_sorted", "front-coding"},
            {DataSet::STR_UUID, "str_uuid", "fsst/binary"},
            {DataSet::STR_HIGHENTROPY, "str_highentropy", "raw"},
            {DataSet::STR_LONGTEXT, "str_longtext", "block-zstd"},
            {DataSet::WIDE_MIXED, "wide_mixed", "mixed"},
    };
    return kAll;
}

inline const char* dataset_name(DataSet ds) {
    for (const auto& info : all_datasets()) {
        if (info.id == ds) return info.name;
    }
    return "unknown";
}

struct GenConfig {
    size_t num_rows = 4096;            // one chunk worth of rows by default
    double null_ratio = 0.0;           // fraction of NULLs (0..1); columns are always nullable-typed
    size_t str_avg_len = 24;           // average string length for the string variants
    size_t cardinality = 1000;         // number of distinct values for low-cardinality variants
    uint64_t seed = 0x5EED5EED5EEDULL; // base seed; per-dataset seeds are derived from this
};

// A deterministic 64-bit stream seeded per (base seed, dataset, salt) so that different
// datasets/columns never share a stream while a given config stays reproducible.
class Rng {
public:
    Rng(uint64_t base_seed, uint64_t salt) : _gen(mix(base_seed, salt)) {}

    uint64_t next_u64() { return _gen(); }
    // uniform in [lo, hi]
    int64_t uniform_i64(int64_t lo, int64_t hi) {
        std::uniform_int_distribution<int64_t> dist(lo, hi);
        return dist(_gen);
    }
    double uniform_double(double lo, double hi) {
        std::uniform_real_distribution<double> dist(lo, hi);
        return dist(_gen);
    }
    double normal(double mean, double stddev) {
        std::normal_distribution<double> dist(mean, stddev);
        return dist(_gen);
    }
    // returns true with probability p
    bool chance(double p) {
        if (p <= 0.0) return false;
        std::uniform_real_distribution<double> dist(0.0, 1.0);
        return dist(_gen) < p;
    }

private:
    static uint64_t mix(uint64_t a, uint64_t b) {
        // splitmix64-style finalizer over a+b to decorrelate salts
        uint64_t x = a + 0x9E3779B97F4A7C15ULL * (b + 1);
        x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ULL;
        x = (x ^ (x >> 27)) * 0x94D049BB133111EBULL;
        return x ^ (x >> 31);
    }
    std::mt19937_64 _gen;
};

namespace detail {

inline std::string make_hex_uuid(Rng& rng) {
    static const char* hex = "0123456789abcdef";
    std::string s;
    s.reserve(36);
    for (int i = 0; i < 32; ++i) {
        if (i == 8 || i == 12 || i == 16 || i == 20) s.push_back('-');
        s.push_back(hex[rng.next_u64() & 0xF]);
    }
    return s;
}

// Compose a url/email-like string from a small fixed set of templates so that short
// substrings ("https://", ".com/", "@example.org") repeat heavily across rows.
inline std::string make_template_string(Rng& rng, size_t /*avg_len*/) {
    static const char* schemes[] = {"https://", "http://"};
    static const char* hosts[] = {"www.example.com", "api.example.org", "cdn.starrocks.io", "mail.example.net"};
    static const char* paths[] = {"/v1/users/", "/query?id=", "/static/img/", "/account/settings/"};
    const char* scheme = schemes[rng.next_u64() % 2];
    const char* host = hosts[rng.next_u64() % 4];
    const char* path = paths[rng.next_u64() % 4];
    // a variable numeric suffix so rows are distinct but share long common prefixes
    return std::string(scheme) + host + path + std::to_string(rng.uniform_i64(0, 1000000));
}

// Long-text rows: sentences drawn from a small vocabulary, so block compressors (Zstd/LZ4)
// find plenty of cross-row redundancy while per-row strings stay distinct.
inline std::string make_longtext(Rng& rng) {
    static const char* vocab[] = {
            "query",    "engine",   "vectorized", "spill",    "memory",  "column",  "encode",  "compression",
            "operator", "pipeline", "chunk",      "storage",  "segment", "rowset",  "tablet",  "replica",
            "shuffle",  "exchange", "aggregate",  "join",     "scan",    "filter",  "project", "sort",
            "buffer",   "flush",    "restore",    "iterator", "metric",  "latency", "through", "bandwidth"};
    constexpr size_t kVocab = sizeof(vocab) / sizeof(vocab[0]);
    size_t words = 60 + rng.next_u64() % 140; // ~60-200 words => ~0.4-1.4KB
    std::string s;
    s.reserve(words * 8);
    for (size_t i = 0; i < words; ++i) {
        if (i) s.push_back(' ');
        s += vocab[rng.next_u64() % kVocab];
    }
    return s;
}

inline std::string make_random_string(Rng& rng, size_t len) {
    static const char* alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    std::string s;
    s.reserve(len);
    for (size_t i = 0; i < len; ++i) {
        s.push_back(alphabet[rng.next_u64() % 62]);
    }
    return s;
}

} // namespace detail

// Build a single-type column for the scalar datasets. Column is nullable so that the
// null_column path (also serialized) is exercised, matching real spill chunks.
inline ColumnPtr build_scalar_column(DataSet ds, const GenConfig& cfg, uint64_t salt) {
    Rng rng(cfg.seed, salt);

    auto append = [&](const MutableColumnPtr& col, auto&& value_fn) {
        for (size_t i = 0; i < cfg.num_rows; ++i) {
            if (cfg.null_ratio > 0.0 && rng.chance(cfg.null_ratio)) {
                col->append_default();
            } else {
                value_fn(col);
            }
        }
    };

    switch (ds) {
    case DataSet::BOOL_RUNS: {
        // long constant runs (geometric-ish, avg ~512) of 0/1, like sorted state flags
        auto col = ColumnHelper::create_column(TypeDescriptor(TYPE_BOOLEAN), true);
        col->reserve(cfg.num_rows);
        uint8_t cur = 0;
        size_t left = 0;
        append(col, [&](const MutableColumnPtr& c) {
            if (left == 0) {
                cur = static_cast<uint8_t>(rng.next_u64() & 1);
                left = 64 + rng.next_u64() % 896; // run of 64..960
            }
            --left;
            c->append_datum(Datum(cur));
        });
        return col;
    }
    case DataSet::BOOL_RANDOM: {
        auto col = ColumnHelper::create_column(TypeDescriptor(TYPE_BOOLEAN), true);
        col->reserve(cfg.num_rows);
        append(col,
               [&](const MutableColumnPtr& c) { c->append_datum(Datum(static_cast<uint8_t>(rng.next_u64() & 1))); });
        return col;
    }
    case DataSet::INT64_OUTLIER: {
        // clustered narrow domain, but ~1% full-domain outliers (PFOR probe)
        auto col = ColumnHelper::create_column(TypeDescriptor(TYPE_BIGINT), true);
        col->reserve(cfg.num_rows);
        append(col, [&](const MutableColumnPtr& c) {
            if (rng.next_u64() % 100 == 0) {
                c->append_datum(Datum(static_cast<int64_t>(rng.next_u64())));
            } else {
                c->append_datum(Datum(rng.uniform_i64(0, 4095)));
            }
        });
        return col;
    }
    case DataSet::DECIMAL64_MONEY: {
        // DECIMAL64(18,2): money-like amounts stored as scaled int64 (cents)
        auto col = ColumnHelper::create_column(TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 18, 2), true);
        col->reserve(cfg.num_rows);
        append(col, [&](const MutableColumnPtr& c) { c->append_datum(Datum(rng.uniform_i64(0, 100000000))); });
        return col;
    }
    case DataSet::DOUBLE_SMOOTH: {
        // random-walk sensor series: smooth, XOR-of-consecutive-values friendly
        auto col = ColumnHelper::create_column(TypeDescriptor(TYPE_DOUBLE), true);
        col->reserve(cfg.num_rows);
        double v = 100.0;
        append(col, [&](const MutableColumnPtr& c) {
            v += rng.normal(0.0, 0.5);
            c->append_datum(Datum(v));
        });
        return col;
    }
    case DataSet::TS_MONOTONIC: {
        // near-monotonic event time: advancing seconds counter with 0-3s jitter,
        // several events may share a second (log/event-time shape)
        auto col = ColumnHelper::create_column(TypeDescriptor(TYPE_DATETIME), true);
        col->reserve(cfg.num_rows);
        int64_t sec = static_cast<int64_t>(salt % 1000) * 3600; // per-salt phase
        append(col, [&](const MutableColumnPtr& c) {
            sec += rng.next_u64() % 4; // 0..3 s forward
            int64_t s = sec;
            int day = static_cast<int>((s / 86400) % 28) + 1;
            int hour = static_cast<int>((s % 86400) / 3600);
            int minute = static_cast<int>((s % 3600) / 60);
            int second = static_cast<int>(s % 60);
            c->append_datum(Datum(TimestampValue::create(2026, 7, day, hour, minute, second)));
        });
        return col;
    }
    case DataSet::DATE_LOWRES: {
        // long runs of the same date (daily-partitioned data spilled in order)
        auto col = ColumnHelper::create_column(TypeDescriptor(TYPE_DATE), true);
        col->reserve(cfg.num_rows);
        size_t left = 0;
        int day = 1, month = 1;
        append(col, [&](const MutableColumnPtr& c) {
            if (left == 0) {
                left = 1024 + rng.next_u64() % 2048; // run of 1K..3K rows
                if (++day > 28) {
                    day = 1;
                    month = month % 12 + 1;
                }
            }
            --left;
            c->append_datum(Datum(DateValue::create(2026, month, day)));
        });
        return col;
    }
    case DataSet::STR_SORTED: {
        // ascending keys sharing long prefixes (index keys / dict entries; front-coding probe)
        auto col = ColumnHelper::create_column(TypeDescriptor::create_varchar_type(1024), true);
        col->reserve(cfg.num_rows);
        uint64_t base = (salt % 1000) * 10000000ULL;
        uint64_t i = 0;
        append(col, [&](const MutableColumnPtr& c) {
            char buf[32];
            int n = snprintf(buf, sizeof(buf), "key_%012llu", static_cast<unsigned long long>(base + i));
            i += 1 + rng.next_u64() % 3; // strictly increasing, small gaps
            c->append_datum(Datum(Slice(buf, n)));
        });
        return col;
    }
    case DataSet::STR_LONGTEXT: {
        auto col = ColumnHelper::create_column(TypeDescriptor::create_varchar_type(65533), true);
        col->reserve(cfg.num_rows);
        append(col, [&](const MutableColumnPtr& c) {
            std::string s = detail::make_longtext(rng);
            c->append_datum(Datum(Slice(s)));
        });
        return col;
    }
    case DataSet::INT64_CONST: {
        auto col = ColumnHelper::create_column(TypeDescriptor(TYPE_BIGINT), true);
        col->reserve(cfg.num_rows);
        append(col, [&](const MutableColumnPtr& c) { c->append_datum(Datum(int64_t{42})); });
        return col;
    }
    case DataSet::INT64_SEQ: {
        auto col = ColumnHelper::create_column(TypeDescriptor(TYPE_BIGINT), true);
        col->reserve(cfg.num_rows);
        int64_t v = 0;
        append(col, [&](const MutableColumnPtr& c) { c->append_datum(Datum(int64_t{v++})); });
        return col;
    }
    case DataSet::INT64_NARROW: {
        auto col = ColumnHelper::create_column(TypeDescriptor(TYPE_BIGINT), true);
        col->reserve(cfg.num_rows);
        append(col, [&](const MutableColumnPtr& c) { c->append_datum(Datum(rng.uniform_i64(0, 9999))); });
        return col;
    }
    case DataSet::INT64_RANDOM: {
        auto col = ColumnHelper::create_column(TypeDescriptor(TYPE_BIGINT), true);
        col->reserve(cfg.num_rows);
        append(col, [&](const MutableColumnPtr& c) { c->append_datum(Datum(static_cast<int64_t>(rng.next_u64()))); });
        return col;
    }
    case DataSet::STR_LOWCARD: {
        // Precompute a dictionary of `cardinality` distinct values, then sample from it.
        Rng dict_rng(cfg.seed, salt ^ 0xD1C7ULL);
        std::vector<std::string> dict;
        dict.reserve(cfg.cardinality);
        for (size_t i = 0; i < cfg.cardinality; ++i) {
            dict.push_back(detail::make_random_string(dict_rng, cfg.str_avg_len));
        }
        auto col = ColumnHelper::create_column(TypeDescriptor::create_varchar_type(1024), true);
        col->reserve(cfg.num_rows);
        append(col, [&](const MutableColumnPtr& c) {
            const std::string& s = dict[rng.next_u64() % dict.size()];
            c->append_datum(Datum(Slice(s)));
        });
        return col;
    }
    case DataSet::STR_TEMPLATE: {
        auto col = ColumnHelper::create_column(TypeDescriptor::create_varchar_type(1024), true);
        col->reserve(cfg.num_rows);
        append(col, [&](const MutableColumnPtr& c) {
            std::string s = detail::make_template_string(rng, cfg.str_avg_len);
            c->append_datum(Datum(Slice(s)));
        });
        return col;
    }
    case DataSet::STR_UUID: {
        auto col = ColumnHelper::create_column(TypeDescriptor::create_varchar_type(1024), true);
        col->reserve(cfg.num_rows);
        append(col, [&](const MutableColumnPtr& c) {
            std::string s = detail::make_hex_uuid(rng);
            c->append_datum(Datum(Slice(s)));
        });
        return col;
    }
    case DataSet::STR_HIGHENTROPY: {
        auto col = ColumnHelper::create_column(TypeDescriptor::create_varchar_type(1024), true);
        col->reserve(cfg.num_rows);
        append(col, [&](const MutableColumnPtr& c) {
            std::string s = detail::make_random_string(rng, cfg.str_avg_len);
            c->append_datum(Datum(Slice(s)));
        });
        return col;
    }
    case DataSet::DOUBLE_DECIMAL: {
        auto col = ColumnHelper::create_column(TypeDescriptor(TYPE_DOUBLE), true);
        col->reserve(cfg.num_rows);
        append(col, [&](const MutableColumnPtr& c) {
            // two-decimal "price-like" values: k/100.0
            int64_t cents = rng.uniform_i64(0, 1000000);
            c->append_datum(Datum(static_cast<double>(cents) / 100.0));
        });
        return col;
    }
    case DataSet::DOUBLE_RANDOM: {
        auto col = ColumnHelper::create_column(TypeDescriptor(TYPE_DOUBLE), true);
        col->reserve(cfg.num_rows);
        append(col, [&](const MutableColumnPtr& c) { c->append_datum(Datum(rng.uniform_double(-1e12, 1e12))); });
        return col;
    }
    default:
        return nullptr;
    }
}

// Build a chunk for the given dataset. For scalar datasets this is a single-column
// chunk; for WIDE_MIXED it is a multi-column chunk shaped like a join-build spill chunk.
inline ChunkPtr build_chunk(DataSet ds, const GenConfig& cfg) {
    auto chunk = std::make_shared<Chunk>();
    if (ds == DataSet::WIDE_MIXED) {
        // A realistic fact-table / join-build side, one column per major type family:
        // id (seq), event time, date, state flag, money, narrow FK, low-card category,
        // template url, smooth metric, trailing hash (no redundancy).
        const DataSet specs[] = {
                DataSet::INT64_SEQ,       DataSet::TS_MONOTONIC, DataSet::DATE_LOWRES, DataSet::BOOL_RUNS,
                DataSet::DECIMAL64_MONEY, DataSet::INT64_NARROW, DataSet::STR_LOWCARD, DataSet::STR_TEMPLATE,
                DataSet::DOUBLE_SMOOTH,   DataSet::INT64_RANDOM,
        };
        SlotId slot = 0;
        for (const auto& spec : specs) {
            chunk->append_column(build_scalar_column(spec, cfg, 0x100 + slot), slot);
            ++slot;
        }
        return chunk;
    }
    chunk->append_column(build_scalar_column(ds, cfg, 0x1), 0);
    return chunk;
}

} // namespace starrocks::spill_bench
