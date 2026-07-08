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

// Benchmarks the Hive/data-lake CSV read path at two levels, through REAL file I/O, so
// an optimization can be compared apples-to-apples across parser versions:
//
//   * Parse  level -- reader only: turn bytes into fields (no conversion).
//   * Scan   level -- reader + convert: fields -> typed Columns, processed in
//                     chunk_size batches (mirrors HdfsTextScanner::get_next),
//                     so output-column memory stays bounded even for a 1GB input.
//
// The benchmark spine is version-agnostic: input bytes in, a Column/checksum out.
// v1 and v2 both find the physical line the same way (CSVReader::next_record(Record*),
// memchr-based) -- that step is shared, not where they diverge. v1 then splits fields
// with the quote/escape-blind split_record(); v2 hands the line to the serde-specific
// splitter HdfsTextScanner::_parse_csv_v2 actually uses today (split_hive_open_csv_line
// for OpenCSVSerde, split_hive_lazy_simple_line for LazySimpleSerDe with ESCAPED BY).
// Neither arm goes through CSVReader::more_rows() -- that generic escape/enclose state
// machine is no longer on the Hive read path; it now only serves broker/stream load's
// csv_scanner.cpp, a different consumer this bench does not model.
//
// Every benchmark reads through real SequentialFile I/O (open + read syscalls), the
// SAME path production uses -- no in-memory-buffer shortcuts. Each iteration re-reads
// the scenario's file from disk; whether that hits the OS page cache or real disk
// depends on machine state, not on anything this bench special-cases.
//
// Every BM_Csv entry additionally reports a wall-clock breakdown as UserCounters,
// all in milliseconds, on top of benchmark's own Time/CPU/bytes_per_second:
//   io_ms      -- time spent inside SequentialFile::read() (real I/O, page-cache hit
//                 or actual disk wait, whichever it happens to be).
//   convert_ms -- Scan level only: time spent turning split fields into typed Columns
//                 (csv::Converter::read_string). Always 0 at Parse level.
//   parse_ms   -- everything else: line-finding (next_record) + field-splitting
//                 (split_record / split_hive_*_line). Derived as
//                 (this benchmark's own wall-clock loop time) - io_ms - convert_ms,
//                 not measured with its own clock calls, to keep the per-row
//                 instrumentation overhead to one clock pair (io) plus one more at
//                 Scan level (convert) instead of three.
//
// The realistic-scale scenarios (unsuffixed, e.g. "lazy" vs "lazy_small") default to
// ~1GB (LONG/VARCHAR/DATETIME/DECIMAL); override the size with env CSV_BENCH_TARGET_MB
// (e.g. =128 for a quick run, or =10240 for a real 10G run). Override where the
// generated file lands with env CSV_BENCH_DATA_DIR (default /tmp/csv_scanner_bench_data)
// -- set this on a shared machine where /tmp is a small, shared root partition rather
// than scratch space.
//
// "arraymap" scenarios add an ARRAY<VARCHAR> and a MAP<VARCHAR,VARCHAR> column to an
// otherwise-scalar table. The *_esc_* variant additionally embeds an escaped
// collection/mapkey delimiter INSIDE the array/map column's own text -- this is the
// one thing the plain "lazy_esc" scenarios above cannot exercise (those only escape a
// TOP-LEVEL field separator). Only the Scan-level *_esc_* benchmarks are registered:
// Parse only ever splits top-level fields (never recurses into array/map), so it can't
// exercise HiveTextArrayReader::split_array_elements's/MapConverter::read_hive_map's
// own escape-aware boundary scan -- Scan does, via NullableConverter -> read_string.
//
// Each benchmark prints a one-line "start"/"done" progress marker to stderr, from
// outside the timed for(auto _ : state) loop -- zero effect on measured time, just
// visibility into which of the (at 10G scale, multi-minute) scenarios is running.
//
// Run e.g.:
//   ./build_Release/src/bench/output/csv_scanner_bench --benchmark_filter=opencsv
//   CSV_BENCH_TARGET_MB=256 ./.../csv_scanner_bench --benchmark_filter=scan_v2_opencsv
//   CSV_BENCH_TARGET_MB=256 ./.../csv_scanner_bench --benchmark_filter=arraymap

#include <benchmark/benchmark.h>

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "column/column.h"
#include "column/column_helper.h"
#include "common/logging.h"
#include "exec/hdfs_scanner/hdfs_scanner_text.h"
#include "formats/csv/converter.h"
#include "formats/csv/csv_reader.h"
#include "fs/fs_util.h"
#include "types/logical_type.h"
#include "types/type_descriptor.h"

namespace starrocks {

// Directory the generated per-scenario CSV files live in. Not cleaned up automatically
// -- rerunning overwrites the same paths (one file per scenario name), it does not
// accumulate; large scenarios (e.g. a 10G run) can leave sizable files here, delete the
// directory by hand if disk space matters. Override with CSV_BENCH_DATA_DIR for runs too
// big for the default location's filesystem.
static std::string bench_data_dir() {
    if (const char* dir = std::getenv("CSV_BENCH_DATA_DIR")) {
        return dir;
    }
    return "/tmp/csv_scanner_bench_data";
}

// Printed to stderr, always from outside a timed for(auto _ : state) loop or dataset
// generation's own measured work, so it costs nothing that counts towards any
// measurement -- just visibility into what would otherwise be several silent minutes
// (dataset generation, or a single multi-minute ~10GB benchmark) with no output at all.
static void log_progress(const std::string& msg) {
    std::cerr << "[csv_scanner_bench] " << msg << std::endl;
}

// ---------------------------------------------------------------------------
// File-backed CSVReader: reads through a real SequentialFile instead of an in-memory
// buffer, so these benchmarks measure the actual open+read path (mirrors production
// buffer behaviour -- CSVBENCHMARK is intentionally NOT defined, so buffInit() still
// compacts/refills like prod). Whether a given read hits the OS page cache or real
// disk depends on machine state; io_ns accumulates the actual wall-clock time spent
// inside SequentialFile::read() either way, so callers can report it separately from
// parsing/converting instead of it silently inflating those numbers.
// ---------------------------------------------------------------------------
class FileCSVReader final : public CSVReader {
public:
    FileCSVReader(const CSVParseOptions& opts, std::unique_ptr<SequentialFile> file, int64_t* io_ns)
            : CSVReader(opts), _file(std::move(file)), _io_ns(io_ns) {}

    Status _fill_buffer() override {
        DCHECK(_buff.free_space() > 0);
        Slice s(_buff.limit(), _buff.free_space());
        const auto t0 = std::chrono::steady_clock::now();
        auto res = _file->read(s.data, s.size);
        *_io_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0).count();
        if (res.status().is_end_of_file()) {
            s.size = 0;
        } else if (!res.ok()) {
            return res.status();
        } else {
            s.size = *res;
        }
        _buff.add_limit(s.size);
        auto n = _buff.available();
        if (s.size == 0) {
            if (n == 0) {
                return Status::EndOfFile(_file->filename());
            } else if (n < _row_delimiter_length ||
                       _buff.find(_parse_options.row_delimiter, n - _row_delimiter_length) == nullptr) {
                // Generator always ends rows with the row delimiter, so this normally
                // never fires; guarded so a truncated/malformed file can't loop forever.
                for (char ch : _parse_options.row_delimiter) {
                    _buff.append(ch);
                }
            }
        }
        return Status::OK();
    }

    // v1 (next_record(Record*)) needs this to find physical line boundaries.
    char* _find_line_delimiter(CSVBuffer& buffer, size_t pos) override {
        return buffer.find(_parse_options.row_delimiter, pos);
    }

private:
    std::unique_ptr<SequentialFile> _file;
    int64_t* _io_ns;
};

// io_ns: caller-owned accumulator: every SequentialFile::read() this reader performs
// adds its wall-clock duration to *io_ns. Callers reset it before a timed run and read
// it back after, same pattern as convert_ns in run_scan_v1/v2 below.
static std::unique_ptr<FileCSVReader> open_file_reader(const CSVParseOptions& opts, const std::string& path,
                                                       int64_t* io_ns) {
    auto file = fs::new_sequential_file(path);
    CHECK(file.ok()) << "failed to open " << path << ": " << file.status().to_string();
    return std::make_unique<FileCSVReader>(opts, std::move(file).value(), io_ns);
}

// ---------------------------------------------------------------------------
// Workload description + generator
// ---------------------------------------------------------------------------
enum class Col { Long, Varchar, Datetime, Decimal, Array, Map };

struct Scenario {
    const char* name;
    char enclose = 0;         // 0 = none
    char escape = 0;          // 0 = none
    bool escape_seq = false;  // put an escaped delimiter inside varchar/array/map fields
    std::vector<Col> schema;  // column kinds, in field order
    int64_t target_bytes = 0; // ~size to generate; 0 => use kRows rows
    // Only consulted when schema contains Col::Array/Col::Map.
    char collection_delim = '^';
    char mapkey_delim = ':';
};

static constexpr int kRows = 200000;    // row-count for the small scenarios
static constexpr int kChunkSize = 4096; // scan batch size (mirrors chunk_size)
static constexpr int kDecPrecision = 18;
static constexpr int kDecScale = 2;

static int64_t target_bytes_for(const Scenario& scn) {
    if (scn.target_bytes <= 0) return 0;
    if (const char* mb = std::getenv("CSV_BENCH_TARGET_MB")) {
        return static_cast<int64_t>(std::atoll(mb)) * 1024 * 1024;
    }
    return scn.target_bytes;
}

static void append_value(std::string& out, Col kind, int64_t r, int c, const Scenario& scn) {
    char buf[32];
    switch (kind) {
    case Col::Varchar: {
        if (scn.enclose != 0) {
            // OpenCSVSerde-style: every field wrapped in `enclose`. The first field
            // additionally embeds a literal field_delim ('|') inside the quotes --
            // exercises split_hive_open_csv_line's quote-aware boundary scan; v1's
            // quote-blind split_record mis-splits this one (see append_value's
            // Col::Array docs for the same pattern applied to array/map elements).
            out += scn.enclose;
            out += (c == 0 ? "a|b" : "abcdef");
            out += scn.enclose;
        } else if (scn.escape_seq) {
            out += 'a';
            out += scn.escape;
            out += '|';
            out += 'b';
        } else {
            out += "abcdef";
        }
        break;
    }
    case Col::Long:
        out += std::to_string(r * 1000003LL + c);
        break;
    case Col::Datetime: {
        int yr = 2020 + static_cast<int>(r % 5), mo = 1 + static_cast<int>(r % 12), da = 1 + static_cast<int>(r % 28);
        int hh = static_cast<int>(r % 24), mi = static_cast<int>(r % 60), ss = static_cast<int>((r * 7) % 60);
        std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d", yr, mo, da, hh, mi, ss);
        out += buf;
        break;
    }
    case Col::Decimal: {
        long ip = static_cast<long>((r * 31 + c) % 1000000);
        int fp = static_cast<int>((r * 7) % 100);
        std::snprintf(buf, sizeof(buf), "%ld.%02d", ip, fp);
        out += buf;
        break;
    }
    case Col::Array: {
        // Hive text array: elements joined by collection_delim, no braces/quotes.
        // escape_seq embeds an escaped collection_delim inside the first element (e.g.
        // "a\^b") -- exercises HiveTextArrayReader::split_array_elements's escape-aware
        // boundary scan; without it, "a" would be a plain element like the other two.
        if (scn.escape_seq) {
            out += 'a';
            out += scn.escape;
            out += scn.collection_delim;
            out += 'b';
        } else {
            out += 'a';
        }
        out += scn.collection_delim;
        out += 'c';
        out += scn.collection_delim;
        out += 'd';
        break;
    }
    case Col::Map: {
        // Hive text map: entries joined by collection_delim, key:value by mapkey_delim.
        // escape_seq embeds an escaped collection_delim inside the first value --
        // exercises MapConverter::read_hive_map's escape-aware entry/kv scan.
        out += 'k';
        out += '1';
        out += scn.mapkey_delim;
        out += 'v';
        if (scn.escape_seq) {
            out += scn.escape;
            out += scn.collection_delim;
            out += 'x';
        }
        out += scn.collection_delim;
        out += 'k';
        out += '2';
        out += scn.mapkey_delim;
        out += 'v';
        out += '2';
        break;
    }
    }
}

struct Dataset {
    std::string path; // on-disk file holding the generated CSV bytes
    int64_t rows = 0;
    int64_t bytes = 0;
};

static Dataset gen(const Scenario& scn) {
    const std::string data_dir = bench_data_dir();
    std::filesystem::create_directories(data_dir);
    Dataset ds;
    ds.path = data_dir + "/" + scn.name + ".csv";
    const std::string meta_path = ds.path + ".meta";

    // Generation is deterministic (same code + scenario + CSV_BENCH_TARGET_MB always
    // produces the same bytes), so if a prior run's file is still sitting there at the
    // exact expected size, with a metadata sidecar recording its row count, reuse both
    // and skip regenerating entirely -- not just the disk write, but the multi-GB
    // in-memory string build too (that build alone can take minutes at the ~10GB scale
    // and used to run silently, with no on-disk write to show for it, on every rerun).
    std::error_code ec;
    const auto existing_size = std::filesystem::file_size(ds.path, ec);
    if (!ec) {
        std::ifstream meta_in(meta_path);
        int64_t meta_rows = 0, meta_bytes = 0;
        if (meta_in.good() && (meta_in >> meta_rows >> meta_bytes) &&
            meta_bytes == static_cast<int64_t>(existing_size)) {
            ds.rows = meta_rows;
            ds.bytes = meta_bytes;
            return ds;
        }
    }

    // Slow path: this function authors the benchmark's INPUT fixture -- it is not
    // itself something being measured, and specifically not a stand-in for the
    // production read path, but there is still no reason for it to hold the whole
    // (possibly ~10GB) file in memory at once. Generate and flush in bounded chunks
    // instead, same "accumulate a bit, write it out, discard, repeat" shape as the
    // real read path's own 8MB buffer (CSVReader::kMinBufferSize) -- caps this
    // function's own memory to one chunk (~8MB) regardless of target size.
    constexpr size_t kGenChunkBytes = 8 * 1024 * 1024;
    std::ofstream out(ds.path, std::ios::binary | std::ios::trunc);
    CHECK(out.good()) << "failed to open " << ds.path << " for writing";
    std::string chunk;
    chunk.reserve(kGenChunkBytes + 1024);
    int64_t bytes_written = 0;
    auto flush_chunk = [&]() {
        out.write(chunk.data(), static_cast<std::streamsize>(chunk.size()));
        CHECK(out.good()) << "failed to write " << ds.path;
        bytes_written += static_cast<int64_t>(chunk.size());
        chunk.clear();
    };

    const int64_t target = target_bytes_for(scn);
    const int ncol = static_cast<int>(scn.schema.size());
    int64_t r = 0;
    auto one_row = [&]() {
        for (int c = 0; c < ncol; ++c) {
            append_value(chunk, scn.schema[c], r, c, scn);
            if (c != ncol - 1) chunk += '|';
        }
        chunk += '\n';
        ++r;
        if (chunk.size() >= kGenChunkBytes) flush_chunk();
    };
    if (target > 0) {
        while (bytes_written + static_cast<int64_t>(chunk.size()) < target) one_row();
    } else {
        for (int i = 0; i < kRows; ++i) one_row();
    }
    if (!chunk.empty()) flush_chunk();

    ds.rows = r;
    ds.bytes = bytes_written;
    std::ofstream meta_out(meta_path, std::ios::trunc);
    meta_out << ds.rows << " " << ds.bytes;
    return ds;
}

// Datasets are cached by scenario name and shared across the Parse/Scan and v1/v2
// arms, so each realistic-scale (multi-GB) scenario is generated (and written to
// disk) only once per process.
static const Dataset& dataset(const Scenario& scn) {
    static std::unordered_map<std::string, Dataset> cache;
    auto it = cache.find(scn.name);
    if (it == cache.end()) {
        it = cache.emplace(scn.name, gen(scn)).first;
    }
    return it->second;
}

static CSVParseOptions make_opts(const Scenario& scn) {
    return CSVParseOptions("\n", "|", /*skip_header*/ 0, /*trim_space*/ false, scn.escape, scn.enclose);
}

// ---------------------------------------------------------------------------
// Parse level -- reader only
// ---------------------------------------------------------------------------
static size_t run_parse_v1(const std::string& path, const CSVParseOptions& opts, int64_t* io_ns) {
    auto reader = open_file_reader(opts, path, io_ns);
    CSVReader::Record record;
    CSVReader::Fields fields;
    size_t sink = 0;
    while (reader->next_record(&record).ok()) {
        fields.clear();
        reader->split_record(record, &fields);
        for (const auto& f : fields) sink += f.size;
    }
    return sink;
}

// v2 == what HdfsTextScanner::_parse_csv_v2 actually does today: find the physical
// line the SAME way v1 does (next_record(Record*), memchr-based -- v1/v2 share this
// step now, it is NOT where the two diverge), then hand the line to the serde-specific
// splitter (split_hive_open_csv_line for OpenCSVSerde, split_hive_lazy_simple_line for
// LazySimpleSerDe with ESCAPED BY). This is NOT CSVReader::more_rows() -- that generic
// escape/enclose state machine is no longer on the Hive read path; it only remains in
// use by broker/stream load's csv_scanner.cpp, a different consumer this bench does
// not model.
static size_t run_parse_v2(const std::string& path, const CSVParseOptions& opts, int64_t* io_ns) {
    auto reader = open_file_reader(opts, path, io_ns);
    CSVReader::Record record;
    HiveTextFields row;
    const char field_delim = opts.column_delimiter.front();
    size_t sink = 0;
    while (reader->next_record(&record).ok()) {
        // CSVReader::Record is a Slice alias -- record IS the line, no wrapping needed.
        if (opts.enclose != 0) {
            split_hive_open_csv_line(record, field_delim, opts.enclose, opts.escape, &row);
        } else {
            split_hive_lazy_simple_line(record, field_delim, opts.escape, &row);
        }
        for (const auto& f : row.fields) sink += f.size;
    }
    return sink;
}

// ---------------------------------------------------------------------------
// Scan level -- reader + convert into typed Columns, processed in chunks
// ---------------------------------------------------------------------------
struct ScanColumns {
    std::vector<TypeDescriptor> types;
    std::vector<std::unique_ptr<csv::Converter>> converters;
    std::vector<MutableColumnPtr> columns;

    void build(const std::vector<Col>& schema) {
        for (Col k : schema) {
            switch (k) {
            case Col::Varchar:
                types.emplace_back(TypeDescriptor::create_varchar_type(64));
                break;
            case Col::Long:
                types.emplace_back(TypeDescriptor(LogicalType::TYPE_BIGINT));
                break;
            case Col::Datetime:
                types.emplace_back(TypeDescriptor(LogicalType::TYPE_DATETIME));
                break;
            case Col::Decimal:
                types.emplace_back(
                        TypeDescriptor::create_decimalv3_type(LogicalType::TYPE_DECIMAL128, kDecPrecision, kDecScale));
                break;
            case Col::Array: {
                TypeDescriptor t = TypeDescriptor::from_logical_type(LogicalType::TYPE_ARRAY);
                t.children.emplace_back(TypeDescriptor::create_varchar_type(64));
                types.emplace_back(std::move(t));
                break;
            }
            case Col::Map: {
                TypeDescriptor t = TypeDescriptor::from_logical_type(LogicalType::TYPE_MAP);
                t.children.emplace_back(TypeDescriptor::create_varchar_type(64));
                t.children.emplace_back(TypeDescriptor::create_varchar_type(64));
                types.emplace_back(std::move(t));
                break;
            }
            }
        }
        for (auto& t : types) {
            converters.emplace_back(csv::get_hive_converter(t, /*nullable*/ true));
            columns.emplace_back(ColumnHelper::create_column(t, /*nullable*/ true));
        }
    }
    void reset() {
        for (auto& c : columns) {
            c->resize(0);
            c->reserve(kChunkSize);
        }
    }
    size_t num_cols() const { return types.size(); }
};

static csv::Converter::Options make_conv_options(const Scenario& scn) {
    csv::Converter::Options o;
    o.is_hive = true;
    o.array_format_type = csv::ArrayFormatType::kHive;
    o.array_hive_collection_delimiter = scn.collection_delim;
    o.array_hive_mapkey_delimiter = scn.mapkey_delim;
    o.array_hive_nested_level = 1;
    o.invalid_field_as_null = true;
    return o;
}

static inline void fill_field(ScanColumns& sc, csv::Converter::Options& opts, int col, const Slice& field) {
    opts.type_desc = &sc.types[col];
    sc.converters[col]->read_string(sc.columns[col].get(), field, opts);
}

// convert_ns accumulates wall-clock time spent in fill_field (csv::Converter::read_string)
// across an entire row at a time (not per-field), keeping the timer-call overhead this
// instrumentation adds to roughly 1 extra clock pair per row on top of io_ns's per-read
// one, instead of one per field.
static void run_scan_v1(const std::string& path, const CSVParseOptions& opts, const Scenario& scn, ScanColumns& sc,
                        int64_t* io_ns, int64_t* convert_ns) {
    auto reader = open_file_reader(opts, path, io_ns);
    csv::Converter::Options conv = make_conv_options(scn);
    CSVReader::Record record;
    CSVReader::Fields fields;
    sc.reset();
    int in_chunk = 0;
    while (reader->next_record(&record).ok()) {
        fields.clear();
        reader->split_record(record, &fields);
        const size_t n = std::min(fields.size(), sc.num_cols());
        const auto t0 = std::chrono::steady_clock::now();
        for (size_t j = 0; j < n; ++j) fill_field(sc, conv, static_cast<int>(j), fields[j]);
        *convert_ns +=
                std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0).count();
        if (++in_chunk == kChunkSize) {
            benchmark::DoNotOptimize(sc.columns.data());
            benchmark::ClobberMemory();
            sc.reset();
            in_chunk = 0;
        }
    }
    benchmark::DoNotOptimize(sc.columns.data());
}

static void run_scan_v2(const std::string& path, const CSVParseOptions& opts, const Scenario& scn, ScanColumns& sc,
                        int64_t* io_ns, int64_t* convert_ns) {
    auto reader = open_file_reader(opts, path, io_ns);
    csv::Converter::Options conv = make_conv_options(scn);
    // Mirrors _parse_csv_v2: only LazySimpleSerDe (no enclose) has a null literal, and
    // NullableConverter needs Options::escape to decide it on the raw (pre-unescape)
    // bytes. OpenCSVSerde has no null literal at all even though it has its own escape
    // character -- its splitter already fully resolves quotes/escapes before any
    // converter sees the bytes, so leave conv.escape at 0 and disable the "\N" check.
    conv.escape = (opts.enclose == 0) ? opts.escape : 0;
    conv.hive_null_literal = (opts.enclose == 0);
    CSVReader::Record record;
    HiveTextFields row;
    const char field_delim = opts.column_delimiter.front();
    sc.reset();
    int in_chunk = 0;
    while (reader->next_record(&record).ok()) {
        if (opts.enclose != 0) {
            split_hive_open_csv_line(record, field_delim, opts.enclose, opts.escape, &row);
        } else {
            split_hive_lazy_simple_line(record, field_delim, opts.escape, &row);
        }
        if (row.all_null_row) {
            // Empty-line all-null row (O2/L3 audit cases); the generator never emits
            // this, kept only so the control flow matches _parse_csv_v2's shape.
            continue;
        }
        // Null (LazySimpleSerDe's raw "\N") is now decided by NullableConverter itself
        // via conv.escape, not by the splitter -- call it uniformly for every field.
        const size_t n = std::min(row.fields.size(), sc.num_cols());
        const auto t0 = std::chrono::steady_clock::now();
        for (size_t j = 0; j < n; ++j) {
            fill_field(sc, conv, static_cast<int>(j), row.fields[j]);
        }
        *convert_ns +=
                std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0).count();
        if (++in_chunk == kChunkSize) {
            benchmark::DoNotOptimize(sc.columns.data());
            benchmark::ClobberMemory();
            sc.reset();
            in_chunk = 0;
        }
    }
    benchmark::DoNotOptimize(sc.columns.data());
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------
enum class Ver { V1, V2 };
enum class Level { Parse, Scan };

static void BM_Csv(benchmark::State& state, Ver ver, Level level, Scenario scn) {
    const Dataset& ds = dataset(scn);
    const CSVParseOptions opts = make_opts(scn);
    int64_t io_ns_total = 0;
    int64_t convert_ns_total = 0;

    log_progress("start " + state.name());
    const auto wall_t0 = std::chrono::steady_clock::now();
    if (level == Level::Parse) {
        for (auto _ : state) {
            size_t sink = 0;
            switch (ver) {
            case Ver::V1:
                sink = run_parse_v1(ds.path, opts, &io_ns_total);
                break;
            case Ver::V2:
                sink = run_parse_v2(ds.path, opts, &io_ns_total);
                break;
            }
            benchmark::DoNotOptimize(sink);
        }
    } else {
        ScanColumns sc;
        sc.build(scn.schema);
        for (auto _ : state) {
            switch (ver) {
            case Ver::V1:
                run_scan_v1(ds.path, opts, scn, sc, &io_ns_total, &convert_ns_total);
                break;
            case Ver::V2:
                run_scan_v2(ds.path, opts, scn, sc, &io_ns_total, &convert_ns_total);
                break;
            }
        }
    }
    const double wall_ms =
            std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - wall_t0).count();
    log_progress("done  " + state.name());

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * ds.rows);
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * ds.bytes);

    // io_ms/convert_ms are measured directly (see FileCSVReader::_fill_buffer and the
    // fill_field brackets in run_scan_v1/v2); parse_ms is whatever's left of this
    // benchmark's own wall-clock loop time -- next_record's line-finding plus
    // split_record/split_hive_*_line's field-splitting, without a third clock pair on
    // every row to measure it directly. All three are wall-clock and per-iteration
    // means, directly comparable/addable to each other (unlike bytes_per_second, which
    // benchmark itself derives from CPU time and can diverge from wall-clock under
    // real disk I/O -- see the perf report for why that distinction matters at scale).
    const double iters = std::max<double>(1.0, static_cast<double>(state.iterations()));
    const double io_ms = (io_ns_total / iters) / 1e6;
    const double convert_ms = (convert_ns_total / iters) / 1e6;
    const double parse_ms = std::max(0.0, wall_ms / iters - io_ms - convert_ms);
    state.counters["io_ms"] = io_ms;
    state.counters["parse_ms"] = parse_ms;
    state.counters["convert_ms"] = convert_ms;
}

// --- schemas (two REAL Hive serde shapes) --------------------------------
// LazySimpleSerDe: typed columns, NO quotes (the common Hive text serde). 16 cols:
// 6 VARCHAR / 5 LONG / 2 DATETIME / 3 DECIMAL, interleaved.
static const std::vector<Col> kLazySchema{
        Col::Long,    Col::Varchar, Col::Long, Col::Varchar,  Col::Datetime, Col::Decimal, Col::Long, Col::Varchar,
        Col::Decimal, Col::Varchar, Col::Long, Col::Datetime, Col::Decimal,  Col::Varchar, Col::Long, Col::Varchar};
// OpenCSVSerde: ALL columns are STRING, and Hive quotes EVERY field on write. So the
// quote-aware path applies to ~100% of fields (12 VARCHAR here).
static const std::vector<Col> kStrSchema{Col::Varchar, Col::Varchar, Col::Varchar, Col::Varchar,
                                         Col::Varchar, Col::Varchar, Col::Varchar, Col::Varchar,
                                         Col::Varchar, Col::Varchar, Col::Varchar, Col::Varchar};
// LazySimpleSerDe with a couple of nested columns mixed into an otherwise-scalar
// table (a realistic shape: most Hive text tables are scalar-only, but ARRAY/MAP
// columns do show up). Exists specifically to give the escape-aware ARRAY/MAP
// splitting added to HiveTextArrayReader/MapConverter a perf number to be judged
// against -- kLazyEsc* below only escapes a TOP-LEVEL field's separator, never one
// INSIDE an array/map column, so it cannot exercise that code at all.
static const std::vector<Col> kLazyArrayMapSchema{Col::Long,  Col::Varchar, Col::Datetime, Col::Decimal,
                                                  Col::Array, Col::Map,     Col::Long,     Col::Varchar};

// --- scenarios -----------------------------------------------------------
// Each pair below is the same shape at two scales: *_small (fixed small row count,
// fits in OS page cache, benchmark auto-repeats for statistical stability) and the
// unsuffixed variant (size set by target_bytes, CSV_BENCH_TARGET_MB-overridable --
// e.g. =10240 for a real 10GB file -- fixed 3 repetitions, see BM_FILE below).

// LazySimple plain: v1 is the PRODUCTION path here, no escape/enclose.
static const Scenario kLazySmall{.name = "lazy_small", .schema = kLazySchema};
static const Scenario kLazy{.name = "lazy", .schema = kLazySchema, .target_bytes = int64_t{1} << 30};

// LazySimple with escape.delim (backslash escaping); v1 can't do escape.
static const Scenario kLazyEscSmall{
        .name = "lazy_esc_small", .escape = '\\', .escape_seq = true, .schema = kLazySchema};
static const Scenario kLazyEsc{.name = "lazy_esc",
                               .escape = '\\',
                               .escape_seq = true,
                               .schema = kLazySchema,
                               .target_bytes = int64_t{1} << 30};

// OpenCSVSerde: all string, EVERY field quoted (enclose != 0 is what append_value's
// Col::Varchar case reads to decide that).
static const Scenario kOpenCsvSmall{.name = "opencsv_small", .enclose = '"', .schema = kStrSchema};
static const Scenario kOpenCsv{
        .name = "opencsv", .enclose = '"', .schema = kStrSchema, .target_bytes = int64_t{1} << 30};

// ARRAY/MAP columns, no escape: v1 and v2 both parse these correctly (a top-level
// field's array/map text is handed down whole either way, and neither serde's
// internal collection/mapkey delimiter clashes with field_delim '|') -- isolates the
// pure array/map CONVERSION cost, the baseline the *_esc_* pair below is measured
// against.
static const Scenario kLazyArrayMapSmall{.name = "lazy_arraymap_small", .schema = kLazyArrayMapSchema};
static const Scenario kLazyArrayMap{
        .name = "lazy_arraymap", .schema = kLazyArrayMapSchema, .target_bytes = int64_t{1} << 30};

// Same shape, escape.delim set AND an escaped separator embedded INSIDE the
// array/map column's text (see append_value's Col::Array/Col::Map cases). v1 has no
// escape awareness at all here and would mis-split -- v2 only, same as kLazyEsc*.
static const Scenario kLazyArrayMapEscSmall{
        .name = "lazy_arraymap_esc_small", .escape = '\\', .escape_seq = true, .schema = kLazyArrayMapSchema};
static const Scenario kLazyArrayMapEsc{.name = "lazy_arraymap_esc",
                                       .escape = '\\',
                                       .escape_seq = true,
                                       .schema = kLazyArrayMapSchema,
                                       .target_bytes = int64_t{1} << 30};

// ===== Small (cache-resident: pure-CPU comparison) =======================
// LazySimple plain: v1 is the PRODUCTION path here.
BENCHMARK_CAPTURE(BM_Csv, parse_v1_lazy_small, Ver::V1, Level::Parse, kLazySmall);
BENCHMARK_CAPTURE(BM_Csv, parse_v2_lazy_small, Ver::V2, Level::Parse, kLazySmall);
BENCHMARK_CAPTURE(BM_Csv, scan_v1_lazy_small, Ver::V1, Level::Scan, kLazySmall);
BENCHMARK_CAPTURE(BM_Csv, scan_v2_lazy_small, Ver::V2, Level::Scan, kLazySmall);
// LazySimple with escape.delim: v1 can't escape -> v2 only.
BENCHMARK_CAPTURE(BM_Csv, parse_v2_lazy_esc_small, Ver::V2, Level::Parse, kLazyEscSmall);
BENCHMARK_CAPTURE(BM_Csv, scan_v2_lazy_esc_small, Ver::V2, Level::Scan, kLazyEscSmall);
// ARRAY/MAP columns, no escape: v1 vs v2 baseline for pure array/map conversion cost.
BENCHMARK_CAPTURE(BM_Csv, parse_v1_arraymap_small, Ver::V1, Level::Parse, kLazyArrayMapSmall);
BENCHMARK_CAPTURE(BM_Csv, parse_v2_arraymap_small, Ver::V2, Level::Parse, kLazyArrayMapSmall);
BENCHMARK_CAPTURE(BM_Csv, scan_v1_arraymap_small, Ver::V1, Level::Scan, kLazyArrayMapSmall);
BENCHMARK_CAPTURE(BM_Csv, scan_v2_arraymap_small, Ver::V2, Level::Scan, kLazyArrayMapSmall);
// Same shape + an escaped separator INSIDE the array/map column. NOTE: Parse level
// does not exist for this one -- run_parse_v2 only ever splits TOP-LEVEL fields, it
// never calls ArrayConverter/MapConverter/HiveTextArrayReader at all (that only
// happens during Scan, via NullableConverter -> read_string), so a parse-level
// arraymap_esc benchmark would silently measure nothing new over the plain arraymap
// one above (same top-level split, marginally different row bytes) -- Scan is the
// only level that actually exercises split_array_elements's/read_hive_map's own
// escape-aware boundary scan. v1 has no escape awareness anywhere, so v2 only.
BENCHMARK_CAPTURE(BM_Csv, scan_v2_arraymap_esc_small, Ver::V2, Level::Scan, kLazyArrayMapEscSmall);
// OpenCSV (all string, all quoted): this is where the quote-aware path applies to the
// WHOLE row, and where convert dominates scan time.
BENCHMARK_CAPTURE(BM_Csv, parse_v2_opencsv_small, Ver::V2, Level::Parse, kOpenCsvSmall);
BENCHMARK_CAPTURE(BM_Csv, scan_v2_opencsv_small, Ver::V2, Level::Scan, kOpenCsvSmall);
// v1 on the SAME opencsv bytes, timing only: split_record() is quote/escape-blind, so it
// mis-splits fields whose quotes wrap an embedded delimiter (column 0 here) and leaves
// the quote characters in as literal data -- its OUTPUT is wrong for this data, do not
// use these numbers to argue v1 "works" on OpenCSVSerde. They exist only to isolate the
// raw cost of v2's quote/escape state machine vs a naive delimiter-only split on
// otherwise identical bytes.
BENCHMARK_CAPTURE(BM_Csv, parse_v1_opencsv_small, Ver::V1, Level::Parse, kOpenCsvSmall);
BENCHMARK_CAPTURE(BM_Csv, scan_v1_opencsv_small, Ver::V1, Level::Scan, kOpenCsvSmall);

// ===== Realistic scale (size set by target_bytes/CSV_BENCH_TARGET_MB; ms unit + =====
// ===== fixed iterations for stability)                                        =====
// Each scenario caches its own dataset -- filter or use CSV_BENCH_TARGET_MB to keep
// memory/time bounded (5 distinct datasets here).
#define BM_FILE(nm, ver, lvl, scn) \
    BENCHMARK_CAPTURE(BM_Csv, nm, ver, lvl, scn)->Unit(benchmark::kMillisecond)->Iterations(3)
// LazySimple (typed, no quotes).
BM_FILE(parse_v1_lazy, Ver::V1, Level::Parse, kLazy);
BM_FILE(parse_v2_lazy, Ver::V2, Level::Parse, kLazy);
BM_FILE(scan_v1_lazy, Ver::V1, Level::Scan, kLazy);
BM_FILE(scan_v2_lazy, Ver::V2, Level::Scan, kLazy);
// OpenCSV (all string, all quoted): the scenario where convert dominates.
BM_FILE(parse_v2_opencsv, Ver::V2, Level::Parse, kOpenCsv);
BM_FILE(scan_v2_opencsv, Ver::V2, Level::Scan, kOpenCsv);
// v1 on the SAME opencsv bytes, timing only -- see the small-scenario comment above.
BM_FILE(parse_v1_opencsv, Ver::V1, Level::Parse, kOpenCsv);
BM_FILE(scan_v1_opencsv, Ver::V1, Level::Scan, kOpenCsv);
// LazySimple escape.delim.
BM_FILE(parse_v2_lazy_esc, Ver::V2, Level::Parse, kLazyEsc);
BM_FILE(scan_v2_lazy_esc, Ver::V2, Level::Scan, kLazyEsc);
// ARRAY/MAP columns: v1 vs v2 baseline (no escape), then v2-only with an escaped
// separator inside the array/map column -- see the small-scale comments above.
BM_FILE(parse_v1_arraymap, Ver::V1, Level::Parse, kLazyArrayMap);
BM_FILE(parse_v2_arraymap, Ver::V2, Level::Parse, kLazyArrayMap);
BM_FILE(scan_v1_arraymap, Ver::V1, Level::Scan, kLazyArrayMap);
BM_FILE(scan_v2_arraymap, Ver::V2, Level::Scan, kLazyArrayMap);
// No parse-level entry here -- see the small-scale comment above (Scan is the only
// level that exercises the array/map escape-aware split at all).
BM_FILE(scan_v2_arraymap_esc, Ver::V2, Level::Scan, kLazyArrayMapEsc);
#undef BM_FILE

// Forces every scenario's dataset into the dataset() cache (generating and writing its
// file) before any benchmark runs -- without this, whichever scenario a
// --benchmark_filter first matches would have its one-time gen() (building up a
// dataset file, chunk by chunk) show up as parse/convert cost in that benchmark's own
// timing instead of as setup cost.
static void warm_all_datasets() {
    for (const Scenario* s : {&kLazySmall, &kLazy, &kLazyEscSmall, &kLazyEsc, &kOpenCsvSmall, &kOpenCsv,
                              &kLazyArrayMapSmall, &kLazyArrayMap, &kLazyArrayMapEscSmall, &kLazyArrayMapEsc}) {
        log_progress("preparing dataset " + std::string(s->name) + " ...");
        (void)dataset(*s);
        log_progress("dataset " + std::string(s->name) + " ready");
    }
}

// Runs during static initialization, before BENCHMARK_MAIN()'s generated main() body
// (benchmark::Initialize/RunSpecifiedBenchmarks) -- static-storage-duration
// initializers in a translation unit always run before that TU's main() executes.
static const bool kDatasetsWarmed = [] {
    warm_all_datasets();
    return true;
}();

} // namespace starrocks

BENCHMARK_MAIN();
