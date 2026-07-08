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

#include "exec/hdfs_scanner/hdfs_scanner.h"
#include "formats/csv/converter.h"
#include "formats/csv/csv_reader.h"

namespace starrocks {

// Per-line field-splitting result for the Hive text serdes. Field slices point
// either into the source line or into |materialized|, which is reserved to the
// line length up front so growth never reallocates (unescaping/unquoting only
// ever shrinks data).
struct HiveTextFields {
    std::vector<Slice> fields;
    std::string materialized;  // storage for fields that needed unescape/unquote
    bool all_null_row = false; // OpenCSVSerde: opencsv's readNext() returned null

    void reset(size_t line_len) {
        fields.clear();
        materialized.clear();
        materialized.reserve(line_len);
        all_null_row = false;
    }
};

// Field boundary finder for ONE physical line of a LazySimpleSerDe (ESCAPED BY)
// table: the escape character makes any following byte literal, so an escaped
// separator does not split. Fields are emitted RAW (escape bytes intact, nothing
// unescaped) -- deciding null and unescaping happen one level down, in
// NullableConverter, once it is known whether this field's type is a scalar leaf or
// a complex type (ARRAY/MAP/STRUCT) that still needs to find its OWN separators in
// the untouched bytes.
void split_hive_lazy_simple_line(const Slice& line, char field_delim, char escape, HiveTextFields* out);

// Field splitting for ONE physical line of an OpenCSVSerde table, byte-compatible
// with the opencsv 2.3 parser bundled in Hive (validated against the exhaustive
// fixture opencsv23_golden.tsv). Notable semantics: quotes toggle quote-mode even
// mid-field; the escape character only escapes a following quote/escape and is
// silently dropped otherwise (an escaped separator still splits!); a line ending
// inside quotes keeps only the completed fields, or becomes an all-null row when
// none completed; there is NO null literal ("\N" is data).
//
// Unlike split_hive_lazy_simple_line, this fully resolves quotes/escapes here and
// emits FINAL field values -- there is no "pass raw bytes down for a complex type
// to re-split" path. That is safe only because OpenCSVSerde has no complex-type
// concept: Hive itself types every OpenCSVSerde column as string, so a field can
// never be handed to ArrayConverter/MapConverter. If a StarRocks external table
// were hand-declared with a MAP/ARRAY column against an OpenCSVSerde-backed table
// (a schema real Hive metadata cannot produce), an escaped element/kv separator
// inside that field would already be resolved away by this function and would be
// mis-split one level down -- the same class of bug fixed for LazySimpleSerDe by
// keeping escape bytes raw for nested elements, just not applicable here.
void split_hive_open_csv_line(const Slice& line, char field_delim, char quote, char escape, HiveTextFields* out);

// This class used by data lake(Hive, Iceberg,... etc), not for broker load.
// Broker load plz refer to csv_scanner.cpp
class HdfsTextScanner final : public HdfsScanner {
public:
    HdfsTextScanner() = default;
    ~HdfsTextScanner() override = default;

    Status do_open(RuntimeState* runtime_state) override;
    void do_update_counter(HdfsScannerProfile* profile) override;
    void do_close(RuntimeState* runtime_state) noexcept override;
    Status do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) override;
    Status do_init(RuntimeState* runtime_state, const HdfsScannerContext& scanner_ctx) override;
    int64_t estimated_mem_usage() const override;

private:
    Status _create_csv_reader();
    Status _setup_compression_type(const TTextFileDesc& text_file_desc);
    Status _setup_delimiter(const TTextFileDesc& text_file_desc);
    StatusOr<bool> _has_utf8_bom() const;
    Status _build_hive_column_name_2_index();
    Status _parse_csv(int chunk_size, ChunkPtr* chunk);
    // Quote/escape-aware parsing path for OpenCSVSerde and for LazySimpleSerDe tables
    // created with ESCAPED BY. Mirrors Hive's own layering: rows are split at physical
    // line boundaries first (next_record), then the matching serde profile splits each
    // line into fields (split_hive_open_csv_line / split_hive_lazy_simple_line).
    Status _parse_csv_v2(int chunk_size, ChunkPtr* chunk);

    using ConverterPtr = std::unique_ptr<csv::Converter>;
    std::string _line_delimiter;
    std::string _field_delimiter;
    char _collection_delimiter;
    char _mapkey_delimiter;
    // Quote/escape characters (0 means unset): quote/escape from OpenCSVSerde, or
    // escape.delim from LazySimpleSerDe (ESCAPED BY). When either is set, _use_v2 is
    // true and parsing goes through _parse_csv_v2.
    char _enclose = 0;
    char _escape = 0;
    bool _use_v2 = false;
    int32_t _skip_header_line_count = 0;
    bool _need_probe_line_delimiter = false;
    // Always set true in data lake now.
    // TODO(SmithCruise) use a hive catalog property to control this behavior
    bool _invalid_field_as_null = true;
    std::vector<Column*> _column_raw_ptrs;
    std::vector<ConverterPtr> _converters;
    std::shared_ptr<CSVReader> _reader = nullptr;
    // _materialize_slots_index_2_csv_column_index[0] = 5 means materialize_slots[0]->column index 5 in csv
    // materialize_slots is StarRocks' table definition, column index is the actual position in csv
    std::vector<size_t> _materialize_slots_index_2_csv_column_index;
    // (TODO) move compressed file don't split logic to FE, _no_data is prone to bugs
    bool _no_data = false;
};
} // namespace starrocks
