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

#include "connector/hive/scanner/hdfs_scanner_text.h"

#include <cstring>
#include <unordered_map>

#include "base/compression/compression_utils.h"
#include "base/compression/stream_decompressor.h"
#include "base/string/utf8_check.h"
#include "column/column_helper.h"
#include "formats/csv/csv_defaults.h"
#include "gutil/strings/substitute.h"
#include "runtime/runtime_state.h"

namespace starrocks {

class HdfsScannerCSVReader : public CSVReader {
public:
    // |file| must outlive HdfsScannerCSVReader
    HdfsScannerCSVReader(RandomAccessFile* file, const std::string& row_delimiter, bool need_probe_line_delimiter,
                         const std::string& column_separator, size_t file_length, char enclose = 0, char escape = 0)
            : CSVReader(CSVParseOptions(row_delimiter, column_separator, 0, false, escape, enclose)) {
        _file = file;
        _offset = 0;
        _remain_length = file_length;
        _file_length = file_length;
        _row_delimiter_length = row_delimiter.size();
        _column_delimiter_length = column_separator.size();
        _need_probe_line_delimiter = need_probe_line_delimiter;
    }

    Status reset(size_t offset, size_t remain_length);

    Status next_record(Record* record);

protected:
    Status _fill_buffer() override;

    void _trim_row_delimeter(Record* record);

    char* _find_line_delimiter(starrocks::CSVBuffer& buffer, size_t pos) override;

private:
    RandomAccessFile* _file;
    size_t _offset = 0;
    size_t _remain_length = 0;
    size_t _file_length = 0;
    bool _should_stop_scan = false;
    bool _should_stop_next = false;
    // Hive TextFile's line delimiter maybe \n, \r or \r\n, we need to probe it
    bool _need_probe_line_delimiter = false;
};

Status HdfsScannerCSVReader::reset(size_t offset, size_t remain_length) {
    RETURN_IF_ERROR(_file->seek(offset));
    _offset = offset;
    _remain_length = remain_length;
    _should_stop_scan = false;
    _should_stop_next = false;
    _buff.skip(_buff.limit() - _buff.position());
    return Status::OK();
}

Status HdfsScannerCSVReader::next_record(Record* record) {
    if (_should_stop_next) {
        return Status::EndOfFile("");
    }
    RETURN_IF_ERROR(CSVReader::next_record(record));
    // We should still read if remain_length is zero(we stop right at row delimiter)
    // because next scan range will skip a record till row delimiter.
    // so it's current reader's responsibility to consume this record.
    size_t consume = record->size + _row_delimiter_length;
    if (_remain_length < consume) {
        _should_stop_next = true;
    } else {
        _remain_length -= consume;
    }
    _trim_row_delimeter(record);
    return Status::OK();
}

Status HdfsScannerCSVReader::_fill_buffer() {
    if (_should_stop_scan) {
        return Status::EndOfFile("");
    }

    DCHECK(_buff.free_space() > 0);
    Slice s = Slice(_buff.limit(), _buff.free_space());

    // It's very critical to call `read` here, because underneath of RandomAccessFile
    // maybe its a SequenceFile because we support to read compressed text file.
    // For uncompressed text file, we can split csv file into chunks and process chunks parallelly.
    // For compressed text file, we only can parse csv file in sequential way.
    ASSIGN_OR_RETURN(s.size, _file->read(s.data, std::min(s.size, _file_length - _offset)));
    _offset += s.size;
    _buff.add_limit(s.size);
    if (s.size == 0) {
        size_t n = _buff.available();
        _should_stop_scan = true;
        // if there is no linger data in buff at all, then we don't need to add a row delimiter. Otherwise we will add a new record.
        // For example, a single column table like "a\nb\nc\n". if we add a trailing row delimiter, then there will be a "" at last.
        if (n == 0) return Status::EndOfFile("");

        // Has reached the end of file but still no record delimiter found, which
        // is valid, according the RFC, add the record delimiter ourself, ONLY IF we have space.
        // But if we don't have any space, which means a single csv record size has exceed buffer max size.
        if (n >= _row_delimiter_length &&
            _buff.find(_parse_options.row_delimiter, n - _row_delimiter_length) == nullptr) {
            if (_buff.free_space() >= _row_delimiter_length) {
                for (char ch : _parse_options.row_delimiter) {
                    _buff.append(ch);
                }
            } else {
                return Status::InternalError("CSV line length exceed limit " + std::to_string(_buff.capacity()) +
                                             " when padding row delimiter");
            }
        }
    }

    return Status::OK();
}

void HdfsScannerCSVReader::_trim_row_delimeter(Record* record) {
    // For default row delemiter which is line break, we need to trim the windows line break
    // if the file was written in windows platfom.
    if (_parse_options.row_delimiter == csv::LINE_DELIM_LF) {
        while (record->size > 0 && record->data[record->size - 1] == '\r') {
            record->size--;
        }
    }
}

char* HdfsScannerCSVReader::_find_line_delimiter(starrocks::CSVBuffer& buffer, size_t pos) {
    // If we can get explicit line.delim from hms, we don't need to probe it
    if (LIKELY(!_need_probe_line_delimiter)) {
        return buffer.find(_parse_options.row_delimiter, pos);
    } else {
        // If we didn't get explicit line.delim from hms, we need to probe it.
        // We will probe '\n' first, most of TextFile's line.delim is '\n'
        // Then we will try to probe '\r'
        // TODO(Smith)
        // We didn't support to treat '\r\n' as line.delim,
        // because our code does not support line separator's length larger than one char.
        char* p = buffer.find(csv::LINE_DELIM_LF, pos);
        if (p != nullptr) {
            _need_probe_line_delimiter = false;
            _parse_options.row_delimiter = csv::LINE_DELIM_LF;
            return p;
        }
        p = buffer.find(csv::LINE_DELIM_CR, pos);
        if (p != nullptr) {
            _need_probe_line_delimiter = false;
            _parse_options.row_delimiter = csv::LINE_DELIM_CR;
            return p;
        }
        return nullptr;
    }
}

void split_hive_lazy_simple_line(const Slice& line, char field_delim, char escape, HiveTextFields* out) {
    out->reset(line.size);
    const char* p = line.data;
    const size_t n = line.size;
    size_t start = 0;
    for (size_t i = 0; i <= n; i++) {
        // Mirrors LazyStruct's separator scan: the escape makes the next byte literal
        // (so an escaped separator, e.g. inside an ARRAY column's raw text, is not a
        // real field boundary); an escape that is the very last byte of the line
        // escapes nothing. Deciding null and unescaping happen ONE level down, in
        // NullableConverter -- this function only finds boundaries and must hand the
        // RAW bytes through untouched: a complex-typed field (ARRAY/MAP/STRUCT) still
        // needs its own escape markers intact to find ITS OWN separators.
        if (i < n && p[i] == escape && i + 1 < n) {
            i++;
            continue;
        }
        if (i < n && p[i] != field_delim) {
            continue;
        }
        out->fields.emplace_back(p + start, i - start);
        start = i + 1;
    }
}

// Returns the offset in [i, n) of the next `quote` or `escape` byte, or (only when
// |stop_at_delim| is set, i.e. not currently inside quotes) the next `field_delim`
// byte; returns n when none of them occur again. Lets split_hive_open_csv_line copy a
// whole run of ordinary bytes in one std::string::append instead of one push_back per
// byte -- the common case, since a quoted OpenCSVSerde field rarely embeds a quote or
// escape.
static size_t find_next_special(const char* p, size_t i, size_t n, char quote, char escape, char field_delim,
                                bool stop_at_delim) {
    size_t stop = n;
    if (const void* f = memchr(p + i, quote, n - i)) {
        stop = static_cast<size_t>(static_cast<const char*>(f) - p);
    }
    if (const void* f = memchr(p + i, escape, n - i)) {
        stop = std::min(stop, static_cast<size_t>(static_cast<const char*>(f) - p));
    }
    if (stop_at_delim) {
        if (const void* f = memchr(p + i, field_delim, n - i)) {
            stop = std::min(stop, static_cast<size_t>(static_cast<const char*>(f) - p));
        }
    }
    return stop;
}

void split_hive_open_csv_line(const Slice& line, char field_delim, char quote, char escape, HiveTextFields* out) {
    out->reset(line.size);
    if (line.size == 0) {
        // opencsv's CSVReader sees no line at all for an empty row (readLine()
        // returns null), so OpenCSVSerde gets readNext() == null: an all-null row.
        out->all_null_row = true;
        return;
    }
    const char* p = line.data;
    const size_t n = line.size;
    std::string& buf = out->materialized; // reserved to |n|: growth never reallocates
    size_t tok_start = 0;                 // current token start inside |buf|
    bool in_quotes = false;
    bool in_field = false;
    size_t i = 0;
    while (i < n) {
        const char c = p[i];
        if (c == escape) {
            // The escape only escapes a following quote/escape while inside a token
            // or quotes; in every other position it is silently dropped, so an
            // "escaped" separator still splits.
            if ((in_quotes || in_field) && i + 1 < n && (p[i + 1] == quote || p[i + 1] == escape)) {
                buf.push_back(p[i + 1]);
                i++;
            }
            i++;
        } else if (c == quote) {
            if ((in_quotes || in_field) && i + 1 < n && p[i + 1] == quote) {
                // Doubled quote -> one literal quote.
                buf.push_back(quote);
                i++;
            } else {
                // opencsv 2.3 quirk, kept bit-for-bit: a quote past index 2 that is
                // not adjacent to a separator stays in the data -- unless the token
                // so far is all whitespace (ignoreLeadingWhiteSpace default), which
                // is discarded.
                if (i > 2 && p[i - 1] != field_delim && i + 1 < n && p[i + 1] != field_delim) {
                    bool all_ws = buf.size() > tok_start;
                    for (size_t j = tok_start; all_ws && j < buf.size(); j++) {
                        all_ws = (buf[j] == ' ' || buf[j] == '\t');
                    }
                    if (all_ws) {
                        buf.resize(tok_start);
                    } else {
                        buf.push_back(c);
                    }
                }
                in_quotes = !in_quotes;
            }
            in_field = !in_field;
            i++;
        } else if (c == field_delim && !in_quotes) {
            out->fields.emplace_back(buf.data() + tok_start, buf.size() - tok_start);
            tok_start = buf.size();
            in_field = false;
            i++;
        } else {
            // Ordinary run: bulk-copy up to the next quote/escape (and up to the next
            // delimiter, if one would end the field) in a single append rather than
            // pushing byte by byte.
            size_t stop = find_next_special(p, i, n, quote, escape, field_delim, /*stop_at_delim=*/!in_quotes);
            buf.append(p + i, stop - i);
            in_field = true;
            i = stop;
        }
    }
    if (in_quotes) {
        // Unterminated quote: opencsv parks the in-progress token as "pending" and,
        // with no further line to feed it, readNext() returns only the fields
        // completed before the quote opened -- or null (all-null row) when none.
        buf.resize(tok_start);
        if (out->fields.empty()) {
            out->all_null_row = true;
        }
        return;
    }
    out->fields.emplace_back(buf.data() + tok_start, buf.size() - tok_start);
}

Status HdfsTextScanner::do_init(RuntimeState* runtime_state, const HdfsScannerContext& scanner_ctx) {
    const TTextFileDesc& text_file_desc = _scanner_ctx->scan_range->text_file_desc;
    RETURN_IF_ERROR(_setup_delimiter(text_file_desc));
    RETURN_IF_ERROR(_setup_compression_type(text_file_desc));

    if (text_file_desc.__isset.skip_header_line_count) {
        _skip_header_line_count = text_file_desc.skip_header_line_count;
    }
    return Status::OK();
}

Status HdfsTextScanner::_setup_delimiter(const TTextFileDesc& text_file_desc) {
    // _field_delimiter and _line_delimiter should use std::string,
    // because the CSVReader is using std::string type as delimiter.
    if (text_file_desc.__isset.field_delim) {
        if (text_file_desc.field_delim.empty()) {
            // Just a piece of defense code
            return Status::Corruption("Hive TextFile's field delim is empty");
        }
        _field_delimiter = text_file_desc.field_delim;
    } else {
        _field_delimiter = csv::DEFAULT_FIELD_DELIM;
    }

    // we should cast string to char now since csv reader only support record delimiter by char.
    if (text_file_desc.__isset.line_delim) {
        if (text_file_desc.line_delim.empty()) {
            // Just a piece of defense code
            return Status::Corruption("Hive TextFile's line delim is empty");
        }
        _line_delimiter = text_file_desc.line_delim.front();
    } else {
        _line_delimiter = csv::DEFAULT_LINE_DELIM;
        // We didn't get explicit line delimiter from hms, so we need to probe it by ourselves
        _need_probe_line_delimiter = true;
    }

    // In Hive, users can specify collection delimiter and mapkey delimiter as string type,
    // but in fact, only the first character of the delimiter will take effect.
    // So here, we only use the first character of collection_delim and mapkey_delim.
    if (text_file_desc.__isset.collection_delim) {
        if (text_file_desc.collection_delim.empty()) {
            // Just a piece of defense code
            return Status::Corruption("Hive TextFile's collection delim is empty");
        }
        _collection_delimiter = text_file_desc.collection_delim.front();
    } else {
        _collection_delimiter = csv::DEFAULT_COLLECTION_DELIM.front();
    }

    if (text_file_desc.__isset.mapkey_delim) {
        if (text_file_desc.mapkey_delim.empty()) {
            // Just a piece of defense code
            return Status::Corruption("Hive TextFile's mapkey delim is empty");
        }
        _mapkey_delimiter = text_file_desc.mapkey_delim.front();
    } else {
        _mapkey_delimiter = csv::DEFAULT_MAPKEY_DELIM.front();
    }

    // OpenCSVSerde carries a quote (enclose) and escape character, and LazySimpleSerDe
    // carries an escape character when the table was created with ESCAPED BY. When
    // either is present we must parse with the quote/escape-aware state machine instead
    // of the naive split_record path, otherwise separators inside quoted fields (or
    // escaped separators like "a\,b") are wrongly treated as column delimiters.
    if (text_file_desc.__isset.enclose && text_file_desc.enclose != 0) {
        _enclose = static_cast<char>(text_file_desc.enclose);
    }
    if (text_file_desc.__isset.escape && text_file_desc.escape != 0) {
        _escape = static_cast<char>(text_file_desc.escape);
    }
    // Gate on enclose/escape so default LazySimpleSerDe tables keep using the
    // original, well-tested split_record path with zero behavior change.
    _use_v2 = (_enclose != 0 || _escape != 0);
    return Status::OK();
}

Status HdfsTextScanner::_setup_compression_type(const TTextFileDesc& text_file_desc) {
    // by default it's unknown compression. we will synthesise informaiton from FE and BE(file extension)
    // parse compression type from FE first.
    CompressionTypePB compression_type;
    if (text_file_desc.__isset.compression_type) {
        compression_type = CompressionUtils::to_compression_pb(text_file_desc.compression_type);
    } else {
        // if FE does not specify compress type, we choose it by looking at filename.
        compression_type = get_compression_type_from_path(_scanner_ctx->file_path);
    }
    if (compression_type != UNKNOWN_COMPRESSION) {
        _compression_type = compression_type;
    } else {
        _compression_type = NO_COMPRESSION;
    }

    // If it's compressed file, we only handle scan range whose offset == 0.
    if (_compression_type != NO_COMPRESSION && _scanner_ctx->scan_range->offset != 0) {
        _no_data = true;
    }
    return Status::OK();
}

Status HdfsTextScanner::do_open(RuntimeState* runtime_state) {
    if (_no_data) {
        return Status::OK();
    }

    RETURN_IF_ERROR(open_random_access_file());

    SCOPED_RAW_TIMER(&_app_stats.reader_init_ns);
    // create csv reader eat lines may throw EOF, we need to handle it
    Status st = _create_csv_reader();
    if (st.is_end_of_file()) {
        _no_data = true;
        return Status::OK();
    } else if (!st.ok()) {
        return st;
    }

    // update materialized columns.
    {
        std::unordered_set<std::string> names;
        for (const auto& column : _scanner_ctx->format_scan_context.materialized_columns) {
            if (column.name() == "___count___") continue;
            names.emplace(column.name());
        }
        RETURN_IF_ERROR(_scanner_ctx->format_scan_context.update_materialized_columns(names));
    }

    RETURN_IF_ERROR(_build_hive_column_name_2_index());
    for (const auto& column : _scanner_ctx->format_scan_context.materialized_columns) {
        // We don't care about _invalid_field_as_null here, if get converter failed,
        // we use DefaultValueConverter instead.
        auto converter = csv::get_hive_converter(column.slot_type(), true);
        DCHECK(converter != nullptr);
        _converters.emplace_back(std::move(converter));
    }
    return Status::OK();
}

void HdfsTextScanner::do_update_counter(HdfsScannerProfile* profile) {
    profile->runtime_profile->add_info_string("TextCompression", CompressionTypePB_Name(_compression_type));
}

void HdfsTextScanner::do_close(RuntimeState* runtime_state) noexcept {
    if (_no_data) {
        return;
    }
    _reader.reset();
}

Status HdfsTextScanner::do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) {
    if (_no_data) {
        return Status::EndOfFile("");
    }
    CHECK(chunk != nullptr);
    if (_use_v2) {
        RETURN_IF_ERROR(_parse_csv_v2(runtime_state->chunk_size(), chunk));
    } else {
        RETURN_IF_ERROR(_parse_csv(runtime_state->chunk_size(), chunk));
    }

    ChunkPtr ck = *chunk;
    // do stats before we filter rows which does not match.
    _app_stats.raw_rows_read += ck->num_rows();
    // conjunct_ctxs_by_slot evaluation is handled uniformly by HdfsScanner::get_next().
    return Status::OK();
}

Status HdfsTextScanner::_parse_csv(int chunk_size, ChunkPtr* chunk) {
    DCHECK_EQ(0, chunk->get()->num_rows());

    int num_columns = chunk->get()->num_columns();
    _column_raw_ptrs.resize(num_columns);
    for (int i = 0; i < num_columns; i++) {
        _column_raw_ptrs[i] = chunk->get()->get_column_raw_ptr_by_index(i);
        _column_raw_ptrs[i]->reserve(chunk_size);
    }

    csv::Converter::Options options;
    // Use to custom Hive array format
    options.is_hive = true;
    options.array_format_type = csv::ArrayFormatType::kHive;
    options.array_hive_collection_delimiter = _collection_delimiter;
    options.array_hive_mapkey_delimiter = _mapkey_delimiter;
    options.array_hive_nested_level = 1;
    options.invalid_field_as_null = _invalid_field_as_null;

    size_t rows_read = 0;

    CSVReader::Fields fields{};
    for (; rows_read < chunk_size; rows_read++) {
        CSVReader::Record record{};
        Status status = down_cast<HdfsScannerCSVReader*>(_reader.get())->next_record(&record);
        if (status.is_end_of_file()) {
            break;
        } else if (!status.ok()) {
            LOG(WARNING) << strings::Substitute("Parse csv file $0 failed: $1", _file->filename(), status.message());
            return status;
        }

        bool validate_res = validate_utf8(record.data, record.size);
        if (!validate_res && options.invalid_field_as_null) {
            VLOG_ROW << "Face csv invalidate UTF-8 character line, append default value for this line";
            chunk->get()->append_default();
            continue;
        } else if (!validate_res) {
            return Status::InternalError("Face csv invalidate UTF-8 character line.");
        }

        fields.resize(0);
        _reader->split_record(record, &fields);

        size_t num_materialize_columns = _scanner_ctx->format_scan_context.materialized_columns.size();

        // Fill materialize columns first, then fill partition column
        for (int j = 0; j < num_materialize_columns; j++) {
            const auto& column_info = _scanner_ctx->format_scan_context.materialized_columns[j];

            size_t chunk_index = column_info.idx_in_chunk;
            size_t csv_index = _materialize_slots_index_2_csv_column_index[j];
            Column* column = _column_raw_ptrs[chunk_index];
            if (csv_index < fields.size()) {
                const Slice& field = fields[csv_index];
                options.type_desc = &(column_info.slot_type());
                if (!_converters[j]->read_string(column, field, options)) {
                    return Status::InternalError(
                            strings::Substitute("CSV converter encountered an error for field: $0, column name is: $1",
                                                field.to_string(), column_info.name()));
                }
            } else {
                // The size of hive_column_names may be larger than fields when new columns are added.
                // The default value should be filled when querying the extra columns that
                // do not exist in the text file.
                column->append_default();
            }
        }
    }

    RETURN_IF_ERROR(_scanner_ctx->format_scan_context.append_side_columns_to_chunk(chunk, rows_read));
    RETURN_IF_ERROR(_scanner_ctx->format_scan_context.evaluate_all_predicates(chunk));

    // Check chunk's row number for each column
    chunk->get()->check_or_die();

    return rows_read > 0 ? Status::OK() : Status::EndOfFile("");
}

Status HdfsTextScanner::_parse_csv_v2(int chunk_size, ChunkPtr* chunk) {
    DCHECK_EQ(0, chunk->get()->num_rows());

    int num_columns = chunk->get()->num_columns();
    _column_raw_ptrs.resize(num_columns);
    for (int i = 0; i < num_columns; i++) {
        _column_raw_ptrs[i] = chunk->get()->get_column_raw_ptr_by_index(i);
        _column_raw_ptrs[i]->reserve(chunk_size);
    }

    csv::Converter::Options options;
    // Use to custom Hive array format
    options.is_hive = true;
    options.array_format_type = csv::ArrayFormatType::kHive;
    options.array_hive_collection_delimiter = _collection_delimiter;
    options.array_hive_mapkey_delimiter = _mapkey_delimiter;
    options.array_hive_nested_level = 1;
    options.invalid_field_as_null = _invalid_field_as_null;

    // The two serdes differ on both knobs (see Options for the full semantics):
    // - LazySimpleSerDe (_enclose == 0): fields still carry raw escape bytes, so pass
    //   ESCAPED BY's escape char down for NullableConverter's raw-bytes "\N" check and
    //   unescape; "\N" is its null literal (serialization.null.format default).
    // - OpenCSVSerde (_enclose != 0): split_hive_open_csv_line already fully resolves
    //   quotes/escapes, so no escape work remains -- and the serde has NO null-literal
    //   concept at all (fields are always strings; input "\\N" must come out as the
    //   2-char string "\N", not NULL), so disable the "\N" check entirely.
    options.escape = (_enclose == 0) ? _escape : 0;
    options.hive_null_literal = (_enclose == 0);

    const size_t num_materialize_columns = _scanner_ctx->format_scan_context.materialized_columns.size();
    const char field_delim = _field_delimiter.front();

    size_t rows_read = 0;
    HiveTextFields row;
    for (; rows_read < chunk_size; rows_read++) {
        CSVReader::Record record{};
        Status status = down_cast<HdfsScannerCSVReader*>(_reader.get())->next_record(&record);
        if (status.is_end_of_file()) {
            break;
        } else if (!status.ok()) {
            LOG(WARNING) << strings::Substitute("Parse csv file $0 failed: $1", _file->filename(), status.message());
            return status;
        }

        bool validate_res = validate_utf8(record.data, record.size);
        if (!validate_res && options.invalid_field_as_null) {
            VLOG_ROW << "Face csv invalidate UTF-8 character line, append default value for this line";
            chunk->get()->append_default();
            continue;
        } else if (!validate_res) {
            return Status::InternalError("Face csv invalidate UTF-8 character line.");
        }

        // Hive layering, reproduced exactly: rows were split at PHYSICAL line
        // boundaries above (LineRecordReader runs before the serde, so quotes and
        // escapes never span lines), and only now does the serde profile split this
        // one line into fields.
        if (_enclose != 0) {
            split_hive_open_csv_line(record, field_delim, _enclose, _escape, &row);
        } else {
            split_hive_lazy_simple_line(record, field_delim, _escape, &row);
        }
        if (row.all_null_row) {
            chunk->get()->append_default();
            continue;
        }

        // Fill materialize columns first, then fill partition column
        for (size_t j = 0; j < num_materialize_columns; j++) {
            const auto& column_info = _scanner_ctx->format_scan_context.materialized_columns[j];

            size_t chunk_index = column_info.idx_in_chunk;
            size_t csv_index = _materialize_slots_index_2_csv_column_index[j];
            Column* column = _column_raw_ptrs[chunk_index];
            if (csv_index < row.fields.size()) {
                // Null (LazySimpleSerDe's raw "\N") is decided by NullableConverter,
                // uniformly at every leaf position -- top-level scalar field, or a
                // scalar nested inside an ARRAY/MAP -- via Options::escape.
                const Slice& field = row.fields[csv_index];
                options.type_desc = &(column_info.slot_type());
                if (!_converters[j]->read_string(column, field, options)) {
                    return Status::InternalError(
                            strings::Substitute("CSV converter encountered an error for field: $0, column name is: $1",
                                                field.to_string(), column_info.name()));
                }
            } else {
                // The size of hive_column_names may be larger than fields when new columns are added.
                column->append_default();
            }
        }
    }

    RETURN_IF_ERROR(_scanner_ctx->format_scan_context.append_side_columns_to_chunk(chunk, rows_read));
    RETURN_IF_ERROR(_scanner_ctx->format_scan_context.evaluate_all_predicates(chunk));

    // Check chunk's row number for each column
    chunk->get()->check_or_die();

    return rows_read > 0 ? Status::OK() : Status::EndOfFile("");
}

Status HdfsTextScanner::_create_csv_reader() {
    const THdfsScanRange* scan_range = _scanner_ctx->scan_range;

    if (_compression_type != NO_COMPRESSION) {
        // we don't know real stream size in adavance, so we set a very large stream size
        auto file_size = static_cast<size_t>(-1);
        _reader = std::make_shared<HdfsScannerCSVReader>(_file.get(), _line_delimiter, _need_probe_line_delimiter,
                                                         _field_delimiter, file_size, _enclose, _escape);
    } else {
        // no compressed file, splittable.
        _reader = std::make_shared<HdfsScannerCSVReader>(_file.get(), _line_delimiter, _need_probe_line_delimiter,
                                                         _field_delimiter, scan_range->file_length, _enclose, _escape);
    }
    auto* reader = down_cast<HdfsScannerCSVReader*>(_reader.get());

    // (TODO) only support uncompressed file to skip utf-8 bom, because compressed input stream didn't support seek() function
    bool has_utf8_bom = false;
    if (_compression_type == NO_COMPRESSION) {
        // if reading start of file, try to skipping UTF-8 BOM
        ASSIGN_OR_RETURN(has_utf8_bom, _has_utf8_bom());
        if (has_utf8_bom) {
            RETURN_IF_ERROR(reader->reset(scan_range->offset + 3, scan_range->length - 3));
        } else {
            // reset offset
            RETURN_IF_ERROR(reader->reset(scan_range->offset, scan_range->length));
        }
    }

    if (scan_range->offset != 0) {
        // Always skip first record of scan range with non-zero offset.
        // Notice that the first record will read by previous scan range.
        CSVReader::Record dummy;
        RETURN_IF_ERROR(reader->next_record(&dummy));
    }

    // skip header line count only in offset = 0
    if (scan_range->offset == 0 && _skip_header_line_count > 0) {
        CSVReader::Record dummy;
        for (int32_t i = 0; i < _skip_header_line_count; i++) {
            RETURN_IF_ERROR(reader->next_record(&dummy));
        }
    }
    return Status::OK();
}

StatusOr<bool> HdfsTextScanner::_has_utf8_bom() const {
    // if reading start of file, skipping UTF-8 BOM
    if (_scanner_ctx->scan_range->offset == 0) {
        auto* reader = down_cast<HdfsScannerCSVReader*>(_reader.get());
        CSVReader::Record first_line;
        RETURN_IF_ERROR(reader->next_record(&first_line));
        if (first_line.size >= 3 && static_cast<unsigned char>(first_line.data[0]) == 0xEF &&
            static_cast<unsigned char>(first_line.data[1]) == 0xBB &&
            static_cast<unsigned char>(first_line.data[2]) == 0xBF) {
            return true;
        }
    }
    return false;
}

Status HdfsTextScanner::_build_hive_column_name_2_index() {
    // For some table like file table, there is no hive_column_names at all.
    // So we use slot order defined in table schema.
    if (_scanner_ctx->hive_column_names.empty()) {
        _materialize_slots_index_2_csv_column_index.resize(
                _scanner_ctx->format_scan_context.materialized_columns.size());
        for (size_t i = 0; i < _scanner_ctx->format_scan_context.materialized_columns.size(); i++) {
            _materialize_slots_index_2_csv_column_index[i] = i;
        }
        return Status::OK();
    }

    const bool case_sensitive = _scanner_ctx->format_scan_context.options.case_sensitive;

    // The map's value is the position of column name in hive's table(Not in StarRocks' table)
    std::unordered_map<std::string, size_t> formatted_hive_column_name_2_index;

    for (size_t i = 0; i < _scanner_ctx->hive_column_names.size(); i++) {
        const std::string& name = _scanner_ctx->hive_column_names[i];
        const std::string formatted_column_name = _scanner_ctx->formatted_name(name);
        formatted_hive_column_name_2_index.emplace(formatted_column_name, i);
    }

    // Assign csv column index
    _materialize_slots_index_2_csv_column_index.resize(_scanner_ctx->format_scan_context.materialized_columns.size());
    for (size_t i = 0; i < _scanner_ctx->format_scan_context.materialized_columns.size(); i++) {
        const auto& column = _scanner_ctx->format_scan_context.materialized_columns[i];
        const std::string formatted_slot_name = column.formatted_name(case_sensitive);
        const auto& it = formatted_hive_column_name_2_index.find(formatted_slot_name);
        if (it == formatted_hive_column_name_2_index.end()) {
            return Status::InternalError("Can not get index of column name: " + formatted_slot_name);
        }
        _materialize_slots_index_2_csv_column_index[i] = it->second;
    }
    return Status::OK();
}

int64_t HdfsTextScanner::estimated_mem_usage() const {
    int64_t value = HdfsScanner::estimated_mem_usage();
    if (value != 0) return value;
    // for compressed text file, if _no_data=true, means _reader is nullptr
    if (_no_data) {
        return 0;
    }
    DCHECK(_reader != nullptr);
    return _reader->buff_capacity() * 3 / 2;
}

} // namespace starrocks
