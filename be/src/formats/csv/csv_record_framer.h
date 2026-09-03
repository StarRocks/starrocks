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

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "formats/csv/csv_parse_options.h"

namespace starrocks {

// Locates record boundaries in a CSV byte stream so that a file can be split into ranges
// that each begin on a real record.
//
// A record boundary is the offset of the first byte of a record. Offset 0 is always one.
// Every other boundary is the offset just past a row delimiter that ends a record.
//
// The framer exists because a row delimiter is not by itself a record boundary: inside an
// enclosed field, or after an escape character, the parser consumes it as ordinary data.
// Whether a given offset sits inside an enclosed field depends on every byte before it, so
// the stream must be fed in order from offset 0 - there is no way to resolve the state at an
// arbitrary offset by looking only at the bytes around it.
//
// The framer deliberately mirrors the state machine in CSVReader::more_rows(), including
// behaviour that is arguably wrong (see csv_record_framer.cpp). Its contract is to agree with
// the parser, not to be independently correct about CSV: a framer that disagreed would hand
// out split points the parser then reads differently, which is the very defect it exists to
// remove. CSVRecordFramerTest pins the two together, and any change to more_rows() must be made
// here in step.
//
// Only framing is performed - no fields are split out, no data is copied, and nothing is
// allocated per record - so the scan runs at roughly the speed of the underlying reads.
class CSVRecordFramer {
public:
    // |min_split_size| suppresses boundaries that fall less than that many bytes past the last
    // one reported, keeping the output proportional to the number of splits wanted rather than
    // to the number of records in the file. Zero reports every boundary.
    CSVRecordFramer(const CSVParseOptions& options, int64_t min_split_size);

    CSVRecordFramer(const CSVRecordFramer&) = delete;
    CSVRecordFramer& operator=(const CSVRecordFramer&) = delete;

    // Feeds the next chunk of the file. Chunks must be contiguous and in order, the first one
    // starting at offset 0. Bytes that cannot yet be classified - a delimiter that may straddle
    // the chunk boundary - are held back and resolved against the following chunk.
    void feed(const char* data, size_t size);

    // Declares the end of the stream and resolves any held-back bytes. A truncated delimiter at
    // end of file is not a delimiter, matching the parser. No boundary is reported at end of
    // file itself, since no record starts there.
    void finish();

    // Record starts in ascending order, the first of which is always 0.
    const std::vector<int64_t>& split_offsets() const { return _split_offsets; }

    // File offset one past the last byte classified so far.
    int64_t position() const { return _stream_pos; }

    // Whether the stream so far ends inside an enclosed field. Exposed for tests, which use it
    // to assert that quote state tracks the parser's across chunk boundaries.
    bool inside_enclosed_field() const { return _state == State::ENCLOSE; }

private:
    // The subset of CSVReader::ParseState that framing depends on. COLUMN_DELIMITER and NEWROW
    // are omitted: the parser uses them only to close off a column or a row before returning to
    // START, which is bookkeeping the framer does not carry.
    enum class State { START, ORDINARY, ESCAPE, ENCLOSE, ENCLOSE_ESCAPE };

    // Runs the state machine over |p[0, n)|, whose first byte is at file offset |_stream_pos|,
    // and returns the number of bytes classified. Stops short of the end while a delimiter could
    // still straddle the chunk boundary, unless |at_eof|.
    size_t _run(const char* p, size_t n, bool at_eof);

    void _emit_boundary(int64_t offset);

    // Mirrors is_row_delimiter() where it stops being a plain comparison: at the end of the data.
    //
    // For a delimiter longer than one byte it walks the buffer and needs a byte after the match. If
    // the walk runs out, the reader is asked for more, and _fill_buffer() invents a row delimiter
    // only when what is left is shorter than one or does not already end with one. So two opposite
    // things happen at the end of a file. A delimiter sitting exactly at the end is not read as one
    // - nothing is invented, and there is no byte after it - and its bytes become the content of a
    // final record. But a file ending in part of a delimiter has one invented behind it, and the
    // two together do complete a delimiter.
    //
    // A one-byte delimiter is compared in place and needs nothing after it, so none of this applies.
    bool _row_delimiter_here(const char* p, size_t i, size_t n, size_t avail, bool at_eof) const {
        if (_row_delimiter_length == 1 || !at_eof || i + _row_delimiter_length < n) {
            return _match_row(p + i, avail);
        }
        const size_t left = n - i;
        const bool ends_with_delimiter =
                left >= _row_delimiter_length &&
                std::memcmp(p + n - _row_delimiter_length, _options.row_delimiter.data(), _row_delimiter_length) == 0;
        std::string tail(p + i, left);
        if (!ends_with_delimiter) {
            tail += _options.row_delimiter;
        }
        return tail.size() > _row_delimiter_length &&
               std::memcmp(tail.data(), _options.row_delimiter.data(), _row_delimiter_length) == 0;
    }

    bool _match_row(const char* p, size_t avail) const {
        if (_row_delimiter_length == 1) {
            return *p == _options.row_delimiter[0];
        }
        return avail >= _row_delimiter_length &&
               std::memcmp(p, _options.row_delimiter.data(), _row_delimiter_length) == 0;
    }

    bool _match_column(const char* p, size_t avail) const {
        if (_column_delimiter_length == 1) {
            return *p == _options.column_delimiter[0];
        }
        return avail >= _column_delimiter_length &&
               std::memcmp(p, _options.column_delimiter.data(), _column_delimiter_length) == 0;
    }

    const CSVParseOptions _options;
    const size_t _row_delimiter_length;
    const size_t _column_delimiter_length;
    // Bytes that must be in hand before the next byte can be classified: enough for the longer
    // delimiter, and never fewer than two, because ENCLOSE peeks one byte past a closing quote
    // to tell a doubled quote from the end of the field.
    const size_t _lookahead;
    const int64_t _min_split_size;

    State _state = State::START;
    State _pre_state = State::START;
    int64_t _stream_pos = 0;
    std::vector<int64_t> _split_offsets;
    // Where the record now being read began, which is only reported once that record is complete.
    //
    // Two things make this later than the obvious moment. A row delimiter met in START is a blank
    // line, which more_rows() consumes without producing a record, so the record begins after it
    // rather than at it. And a trailing fragment with no row delimiter after it is never a record
    // either, so a start is worth reporting only once the record starting there has ended.
    // Reporting either would offer a split point the parser does not read as a record start.
    int64_t _next_start = 0;
    bool _in_record = false;
    // Bytes received but not yet classified. Between calls this is the held-back tail of the
    // last chunk, fewer than |_lookahead| bytes, which the next chunk is appended to.
    std::string _pending;
};

} // namespace starrocks
