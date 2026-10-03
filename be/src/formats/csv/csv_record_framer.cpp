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

#include "formats/csv/csv_record_framer.h"

#include <algorithm>

namespace starrocks {

CSVRecordFramer::CSVRecordFramer(const CSVParseOptions& options, int64_t min_split_size)
        : _options(options),
          _row_delimiter_length(options.row_delimiter.size()),
          _column_delimiter_length(options.column_delimiter.size()),
          _lookahead(std::max<size_t>(std::max(_row_delimiter_length, _column_delimiter_length), 2)),
          _min_split_size(std::max<int64_t>(min_split_size, 1)) {
    _split_offsets.push_back(0);
}

void CSVRecordFramer::feed(const char* data, size_t size) {
    if (size == 0) {
        return;
    }
    // Appending rather than running over |data| in place costs one copy of the chunk, which
    // buys a single obviously-correct path for delimiters straddling a chunk boundary. Framing
    // is limited by the read behind it, not by this copy; if that ever stops being true, the
    // parity test is what makes a zero-copy rewrite safe to attempt.
    _pending.append(data, size);
    const size_t consumed = _run(_pending.data(), _pending.size(), false);
    _stream_pos += static_cast<int64_t>(consumed);
    _pending.erase(0, consumed);
}

void CSVRecordFramer::finish() {
    if (!_pending.empty()) {
        const size_t consumed = _run(_pending.data(), _pending.size(), true);
        _stream_pos += static_cast<int64_t>(consumed);
        _pending.erase(0, consumed);
    }
    // Whatever record is still open ends here, and where it began is a boundary. A _next_start
    // inside the appended bytes is not: the tail completed a delimiter rather than starting a
    // record, and nothing in the file begins there.
    if (_in_record && _next_start < _stream_pos) {
        _emit_boundary(_next_start);
    }
    _in_record = false;
    // A file ending in a row delimiter leaves a boundary at end of file. Nothing starts there, and
    // a range beginning at it would be empty, so it is not reported.
    if (_split_offsets.size() > 1 && _split_offsets.back() == _stream_pos) {
        _split_offsets.pop_back();
    }
}

void CSVRecordFramer::_emit_boundary(int64_t offset) {
    if (offset > _split_offsets.back() && offset - _split_offsets.back() >= _min_split_size) {
        _split_offsets.push_back(offset);
    }
}

// Mirrors the state machine in CSVReader::more_rows(), keeping its states, its order of tests and
// its transitions, with the column and row bookkeeping dropped. The correspondence is the whole
// point: the framer has to predict what the parser will do rather than improve on it, because a
// split point the parser then reads differently is the defect this exists to remove.
// CSVRecordFramerTest pins the two together, so any change to more_rows() belongs here in the same
// commit.
size_t CSVRecordFramer::_run(const char* p, size_t n, bool at_eof) {
    size_t i = 0;
    while (i < n) {
        // Hold back a row delimiter's worth beyond the usual lookahead. Whether a multi-byte
        // delimiter is read as one depends on there being a byte after it, which is only knowable
        // at end of file, so that decision must not be taken while more data could still arrive.
        if (!at_eof && n - i < _lookahead + _row_delimiter_length) {
            break;
        }
        const size_t avail = n - i;
        switch (_state) {
        case State::START:
            if (_options.trim_space && p[i] == ' ') {
                ++i;
                break;
            }
            if (_row_delimiter_here(p, i, n, avail, at_eof)) {
                i += _row_delimiter_length;
                if (_in_record) {
                    // A column was started before this, so the record is real and ends here - the
                    // same distinction more_rows() draws with _columns.size() != 0.
                    _emit_boundary(_next_start);
                    _in_record = false;
                } else {
                    // A blank record: more_rows() consumes it without producing a record and
                    // carries the start forward, so carry it forward here rather than report it.
                }
                _next_start = _stream_pos + static_cast<int64_t>(i);
                _state = State::START;
                break;
            }
            _in_record = true;
            if (_match_column(p + i, avail)) {
                i += _column_delimiter_length;
                _state = State::START;
                break;
            }
            if (p[i] == _options.escape) {
                _pre_state = State::ORDINARY;
                _state = State::ESCAPE;
                ++i;
                break;
            }
            if (p[i] == _options.enclose) {
                ++i;
                _state = State::ENCLOSE;
                break;
            }
            _state = State::ORDINARY;
            ++i;
            break;

        case State::ENCLOSE:
            if (p[i] == _options.enclose) {
                ++i;
                _pre_state = State::ENCLOSE;
                _state = (i < n && p[i] == _options.enclose) ? State::ENCLOSE_ESCAPE : State::ORDINARY;
                break;
            }
            if (p[i] == _options.escape) {
                _pre_state = State::ENCLOSE;
                _state = State::ESCAPE;
                ++i;
                break;
            }
            ++i;
            break;

        case State::ENCLOSE_ESCAPE:
            _state = _pre_state;
            ++i;
            break;

        case State::ESCAPE:
            if (p[i] == _options.enclose) {
                _state = _pre_state;
                ++i;
                break;
            }
            if (p[i] == _options.escape) {
                _state = _pre_state;
                ++i;
                break;
            }
            if (_row_delimiter_here(p, i, n, avail, at_eof)) {
                i += _row_delimiter_length;
                _state = _pre_state;
                break;
            }
            if (_match_column(p + i, avail)) {
                i += _column_delimiter_length;
                _state = _pre_state;
                break;
            }
            _state = _pre_state;
            ++i;
            break;

        case State::ORDINARY:
            if (_options.trim_space && _pre_state == State::ENCLOSE) {
                _pre_state = State::ORDINARY;
            }
            if (_row_delimiter_here(p, i, n, avail, at_eof)) {
                // The record is complete, so where it began is a place a range may begin too.
                i += _row_delimiter_length;
                _emit_boundary(_next_start);
                _next_start = _stream_pos + static_cast<int64_t>(i);
                _in_record = false;
                _state = State::START;
                break;
            }
            if (_match_column(p + i, avail)) {
                i += _column_delimiter_length;
                _state = State::START;
                break;
            }
            if (p[i] == _options.escape) {
                _pre_state = _state;
                _state = State::ESCAPE;
                ++i;
                break;
            }
            if (p[i] == _options.enclose) {
                _pre_state = _state;
                ++i;
                _state = (i < n && p[i] == _options.enclose) ? State::ENCLOSE_ESCAPE : State::ORDINARY;
                break;
            }
            ++i;
            _state = State::ORDINARY;
            break;
        }
    }
    return i;
}

} // namespace starrocks
