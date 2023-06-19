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

#include "formats/csv/csv_reader.h"

#include <unordered_set>

namespace starrocks {

using Field = Slice;

static std::pair<const char*, size_t> trim(const char* value, size_t len) {
    size_t begin = 0;

    while (begin < len && value[begin] == ' ') {
        ++begin;
    }

    size_t end = len - 1;

    while (end > begin && value[end] == ' ') {
        --end;
    }

    return std::make_pair(value + begin, end - begin + 1);
}

inline bool CSVReader::is_column_delimiter(bool expandBuffer) {
    if (LIKELY(_column_delimiter_length == 1)) {
        if (*(_buff.position()) == _parse_options.column_delimiter[0]) {
            _buff.skip(1);
            return true;
        }
    } else {
        int i = 0;
        const char* base_ptr = _buff.base_ptr();
        size_t p = _buff.position_offset();
        while (i < _column_delimiter_length && p < _buff.limit_offset() &&
               *(base_ptr + p) == _parse_options.column_delimiter[i]) {
            i++;
            p++;
            if (UNLIKELY(_buff.limit_offset() - p < 1)) {
                Status s = readMore(expandBuffer);
                if (!s.ok()) {
                    return false;
                }
                if (_buff.limit_offset() - p < 1) {
                    return false;
                }
            }
        }
        if (i == _column_delimiter_length) {
            _buff.skip(_column_delimiter_length);
            return true;
        }
    }
    return false;
}

inline bool CSVReader::is_row_delimiter(bool expandBuffer) {
    if (LIKELY(_row_delimiter_length == 1)) {
        if (*(_buff.position()) == _parse_options.row_delimiter[0]) {
            _buff.skip(1);
            return true;
        }
    } else {
        int i = 0;
        const char* base_ptr = _buff.base_ptr();
        size_t p = _buff.position_offset();
        while (i < _row_delimiter_length && p < _buff.limit_offset() &&
               *(base_ptr + p) == _parse_options.row_delimiter[i]) {
            i++;
            p++;
            if (UNLIKELY(_buff.limit_offset() - p < 1)) {
                Status s = readMore(expandBuffer);
                if (!s.ok()) {
                    return false;
                }
                if (_buff.limit_offset() - p < 1) {
                    return false;
                }
            }
        }
        if (i == _row_delimiter_length) {
            _buff.skip(_row_delimiter_length);
            return true;
        }
    }
    return false;
}

char* CSVReader::buffBasePtr() {
    return _buff.base_ptr();
}

char* CSVReader::escapeDataPtr() {
    return _escape_data.data();
}

Status CSVReader::buffInit() {
#ifndef CSVBENCHMARK
    _buff.compact();
    if (_buff.free_space() > 0) {
        return _fill_buffer();
    }
#endif
    return Status::OK();
}

Status CSVReader::readMore(bool expandBuffer) {
    if (_buff.free_space() > 0) {
        return _fill_buffer();
    } else {
        if (expandBuffer) {
            RETURN_IF_ERROR(_expand_buffer_loosely());
            return _fill_buffer();
        } else {
            return Status::OK();
        }
    }
}

#define DOUBLE_CHECK_AVAILABLE()                \
    if (_buff.available() < 1 && !notGetLine) { \
        curState = NEWROW;                      \
        reachBuffEnd = true;                    \
        goto newrow_label;                      \
    }

#define READ_MORE()                        \
    if (UNLIKELY(_buff.available() < 1)) { \
        status = readMore(notGetLine);     \
        if (!status.ok()) {                \
            curState = NEWROW;             \
            goto newrow_label;             \
        }                                  \
        DOUBLE_CHECK_AVAILABLE()           \
    }

#define READ_MORE_ENCLOSE()                \
    if (UNLIKELY(_buff.available() < 1)) { \
        status = readMore(notGetLine);     \
        if (!status.ok()) {                \
            is_enclose_column = true;      \
            curState = NEWROW;             \
            goto newrow_label;             \
        }                                  \
        DOUBLE_CHECK_AVAILABLE()           \
    }

Status CSVReader::more_rows() {
    if (UNLIKELY(_limit > 0 && _parsed_bytes > _limit)) {
        return Status::EndOfFile("Reached limit");
    }
    Status status = Status::OK();
    status = buffInit();
    if (!status.ok()) {
        return status;
    }
    _escape_data.clear();
    _escape_pos.clear();
    ParseState curState = START;
    ParseState preState = curState;
    // parsed_start and parsed_end record a row at the start and end of the buff.
    size_t parsed_start = _buff.position_offset();
    size_t parsed_end = 0;
    // If column_start is initialized to the maximum value, the START state is not entered.
    size_t column_start = std::string::npos;
    size_t column_end = 0;
    size_t white_space_start = std::string::npos;
    size_t column_length = 0;
    bool is_enclose_column = false;
    bool is_escape_column = false;
    bool notGetLine = true;
    bool reachBuffEnd = false;
    _columns.clear();
    while (true) {
        // At the end of a row, or the end of a column, no new data is read.
        if (LIKELY(curState != NEWROW && curState != COLUMN_DELIMITER)) {
            READ_MORE()
        }
        // Advance to the next state each time based on the current state + the current character stream.
        switch (curState) {
        case START:
            // Trim space is enabled and the leading space is skipped in the START state
            if (UNLIKELY(_parse_options.trim_space)) {
                while (*(_buff.position()) == ' ') {
                    _buff.skip(1);
                    READ_MORE()
                }
            }
            column_start = _buff.position_offset();

            // newrow
            if (UNLIKELY(is_row_delimiter(notGetLine))) {
                curState = NEWROW;
                break;
            }

            // delimiter
            if (UNLIKELY(is_column_delimiter(notGetLine))) {
                curState = COLUMN_DELIMITER;
                break;
            }

            // escape
            if (UNLIKELY(*(_buff.position()) == _parse_options.escape)) {
                // trick here.
                preState = ORDINARY;
                curState = ESCAPE;
                _escape_pos.insert(_buff.position_offset());
                _buff.skip(1);
                break;
            }

            // enclose
            if (UNLIKELY(*(_buff.position()) == _parse_options.enclose)) {
                _buff.skip(1);
                READ_MORE()
                column_start = _buff.position_offset();
                if (*(_buff.position()) != _parse_options.enclose) {
                    curState = ENCLOSE;
                    is_enclose_column = true;
                    break;
                } else {
                    // ""something
                    // We need to determine whether the column is empty or escaped.
                    _buff.skip(1);
                    READ_MORE_ENCLOSE()
                    // ""rowseperator
                    if (is_row_delimiter(notGetLine)) {
                        is_enclose_column = true;
                        curState = NEWROW;
                        break;
                    }

                    // ""delimiter
                    if (is_column_delimiter(notGetLine)) {
                        is_enclose_column = true;
                        curState = COLUMN_DELIMITER;
                        break;
                    }

                    // ""ordinary characters
                    curState = ORDINARY;
                    break;
                }
            }
            curState = ORDINARY;
            _buff.skip(1);
            break;

        // ENCLOSE state can only be entered with a START state.
        // How does the START state transition to the ENCLOSE state：
        // 1. trimspace is not enabled
        //    a. "some
        //    b. ""aaa, enclose is just used as an escape and you can't go into ENCLOSE state.
        //    c. "", empty columns must be read next to column delimiters or newrows.
        // 2. trimspace is enabled
        //    Remove the leading space before judging.
        case ENCLOSE:
            // enclose character again, There are two possibilities:
            // 1. ENCLOSE state is over
            // 2. enclose to escape
            if (*(_buff.position()) == _parse_options.enclose) {
                _buff.skip(1);
                READ_MORE_ENCLOSE()
                if (*(_buff.position()) == _parse_options.enclose) {
                    preState = curState;
                    curState = ENCLOSE_ESCAPE;
                    _escape_pos.insert(_buff.position_offset() - 1);
                } else {
                    preState = curState;
                    curState = ORDINARY;
                }
                break;
            }
            // escape
            if (*(_buff.position()) == _parse_options.escape) {
                preState = curState;
                curState = ESCAPE;
                _escape_pos.insert(_buff.position_offset());
                _buff.skip(1);
                break;
            }
            // other character
            _buff.skip(1);
            break;

        case ENCLOSE_ESCAPE:
            curState = preState;
            _buff.skip(1);
            break;

        case ESCAPE:
            // escape enclose
            if (*(_buff.position()) == _parse_options.enclose) {
                curState = preState;
                _buff.skip(1);
                break;
            }
            // escape escape
            if (*(_buff.position()) == _parse_options.escape) {
                curState = preState;
                _buff.skip(1);
                break;
            }
            // escape row COLUMN_DELIMITER
            if (is_row_delimiter(notGetLine)) {
                curState = preState;
                break;
            }
            // escape column separator
            if (is_column_delimiter(notGetLine)) {
                curState = preState;
                break;
            }

        case ORDINARY:
            if (UNLIKELY(_parse_options.trim_space && preState == ENCLOSE)) {
                if (*(_buff.position()) == ' ') {
                    white_space_start = _buff.position_offset();
                }
                preState = ORDINARY;
            }

            // newrow
            if (UNLIKELY(is_row_delimiter(notGetLine))) {
                curState = NEWROW;
                break;
            }

            // delimiter
            if (UNLIKELY(is_column_delimiter(notGetLine))) {
                curState = COLUMN_DELIMITER;
                break;
            }

            // escape
            if (UNLIKELY(*(_buff.position()) == _parse_options.escape)) {
                preState = curState;
                curState = ESCAPE;
                _escape_pos.insert(_buff.position_offset());
                _buff.skip(1);
                break;
            }

            // enclose
            if (UNLIKELY(*(_buff.position()) == _parse_options.enclose)) {
                preState = curState;
                curState = ENCLOSE_ESCAPE;
                _escape_pos.insert(_buff.position_offset());
                _buff.skip(1);
                break;
            }

            _buff.skip(1);
            curState = ORDINARY;
            break;

        case COLUMN_DELIMITER:
            if (UNLIKELY(_parse_options.trim_space && white_space_start != std::string::npos)) {
                column_end = white_space_start;
            } else {
                column_end = _buff.position_offset();
            }
            // The column has an escape and needs to be stripped of the escape character and copied to a separate storage space.
            if (UNLIKELY(_escape_pos.size() > 0)) {
                is_escape_column = true;
                size_t new_column_start = _escape_data.size();
                for (size_t i = column_start; i < column_end; i++) {
                    if (_escape_pos.count(i)) {
                        continue;
                    }
                    _escape_data.push_back(_buff.get_char(i));
                }
                _escape_pos.clear();
                column_start = new_column_start;
                column_end = _escape_data.size();
            }

            if (UNLIKELY(is_enclose_column)) {
                if (UNLIKELY(_parse_options.trim_space && white_space_start != std::string::npos)) {
                    column_length = column_end - column_start - 1;
                } else {
                    // Remove the last enclose character.
                    column_length = column_end - _column_delimiter_length - column_start - 1;
                }
            } else {
                column_length = column_end - _column_delimiter_length - column_start;
            }
            // push column
            _columns.emplace_back(column_start, column_length, is_escape_column);
            is_escape_column = false;
            curState = START;
            is_enclose_column = false;
            white_space_start = std::string::npos;
            break;

        newrow_label:
        case NEWROW:
            if (reachBuffEnd) {
                // We dot get the complete row, back the read pointer, continue next time.
                _buff.set_position_offset(parsed_start);
                return Status::OK();
            }
            _parsed_bytes += _buff.position_offset() - parsed_start;
            parsed_end = _buff.position_offset();
            // push row
            if (LIKELY(status.ok() || status.is_end_of_file())) {
                // For empty row, skip it. And restart state machine.
                if (UNLIKELY(_columns.size() == 0 &&
                             _buff.position_offset() - _row_delimiter_length - column_start == 0)) {
                    curState = START;
                    is_enclose_column = false;
                    is_escape_column = false;
                    if (status.is_end_of_file()) {
                        return status;
                    }
                    parsed_start = _buff.position_offset();
                    column_start = _buff.position_offset();
                    break;
                }
                if (column_start != std::string::npos) {
                    if (UNLIKELY(_parse_options.trim_space && white_space_start != std::string::npos)) {
                        column_end = white_space_start;
                    } else {
                        column_end = _buff.position_offset();
                    }
                    if (UNLIKELY(_escape_pos.size() > 0)) {
                        is_escape_column = true;
                        size_t new_column_start = _escape_data.size();
                        for (size_t i = column_start; i < column_end; i++) {
                            if (_escape_pos.count(i)) {
                                continue;
                            }
                            _escape_data.push_back(_buff.get_char(i));
                        }
                        _escape_pos.clear();
                        column_start = new_column_start;
                        column_end = _escape_data.size();
                    }

                    if (UNLIKELY(is_enclose_column)) {
                        if (UNLIKELY(_parse_options.trim_space && white_space_start != std::string::npos)) {
                            column_length = column_end - column_start - 1;
                        } else {
                            // Remove the last enclose character.
                            column_length = column_end - _row_delimiter_length - column_start - 1;
                        }
                    } else {
                        column_length = column_end - _row_delimiter_length - column_start;
                    }
                    _columns.emplace_back(column_start, column_length, is_escape_column);
                    notGetLine = false;
                    column_start = std::string::npos;
                    CSVRow newRow;
                    newRow.columns = _columns;
                    newRow.parsed_start = parsed_start;
                    newRow.parsed_end = parsed_end;
                    _csv_buff.push(newRow);
                }
            }
            if (UNLIKELY(!status.ok())) {
                return status;
            }
            _columns.clear();
            curState = START;
            is_escape_column = false;
            is_enclose_column = false;
            white_space_start = std::string::npos;
            parsed_start = _buff.position_offset();
            if (UNLIKELY(_limit > 0 && _parsed_bytes > _limit)) {
                return Status::EndOfFile("Reached limit");
            }
            break;

        default:
            return Status::NotSupported("Not supported state when csv parsing");
        }
    }
}

Status CSVReader::next_record(CSVRow& row) {
    row.columns.clear();
    if (_csv_buff.empty()) {
        Status status = more_rows();
        if (!status.ok() && !status.is_end_of_file()) {
            return status;
        }
    }
    if (UNLIKELY(_csv_buff.empty())) {
        return Status::EndOfFile("Reached limit");
    }
    const CSVRow newRow = _csv_buff.front();
    _csv_buff.pop();
    row.columns = newRow.columns;
    row.parsed_start = newRow.parsed_start;
    row.parsed_end = newRow.parsed_end;

    return Status::OK();
}

Status CSVReader::next_record(Record* record) {
    if (_limit > 0 && _parsed_bytes > _limit) {
        return Status::EndOfFile("Reached limit");
    }
    char* d;
    size_t pos = 0;
    while ((d = _buff.find(_parse_options.row_delimiter, pos)) == nullptr) {
        pos = _buff.available();
        _buff.compact();
        if (_buff.free_space() == 0) {
            RETURN_IF_ERROR(_expand_buffer());
        }
        // fill_buffer does a memory copy: it reads the data from the file and writes it to _buff
        RETURN_IF_ERROR(_fill_buffer());
        // When _row_delimiter_length larger than 1, for example $$$
        // we may read $$ first and _buff.find will return nullptr, after
        // filling buffer we read $, however we will find from $ and skip $$
        // so we may get wrong result here. we can always set pos = 0, but
        // for single char _row_delimiter we keep the same logic as before.
        pos = _row_delimiter_length > 1 ? 0 : pos;
    }
    size_t l = d - _buff.position();
    *record = Record(_buff.position(), l);
    _buff.skip(l + _row_delimiter_length);
    //               ^^ skip record delimiter.
    _parsed_bytes += l + _row_delimiter_length;
    return Status::OK();
}

Status CSVReader::_expand_buffer() {
    if (UNLIKELY(_storage.size() >= kMaxBufferSize)) {
        return Status::InternalError("CSV line length exceed limit " + std::to_string(kMaxBufferSize));
    }
    size_t new_capacity = std::min(_storage.size() * 2, kMaxBufferSize);
    DCHECK_EQ(_storage.data(), _buff.position()) << "should compact buffer before expand";
    _storage.resize(new_capacity);
    CSVBuffer new_buff(_storage.data(), _storage.size());
    new_buff.add_limit(_buff.available());
    DCHECK_EQ(_storage.data(), new_buff.position());
    DCHECK_EQ(_buff.available(), new_buff.available());
    _buff = new_buff;
    return Status::OK();
}

// _expand_buffer must ensure that there is no data in the buffer. However, after we introduced the state machine parser,
// this constraint became too strong and we introduced a more relaxed buffer expansion function.
Status CSVReader::_expand_buffer_loosely() {
    if (UNLIKELY(_storage.size() >= kMaxBufferSize)) {
        return Status::InternalError("CSV line length exceed limit " + std::to_string(kMaxBufferSize));
    }
    size_t new_capacity = std::min(_storage.size() * 2, kMaxBufferSize);
    _storage.resize(new_capacity);
    CSVBuffer new_buff(_storage.data(), _storage.size());
    new_buff.set_position_offset(_buff.position_offset());
    new_buff.set_limit_offset(_buff.limit_offset());
    _buff = new_buff;
    return Status::OK();
}

void CSVReader::split_record(const Record& record, Fields* columns) const {
    DCHECK(_column_delimiter_length > 0);

    const char* value = record.data;
    const char* ptr = record.data;
    const size_t size = record.size;

    if (_column_delimiter_length == 1) {
        for (size_t i = 0; i < size; ++i, ++ptr) {
            if (*ptr == _parse_options.column_delimiter[0]) {
                if (_parse_options.trim_space) {
                    std::pair<const char*, size_t> newPos = trim(value, ptr - value);
                    columns->emplace_back(newPos.first, newPos.second);
                } else {
                    columns->emplace_back(value, ptr - value);
                }
                value = ptr + 1;
            }
        }
    } else {
        const auto* const base = ptr;

        do {
            ptr = static_cast<char*>(memmem(value, size - (value - base), _parse_options.column_delimiter.data(),
                                            _column_delimiter_length));
            if (ptr != nullptr) {
                if (_parse_options.trim_space) {
                    std::pair<const char*, size_t> newPos = trim(value, ptr - value);
                    columns->emplace_back(newPos.first, newPos.second);
                } else {
                    columns->emplace_back(value, ptr - value);
                }
                value = ptr + _column_delimiter_length;
            }
        } while (ptr != nullptr);

        ptr = record.data + size;
    }
    if (_parse_options.trim_space) {
        std::pair<const char*, size_t> newPos = trim(value, ptr - value);
        columns->emplace_back(newPos.first, newPos.second);
    } else {
        columns->emplace_back(value, ptr - value);
    }
}

} // namespace starrocks
