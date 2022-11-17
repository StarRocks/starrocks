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

namespace starrocks::vectorized {

using Field = Slice;

static std::pair<const char*, size_t> trim(const char* value, size_t len) {
    int32_t begin = 0;

    while (begin < len && value[begin] == ' ') {
        ++begin;
    }

    int32_t end = len - 1;

    while (end > begin && value[end] == ' ') {
        --end;
    }
    
    return std::make_pair(value + begin,  end - begin + 1);
}

bool CSVReader::isColumnSeparator(CSVBuffer& buff) {
    if (_column_separator_length == 1) {
        if (*(buff.position()) == _column_separator[0]) {
            buff.skip(1);
            return true;
        }
    } else {
        int i = 0;
        const char* base_ptr = _buff.base_ptr(); 
        size_t p = _buff.position_offset();
        while (i < _column_separator_length && p < _buff.limit_offset() && *(base_ptr+p) == _column_separator[i]) {
            i++;
            p++;
            if (_buff.limit_offset() - p < 1) {
                if (_buff.free_space() == 0) {
                    if (!_expand_buffer_loosely().ok()) {
                        return false;
                    } else {
                        base_ptr = _buff.base_ptr();
                    }
                }
                if (!_fill_buffer().ok()) {
                    return false;
                }
            }
        }
        if (i == _column_separator_length) {
            _buff.skip(_column_separator_length);
            return true;
        }
    }
    return false; 
}

bool CSVReader::isRowDelimiter(CSVBuffer& buff) {
    if (_row_delimiter_length == 1) {
        if (*(buff.position()) == _row_delimiter[0]) {
            buff.skip(1);
            return true;
        }
    } else {
        int i = 0;
        const char* base_ptr = _buff.base_ptr(); 
        size_t p = _buff.position_offset();
        while (i < _row_delimiter_length && p < _buff.limit_offset() && *(base_ptr+p) == _row_delimiter[i]) {
            i++;
            p++;
            if (_buff.limit_offset() - p < 1) {
                if (_buff.free_space() == 0) {
                    if (!_expand_buffer_loosely().ok()) {
                        return false;
                    } else {
                        base_ptr = _buff.base_ptr();
                    }
                }
                if (!_fill_buffer().ok()) {
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

Status CSVReader::readMore(CSVBuffer& buff) {
    if(buff.available() < 1) {
        if (buff.free_space() == 0) {
            Status s = _expand_buffer_loosely();
            if (!s.ok()) {
                return s;
            }
        }
        return _fill_buffer();
    }
    return Status::OK();
}

char* CSVReader::buffBasePtr() {
    return _buff.base_ptr();
}

char* CSVReader::escapeDataPtr() {
    return _escape_data.data();
}

Status CSVReader::next_record(FieldOffsets* fields, size_t& parsed_start, size_t& parsed_end) {
    fields->clear();
    if (_limit > 0 && _parsed_bytes > _limit) {
        return Status::EndOfFile("Reached limit");
    }
    _buff.compact();
    _escape_data.clear();
    std::unordered_set<size_t> escape_pos;
    ParseState curState = START;
    ParseState preState = curState;
    // If filed_start is initialized to the maximum value, the START state is not entered.
    size_t filed_start = std::string::npos;
    parsed_start = _buff.position_offset();
    bool is_enclose_field = false;
    bool is_escape_field = false;
    size_t filed_end = 0;
    Status status = Status::OK();
    while (true) {
        // At the end of a row, or the end of a field, no new data is read.
        if (curState != NEWLINE && curState != DELIMITER) {
            status = readMore(_buff);
            if (!status.ok()) {
                curState = NEWLINE;
                goto newline_label;
            }
        }
        // Advance to the next state each time based on the current state + the current character stream.
        switch (curState)
        {
        case START:
            // Trim space is enabled and the leading space is skipped in the START state
            if (_trim_space) {
                while (*(_buff.position()) == ' ') {
                    _buff.skip(1);
                    status = readMore(_buff);
                    if (!status.ok()) {
                        curState = NEWLINE;
                        goto newline_label;
                    }
                }
            }
            filed_start = _buff.position_offset();

            // newline
            if (isRowDelimiter(_buff)) {
                curState = NEWLINE;
                break;
            }

            // delimiter
            if (isColumnSeparator(_buff)) {
                curState = DELIMITER;
                break;
            }

            // escape
            if (*(_buff.position()) == _escape) {
                // trick here.
                preState = ORDINARY;
                curState = ESCAPE;
                escape_pos.insert(_buff.position_offset());
                _buff.skip(1);
                // filed_start = _buff.position_offset();
                break;
            }

            // enclose
            if (*(_buff.position()) == _enclose) {
                _buff.skip(1);
                status = readMore(_buff);
                if (!status.ok()) {
                    // TODO(yangzaorang): Just reading a single enclose character from the START state is an illegal pattern and can be exited directly
                    curState = NEWLINE;
                    goto newline_label;
                }
                filed_start = _buff.position_offset();
                if (*(_buff.position()) != _enclose) {
                    curState = ENCLOSE;
                    is_enclose_field = true;
                    break;
                } else {
                    // ""something
                    // We need to determine whether the field is empty or escaped.
                    _buff.skip(1);
                    status = readMore(_buff);
                    if (!status.ok()) {
                        is_enclose_field = true;
                        curState = NEWLINE;
                        goto newline_label;
                    }
                    // ""rowseperator
                    if (isRowDelimiter(_buff)) {
                        is_enclose_field = true;
                        curState = NEWLINE;
                        break;
                    }
                    
                    // ""delimiter
                    if (isColumnSeparator(_buff)) {
                        is_enclose_field = true;
                        curState = DELIMITER;
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
        //    c. "", empty fields must be read next to field delimiters or newlines.
        // 2. trimspace is enabled
        //    Remove the leading space before judging.
        case ENCLOSE:
            // enclose character again, There are two possibilities:
            // 1. ENCLOSE state is over
            // 2. enclose to escape
            if (*(_buff.position()) == _enclose) {
                _buff.skip(1);

                status = readMore(_buff);
                if (!status.ok()) {
                    is_enclose_field = true;
                    curState = NEWLINE;
                    goto newline_label;
                }
                if (*(_buff.position()) == _enclose) {
                    preState = curState;
                    curState = ENCLOSE_ESCAPE;
                    escape_pos.insert(_buff.position_offset() - 1);
                } else {
                    curState = ORDINARY;
                }
                break;
            }
            // escape
            if (*(_buff.position()) == _escape) {
                preState = curState;
                curState = ESCAPE;
                escape_pos.insert(_buff.position_offset());
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

        // TODO: review ESCAPE
        case ESCAPE:
            // escape enclose
            if (*(_buff.position()) == _enclose) {
                curState = preState;
                _buff.skip(1);
                break;
            }
            // escape escape
            if (*(_buff.position()) == _escape) {
                curState = preState;
                _buff.skip(1);
                break;
            }
            // escape row DELIMITER
            if (isRowDelimiter(_buff)) {
                curState = preState;
                break;
            }
            // escape column separator
            if (isColumnSeparator(_buff)) {
                curState = preState;
                break;
            }

        case ORDINARY:
            // newline
            if (isRowDelimiter(_buff)) {
                curState = NEWLINE;
                break;
            }
            
            // delimiter
            if (isColumnSeparator(_buff)) {
                curState = DELIMITER;
                break;
            }
        
            // escape
            if (*(_buff.position()) == _escape) {
                preState = curState;
                curState = ESCAPE;
                escape_pos.insert(_buff.position_offset());
                _buff.skip(1);
                break;
            }

            // enclose
            if (*(_buff.position()) == _enclose) {
                preState = curState;
                curState = ENCLOSE_ESCAPE;
                escape_pos.insert(_buff.position_offset());
                _buff.skip(1);
                break;
            }

            _buff.skip(1);
            curState = ORDINARY;
            break;

        case DELIMITER:
            filed_end = _buff.position_offset();
            // The field has an escape and needs to be stripped of the escape character and copied to a separate storage space.
            if (escape_pos.size() > 0) {
                is_escape_field = true;
                size_t new_filed_start = _escape_data.size();
                for (size_t i = filed_start; i<_buff.position_offset(); i++) {
                    if (escape_pos.count(i)) {
                        continue;
                    }
                    _escape_data.push_back(_buff.get_char(i));
                }
                escape_pos.clear();
                filed_start = new_filed_start;
                filed_end = _escape_data.size();
            }
            // push field
            if (is_enclose_field) {
                if (_trim_space) {
                    const char* basePtr = _buff.base_ptr();
                    if (is_escape_field) {
                        basePtr = _escape_data.data();
                    }
                    std::pair<const char*, size_t> newPos = trim(basePtr+filed_start, filed_end - _column_separator_length - filed_start - 1);
                    fields->emplace_back(newPos.first - basePtr, newPos.second, is_escape_field);
                } else {
                    // Remove the last enclose character.
                    fields->emplace_back(filed_start, filed_end - _column_separator_length - filed_start - 1, is_escape_field);
                }
                
            } else {
                if (_trim_space) {
                    const char* basePtr = _buff.base_ptr();
                    if (is_escape_field) {
                        basePtr = _escape_data.data();
                    }
                    std::pair<const char*, size_t> newPos = trim(basePtr+filed_start, filed_end - _column_separator_length - filed_start);
                    fields->emplace_back(newPos.first - basePtr, newPos.second, is_escape_field);
                } else {
                    fields->emplace_back(filed_start, filed_end - _column_separator_length - filed_start, is_escape_field);
                }

            }
            is_escape_field = false;
            curState = START;
            is_enclose_field = false;
            break;

    newline_label:
        case NEWLINE:
            _parsed_bytes += _buff.position_offset() - parsed_start;
            parsed_end = _buff.position_offset();
            // push line
            if (status.ok() || status.is_end_of_file()) {
                // Skip empty line.
                if (fields->size() == 0 && _buff.position_offset() - _row_delimiter_length - filed_start == 0) {
                    curState = START;
                    is_enclose_field = false;
                    return status;
                }
                if (filed_start != std::string::npos) {
                    filed_end = _buff.position_offset();
                    if (escape_pos.size() > 0) {
                        is_escape_field = true;
                        size_t new_filed_start = _escape_data.size();
                        for (size_t i = filed_start; i<_buff.position_offset(); i++) {
                            if (escape_pos.count(i)) {
                                continue;
                            }
                            _escape_data.push_back(_buff.get_char(i));
                        }
                        escape_pos.clear();
                        filed_start = new_filed_start;
                        filed_end = _escape_data.size();
                    }

                    if (is_enclose_field) {
                        if (_trim_space) {
                            const char* basePtr = _buff.base_ptr();
                            if (is_escape_field) {
                                basePtr = _escape_data.data();
                            }
                            std::pair<const char*, size_t> newPos = trim(basePtr+filed_start, filed_end - _row_delimiter_length - filed_start - 1);
                            fields->emplace_back(newPos.first - basePtr, newPos.second, is_escape_field);
                        } else {
                            fields->emplace_back(filed_start, filed_end - _row_delimiter_length - filed_start - 1, is_escape_field);
                        }
                    } else {
                        if (_trim_space) {
                            const char* basePtr = _buff.base_ptr();
                            if (is_escape_field) {
                                basePtr = _escape_data.data();
                            }
                            std::pair<const char*, size_t> newPos = trim(basePtr+filed_start, filed_end - _row_delimiter_length - filed_start);
                            fields->emplace_back(newPos.first - basePtr, newPos.second, is_escape_field);                
                        } else {
                            fields->emplace_back(filed_start, filed_end - _row_delimiter_length - filed_start, is_escape_field);
                        }
                    }
                }
            }
            curState = START;
            is_escape_field = false;
            is_enclose_field = false;
            return status;
        default:
            return Status::NotSupported("Not supported state when csv parsing");
        }
    }
}

Status CSVReader::next_record(Record* record) {
    if (_limit > 0 && _parsed_bytes > _limit) {
        return Status::EndOfFile("Reached limit");
    }
    char* d;
    size_t pos = 0;
    while ((d = _buff.find(_row_delimiter, pos)) == nullptr) {
        pos = _buff.available();
        _buff.compact();
        if (_buff.free_space() == 0) {
            RETURN_IF_ERROR(_expand_buffer());
        }
        // fill_buffer does a memory copy: it reads the data from the file and writes it to _buff
        RETURN_IF_ERROR(_fill_buffer());
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

void CSVReader::split_record(const Record& record, Fields* fields) const {
    const char* value = record.data;
    const char* ptr = record.data;
    const size_t size = record.size;

    if (_column_separator_length == 1) {
        for (size_t i = 0; i < size; ++i, ++ptr) {
            if (*ptr == _column_separator[0]) {
                if (_trim_space) {
                    std::pair<const char*, size_t> newPos = trim(value, ptr - value);
                    fields->emplace_back(newPos.first, newPos.second);
                } else {
                    fields->emplace_back(value, ptr - value);
                }
                value = ptr + 1;
            }
        }
    } else {
        const auto* const base = ptr;

        do {
            ptr = static_cast<char*>(
                    memmem(value, size - (value - base), _column_separator.data(), _column_separator_length));
            if (ptr != nullptr) {
                if (_trim_space) {
                    std::pair<const char*, size_t> newPos = trim(value, ptr - value);
                    fields->emplace_back(newPos.first, newPos.second);
                } else {
                    fields->emplace_back(value, ptr - value);
                }
                value = ptr + _column_separator_length;
            }
        } while (ptr != nullptr);

        ptr = record.data + size;
    }
    if (_trim_space) {
        std::pair<const char*, size_t> newPos = trim(value, ptr - value);
        fields->emplace_back(newPos.first, newPos.second);
    } else {
        fields->emplace_back(value, ptr - value);
    }
}

} // namespace starrocks::vectorized
