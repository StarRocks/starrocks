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

namespace starrocks::vectorized {

using Field = Slice;

static Field trim(const char* value, int len) {
    int32_t begin = 0;

    while (begin < len && value[begin] == ' ') {
        ++begin;
    }

    int32_t end = len - 1;

    while (end > begin && value[end] == ' ') {
        --end;
    }
    return Field(value + begin, end - begin + 1);
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
        // 我们要判断接下来的一段字节流是否是row delimiter
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
// 希望buffer可以在有数据的时候也能扩容
// 当剩余的可读取的字节为0时，读取更多数据。
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

// 这个函数我们要从状态START开始，读取下一条记录。
// 重载。
Status CSVReader::next_record(FieldOffsets* fields) {
    fields->clear();
    if (_limit > 0 && _parsed_bytes > _limit) {
        return Status::EndOfFile("Reached limit");
    }
    // TODO: 每次读一条记录时先做一次compact，这里性能会有问题
    _buff.compact();
    ParseState curState = START;
    ParseState preState = curState;
    // 将filed_start初始化为最大值，表示未进入START状态
    size_t filed_start = std::string::npos;
    size_t parsed_start = _buff.position_offset();
    bool is_enclose_field = false;
    Status status = Status::OK();
    while (true) {
        // 到了一行的行尾,或者字段尾部，此次不再读新的数据
        if (curState != NEWLINE && curState != DELIMITER) {
            status = readMore(_buff);
            if (!status.ok()) {
                curState = NEWLINE;
                goto newline_label;
            }
        }
        // 每一次根据当前的状态+当前的字符流，来推进到下一个状态
        switch (curState)
        {
        case START:
            // 开启了trim space，START状态下跳过leading space
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
            // 我们记录字段开始的位置
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
                _buff.skip(1);
                // filed_start = _buff.position_offset();
                break;
            }

            // enclose
            if (*(_buff.position()) == _enclose) {
                _buff.skip(1);
                // TODO: 这里还需要调整
                status = readMore(_buff);
                if (!status.ok()) {
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
                    // 空字段还是转义？
                    _buff.skip(1);
                    status = readMore(_buff);
                    if (!status.ok()) {
                        is_enclose_field = true;
                        curState = NEWLINE;
                        goto newline_label;
                    }                    // ""rowseperator
                    if (isRowDelimiter(_buff)) {
                        is_enclose_field = true;
                        curState = NEWLINE;
                        break;
                    }
                    
                    // delimiter
                    if (isColumnSeparator(_buff)) {
                        is_enclose_field = true;
                        curState = DELIMITER;
                        break;
                    }

                    // ""普通字符,此时第一个enclose为转义字符
                    curState = ORDINARY;
                    break;
                }
            }
            curState = ORDINARY;
            _buff.skip(1);
            break;

        // 只有在START状态下才能进入ENCLOSE状态
        // START状态如何转换到ENCLOSE状态：
        // 1. 没有开启trimspace
        //    a. "some
        //    b. ""aaa, enclose只是用于转义，不能进入enclose状态
        //    c. ""空字段，空字段接下来读到的一定是字段分隔符或者换行符
        // 2. 开启了trimspace
        //    去除空格后再判断
        case ENCLOSE:
            // ENCLOSE状态下再次遇到enclose, enclose状态结束
            if (*(_buff.position()) == _enclose) {
                curState = ORDINARY;
                _buff.skip(1);
                break;
            }
            // escape
            if (*(_buff.position()) == _escape) {
                preState = curState;
                curState = ESCAPE;
                _buff.skip(1);
                break;
            }
            // other character
            _buff.skip(1);
            break;

        // TODO: review ESCAPE
        case ESCAPE:
            // 转义enclose
            if (*(_buff.position()) == _enclose) {
                curState = preState;
                _buff.skip(1);
                break;
            }
            // 转义 escape自身
            if (*(_buff.position()) == _escape) {
                // TODO: 需要处理新的数据
                curState = preState;
                _buff.skip(1);
                break;
            }
            // 转义 row DELIMITER
            if (isRowDelimiter(_buff)) {
                curState = preState;
                break;
            }
            // 转义column separator
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
                _buff.skip(1);
                break;
            }
            _buff.skip(1);
            curState = ORDINARY;
            break;

        case DELIMITER:
            // push field
            if (is_enclose_field) {
                // enclose字段要去除字段最后的enclose character
                fields->emplace_back(filed_start, _buff.position_offset() - _column_separator_length - filed_start - 1);
            } else {
                fields->emplace_back(filed_start, _buff.position_offset() - _column_separator_length - filed_start);
            }
            curState = START;
            is_enclose_field = false;
            break;

    newline_label:
        case NEWLINE:
            _parsed_bytes += _buff.position_offset() - parsed_start;
            // push line
            // 如果未发生异常，那么将行分隔符从字段中去除，发生异常，不做修饰了。
            if (status.ok() || status.is_end_of_file()) {
                // 跳过空行
                if (fields->size() == 0 && _buff.position_offset() - _row_delimiter_length - filed_start == 0) {
                    curState = START;
                    is_enclose_field = false;
                    return status;
                }
                if (filed_start != std::string::npos) {
                    if (is_enclose_field) {
                        fields->emplace_back(filed_start, _buff.position_offset() - _row_delimiter_length - filed_start - 1);
                    } else {
                        fields->emplace_back(filed_start, _buff.position_offset() - _row_delimiter_length - filed_start);
                    }
                }
            }
            curState = START;
            is_enclose_field = false;
            return status;
        default:
            return Status::NotSupported("Not supported state when csv parsing");
        }
        // 每一次switch之后去判断是否要读新的数据
        // 这块确认好了，再往下演进：
        // 1. 如何读，何时读数据
        // 当available没有剩余数据时，那么读数据。
        // 每一次进行next record时，都做一次compact，当然这里会有性能开销，考虑如何优化
        // 2. 读新的数据的边界
        // 3. 这种方式处理多字节的delimiter和newline符号是否可行，或者对其他的状态流转是否可行
        // 4. 对于转义在某种情况下会破坏我们感兴趣内存的连续性，如何处理？
        // 到后面再做转义。需要多做一次扫描。这个方案不行，因为还有enclose转义。后面再考虑这个问题。
        // 4.5 循环何时退出
        // a. 拿到了一行完整记录. b. 超过缓冲区大小，但是还没有找到相关记录
        // 5. 如何处理空行
        // 6. 如何处理end of file
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
        //  fill_buffer 进行了一次内存拷贝：将数据从文件中读取，写入到_buff中
        RETURN_IF_ERROR(_fill_buffer());
    }
    size_t l = d - _buff.position();
    *record = Record(_buff.position(), l);
    _buff.skip(l + _row_delimiter_length);
    //               ^^ skip record delimiter.
    _parsed_bytes += l + _row_delimiter_length;
    return Status::OK();
}

// 将底层的storage(vector)扩大
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

// _expand_buffer必须保证缓冲区中没有数据才可以扩容buffer，但是我们引入状态机解析器后，这个约束太强了，
// 引入一个更宽松的buffer扩容函数。
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
                    fields->emplace_back(trim(value, ptr - value));
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
                    fields->emplace_back(trim(value, ptr - value));
                } else {
                    fields->emplace_back(value, ptr - value);
                }
                value = ptr + _column_separator_length;
            }
        } while (ptr != nullptr);

        ptr = record.data + size;
    }
    if (_trim_space) {
        fields->emplace_back(trim(value, ptr - value));
    } else {
        fields->emplace_back(value, ptr - value);
    }
}

} // namespace starrocks::vectorized
